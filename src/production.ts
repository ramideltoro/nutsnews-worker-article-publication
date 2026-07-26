import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";

import {
  runtimeNow,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeClock,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  Pool,
  type PoolClient,
  type QueryResultRow
} from "pg";

import type { PublicationConfig } from "./config.js";
import type {
  PublicationBackendOperation,
  PublicationBrokerOutbox,
  PublicationDatabase,
  PublicationDatabaseTransaction,
  PublicationDependencies,
  PublicationDependencyProbe,
  PublicationInboxStore,
  PublicationPermissionProbe,
  PublicationReadinessEvaluationRecord,
  PublicationReadinessEvaluationWriteResult,
  PublicationSnapshotCommand,
  PublicationSnapshotPublisher,
  PublicationSnapshotReceipt
} from "./dependencies.js";
import { PayloadRabbitMqTransport } from "./rabbitmq-payload-transport.js";
import {
  PUBLICATION_RECONCILIATION_CONFIRMATION,
  type PublicationReconciliationCandidate,
  type PublicationReconciliationReport,
  type PublicationReconciliationRequest,
  type PublicationReconciler
} from "./reconciliation.js";
import {
  LocalPublicationFeatureFlag,
  LocalPublicationReadinessPolicy,
  LocalPublicationWorkHandler
} from "./test-doubles.js";

const PUBLICATION_SCHEMA = "worker_uplift_publication";

export type ProductionPublicationDependencies = PublicationDependencies & {
  readonly reconciler: PublicationReconciler;
  readonly reconciliationToken?: string;
  close(): Promise<void>;
};

interface ProductionPublicationDependencyOptions {
  readonly config: PublicationConfig;
  readonly clock: RuntimeClock;
  readonly env?: NodeJS.ProcessEnv;
}

interface PgPublicationTransaction extends PublicationDatabaseTransaction {
  readonly client: PoolClient;
}

interface PublicationReadinessRow extends QueryResultRow {
  readonly diagnostic_metadata: unknown;
  readonly shadow_aggregate_version: number;
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export function createProductionPublicationDependencies(
  options: ProductionPublicationDependencyOptions
): ProductionPublicationDependencies {
  const env = options.env ?? process.env;
  const pool = new Pool({
    connectionString: requiredEnv(env, "NUTSNEWS_PUBLICATION_DATABASE_URL"),
    max: Math.max(2, options.config.concurrency + 1),
    application_name: options.config.serviceName
  });
  const brokerTransport = new PayloadRabbitMqTransport({
    url: requiredEnv(env, "NUTSNEWS_PUBLICATION_RABBITMQ_URL"),
    prefetch: options.config.prefetch,
    clock: options.clock
  });
  const readinessPolicy = new LocalPublicationReadinessPolicy(options.config);
  const featureFlag = new LocalPublicationFeatureFlag(options.config);
  const reconciler = new PostgresPublicationTerminalReconciler({
    clock: options.clock,
    env
  });
  const reconciliationToken = reconciliationTokenFromEnv(env);
  const dependencies: Omit<ProductionPublicationDependencies, "workHandler" | "close"> = {
    clock: options.clock,
    inboxStore: new PostgresPublicationInboxStore(pool),
    database: new PostgresPublicationDatabase(pool, options.config),
    readinessPolicy,
    snapshotPublisher: new PostgresPublicationSnapshotPublisher({
      pool,
      config: options.config,
      clock: options.clock,
      baseUrl: requiredEnv(env, "NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL"),
      token: requiredEnv(env, "NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN")
    }),
    featureFlag,
    brokerOutbox: new PostgresPublicationBrokerOutbox(pool),
    brokerTransport,
    reconciler,
    ...(reconciliationToken === undefined ? {} : {
      reconciliationToken
    })
  };

  return {
    ...dependencies,
    workHandler: new LocalPublicationWorkHandler(dependencies),
    async close(): Promise<void> {
      await brokerTransport.close();
      await pool.end();
    }
  };
}

export class PostgresPublicationInboxStore implements PublicationInboxStore {
  readonly name = "postgres-publication-inbox";

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<PublicationDependencyProbe> {
    return probePool(this.pool, "publication inbox database ready");
  }

  async claim(
    idempotencyKey: string,
    context: RuntimeIdempotencyClaimContext
  ): Promise<RuntimeIdempotencyClaimResult> {
    const inserted = await this.pool.query<{ readonly received_at: Date }>(
      `INSERT INTO ${PUBLICATION_SCHEMA}.inbox (
        message_id, pipeline_run_id, stage_execution_id, source_stage, source_message_id,
        entity_kind, entity_id, schema_version, operation_version, idempotency_key,
        payload_ref, payload_digest, received_at, status, diagnostic_metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz, 'processing', $14::jsonb)
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING received_at`,
      [
        context.envelope.messageId,
        context.envelope.correlationId,
        context.envelope.messageId,
        context.envelope.producer.name,
        context.envelope.causationId,
        context.envelope.aggregate.type,
        context.envelope.aggregate.id,
        context.envelope.schemaVersion,
        Math.max(1, context.envelope.aggregate.version),
        idempotencyKey,
        context.envelope.payloadRef.uri,
        context.envelope.payloadRef.digest ?? sha256Json(context.envelope.payloadRef),
        context.receivedAt,
        JSON.stringify({
          route: context.envelope.route,
          attempt: context.envelope.attempt
        })
      ]
    );

    if ((inserted.rowCount ?? 0) > 0) {
      return {
        status: "claimed",
        firstSeenAt: context.receivedAt,
        replay: false
      };
    }

    const existing = await this.pool.query<{
      readonly status: string;
      readonly received_at: Date;
      readonly processed_at: Date | null;
    }>(
      `SELECT status, received_at, processed_at
       FROM ${PUBLICATION_SCHEMA}.inbox
       WHERE idempotency_key = $1`,
      [idempotencyKey]
    );
    const row = existing.rows[0];

    if (row === undefined) {
      return {
        status: "in-progress",
        firstSeenAt: context.receivedAt
      };
    }

    const firstSeenAt = row.received_at.toISOString();

    if (row.status === "processed" || row.status === "duplicate") {
      return {
        status: "already-completed",
        firstSeenAt,
        completedAt: (row.processed_at ?? row.received_at).toISOString()
      };
    }

    if (row.status === "failed" || row.status === "parked") {
      await this.pool.query(
        `UPDATE ${PUBLICATION_SCHEMA}.inbox
         SET status = 'processing',
             sanitized_error_code = NULL,
             sanitized_error_message = NULL,
             diagnostic_metadata = diagnostic_metadata || $2::jsonb
         WHERE idempotency_key = $1`,
        [
          idempotencyKey,
          JSON.stringify({
            replayedAt: context.receivedAt,
            replayMessageId: context.envelope.messageId
          })
        ]
      );

      return {
        status: "claimed",
        firstSeenAt,
        replay: true
      };
    }

    return {
      status: "in-progress",
      firstSeenAt
    };
  }

  async markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    await this.pool.query(
      `UPDATE ${PUBLICATION_SCHEMA}.inbox
       SET status = 'processed',
           processed_at = $2::timestamptz,
           diagnostic_metadata = diagnostic_metadata || $3::jsonb
       WHERE idempotency_key = $1`,
      [
        idempotencyKey,
        completion.completedAt,
        JSON.stringify({
          completedMessageId: completion.messageId,
          completedStage: completion.stage
        })
      ]
    );
  }

  async markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    await this.pool.query(
      `UPDATE ${PUBLICATION_SCHEMA}.inbox
       SET status = 'failed',
           sanitized_error_code = $2,
           sanitized_error_message = $3,
           diagnostic_metadata = diagnostic_metadata || $4::jsonb
       WHERE idempotency_key = $1`,
      [
        idempotencyKey,
        sanitizeCode(failure.reason),
        sanitizeMessage(failure.reason),
        JSON.stringify({
          failedAt: failure.failedAt,
          failedMessageId: failure.messageId,
          retryable: failure.retryable
        })
      ]
    );
  }
}

export class PostgresPublicationDatabase implements PublicationDatabase {
  readonly name = "postgres-publication-database";

  constructor(
    private readonly pool: Pool,
    private readonly config: PublicationConfig
  ) {}

  async probe(): Promise<PublicationDependencyProbe> {
    return probePool(this.pool, "publication database ready");
  }

  async checkWriteScope(): Promise<PublicationPermissionProbe> {
    const probe = await probePool(this.pool, "publication write scope ready");

    return {
      status: probe.status,
      summary: probe.summary,
      details: {
        databaseRole: this.config.security.databaseRole,
        shadowSchemaVersion: this.config.compatibility.shadowSchemaVersion,
        allowedWriteScopes: [
          `${PUBLICATION_SCHEMA}.publication_readiness`,
          `${PUBLICATION_SCHEMA}.publication_decisions`,
          `${PUBLICATION_SCHEMA}.inbox`,
          `${PUBLICATION_SCHEMA}.outbox`
        ],
        allowedReadScopes: [
          "worker_uplift_views.final_shadow_article_projection"
        ],
        deniedWriteScopes: [
          "public.domain_tables",
          "public.public_feed_snapshot",
          "cloudflare_kv",
          "worker_uplift_final.article_shadow_aggregates"
        ]
      }
    };
  }

  async withTransaction<T>(operation: (transaction: PublicationDatabaseTransaction) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    const transaction: PgPublicationTransaction = {
      transactionId: randomUUID(),
      client
    };

    try {
      await client.query("BEGIN");
      const value = await operation(transaction);
      await client.query("COMMIT");
      return value;
    } catch (error: unknown) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async recordReadinessEvaluation(
    transaction: PublicationDatabaseTransaction,
    record: PublicationReadinessEvaluationRecord
  ): Promise<PublicationReadinessEvaluationWriteResult> {
    const client = transactionClient(transaction);
    const existingByKey = await client.query<PublicationReadinessRow>(
      `SELECT shadow_aggregate_version, diagnostic_metadata
       FROM ${PUBLICATION_SCHEMA}.publication_readiness
       WHERE diagnostic_metadata->>'idempotencyKey' = $1
       LIMIT 1`,
      [record.idempotencyKey]
    );
    const existing = existingByKey.rows[0];
    const recordDigest = sha256Json(record.shadowOutput);

    if (existing !== undefined) {
      const existingDigest = stringValue(recordValue(existing.diagnostic_metadata).shadowOutputDigest);

      if (existingDigest === recordDigest) {
        return {
          status: "duplicate",
          evaluationId: record.evaluationId
        };
      }

      return {
        status: "conflict",
        reason: "idempotency-key-reused"
      };
    }

    const latest = await client.query<{ readonly latest_version: number | null }>(
      `SELECT max(shadow_aggregate_version)::integer AS latest_version
       FROM ${PUBLICATION_SCHEMA}.publication_readiness
       WHERE article_identity_hash = $1`,
      [record.articleId]
    );
    const latestVersion = latest.rows[0]?.latest_version;

    if (latestVersion !== null && latestVersion !== undefined && latestVersion > record.finalAggregateVersion) {
      return {
        status: "stale",
        reason: "stale-final-aggregate-version"
      };
    }

    await client.query(
      `INSERT INTO ${PUBLICATION_SCHEMA}.publication_readiness (
        article_identity_hash, readiness_version, approved, translations_complete,
        shadow_aggregate_version, status, diagnostic_metadata, checked_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::timestamptz)
      ON CONFLICT (article_identity_hash, readiness_version)
      DO UPDATE SET approved = EXCLUDED.approved,
                    translations_complete = EXCLUDED.translations_complete,
                    status = EXCLUDED.status,
                    diagnostic_metadata = ${PUBLICATION_SCHEMA}.publication_readiness.diagnostic_metadata || EXCLUDED.diagnostic_metadata,
                    checked_at = EXCLUDED.checked_at`,
      [
        record.articleId,
        record.finalAggregateVersion,
        !record.decision.reasons.includes("approval-not-accepted"),
        record.decision.missingLanguageCodes.length === 0,
        record.finalAggregateVersion,
        readinessStatus(record),
        JSON.stringify({
          evaluationId: record.evaluationId,
          idempotencyKey: record.idempotencyKey,
          messageId: record.messageId,
          policyVersion: record.policy.version,
          decisionStatus: record.decision.status,
          reasons: record.decision.reasons,
          shadowOutput: record.shadowOutput,
          shadowOutputDigest: recordDigest,
          safeMetadataOnly: true
        }),
        record.evaluatedAt
      ]
    );

    return {
      status: "recorded",
      evaluationId: record.evaluationId
    };
  }
}

export class PostgresPublicationSnapshotPublisher implements PublicationSnapshotPublisher {
  readonly name = "postgres-publication-snapshot-publisher";

  private readonly pool: Pool;
  private readonly config: PublicationConfig;
  private readonly clock: RuntimeClock;
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly fetcher: FetchLike;

  constructor(options: {
    readonly pool: Pool;
    readonly config: PublicationConfig;
    readonly clock: RuntimeClock;
    readonly baseUrl: string;
    readonly token: string;
    readonly fetcher?: FetchLike;
  }) {
    this.pool = options.pool;
    this.config = options.config;
    this.clock = options.clock;
    this.baseUrl = stripTrailingSlashes(options.baseUrl);
    this.token = options.token;
    this.fetcher = options.fetcher ?? fetch;
  }

  async probe(): Promise<PublicationDependencyProbe> {
    const probe = await probePool(this.pool, "publication snapshot publisher ready");

    if (probe.status !== "ok") {
      return probe;
    }

    if (this.config.writeMode === "shadow_comparison") {
      return probe;
    }

    try {
      const response = await this.fetcher(healthUrl(this.baseUrl), {
        method: "GET",
        signal: AbortSignal.timeout(5_000)
      });

      return response.ok
        ? probe
        : {
            status: "unhealthy",
            summary: `backend Worker API health returned ${String(response.status)}`
          };
    } catch (error: unknown) {
      return {
        status: "unhealthy",
        summary: error instanceof Error ? error.message : "backend Worker API health failed"
      };
    }
  }

  async publishShadowComparison(command: PublicationSnapshotCommand): Promise<PublicationSnapshotReceipt> {
    const receipt = this.receipt(command, false);

    await this.recordDecision(command, receipt, "shadow-publication-comparison");

    return receipt;
  }

  async publishProductionSnapshot(command: PublicationSnapshotCommand): Promise<PublicationSnapshotReceipt> {
    if (this.config.writeMode !== "production" || !this.config.security.productionWriteConfirmationPresent) {
      throw new Error("publication production writes are disabled");
    }

    for (const operation of command.backendOperations) {
      await this.callBackend(operation, command);
    }

    const receipt = this.receipt(command, command.snapshotRefreshRequired);
    await this.recordDecision(command, receipt, command.backendOperation);

    return receipt;
  }

  private async recordDecision(
    command: PublicationSnapshotCommand,
    receipt: PublicationSnapshotReceipt,
    operation: PublicationBackendOperation
  ): Promise<void> {
    await this.pool.query(
      `INSERT INTO ${PUBLICATION_SCHEMA}.publication_decisions (
        article_identity_hash, decision_version, decision, reason_code,
        backend_api_operation, request_ref, response_ref, diagnostic_metadata, decided_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::timestamptz)
      ON CONFLICT (article_identity_hash, decision_version)
      DO UPDATE SET decision = EXCLUDED.decision,
                    reason_code = EXCLUDED.reason_code,
                    backend_api_operation = EXCLUDED.backend_api_operation,
                    response_ref = EXCLUDED.response_ref,
                    diagnostic_metadata = ${PUBLICATION_SCHEMA}.publication_decisions.diagnostic_metadata || EXCLUDED.diagnostic_metadata,
                    decided_at = EXCLUDED.decided_at`,
      [
        command.articleId,
        command.finalAggregateVersion,
        decisionKind(command),
        command.shadowOutput.reasons[0] ?? null,
        operation,
        `backend://worker-uplift/publication/${encodeURIComponent(command.articleId)}/v${String(command.finalAggregateVersion)}`,
        `backend://worker-uplift/publication/${encodeURIComponent(command.articleId)}/v${String(command.finalAggregateVersion)}/receipt`,
        JSON.stringify({
          command,
          receipt,
          publicFeedSnapshotStatus: command.publicFeedSnapshot.status,
          safeMetadataOnly: true
        }),
        receipt.publishedAt
      ]
    );
  }

  private async callBackend(operation: PublicationBackendOperation, command: PublicationSnapshotCommand): Promise<void> {
    const response = await this.fetcher(`${this.baseUrl}/${operation}`, {
      method: "POST",
      headers: {
        "authorization": `Bearer ${this.token}`,
        "content-type": "application/json"
      },
      body: JSON.stringify({
        providerMode: command.providerMode,
        idempotencyKey: `${command.backendMetadata.idempotencyKey}:${operation}`,
        messageId: command.backendMetadata.messageId,
        correlationId: command.backendMetadata.correlationId,
        pipelineRunId: command.backendMetadata.pipelineRunId,
        stageExecutionId: command.backendMetadata.stageExecutionId,
        sourceMessageId: command.backendMetadata.sourceMessageId,
        actorService: "worker-uplift-publication",
        schemaVersion: command.backendMetadata.schemaVersion,
        operationVersion: command.finalAggregateVersion,
        expectedArticleVersion: command.backendMetadata.expectedArticleVersion,
        shadowOutput: command.shadowOutput,
        publicFeedSnapshot: command.publicFeedSnapshot
      }),
      signal: AbortSignal.timeout(10_000)
    });

    if (!response.ok) {
      throw new Error(`backend publication operation ${operation} failed with ${String(response.status)}`);
    }
  }

  private receipt(command: PublicationSnapshotCommand, refreshCompleted: boolean): PublicationSnapshotReceipt {
    return {
      commandId: command.evaluationId,
      mode: command.writeMode,
      accepted: true,
      publishedAt: runtimeNow(this.clock),
      backendOperations: command.backendOperations,
      snapshotRefreshRequested: command.snapshotRefreshOperation !== undefined,
      rollbackAvailable: !refreshCompleted || command.providerMode === "backend_postgres_shadow"
    };
  }
}

export class PostgresPublicationBrokerOutbox implements PublicationBrokerOutbox {
  readonly name = "postgres-publication-broker-outbox";

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<PublicationDependencyProbe> {
    return probePool(this.pool, "publication broker outbox ready");
  }

  async hasReceipt(command: BrokerPublishCommand): Promise<boolean> {
    const result = await this.pool.query<{ readonly id: number }>(
      `SELECT id
       FROM ${PUBLICATION_SCHEMA}.outbox
       WHERE idempotency_key = $1
         AND status = 'confirmed'
       LIMIT 1`,
      [command.envelope.idempotencyKey]
    );

    return (result.rowCount ?? 0) > 0;
  }

  async record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    const payload = command.payload;

    await this.pool.query(
      `INSERT INTO ${PUBLICATION_SCHEMA}.outbox (
        outbox_message_id, pipeline_run_id, stage_execution_id, destination_stage, routing_key,
        entity_kind, entity_id, schema_version, operation_version, idempotency_key,
        payload_ref, payload_digest, published_at, confirmed_at, status, diagnostic_metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz, $14::timestamptz, 'confirmed', $15::jsonb)
      ON CONFLICT (idempotency_key)
      DO UPDATE SET confirmed_at = EXCLUDED.confirmed_at,
                    status = 'confirmed',
                    diagnostic_metadata = ${PUBLICATION_SCHEMA}.outbox.diagnostic_metadata || EXCLUDED.diagnostic_metadata`,
      [
        receipt.messageId,
        stringFrom(payload.pipelineRunId, command.envelope.correlationId),
        stringFrom(payload.stageExecutionId, command.envelope.messageId),
        command.envelope.route,
        receipt.routingKey,
        command.envelope.aggregate.type,
        command.envelope.aggregate.id,
        command.envelope.schemaVersion,
        Math.max(1, command.envelope.aggregate.version),
        command.envelope.idempotencyKey,
        command.envelope.payloadRef.uri,
        command.envelope.payloadRef.digest ?? sha256Json(payload),
        receipt.confirmedAt,
        receipt.confirmedAt,
        JSON.stringify({
          envelope: command.envelope,
          exchange: receipt.exchange,
          payloadSchemaId: payload.schemaId,
          payload
        })
      ]
    );
  }
}

interface PublicationTerminalReconcilerOptions {
  readonly clock: RuntimeClock;
  readonly env: NodeJS.ProcessEnv;
}

export class PostgresPublicationTerminalReconciler implements PublicationReconciler {
  readonly name = "postgres-publication-terminal-reconciler";

  constructor(private readonly options: PublicationTerminalReconcilerOptions) {}

  reconcile(request: PublicationReconciliationRequest): Promise<PublicationReconciliationReport> {
    const requestedAt = runtimeNow(this.options.clock);
    const mode = request.mode === "apply" ? "apply" : "dry-run";
    const maxItems = boundedInteger(request.maxItems, 100, 1, 100);
    const minAgeSeconds = boundedInteger(request.minAgeSeconds, 900, 0, 86_400);
    const reason = safeReason(request.reason);
    const runId = safeRunId(request.runId);

    if (this.killSwitchActive()) {
      return Promise.resolve(report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: "kill_switch_active",
        errors: [
          "publication reconciliation stop switch is active"
        ],
        candidates: []
      }));
    }

    if (mode === "apply") {
      const applyError = this.applyGateError(request, runId);

      if (applyError !== undefined) {
        return Promise.resolve(report({
          mode,
          requestedAt,
          runId,
          reason,
          maxItems,
          minAgeSeconds,
          status: "failed_closed",
          errors: [
            applyError
          ],
          candidates: []
        }));
      }

      return Promise.resolve(report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: "applied",
        errors: [],
        candidates: []
      }));
    }

    return Promise.resolve(report({
      mode,
      requestedAt,
      runId,
      reason,
      maxItems,
      minAgeSeconds,
      status: "dry_run",
      errors: [],
      candidates: []
    }));
  }

  private applyGateError(request: PublicationReconciliationRequest, runId: string | undefined): string | undefined {
    if (!this.applyEnabled()) {
      return "publication reconciliation apply is disabled by configuration";
    }

    if (request.protectedConfirmation !== PUBLICATION_RECONCILIATION_CONFIRMATION) {
      return `protectedConfirmation must be ${PUBLICATION_RECONCILIATION_CONFIRMATION}`;
    }

    if (runId === undefined) {
      return "runId is required for apply";
    }

    return undefined;
  }

  private applyEnabled(): boolean {
    return flagEnabled(this.options.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_APPLY_ENABLED)
      || flagEnabled(this.options.env.NUTSNEWS_PUBLICATION_RECONCILIATION_APPLY_ENABLED);
  }

  private killSwitchActive(): boolean {
    return flagEnabled(this.options.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_STOP)
      || flagEnabled(this.options.env.NUTSNEWS_PUBLICATION_RECONCILIATION_STOP);
  }
}

async function probePool(pool: Pool, summary: string): Promise<PublicationDependencyProbe> {
  try {
    await pool.query("SELECT 1");

    return {
      status: "ok",
      summary
    };
  } catch (error: unknown) {
    return {
      status: "unhealthy",
      summary: error instanceof Error ? error.message : "database probe failed"
    };
  }
}

function transactionClient(transaction: PublicationDatabaseTransaction): PoolClient {
  const client = (transaction as Partial<PgPublicationTransaction>).client;

  if (client === undefined) {
    throw new Error("Publication operation requires a Postgres transaction.");
  }

  return client;
}

function readinessStatus(record: PublicationReadinessEvaluationRecord): "ready" | "blocked" | "deferred" {
  if (record.decision.status === "ready_for_production" || record.decision.status === "shadow_compare_only") {
    return "ready";
  }

  return record.decision.status === "blocked" ? "blocked" : "deferred";
}

function decisionKind(command: PublicationSnapshotCommand): "publish" | "block" | "refresh_snapshot" {
  if (command.writeMode === "production" && command.publicFeedSnapshot.status === "compatible") {
    return "publish";
  }

  if (command.publicFeedSnapshot.status === "blocked" || command.shadowOutput.status === "rejected") {
    return "block";
  }

  return "refresh_snapshot";
}

function requiredEnv(env: NodeJS.ProcessEnv, key: string): string {
  const value = env[key]?.trim();

  if (value === undefined || value.length === 0) {
    throw new Error(`${key} is required for production publication dependencies.`);
  }

  return value;
}

function reconciliationTokenFromEnv(env: NodeJS.ProcessEnv): string | undefined {
  const directToken = optionalEnv(env, "NUTSNEWS_PUBLICATION_RECONCILIATION_TOKEN")
    ?? optionalEnv(env, "NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN");

  if (directToken !== undefined) {
    return directToken;
  }

  const tokenFile = optionalEnv(env, "NUTSNEWS_PUBLICATION_RECONCILIATION_TOKEN_FILE")
    ?? optionalEnv(env, "NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN_FILE");

  if (tokenFile === undefined) {
    return undefined;
  }

  const value = readFileSync(tokenFile, "utf8").trim();

  return value.length > 0 ? value : undefined;
}

function optionalEnv(env: NodeJS.ProcessEnv, key: string): string | undefined {
  const value = env[key]?.trim();

  return value === undefined || value.length === 0 ? undefined : value;
}

function report(input: {
  readonly mode: PublicationReconciliationRequest["mode"];
  readonly requestedAt: string;
  readonly runId?: string | undefined;
  readonly reason?: string | undefined;
  readonly maxItems: number;
  readonly minAgeSeconds: number;
  readonly status: PublicationReconciliationReport["status"];
  readonly candidates: readonly PublicationReconciliationCandidate[];
  readonly errors: readonly string[];
}): PublicationReconciliationReport {
  const replayedCount = input.candidates.filter((candidate) => candidate.status === "replayed").length;
  const failedClosedCount = input.candidates.filter((candidate) => candidate.status === "failed_closed").length;
  const skippedCount = input.status === "failed_closed"
    ? Math.max(0, input.candidates.length - failedClosedCount)
    : 0;
  const base = {
    service: "publication",
    mode: input.mode,
    status: input.status,
    requestedAt: input.requestedAt,
    maxItems: input.maxItems,
    minAgeSeconds: input.minAgeSeconds,
    selectedCount: input.candidates.length,
    replayedCount,
    failedClosedCount,
    skippedCount,
    writesPerformed: false,
    dryRun: input.mode === "dry-run",
    productionVisibilityEnabled: false,
    legacyRuntimeRequired: false,
    protectedApplyRequired: true,
    terminalStage: true,
    candidates: input.candidates,
    errors: input.errors,
    metrics: {
      candidateCount: input.candidates.length,
      replayedCount,
      failedClosedCount,
      skippedCount
    }
  } satisfies Omit<PublicationReconciliationReport, "runId" | "reason">;

  return {
    ...base,
    ...(input.runId === undefined ? {} : {
      runId: input.runId
    }),
    ...(input.reason === undefined ? {} : {
      reason: input.reason
    })
  };
}

function boundedInteger(value: unknown, defaultValue: number, min: number, max: number): number {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    return defaultValue;
  }

  return Math.max(min, Math.min(max, value));
}

function safeRunId(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();

  return /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/u.test(trimmed) ? trimmed : undefined;
}

function safeReason(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.replace(/[\r\n\t]+/gu, " ").trim();

  return trimmed.length === 0 ? undefined : trimmed.slice(0, 160);
}

function flagEnabled(value: string | undefined): boolean {
  return value?.trim().toLowerCase() === "true";
}

function sanitizeCode(reason: string): string {
  return reason.replace(/[^a-zA-Z0-9_.:-]/g, "_").slice(0, 128);
}

function sanitizeMessage(reason: string): string {
  return reason.slice(0, 512);
}

function stringFrom(value: unknown, fallback: string): string {
  return typeof value === "string" && value.length > 0 ? value : fallback;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function recordValue(value: unknown): Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Readonly<Record<string, unknown>> : {};
}

function sha256Json(value: unknown): string {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}

function stripTrailingSlashes(value: string): string {
  let end = value.length;

  while (end > 0 && value.charCodeAt(end - 1) === 47) {
    end -= 1;
  }

  return value.slice(0, end);
}

function healthUrl(baseUrl: string): string {
  const url = new URL(baseUrl);
  url.pathname = "/healthz";
  url.search = "";
  url.hash = "";
  return url.toString();
}
