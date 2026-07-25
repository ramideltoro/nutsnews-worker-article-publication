import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createInMemoryIdempotencyStore,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport,
  type RuntimeClock,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";

import type { PublicationConfig } from "./config.js";
import {
  PUBLICATION_PRODUCTION_CONFIRMATION,
  loadPublicationConfig
} from "./config.js";
import type {
  PublicationBrokerOutbox,
  PublicationDatabase,
  PublicationDatabaseTransaction,
  PublicationDependencies,
  PublicationDependencyProbe,
  PublicationFeatureFlag,
  PublicationFeatureFlagSnapshot,
  PublicationInboxStore,
  PublicationPermissionProbe,
  PublicationReadinessDecision,
  PublicationReadinessEvaluationRecord,
  PublicationReadinessEvaluationWriteResult,
  PublicationReadinessInput,
  PublicationReadinessPolicy,
  PublicationReadinessPolicySnapshot,
  PublicationSnapshotCommand,
  PublicationSnapshotPublisher,
  PublicationSnapshotReceipt,
  PublicationWorkHandler,
  PublicationWorkTools
} from "./dependencies.js";

export class ManualPublicationClock implements RuntimeClock {
  private current: Date;

  constructor(initial = "2026-07-23T00:00:00.000Z") {
    this.current = new Date(initial);
  }

  now(): Date {
    return new Date(this.current.getTime());
  }

  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

export class InMemoryPublicationInboxStore implements PublicationInboxStore {
  readonly name = "local-publication-inbox";
  status: PublicationDependencyProbe["status"] = "ok";
  private readonly store;

  constructor(clock: RuntimeClock = new ManualPublicationClock()) {
    this.store = createInMemoryIdempotencyStore(clock);
  }

  probe(): PublicationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local publication inbox ready" : "local publication inbox degraded"
    };
  }

  claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    return this.store.claim(idempotencyKey, context);
  }

  markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    return this.store.markCompleted(idempotencyKey, completion);
  }

  markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    return this.store.markFailed(idempotencyKey, failure);
  }
}

export class LocalPublicationDatabase implements PublicationDatabase {
  readonly name = "local-publication-database";
  status: PublicationDependencyProbe["status"] = "ok";
  databaseRole = "nutsnews_worker_publication";
  shadowSchemaVersion = "worker-uplift-shadow-v1";
  allowedWriteScopes = [
    "worker_uplift.publication_readiness",
    "worker_uplift.publication_comparison",
    "worker_uplift.publication_outbox"
  ];
  allowedReadScopes = [
    "worker_uplift.final_article_shadow"
  ];
  deniedWriteScopes = [
    "public.domain_tables",
    "public.public_feed_snapshot",
    "legacy_ingestion_tables",
    "cloudflare_kv"
  ];
  readonly transactions: PublicationDatabaseTransaction[] = [];
  readonly evaluations: PublicationReadinessEvaluationRecord[] = [];
  failNextRecord = false;
  private readonly evaluationByKey = new Map<string, PublicationReadinessEvaluationRecord>();

  probe(): PublicationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local publication database ready" : "local publication database degraded"
    };
  }

  checkWriteScope(): PublicationPermissionProbe {
    return {
      status: this.status,
      summary: "local publication write scope ready",
      details: {
        databaseRole: this.databaseRole,
        shadowSchemaVersion: this.shadowSchemaVersion,
        allowedWriteScopes: this.allowedWriteScopes,
        allowedReadScopes: this.allowedReadScopes,
        deniedWriteScopes: this.deniedWriteScopes
      }
    };
  }

  async withTransaction<T>(operation: (transaction: PublicationDatabaseTransaction) => Promise<T>): Promise<T> {
    const transaction = {
      transactionId: `local-publication-transaction-${String(this.transactions.length + 1)}`
    };

    this.transactions.push(transaction);
    return operation(transaction);
  }

  recordReadinessEvaluation(
    transaction: PublicationDatabaseTransaction,
    record: PublicationReadinessEvaluationRecord
  ): Promise<PublicationReadinessEvaluationWriteResult> {
    void transaction;

    if (this.failNextRecord) {
      this.failNextRecord = false;
      return Promise.resolve({
        status: "conflict",
        reason: "local-publication-record-conflict"
      });
    }

    const existing = this.evaluationByKey.get(record.idempotencyKey);

    if (existing !== undefined) {
      return Promise.resolve({
        status: existing.evaluationId === record.evaluationId ? "duplicate" : "conflict",
        ...(existing.evaluationId === record.evaluationId ? {
          evaluationId: existing.evaluationId
        } : {
          reason: "idempotency-key-reused"
        })
      } as PublicationReadinessEvaluationWriteResult);
    }

    this.evaluationByKey.set(record.idempotencyKey, record);
    this.evaluations.push(record);
    return Promise.resolve({
      status: "recorded",
      evaluationId: record.evaluationId
    });
  }
}

export class LocalPublicationReadinessPolicy implements PublicationReadinessPolicy {
  readonly name = "local-readiness-policy";
  status: PublicationDependencyProbe["status"] = "ok";
  policy: PublicationReadinessPolicySnapshot;

  constructor(config: PublicationConfig = loadPublicationConfig()) {
    this.policy = {
      policyId: config.readiness.policyId,
      version: "local-policy-v1",
      source: "backend-worker-api",
      writeMode: config.writeMode,
      requiredChecks: [
        "canonical-identity",
        "accepted-approval",
        "persisted-source-summary",
        "current-article-version",
        "no-blocking-state"
      ]
    };
  }

  probe(): PublicationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local readiness policy ready" : "local readiness policy degraded"
    };
  }

  getCurrentPolicy(): PublicationReadinessPolicySnapshot {
    return this.policy;
  }

  evaluate(input: PublicationReadinessInput): PublicationReadinessDecision {
    const readinessStatus = input.payload.readinessStatus;
    const snapshotRefreshRequired = input.payload.snapshotRefreshRequired === true;

    if (readinessStatus === "ready" && input.featureFlag.enabled && input.policy.writeMode === "production") {
      return {
        status: "ready_for_production",
        reasons: [
          "publication-readiness-ready",
          "protected-production-mode"
        ],
        snapshotRefreshRequired
      };
    }

    if (readinessStatus === "ready") {
      return {
        status: "shadow_compare_only",
        reasons: [
          "publication-readiness-ready",
          "shadow-comparison-default"
        ],
        snapshotRefreshRequired
      };
    }

    return {
      status: "blocked",
      reasons: [
        typeof readinessStatus === "string" ? readinessStatus : "readiness-status-missing"
      ],
      snapshotRefreshRequired: false
    };
  }
}

export class LocalPublicationFeatureFlag implements PublicationFeatureFlag {
  readonly name = "local-publication-feature-flag";
  status: PublicationDependencyProbe["status"] = "ok";
  snapshot: PublicationFeatureFlagSnapshot;

  constructor(config: PublicationConfig = loadPublicationConfig()) {
    this.snapshot = {
      flag: config.readiness.featureFlag,
      enabled: true,
      writeMode: config.writeMode
    };
  }

  probe(): PublicationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local publication feature flag ready" : "local publication feature flag degraded"
    };
  }

  resolve(): PublicationFeatureFlagSnapshot {
    return this.snapshot;
  }
}

export class LocalPublicationSnapshotPublisher implements PublicationSnapshotPublisher {
  readonly name = "local-publication-snapshot-publisher";
  status: PublicationDependencyProbe["status"] = "ok";
  productionWritesEnabled = false;
  readonly shadowComparisons: PublicationSnapshotCommand[] = [];
  readonly productionPublishes: PublicationSnapshotCommand[] = [];
  private readonly clock: RuntimeClock;

  constructor(clock: RuntimeClock = new ManualPublicationClock()) {
    this.clock = clock;
  }

  probe(): PublicationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local snapshot publisher ready" : "local snapshot publisher degraded"
    };
  }

  publishShadowComparison(command: PublicationSnapshotCommand): PublicationSnapshotReceipt {
    this.shadowComparisons.push(command);
    return {
      commandId: command.evaluationId,
      mode: "shadow_comparison",
      accepted: true,
      publishedAt: runtimeNow(this.clock)
    };
  }

  publishProductionSnapshot(command: PublicationSnapshotCommand): PublicationSnapshotReceipt {
    if (!this.productionWritesEnabled) {
      throw new Error("production-publication-disabled");
    }

    this.productionPublishes.push(command);
    return {
      commandId: command.evaluationId,
      mode: "production",
      accepted: true,
      publishedAt: runtimeNow(this.clock)
    };
  }
}

export class LocalPublicationBrokerOutbox implements PublicationBrokerOutbox {
  readonly name = "local-publication-broker-outbox";
  status: PublicationDependencyProbe["status"] = "ok";
  private mutableRecords: { command: BrokerPublishCommand; receipt: BrokerPublishReceipt }[] = [];

  get records(): readonly { readonly command: BrokerPublishCommand; readonly receipt: BrokerPublishReceipt }[] {
    return this.mutableRecords;
  }

  probe(): PublicationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local publication broker outbox ready" : "local publication broker outbox degraded"
    };
  }

  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    this.mutableRecords.push({
      command,
      receipt
    });
    return Promise.resolve();
  }

  hasReceipt(command: BrokerPublishCommand): Promise<boolean> {
    return Promise.resolve(this.mutableRecords.some((record) => record.command.envelope.messageId === command.envelope.messageId));
  }
}

export class LocalPublicationBrokerTransport implements RuntimeBrokerTransport {
  readonly name = "local-publication-broker";
  readonly inFlightDeliveryCount = 0;
  readonly published: BrokerPublishCommand[] = [];
  routes: readonly WorkerRoute[] = [];
  private handler: BrokerDeliveryHandler | undefined;
  private connected = false;

  connect(): Promise<void> {
    this.connected = true;
    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.routes = routes;
    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    this.published.push(command);
    const route = getWorkerRoute(command.envelope.route);
    return Promise.resolve({
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    if (!this.connected) {
      return Promise.reject(new Error("local publication broker is not connected"));
    }

    if (stage !== "publication") {
      return Promise.reject(new Error(`local publication broker cannot consume ${stage}`));
    }

    this.handler = handler;
    return Promise.resolve({
      stage,
      cancel: () => {
        this.handler = undefined;
        return Promise.resolve();
      }
    });
  }

  deliverPublication(delivery: RuntimeMessageDelivery = createMinimalPublicationDelivery()): Promise<RuntimeMessageProcessingResult> {
    if (this.handler === undefined) {
      return Promise.reject(new Error("local publication consumer is not registered"));
    }

    return this.handler(delivery);
  }

  close(): Promise<void> {
    this.connected = false;
    this.handler = undefined;
    return Promise.resolve();
  }
}

export class LocalPublicationWorkHandler implements PublicationWorkHandler {
  readonly name = "local-publication-shadow-handler";
  private readonly dependencies: Pick<PublicationDependencies, "clock" | "featureFlag" | "readinessPolicy">;

  constructor(dependencies: Pick<PublicationDependencies, "clock" | "featureFlag" | "readinessPolicy">) {
    this.dependencies = dependencies;
  }

  async handle(context: Parameters<PublicationWorkHandler["handle"]>[0], tools: PublicationWorkTools): Promise<{ readonly status: "ok" } | { readonly status: "retry"; readonly reason: string }> {
    const articleId = typeof context.payload.articleId === "string" ? context.payload.articleId : context.envelope.aggregate.id;
    const policy = await this.dependencies.readinessPolicy.getCurrentPolicy();
    const featureFlag = await this.dependencies.featureFlag.resolve();
    const decision = await this.dependencies.readinessPolicy.evaluate({
      articleId,
      payload: context.payload,
      policy,
      featureFlag
    });
    const evaluationId = `publication-evaluation:${context.envelope.idempotencyKey}`;
    const record: PublicationReadinessEvaluationRecord = {
      evaluationId,
      articleId,
      messageId: context.envelope.messageId,
      idempotencyKey: context.envelope.idempotencyKey,
      policy,
      decision,
      writeMode: featureFlag.writeMode,
      evaluatedAt: runtimeNow(this.dependencies.clock)
    };

    return tools.withTransaction(async (transaction) => {
      const write = await tools.recordReadinessEvaluation(transaction, record);

      if (write.status === "conflict") {
        return {
          status: "retry",
          reason: write.reason
        };
      }

      const command = {
        evaluationId,
        articleId,
        policyVersion: policy.version,
        writeMode: featureFlag.writeMode,
        snapshotRefreshRequired: decision.snapshotRefreshRequired
      };

      if (featureFlag.writeMode === "production" && decision.status === "ready_for_production") {
        await tools.publishProductionSnapshot(command);
        return {
          status: "ok"
        };
      }

      await tools.publishShadowComparison(command);
      return {
        status: "ok"
      };
    });
  }
}

export function createLocalPublicationDependencies(config = loadPublicationConfig()): PublicationDependencies {
  const clock = new ManualPublicationClock();
  const readinessPolicy = new LocalPublicationReadinessPolicy(config);
  const featureFlag = new LocalPublicationFeatureFlag(config);
  const dependencies = {
    clock,
    inboxStore: new InMemoryPublicationInboxStore(clock),
    database: new LocalPublicationDatabase(),
    readinessPolicy,
    snapshotPublisher: new LocalPublicationSnapshotPublisher(clock),
    featureFlag,
    brokerOutbox: new LocalPublicationBrokerOutbox(),
    brokerTransport: new LocalPublicationBrokerTransport(),
    workHandler: undefined as unknown as PublicationWorkHandler
  };

  return {
    ...dependencies,
    workHandler: new LocalPublicationWorkHandler(dependencies)
  };
}

export function createProductionCapableLocalPublicationConfig(): PublicationConfig {
  return loadPublicationConfig({
    NUTSNEWS_PUBLICATION_DEPENDENCY_MODE: "production",
    NUTSNEWS_PUBLICATION_DATABASE_URL: "postgres://example.invalid/publication",
    NUTSNEWS_PUBLICATION_RABBITMQ_URL: "amqp://example.invalid",
    NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL: "https://backend.example.invalid/worker",
    NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN: "secret-not-real",
    NUTSNEWS_PUBLICATION_WRITE_MODE: "production",
    NUTSNEWS_PUBLICATION_PRODUCTION_WRITE_CONFIRMATION: PUBLICATION_PRODUCTION_CONFIRMATION,
    NUTSNEWS_PUBLICATION_TELEMETRY_LOGS: "silent"
  });
}

export function createMinimalPublicationEnvelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  const route = getWorkerRoute("publication");
  const occurredAt = "2026-07-23T00:00:00.000Z";
  const envelope = {
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "publication",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4811",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4711",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4611",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "persistence:publication:article-001",
    aggregate: {
      type: "article",
      id: "article-001",
      version: 1
    },
    occurredAt,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: occurredAt
    },
    producer: {
      name: "persistence",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/persistence/article-001/publication-readiness",
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(createMinimalPublicationPayload())
    },
    ...overrides
  };

  return assertWorkerEnvelope(envelope);
}

export function createMinimalPublicationPayload(
  overrides: Readonly<Record<string, unknown>> = {}
): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.publicationReadiness,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3611",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4712",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4711",
    idempotencyKey: "persistence:publication:article-001",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: "2026-07-23T00:00:00.000Z",
    articleId: "article-001",
    readinessStatus: "ready",
    requiredLanguageCodes: [
      "fr",
      "ja",
      "de-CH",
      "de",
      "el"
    ],
    availableLanguageCodes: [
      "fr",
      "ja",
      "de-CH",
      "de",
      "el"
    ],
    missingLanguageCodes: [],
    snapshotRefreshRequired: true,
    ...overrides
  };
}

export function createMinimalPublicationDelivery(): RuntimeMessageDelivery {
  return {
    envelope: createMinimalPublicationEnvelope(),
    payload: createMinimalPublicationPayload(),
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}
