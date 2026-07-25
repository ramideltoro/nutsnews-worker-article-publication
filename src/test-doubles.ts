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
  type RuntimeMessageContext,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure,
  type RuntimeHandlerResult,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";

import type { PublicationConfig } from "./config.js";
import {
  PUBLICATION_PRODUCTION_CONFIRMATION,
  loadPublicationConfig
} from "./config.js";
import type {
  PublicationBackendCommandMetadata,
  PublicationBackendOperation,
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
import {
  BACKEND_CAPTURED_PUBLICATION_POLICY,
  evaluatePolicyDrivenPublicationReadiness
} from "./publication-gate.js";
import {
  BACKEND_PUBLIC_FEED_SNAPSHOT_CONTRACT,
  buildPublicFeedSnapshotCompatibility
} from "./public-feed-snapshot.js";

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
  readonly shadowOutputs: PublicationReadinessEvaluationRecord["shadowOutput"][] = [];
  failNextRecord = false;
  private readonly evaluationByKey = new Map<string, PublicationReadinessEvaluationRecord>();
  private readonly latestByArticle = new Map<string, PublicationReadinessEvaluationRecord>();

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
      const sameDecision = JSON.stringify(existing.shadowOutput) === JSON.stringify(record.shadowOutput);

      if (existing.evaluationId === record.evaluationId && sameDecision) {
        return Promise.resolve({
          status: "duplicate",
          evaluationId: existing.evaluationId
        });
      }

      return Promise.resolve({
        status: "conflict",
        reason: "idempotency-key-reused"
      });
    }

    const latest = this.latestByArticle.get(record.articleId);

    if (latest !== undefined && latest.finalAggregateVersion > record.finalAggregateVersion) {
      return Promise.resolve({
        status: "stale",
        reason: "stale-final-aggregate-version"
      });
    }

    if (
      latest?.finalAggregateVersion === record.finalAggregateVersion
      && JSON.stringify(latest.shadowOutput) !== JSON.stringify(record.shadowOutput)
    ) {
      return Promise.resolve({
        status: "conflict",
        reason: "conflicting-publication-decision"
      });
    }

    this.evaluationByKey.set(record.idempotencyKey, record);
    this.latestByArticle.set(record.articleId, record);
    this.evaluations.push(record);
    this.shadowOutputs.push(record.shadowOutput);
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
      ...BACKEND_CAPTURED_PUBLICATION_POLICY,
      policyId: config.readiness.policyId,
      writeMode: config.writeMode,
      requiredChecks: [
        "canonical-identity",
        "valid-enrichment-policy",
        "accepted-approval",
        "persisted-source-summary",
        "current-article-version",
        "no-blocking-state",
        "backend-translation-policy"
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
    return evaluatePolicyDrivenPublicationReadiness(input);
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
  singleWriterEnabled = false;
  cutoverState: "shadow" | "cutover-approved" | "rollback-active" = "shadow";
  failNextRefresh = false;
  readonly shadowComparisons: PublicationSnapshotCommand[] = [];
  readonly productionPublishes: PublicationSnapshotCommand[] = [];
  readonly partialRefreshFailures: PublicationSnapshotCommand[] = [];
  private readonly clock: RuntimeClock;
  private readonly shadowReceipts = new Map<string, {
    readonly digest: string;
    readonly receipt: PublicationSnapshotReceipt;
  }>();
  private readonly productionStates = new Map<string, {
    readonly digest: string;
    readonly command: PublicationSnapshotCommand;
    refreshCompleted: boolean;
    receipt: PublicationSnapshotReceipt | undefined;
  }>();

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
    if (command.providerMode !== "backend_postgres_shadow" || command.backendOperation !== "shadow-publication-comparison") {
      throw new Error("shadow-publication-command-invalid");
    }

    if (!sameOperations(command.backendOperations, [
      "shadow-publication-comparison"
    ]) || command.snapshotRefreshOperation !== undefined) {
      throw new Error("shadow-publication-operation-scope-invalid");
    }

    const digest = commandDigest(command);
    const existing = this.shadowReceipts.get(command.evaluationId);

    if (existing !== undefined) {
      if (existing.digest !== digest) {
        throw new Error("shadow-publication-command-conflict");
      }

      return existing.receipt;
    }

    this.shadowComparisons.push(command);
    const receipt: PublicationSnapshotReceipt = {
      commandId: command.evaluationId,
      mode: "shadow_comparison",
      accepted: true,
      publishedAt: runtimeNow(this.clock),
      backendOperations: command.backendOperations,
      snapshotRefreshRequested: false,
      rollbackAvailable: command.publicFeedSnapshot.recovery.rollbackAvailable
    };

    this.shadowReceipts.set(command.evaluationId, {
      digest,
      receipt
    });
    return receipt;
  }

  publishProductionSnapshot(command: PublicationSnapshotCommand): PublicationSnapshotReceipt {
    if (
      command.providerMode !== "backend_postgres_primary"
      || command.backendOperation !== "uplift-publish-articles-batch"
    ) {
      throw new Error("production-publication-command-not-scoped");
    }

    const expectedOperations: readonly PublicationBackendOperation[] = command.snapshotRefreshRequired
      ? [
          "uplift-publish-articles-batch",
          "uplift-refresh-public-feed-snapshot"
        ]
      : [
          "uplift-publish-articles-batch"
        ];

    if (!sameOperations(command.backendOperations, expectedOperations)) {
      throw new Error("production-publication-operation-scope-invalid");
    }

    if (
      command.snapshotRefreshRequired
      && command.snapshotRefreshOperation !== BACKEND_PUBLIC_FEED_SNAPSHOT_CONTRACT.productionRefreshOperation
    ) {
      throw new Error("production-public-feed-refresh-command-missing");
    }

    if (command.publicFeedSnapshot.status !== "compatible") {
      throw new Error("public-feed-snapshot-contract-mismatch");
    }

    if (!this.productionWritesEnabled || !this.singleWriterEnabled || this.cutoverState !== "cutover-approved") {
      throw new Error("production-publication-disabled");
    }

    const digest = commandDigest(command);
    const existing = this.productionStates.get(command.evaluationId);

    if (existing !== undefined) {
      if (existing.digest !== digest) {
        throw new Error("production-publication-command-conflict");
      }

      if (existing.receipt !== undefined) {
        return existing.receipt;
      }

      if (command.snapshotRefreshRequired && !existing.refreshCompleted) {
        this.completeRefreshOrThrow(existing);
      }

      existing.receipt = this.productionReceipt(existing.command);
      return existing.receipt;
    }

    const state: {
      readonly digest: string;
      readonly command: PublicationSnapshotCommand;
      refreshCompleted: boolean;
      receipt: PublicationSnapshotReceipt | undefined;
    } = {
      digest,
      command,
      refreshCompleted: !command.snapshotRefreshRequired,
      receipt: undefined
    };

    this.productionStates.set(command.evaluationId, state);
    this.productionPublishes.push(command);

    if (command.snapshotRefreshRequired) {
      this.completeRefreshOrThrow(state);
    }

    state.receipt = this.productionReceipt(command);
    return state.receipt;
  }

  private completeRefreshOrThrow(state: {
    readonly command: PublicationSnapshotCommand;
    refreshCompleted: boolean;
  }): void {
    if (this.failNextRefresh) {
      this.failNextRefresh = false;
      this.partialRefreshFailures.push(state.command);
      throw new Error("public-feed-snapshot-refresh-partial-failure");
    }

    state.refreshCompleted = true;
  }

  private productionReceipt(command: PublicationSnapshotCommand): PublicationSnapshotReceipt {
    return {
      commandId: command.evaluationId,
      mode: "production",
      accepted: true,
      publishedAt: runtimeNow(this.clock),
      backendOperations: command.backendOperations,
      snapshotRefreshRequested: command.snapshotRefreshOperation !== undefined,
      rollbackAvailable: command.publicFeedSnapshot.recovery.rollbackAvailable
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

  async handle(context: Parameters<PublicationWorkHandler["handle"]>[0], tools: PublicationWorkTools): Promise<RuntimeHandlerResult> {
    const articleId = typeof context.payload.articleId === "string" ? context.payload.articleId : context.envelope.aggregate.id;
    const policy = await this.dependencies.readinessPolicy.getCurrentPolicy();
    const featureFlag = await this.dependencies.featureFlag.resolve();
    const decision = await this.dependencies.readinessPolicy.evaluate({
      articleId,
      envelopeArticleVersion: context.envelope.aggregate.version,
      payload: context.payload,
      policy,
      featureFlag
    });
    const evaluationId = `publication-evaluation:${context.envelope.idempotencyKey}`;
    const publicFeedSnapshot = buildPublicFeedSnapshotCompatibility({
      payload: context.payload,
      decision
    });
    const shadowOutput = {
      articleId: decision.articleId,
      articleVersion: decision.articleVersion,
      finalAggregateVersion: decision.finalAggregateVersion,
      policyVersion: decision.policyVersion,
      status: decision.status,
      reasons: decision.reasons,
      requiredLanguageCodes: decision.requiredLanguageCodes,
      availableLanguageCodes: decision.availableLanguageCodes,
      missingLanguageCodes: decision.missingLanguageCodes,
      snapshotRefreshRequired: decision.snapshotRefreshRequired,
      publicFeedSnapshot
    };
    const record: PublicationReadinessEvaluationRecord = {
      evaluationId,
      articleId: decision.articleId,
      articleVersion: decision.articleVersion,
      finalAggregateVersion: decision.finalAggregateVersion,
      messageId: context.envelope.messageId,
      idempotencyKey: context.envelope.idempotencyKey,
      policy,
      decision,
      writeMode: decision.writeMode,
      evaluatedAt: runtimeNow(this.dependencies.clock),
      shadowOutput
    };

    return tools.withTransaction(async (transaction) => {
      const write = await tools.recordReadinessEvaluation(transaction, record);

      if (write.status === "conflict" || write.status === "stale") {
        return {
          status: "terminal-failure",
          reason: write.reason
        };
      }

      const command = {
        evaluationId,
        articleId: decision.articleId,
        articleVersion: decision.articleVersion,
        finalAggregateVersion: decision.finalAggregateVersion,
        backendOperation: decision.backendOperation,
        backendOperations: backendOperationsForDecision(decision),
        snapshotRefreshOperation: decision.status === "ready_for_production" && decision.snapshotRefreshRequired
          ? BACKEND_PUBLIC_FEED_SNAPSHOT_CONTRACT.productionRefreshOperation
          : undefined,
        providerMode: decision.providerMode,
        policyVersion: decision.policyVersion,
        writeMode: decision.writeMode,
        requiredLanguageCodes: decision.requiredLanguageCodes,
        availableLanguageCodes: decision.availableLanguageCodes,
        missingLanguageCodes: decision.missingLanguageCodes,
        snapshotRefreshRequired: decision.snapshotRefreshRequired,
        shadowOutput,
        publicFeedSnapshot,
        backendMetadata: backendMetadataFromContext(context, decision)
      };

      if (decision.status === "ready_for_production") {
        if (publicFeedSnapshot.status !== "compatible") {
          return {
            status: "terminal-failure",
            reason: publicFeedSnapshot.reasons.find((reason) => reason.includes("mismatch")) ?? "public-feed-snapshot-contract-mismatch"
          };
        }

        await tools.publishProductionSnapshot(command);
        return {
          status: "ok"
        };
      }

      await tools.publishShadowComparison(command);

      if (decision.terminal) {
        return {
          status: "terminal-failure",
          reason: decision.reasons[0] ?? "publication-decision-rejected"
        };
      }

      return {
        status: "ok"
      };
    });
  }
}

function backendOperationsForDecision(decision: PublicationReadinessDecision): readonly PublicationBackendOperation[] {
  if (decision.status !== "ready_for_production") {
    return [
      "shadow-publication-comparison"
    ];
  }

  if (!decision.snapshotRefreshRequired) {
    return [
      "uplift-publish-articles-batch"
    ];
  }

  return [
    "uplift-publish-articles-batch",
    BACKEND_PUBLIC_FEED_SNAPSHOT_CONTRACT.productionRefreshOperation
  ];
}

function backendMetadataFromContext(
  context: RuntimeMessageContext,
  decision: PublicationReadinessDecision
): PublicationBackendCommandMetadata {
  const publicationRef = recordValue(context.payload.publicationRef);

  return {
    idempotencyKey: context.envelope.idempotencyKey,
    messageId: context.envelope.messageId,
    correlationId: context.envelope.correlationId,
    pipelineRunId: stringValue(context.payload.pipelineRunId) ?? "unknown-pipeline-run",
    stageExecutionId: stringValue(context.payload.stageExecutionId) ?? "unknown-stage-execution",
    sourceMessageId: stringValue(context.payload.sourceMessageId) ?? context.envelope.causationId,
    actorService: "nutsnews-worker-article-publication",
    schemaVersion: typeof context.payload.schemaVersion === "number" ? context.payload.schemaVersion : 1,
    operationVersion: stringValue(publicationRef.operationVersion) ?? "public-feed-snapshot-compat-v1",
    expectedArticleVersion: decision.articleVersion
  };
}

function sameOperations(left: readonly PublicationBackendOperation[], right: readonly PublicationBackendOperation[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function commandDigest(command: PublicationSnapshotCommand): string {
  return JSON.stringify(command);
}

function recordValue(value: unknown): Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Readonly<Record<string, unknown>> : {};
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
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
  const publicationRef = {
    policyVersion: BACKEND_CAPTURED_PUBLICATION_POLICY.version,
    articleVersion: 1,
    currentArticleVersion: 1,
    finalAggregateVersion: 1,
    originalUrl: "https://example.com/article-001",
    canonicalIdentityHash: "sha256:article-001-canonical",
    canonicalIdentityValid: true,
    enrichmentPolicyValid: true,
    approvalStatus: "accepted",
    sourceSummaryPersisted: true,
    persistedSourceSummaryRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/final/article-001/source-summary",
      mediaType: "application/json"
    },
    processingState: "clear",
    operationVersion: "public-feed-snapshot-compat-v1",
    publicFeedSnapshotRequest: {
      limit: 6,
      offset: 0,
      category: "all",
      languageCode: "en"
    },
    publicFeedSnapshot: {
      id: "article-001",
      source: "example",
      title: "Sanitized public-feed title",
      originalUrl: "https://example.com/article-001",
      imageUrl: "https://example.invalid/article-001.jpg",
      publishedAt: "2026-07-23T00:00:00.000Z",
      publishedOnSiteAt: "2026-07-23T00:00:00.000Z",
      aiSummary: "Sanitized public-feed summary.",
      category: "world",
      positivityScore: 0.82,
      status: "published",
      snapshotRank: 1,
      summaries: [
        {
          languageCode: "fr",
          title: "Titre public sanitise",
          summary: "Resume public sanitise."
        },
        {
          languageCode: "ja",
          title: "Sanitized Japanese title",
          summary: "Sanitized Japanese summary."
        },
        {
          languageCode: "de-CH",
          title: "Sanitized Swiss German title",
          summary: "Sanitized Swiss German summary."
        },
        {
          languageCode: "de",
          title: "Sanitized German title",
          summary: "Sanitized German summary."
        },
        {
          languageCode: "el",
          title: "Sanitized Greek title",
          summary: "Sanitized Greek summary."
        }
      ]
    },
    ...recordOverride(overrides.publicationRef)
  };
  const { publicationRef: _ignoredPublicationRef, ...topLevelOverrides } = overrides;

  void _ignoredPublicationRef;

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
    publicationRef,
    ...topLevelOverrides
  };
}

function recordOverride(value: unknown): Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Readonly<Record<string, unknown>> : {};
}

export function createMinimalPublicationDelivery(): RuntimeMessageDelivery {
  return {
    envelope: createMinimalPublicationEnvelope(),
    payload: createMinimalPublicationPayload(),
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}
