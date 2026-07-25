import type {
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport,
  RuntimeClock,
  RuntimeHandlerResult,
  RuntimeIdempotencyStore,
  RuntimeMessageContext
} from "@ramideltoro/nutsnews-worker-runtime";

import type { PublicationWriteMode } from "./config.js";

export interface PublicationDependencyProbe {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
}

export interface PublicationDatabaseTransaction {
  readonly transactionId: string;
}

export interface PublicationPermissionProbe extends PublicationDependencyProbe {
  readonly details: {
    readonly databaseRole: string;
    readonly shadowSchemaVersion: string;
    readonly allowedWriteScopes: readonly string[];
    readonly allowedReadScopes: readonly string[];
    readonly deniedWriteScopes: readonly string[];
  };
}

export interface PublicationInboxStore extends RuntimeIdempotencyStore {
  readonly name: string;
  probe(): PublicationDependencyProbe | Promise<PublicationDependencyProbe>;
}

export interface PublicationDatabase {
  readonly name: string;
  probe(): PublicationDependencyProbe | Promise<PublicationDependencyProbe>;
  checkWriteScope(): PublicationPermissionProbe | Promise<PublicationPermissionProbe>;
  withTransaction<T>(operation: (transaction: PublicationDatabaseTransaction) => Promise<T>): Promise<T>;
  recordReadinessEvaluation(
    transaction: PublicationDatabaseTransaction,
    record: PublicationReadinessEvaluationRecord
  ): Promise<PublicationReadinessEvaluationWriteResult>;
}

export interface PublicationReadinessPolicySnapshot {
  readonly policyId: string;
  readonly version: string;
  readonly capturedAt: string;
  readonly source: "backend-worker-api";
  readonly writeMode: PublicationWriteMode;
  readonly requiredLanguageCodes: readonly string[];
  readonly holdForTranslations: boolean;
  readonly minimumLanguageCodes: readonly string[];
  readonly backlogTreatment: "block_until_recovered" | "allow_non_blocking";
  readonly timeoutTreatment: "block" | "allow_after_timeout";
  readonly stale: boolean;
  readonly scopedPublicationOperation: "uplift-publish-articles-batch";
  readonly requiredChecks: readonly string[];
}

export interface PublicationReadinessDecision {
  readonly status: "shadow_compare_only" | "ready_for_production" | "blocked" | "rejected";
  readonly terminal: boolean;
  readonly reasons: readonly string[];
  readonly articleId: string;
  readonly articleVersion: number;
  readonly finalAggregateVersion: number;
  readonly policyVersion: string;
  readonly requiredLanguageCodes: readonly string[];
  readonly availableLanguageCodes: readonly string[];
  readonly missingLanguageCodes: readonly string[];
  readonly snapshotRefreshRequired: boolean;
  readonly writeMode: PublicationWriteMode;
  readonly backendOperation: "shadow-publication-comparison" | "uplift-publish-articles-batch";
  readonly providerMode: "backend_postgres_shadow" | "backend_postgres_primary";
  readonly featureFlag: string;
}

export interface PublicationReadinessInput {
  readonly articleId: string;
  readonly envelopeArticleVersion: number;
  readonly payload: Readonly<Record<string, unknown>>;
  readonly policy: PublicationReadinessPolicySnapshot;
  readonly featureFlag: PublicationFeatureFlagSnapshot;
}

export interface PublicationReadinessPolicy {
  readonly name: string;
  probe(): PublicationDependencyProbe | Promise<PublicationDependencyProbe>;
  getCurrentPolicy(): PublicationReadinessPolicySnapshot | Promise<PublicationReadinessPolicySnapshot>;
  evaluate(input: PublicationReadinessInput): PublicationReadinessDecision | Promise<PublicationReadinessDecision>;
}

export interface PublicationFeatureFlagSnapshot {
  readonly flag: string;
  readonly enabled: boolean;
  readonly writeMode: PublicationWriteMode;
}

export interface PublicationFeatureFlag {
  readonly name: string;
  probe(): PublicationDependencyProbe | Promise<PublicationDependencyProbe>;
  resolve(): PublicationFeatureFlagSnapshot | Promise<PublicationFeatureFlagSnapshot>;
}

export interface PublicationReadinessEvaluationRecord {
  readonly evaluationId: string;
  readonly articleId: string;
  readonly articleVersion: number;
  readonly finalAggregateVersion: number;
  readonly messageId: string;
  readonly idempotencyKey: string;
  readonly policy: PublicationReadinessPolicySnapshot;
  readonly decision: PublicationReadinessDecision;
  readonly writeMode: PublicationWriteMode;
  readonly evaluatedAt: string;
  readonly shadowOutput: PublicationShadowOutput;
}

export type PublicationReadinessEvaluationWriteResult =
  | {
      readonly status: "recorded";
      readonly evaluationId: string;
    }
  | {
      readonly status: "duplicate";
      readonly evaluationId: string;
    }
  | {
      readonly status: "conflict";
      readonly reason: string;
    }
  | {
      readonly status: "stale";
      readonly reason: string;
    };

export interface PublicationShadowOutput {
  readonly articleId: string;
  readonly articleVersion: number;
  readonly finalAggregateVersion: number;
  readonly policyVersion: string;
  readonly status: PublicationReadinessDecision["status"];
  readonly reasons: readonly string[];
  readonly requiredLanguageCodes: readonly string[];
  readonly availableLanguageCodes: readonly string[];
  readonly missingLanguageCodes: readonly string[];
  readonly snapshotRefreshRequired: boolean;
}

export interface PublicationSnapshotCommand {
  readonly evaluationId: string;
  readonly articleId: string;
  readonly articleVersion: number;
  readonly finalAggregateVersion: number;
  readonly backendOperation: PublicationReadinessDecision["backendOperation"];
  readonly providerMode: PublicationReadinessDecision["providerMode"];
  readonly policyVersion: string;
  readonly writeMode: PublicationWriteMode;
  readonly requiredLanguageCodes: readonly string[];
  readonly availableLanguageCodes: readonly string[];
  readonly missingLanguageCodes: readonly string[];
  readonly snapshotRefreshRequired: boolean;
  readonly shadowOutput: PublicationShadowOutput;
}

export interface PublicationSnapshotReceipt {
  readonly commandId: string;
  readonly mode: PublicationWriteMode;
  readonly accepted: boolean;
  readonly publishedAt: string;
}

export interface PublicationSnapshotPublisher {
  readonly name: string;
  probe(): PublicationDependencyProbe | Promise<PublicationDependencyProbe>;
  publishShadowComparison(command: PublicationSnapshotCommand): PublicationSnapshotReceipt | Promise<PublicationSnapshotReceipt>;
  publishProductionSnapshot(command: PublicationSnapshotCommand): PublicationSnapshotReceipt | Promise<PublicationSnapshotReceipt>;
}

export interface PublicationBrokerOutbox {
  readonly name: string;
  probe(): PublicationDependencyProbe | Promise<PublicationDependencyProbe>;
  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  hasReceipt(command: BrokerPublishCommand): Promise<boolean>;
}

export interface PublicationWorkTools {
  publishBroker(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  recordBrokerOutbox(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  withTransaction<T>(operation: (transaction: PublicationDatabaseTransaction) => Promise<T>): Promise<T>;
  recordReadinessEvaluation(
    transaction: PublicationDatabaseTransaction,
    record: PublicationReadinessEvaluationRecord
  ): Promise<PublicationReadinessEvaluationWriteResult>;
  publishShadowComparison(command: PublicationSnapshotCommand): Promise<PublicationSnapshotReceipt>;
  publishProductionSnapshot(command: PublicationSnapshotCommand): Promise<PublicationSnapshotReceipt>;
}

export interface PublicationWorkHandler {
  readonly name: string;
  handle(context: RuntimeMessageContext, tools: PublicationWorkTools): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface PublicationDependencies {
  readonly clock: RuntimeClock;
  readonly inboxStore: PublicationInboxStore;
  readonly database: PublicationDatabase;
  readonly readinessPolicy: PublicationReadinessPolicy;
  readonly snapshotPublisher: PublicationSnapshotPublisher;
  readonly featureFlag: PublicationFeatureFlag;
  readonly brokerOutbox: PublicationBrokerOutbox;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly workHandler: PublicationWorkHandler;
}
