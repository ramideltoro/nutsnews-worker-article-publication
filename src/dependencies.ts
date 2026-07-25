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
  readonly source: "backend-worker-api";
  readonly writeMode: PublicationWriteMode;
  readonly requiredChecks: readonly string[];
}

export interface PublicationReadinessDecision {
  readonly status: "shadow_compare_only" | "ready_for_production" | "blocked";
  readonly reasons: readonly string[];
  readonly snapshotRefreshRequired: boolean;
}

export interface PublicationReadinessInput {
  readonly articleId: string;
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
  readonly messageId: string;
  readonly idempotencyKey: string;
  readonly policy: PublicationReadinessPolicySnapshot;
  readonly decision: PublicationReadinessDecision;
  readonly writeMode: PublicationWriteMode;
  readonly evaluatedAt: string;
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
    };

export interface PublicationSnapshotCommand {
  readonly evaluationId: string;
  readonly articleId: string;
  readonly policyVersion: string;
  readonly writeMode: PublicationWriteMode;
  readonly snapshotRefreshRequired: boolean;
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
