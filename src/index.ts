import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createRuntimeShutdownController,
  SYSTEM_RUNTIME_CLOCK,
  getRuntimePackageMetadata,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadPublicationConfig,
  type PublicationConfig
} from "./config.js";
import { createPublicationHttpServer } from "./http.js";
import { createPublicationPrometheusTelemetrySink } from "./metrics.js";
import { createProductionPublicationDependencies } from "./production.js";
import type { PublicationDependencies } from "./dependencies.js";
import type { PublicationReconciler } from "./reconciliation.js";
import { createPublicationService } from "./service.js";
import { createLocalPublicationDependencies } from "./test-doubles.js";

export {
  PUBLICATION_CONFIG_SCHEMA,
  PUBLICATION_DEFAULT_POLICY_ID,
  PUBLICATION_PRODUCTION_CONFIRMATION,
  PUBLICATION_SERVICE_NAME,
  PUBLICATION_SERVICE_VERSION,
  PublicationConfigError,
  loadPublicationConfig,
  type PublicationConfig,
  type PublicationDependencyMode,
  type PublicationTelemetryLogMode,
  type PublicationWriteMode
} from "./config.js";
export type {
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
export {
  createPublicationHttpServer,
  type PublicationHttpServer
} from "./http.js";
export {
  PUBLICATION_STAGE_LATENCY_BUCKETS_SECONDS,
  PUBLICATION_STAGE_OUTCOMES,
  createPublicationPrometheusTelemetrySink,
  type PublicationMetricsSink,
  type PublicationPrometheusTelemetrySink,
  type PublicationPrometheusTelemetrySinkOptions,
  type PublicationStageOutcome
} from "./metrics.js";
export {
  PUBLICATION_CLAIMED_OPERATION_BUDGET,
  PUBLICATION_HANDLER_DEADLINE_MS,
  PUBLICATION_INBOX_CLAIM_LEASE_MS,
  PublicationOperationDeadlineError,
  createPublicationOperationDeadline,
  type PublicationOperationDeadline
} from "./operation-deadline.js";
export {
  PUBLICATION_RECONCILIATION_CONFIRMATION,
  PUBLICATION_RECONCILIATION_PATH,
  type PublicationReconciliationCandidate,
  type PublicationReconciliationReport,
  type PublicationReconciliationRequest,
  type PublicationReconciler
} from "./reconciliation.js";
export {
  PostgresPublicationBrokerOutbox,
  PostgresPublicationDatabase,
  PostgresPublicationInboxStore,
  PostgresPublicationSnapshotPublisher,
  PostgresPublicationTerminalReconciler,
  createProductionPublicationDependencies,
  type ProductionPublicationDependencies
} from "./production.js";
export {
  PayloadRabbitMqTransport
} from "./rabbitmq-payload-transport.js";
export {
  BACKEND_CAPTURED_PUBLICATION_POLICY,
  evaluatePolicyDrivenPublicationReadiness,
  normalizePublicationAggregate,
  type NormalizedPublicationAggregate,
  type PolicyDrivenPublicationInput
} from "./publication-gate.js";
export {
  BACKEND_PUBLIC_FEED_SNAPSHOT_CONTRACT,
  buildPublicFeedSnapshotCompatibility,
  type PublicFeedSnapshotCompatibilityInput
} from "./public-feed-snapshot.js";
export {
  createPublicationService,
  type PublicationService
} from "./service.js";
export {
  InMemoryPublicationInboxStore,
  LocalPublicationBrokerOutbox,
  LocalPublicationBrokerTransport,
  LocalPublicationDatabase,
  LocalPublicationFeatureFlag,
  LocalPublicationReadinessPolicy,
  LocalPublicationSnapshotPublisher,
  LocalPublicationWorkHandler,
  ManualPublicationClock,
  createLocalPublicationDependencies,
  createMinimalPublicationDelivery,
  createMinimalPublicationEnvelope,
  createMinimalPublicationPayload,
  createProductionCapableLocalPublicationConfig
} from "./test-doubles.js";

export interface PublicationApplication {
  readonly config: PublicationConfig;
  start(): Promise<void>;
  stop(): Promise<void>;
}

export interface PublicationApplicationOptions {
  readonly dependencies?: PublicationDependencies;
}

export function createPublicationApplication(
  config = loadPublicationConfig(),
  options: PublicationApplicationOptions = {}
): PublicationApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host,
    revision: config.buildRevision,
    deployment: config.dependencyMode === "production"
      ? config.writeMode === "production" ? "production" : "shadow"
      : config.environment === "test" ? "test" : "local",
    adapter: config.dependencyMode === "production" ? "production" : "in_memory"
  } as const;
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
      ? createPublicationPrometheusTelemetrySink({
        identity,
        expectedActive: config.dependencyMode === "production"
          && config.writeMode === "production"
          && config.security.productionWriteConfirmationPresent
      })
    : undefined;
  const telemetry = combineTelemetrySinks(logSink, metrics);
  const dependencies = options.dependencies ?? (
    config.dependencyMode === "production"
      ? createProductionPublicationDependencies({
          config,
          clock: SYSTEM_RUNTIME_CLOCK,
          ...(telemetry === undefined ? {} : {
            telemetry
          })
        })
      : createLocalPublicationDependencies(config)
  );
  const service = createPublicationService({
    config,
    dependencies,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const httpServer = createPublicationHttpServer({
    config,
    service,
    ...(hasReconciler(dependencies) ? {
      reconciler: dependencies.reconciler
    } : {}),
    ...(hasReconciliationToken(dependencies) ? {
      reconciliationToken: dependencies.reconciliationToken
    } : {}),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const shutdown = createRuntimeShutdownController({
    callbacks: [
      async () => {
        await httpServer.close();
      },
      async () => {
        await service.stop();
      },
      async () => {
        if (hasClose(dependencies)) {
          await dependencies.close();
        }
      }
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(logSink === undefined ? {} : {
      telemetryFlusher: {
        flush: async () => {
          try {
            await logSink.flush();
          } catch {
            // Telemetry flushing is best effort and must not block shutdown.
          }
        }
      }
    })
  });

  return {
    config,
    async start(): Promise<void> {
      assertPackageCompatibility();
      let listenerStarted = false;

      try {
        await httpServer.listen();
        listenerStarted = true;
        shutdown.start();
        await service.start();
      } catch (error: unknown) {
        shutdown.stop();
        await cleanupFailedInitialization(httpServer, service, dependencies, listenerStarted);
        throw error;
      }
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    }
  };
}

async function cleanupFailedInitialization(
  httpServer: ReturnType<typeof createPublicationHttpServer>,
  service: ReturnType<typeof createPublicationService>,
  dependencies: PublicationDependencies,
  listenerStarted: boolean
): Promise<void> {
  if (listenerStarted) {
    await ignoreCleanupFailure(() => httpServer.close());
  }

  await ignoreCleanupFailure(() => service.stop());

  if (hasClose(dependencies)) {
    await ignoreCleanupFailure(() => dependencies.close());
  }
}

async function ignoreCleanupFailure(operation: () => Promise<void>): Promise<void> {
  try {
    await operation();
  } catch {
    // Preserve the initialization error after attempting every cleanup step.
  }
}

function hasClose(value: unknown): value is { close(): Promise<void> } {
  return typeof value === "object"
    && value !== null
    && "close" in value
    && typeof value.close === "function";
}

function hasReconciler(value: unknown): value is { readonly reconciler: PublicationReconciler } {
  return typeof value === "object"
    && value !== null
    && "reconciler" in value
    && typeof value.reconciler === "object"
    && value.reconciler !== null;
}

function hasReconciliationToken(value: unknown): value is { readonly reconciliationToken: string } {
  return typeof value === "object"
    && value !== null
    && "reconciliationToken" in value
    && typeof value.reconciliationToken === "string"
    && value.reconciliationToken.length > 0;
}

function combineTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks.filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        try {
          await sink.emit(event);
        } catch {
          // Each telemetry sink is isolated so another sink can still receive the event.
        }
      }
    }
  };
}

export const SUPPORTED_CONTRACTS_PACKAGE_VERSION = "1.0.0";
export const SUPPORTED_RUNTIME_PACKAGE_VERSION = "1.0.0";

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== SUPPORTED_CONTRACTS_PACKAGE_VERSION) {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== SUPPORTED_RUNTIME_PACKAGE_VERSION) {
    throw new Error(`Unsupported runtime package version ${runtimeVersion}.`);
  }
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const application = createPublicationApplication();

  application.start().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "failed to start publication");
    process.exitCode = 1;
  });
}
