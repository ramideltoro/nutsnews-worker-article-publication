import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadPublicationConfig,
  type PublicationConfig
} from "./config.js";
import { createPublicationHttpServer } from "./http.js";
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

export function createPublicationApplication(config = loadPublicationConfig()): PublicationApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host
  };
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
    ? createPrometheusRuntimeTelemetrySink({
        identity
      })
    : undefined;
  const telemetry = combineTelemetrySinks(logSink, metrics);
  const dependencies = createLocalPublicationDependencies(config);
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
      }
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(logSink === undefined ? {} : {
      telemetryFlusher: logSink
    })
  });

  return {
    config,
    async start(): Promise<void> {
      assertPackageCompatibility();
      await service.start();
      await httpServer.listen();
      shutdown.start();
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    }
  };
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
        await sink.emit(event);
      }
    }
  };
}

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== "0.4.0") {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== "0.4.0") {
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
