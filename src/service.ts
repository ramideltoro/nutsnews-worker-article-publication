import {
  getWorkerRoute
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  createRuntimeMessageProcessor,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type PrometheusRuntimeTelemetrySink,
  type RuntimeHealthCheck,
  type RuntimeHealthProbeSet,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { PublicationConfig } from "./config.js";
import type {
  PublicationDependencies,
  PublicationDependencyProbe
} from "./dependencies.js";

export interface PublicationServiceOptions {
  readonly config: PublicationConfig;
  readonly dependencies: PublicationDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: PrometheusRuntimeTelemetrySink;
}

export interface PublicationService {
  readonly broker: BrokerLifecycle;
  readonly health: RuntimeHealthProbeSet;
  readonly isStarted: boolean;
  readonly isDraining: boolean;
  readonly consumer: BrokerConsumerHandle | undefined;
  start(): Promise<void>;
  stop(): Promise<void>;
  processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult>;
}

export function createPublicationService(options: PublicationServiceOptions): PublicationService {
  const publicationRoute = getWorkerRoute("publication");
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      publicationRoute
    ],
    clock: options.dependencies.clock,
    ...(options.telemetry === undefined ? {} : {
      telemetry: options.telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  const processor = createRuntimeMessageProcessor({
    stage: "publication",
    idempotencyStore: options.dependencies.inboxStore,
    clock: options.dependencies.clock,
    ...(options.telemetry === undefined ? {} : {
      telemetry: options.telemetry
    }),
    handler: async (context) => {
      try {
        return await drain.track(async () => {
          options.metrics?.setInFlight(publicationRoute.mainQueue.name, drain.inFlight);
          const result = await options.dependencies.workHandler.handle(context, {
            publishBroker: (command) => broker.publish(command),
            recordBrokerOutbox: (command, receipt) => options.dependencies.brokerOutbox.record(command, receipt),
            withTransaction: (operation) => options.dependencies.database.withTransaction(operation),
            recordReadinessEvaluation: (transaction, record) => options.dependencies.database.recordReadinessEvaluation(transaction, record),
            publishShadowComparison: async (command) => options.dependencies.snapshotPublisher.publishShadowComparison(command),
            publishProductionSnapshot: async (command) => options.dependencies.snapshotPublisher.publishProductionSnapshot(command)
          });

          await emitRuntimeTelemetry(options.telemetry, {
            name: "runtime.dependency.observed",
            level: result.status === "ok" ? "info" : "warn",
            at: runtimeNow(options.dependencies.clock),
            stage: "publication",
            queue: publicationRoute.mainQueue.name,
            outcome: result.status === "ok" ? "success" : result.status === "retry" ? "retry" : "failure",
            attributes: {
              event: "publication.message.delegated",
              dependency: options.dependencies.workHandler.name,
              writeMode: options.config.writeMode,
              policyId: options.config.readiness.policyId,
              productionWriteConfirmationPresent: options.config.security.productionWriteConfirmationPresent
            }
          });

          return result;
        });
      } finally {
        options.metrics?.setInFlight(publicationRoute.mainQueue.name, drain.inFlight);
      }
    }
  });
  let started = false;
  let consumer: BrokerConsumerHandle | undefined;

  const service = {
    get broker(): BrokerLifecycle {
      return broker;
    },
    get health(): RuntimeHealthProbeSet {
      return createRuntimeHealthProbeSet({
        livenessChecks: [
          livenessCheck()
        ],
        startupChecks: [
          startupCheck(() => started)
        ],
        readinessChecks: [
          brokerReadinessCheck(broker),
          dependencyReadinessCheck("publication-inbox", options.dependencies.inboxStore),
          dependencyReadinessCheck("publication-database", options.dependencies.database),
          dependencyReadinessCheck("readiness-policy", options.dependencies.readinessPolicy),
          dependencyReadinessCheck("snapshot-publisher", options.dependencies.snapshotPublisher),
          dependencyReadinessCheck("feature-flag", options.dependencies.featureFlag),
          dependencyReadinessCheck("broker-outbox", options.dependencies.brokerOutbox),
          databaseWriteScopeCheck(options),
          writeModeCheck(options.config)
        ],
        clock: options.dependencies.clock,
        ...(options.telemetry === undefined ? {} : {
          telemetry: options.telemetry
        })
      });
    },
    get isStarted(): boolean {
      return started;
    },
    get isDraining(): boolean {
      return drain.isDraining;
    },
    get consumer(): BrokerConsumerHandle | undefined {
      return consumer;
    },
    async start(): Promise<void> {
      if (started) {
        return;
      }

      await broker.start();
      consumer = await broker.consume("publication", processor);
      started = true;
      options.metrics?.recordDependencyLatency(publicationRoute.mainQueue.name, 0, "success");
      options.metrics?.setInFlight(publicationRoute.mainQueue.name, drain.inFlight);
      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.dependency.observed",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "publication",
        queue: publicationRoute.mainQueue.name,
        outcome: "success",
        attributes: {
          dependency: "publication-shell",
          mode: options.config.dependencyMode,
          prefetch: options.config.prefetch,
          concurrency: options.config.concurrency,
          databaseRole: options.config.security.databaseRole,
          backendApiIdentity: options.config.security.backendApiIdentity,
          writeMode: options.config.writeMode
        }
      });
    },
    async stop(): Promise<void> {
      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      options.metrics?.setShutdownDraining(true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      options.metrics?.setShutdownDraining(false);
      options.metrics?.setInFlight(publicationRoute.mainQueue.name, drain.inFlight);
      consumer = undefined;
      started = false;
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies PublicationService;

  return service;
}

function livenessCheck(): RuntimeHealthCheck {
  return {
    name: "process",
    critical: true,
    check: () => "ok"
  };
}

function startupCheck(isStarted: () => boolean): RuntimeHealthCheck {
  return {
    name: "service-started",
    critical: true,
    check: () => isStarted() ? "ok" : "unhealthy"
  };
}

function brokerReadinessCheck(broker: BrokerLifecycle): RuntimeHealthCheck {
  return {
    name: "broker-lifecycle",
    critical: true,
    check: () => broker.state === "ready"
      ? {
          status: "ok",
          details: {
            state: broker.state
          }
        }
      : {
          status: "unhealthy",
          details: {
            state: broker.state
          }
        }
  };
}

function dependencyReadinessCheck(
  name: string,
  dependency: {
    readonly name: string;
    probe(): PublicationDependencyProbe | Promise<PublicationDependencyProbe>;
  }
): RuntimeHealthCheck {
  return {
    name,
    critical: true,
    check: async () => {
      const probe = await dependency.probe();

      return {
        status: probe.status,
        details: {
          dependency: dependency.name,
          summary: probe.summary
        }
      };
    }
  };
}

function databaseWriteScopeCheck(options: PublicationServiceOptions): RuntimeHealthCheck {
  return {
    name: "database-write-scope",
    critical: true,
    check: async () => {
      const probe = await options.dependencies.database.checkWriteScope();

      return {
        status: probe.status,
        details: {
          databaseRole: probe.details.databaseRole,
          shadowSchemaVersion: probe.details.shadowSchemaVersion,
          allowedWriteScopes: probe.details.allowedWriteScopes.join(","),
          allowedReadScopes: probe.details.allowedReadScopes.join(","),
          deniedWriteScopes: probe.details.deniedWriteScopes.join(","),
          expectedDatabaseRole: options.config.security.databaseRole,
          expectedShadowSchemaVersion: options.config.compatibility.shadowSchemaVersion
        }
      };
    }
  };
}

function writeModeCheck(config: PublicationConfig): RuntimeHealthCheck {
  return {
    name: "publication-write-mode",
    critical: true,
    check: () => config.writeMode === "shadow_comparison"
      ? {
          status: "ok",
          details: {
            writeMode: config.writeMode,
            productionWriteConfirmationPresent: config.security.productionWriteConfirmationPresent,
            reason: "shadow-comparison-default"
          }
        }
      : {
          status: config.security.productionWriteConfirmationPresent ? "ok" : "unhealthy",
          details: {
            writeMode: config.writeMode,
            productionWriteConfirmationPresent: config.security.productionWriteConfirmationPresent,
            reason: config.security.productionWriteConfirmationPresent ? "protected-confirmation-present" : "missing-protected-confirmation"
          }
        }
  };
}
