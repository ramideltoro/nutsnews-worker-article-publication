import {
  getRetryDestination,
  getWorkerRoute,
  validateWorkerEnvelope,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createBrokerConsumerReadinessCheck,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  createRuntimeMessageProcessor,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type RuntimeHealthCheck,
  type RuntimeHealthReport,
  type RuntimeHealthProbeSet,
  type RuntimeIdempotencyStore,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { PublicationConfig } from "./config.js";
import type {
  PublicationMetricsSink,
  PublicationPrometheusTelemetrySink
} from "./metrics.js";
import type {
  PublicationDependencies,
  PublicationDependencyProbe
} from "./dependencies.js";

export interface PublicationServiceOptions {
  readonly config: PublicationConfig;
  readonly dependencies: PublicationDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: PublicationMetricsSink;
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
  const telemetry = bestEffortTelemetrySink(options.telemetry);
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      publicationRoute
    ],
    clock: options.dependencies.clock,
    ...(telemetry === undefined ? {} : {
      telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  const sharedProcessor = createRuntimeMessageProcessor({
    stage: "publication",
    idempotencyStore: classifyInboxStoreFailures(options.dependencies.inboxStore),
    clock: options.dependencies.clock,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    handler: async (context) => {
      try {
        return await drain.track(async () => {
          setInFlight(options.metrics, publicationRoute.mainQueue.name, drain.inFlight);
          const result = await options.dependencies.workHandler.handle(context, {
            publishBroker: (command) => broker.publish(command),
            recordBrokerOutbox: (command, receipt) => options.dependencies.brokerOutbox.record(command, receipt),
            withTransaction: (operation) => options.dependencies.database.withTransaction(operation),
            recordReadinessEvaluation: (transaction, record) => options.dependencies.database.recordReadinessEvaluation(transaction, record),
            publishShadowComparison: async (command) => options.dependencies.snapshotPublisher.publishShadowComparison(command),
            publishProductionSnapshot: async (command) => options.dependencies.snapshotPublisher.publishProductionSnapshot(command)
          });

          await emitRuntimeTelemetry(telemetry, {
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
        setInFlight(options.metrics, publicationRoute.mainQueue.name, drain.inFlight);
      }
    }
  });
  const processor = async (delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> => {
    const startedAtMs = options.dependencies.clock.now().getTime();

    try {
      return await sharedProcessor(delivery);
    } catch (error: unknown) {
      return completeProcessorFailure(
        delivery,
        error,
        telemetry,
        options.dependencies.clock,
        startedAtMs
      );
    }
  };
  let started = false;
  let consumer: BrokerConsumerHandle | undefined;

  const service = {
    get broker(): BrokerLifecycle {
      return broker;
    },
    get health(): RuntimeHealthProbeSet {
      const probes = createRuntimeHealthProbeSet({
        livenessChecks: [
          livenessCheck()
        ],
        startupChecks: [
          startupCheck(() => started)
        ],
        readinessChecks: [
          brokerReadinessCheck(broker),
          createBrokerConsumerReadinessCheck(broker, "publication"),
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
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      });

      return observeHealthProbes(probes, options.metrics);
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
      const brokerConsumer = await broker.consume("publication", processor);
      consumer = {
        stage: brokerConsumer.stage,
        cancel: async () => {
          await brokerConsumer.cancel();
          setHealthProbe(options.metrics, "readiness", "unhealthy");
        }
      };
      started = true;
      setHealthProbe(options.metrics, "startup", "ok");
      setInFlight(options.metrics, publicationRoute.mainQueue.name, drain.inFlight);
      await emitRuntimeTelemetry(telemetry, {
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
      setShutdownDraining(options.metrics, true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      setShutdownDraining(options.metrics, false);
      setInFlight(options.metrics, publicationRoute.mainQueue.name, drain.inFlight);
      setHealthProbe(options.metrics, "startup", "unhealthy");
      setHealthProbe(options.metrics, "readiness", "unhealthy");
      consumer = undefined;
      started = false;
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies PublicationService;

  return service;
}

function setHealthProbe(
  metrics: PublicationMetricsSink | undefined,
  probe: "liveness" | "startup" | "readiness",
  outcome: "ok" | "degraded" | "unhealthy"
): void {
  runBestEffort(() => {
    if (isPublicationMetrics(metrics)) {
      metrics.setHealthProbe(probe, outcome);
    }
  });
}

function setInFlight(
  metrics: PublicationMetricsSink | undefined,
  queue: string,
  value: number
): void {
  runBestEffort(() => metrics?.setInFlight(queue, value));
}

function setShutdownDraining(
  metrics: PublicationMetricsSink | undefined,
  draining: boolean
): void {
  runBestEffort(() => metrics?.setShutdownDraining(draining));
}

function runBestEffort(operation: () => unknown): void {
  try {
    const result = operation();

    if (isPromiseLike(result)) {
      void result.then(undefined, () => undefined);
    }
  } catch {
    // Telemetry is deliberately non-semantic and must never alter message handling.
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return typeof value === "object" && value !== null && "then" in value && typeof value.then === "function";
}

function bestEffortTelemetrySink(sink: RuntimeTelemetrySink | undefined): RuntimeTelemetrySink | undefined {
  if (sink === undefined) {
    return undefined;
  }

  return {
    emit: async (event) => {
      try {
        await sink.emit(event);
      } catch {
        // Telemetry is deliberately non-semantic and must never alter message handling.
      }
    }
  };
}

function observeHealthProbes(
  probes: RuntimeHealthProbeSet,
  metrics: PublicationMetricsSink | undefined
): RuntimeHealthProbeSet {
  const observe = async <T extends RuntimeHealthReport>(
    probe: "liveness" | "startup" | "readiness",
    operation: () => Promise<T>
  ): Promise<T> => {
    const report = await operation();
    setHealthProbe(metrics, probe, report.status);

    return report;
  };

  return {
    liveness: () => observe("liveness", () => probes.liveness()),
    startup: () => observe("startup", () => probes.startup()),
    readiness: () => observe("readiness", () => probes.readiness())
  };
}

function isPublicationMetrics(
  metrics: PublicationMetricsSink | undefined
): metrics is PublicationPrometheusTelemetrySink {
  return metrics !== undefined && "setHealthProbe" in metrics && typeof metrics.setHealthProbe === "function";
}

class PublicationInboxStoreError extends Error {
  readonly telemetryReason: string;

  constructor(telemetryReason: string) {
    super(telemetryReason);
    this.name = "PublicationInboxStoreError";
    this.telemetryReason = telemetryReason;
  }
}

function classifyInboxStoreFailures(store: RuntimeIdempotencyStore): RuntimeIdempotencyStore {
  return {
    claim: async (idempotencyKey, context) => inboxStoreOperation(
      "idempotency-claim-error",
      () => store.claim(idempotencyKey, context)
    ),
    markCompleted: async (idempotencyKey, completion) => inboxStoreOperation(
      "idempotency-completion-error",
      () => store.markCompleted(idempotencyKey, completion)
    ),
    markFailed: async (idempotencyKey, failure) => inboxStoreOperation(
      "idempotency-failure-record-error",
      () => store.markFailed(idempotencyKey, failure)
    )
  };
}

async function inboxStoreOperation<T>(reason: string, operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch {
    throw new PublicationInboxStoreError(reason);
  }
}

async function completeProcessorFailure(
  delivery: RuntimeMessageDelivery,
  error: unknown,
  telemetry: RuntimeTelemetrySink | undefined,
  clock: PublicationDependencies["clock"],
  startedAtMs: number
): Promise<RuntimeMessageProcessingResult> {
  const queue = getWorkerRoute("publication").mainQueue.name;
  const durationMs = Math.max(0, clock.now().getTime() - startedAtMs);
  const envelopeResult = validateWorkerEnvelope(delivery.envelope);

  if (!envelopeResult.ok) {
    const issues = envelopeResult.issues.map((issue) => ({
      path: issue.path,
      code: issue.code,
      message: issue.message
    }));
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.invalid",
      level: "warn",
      at: runtimeNow(clock),
      stage: "publication",
      queue,
      durationMs,
      outcome: "failure",
      attributes: {
        issueCode: issues[0]?.code ?? "invalid-envelope",
        issuePath: issues[0]?.path ?? "$"
      }
    });

    return {
      action: "dlq",
      reason: "invalid-envelope",
      issues
    };
  }

  const envelope = envelopeResult.value;

  if (envelope.route !== "publication") {
    const issues = [
      {
        path: "$.route",
        code: "stage-mismatch",
        message: `Envelope route ${envelope.route} does not match processor stage publication.`
      }
    ];
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.invalid",
      level: "warn",
      at: runtimeNow(clock),
      stage: "publication",
      ...envelopeTelemetryFields(envelope, queue, durationMs),
      outcome: "failure",
      attributes: {
        issueCode: "stage-mismatch",
        issuePath: "$.route"
      }
    });

    return terminalFailureResult(envelope, "stage-mismatch", issues);
  }

  const reason = error instanceof PublicationInboxStoreError
    ? error.telemetryReason
    : "processor-error";
  const result = retryOrDlqResult(envelope, reason);
  const destination = result.destination.name;
  const event: RuntimeTelemetryEvent = result.action === "retry"
    ? {
        name: "runtime.message.retry",
        level: "warn",
        at: runtimeNow(clock),
        stage: "publication",
        ...envelopeTelemetryFields(envelope, queue, durationMs),
        outcome: "retry",
        attributes: {
          reason,
          destination
        }
      }
    : {
        name: "runtime.message.dlq",
        level: "error",
        at: runtimeNow(clock),
        stage: "publication",
        ...envelopeTelemetryFields(envelope, queue, durationMs),
        outcome: "dlq",
        attributes: {
          reason,
          destination
        }
      };
  await emitRuntimeTelemetry(telemetry, event);

  return result;
}

function retryOrDlqResult(envelope: WorkerMessageEnvelope, reason: string) {
  const destination = getRetryDestination(envelope.route, envelope.attempt.count);

  return "ttlMs" in destination
    ? {
        action: "retry",
        reason,
        envelope,
        destination
      } as const
    : {
        action: "dlq",
        reason,
        envelope,
        destination
      } as const;
}

function terminalFailureResult(
  envelope: WorkerMessageEnvelope,
  reason: string,
  issues: readonly { readonly path: string; readonly code: string; readonly message: string }[]
): RuntimeMessageProcessingResult {
  const destination = getRetryDestination(envelope.route, envelope.attempt.max);

  return "routingKey" in destination && !("ttlMs" in destination)
    ? {
        action: "dlq",
        reason,
        envelope,
        destination,
        issues
      }
    : {
        action: "dlq",
        reason,
        envelope,
        issues
      };
}

function envelopeTelemetryFields(
  envelope: WorkerMessageEnvelope,
  queue: string,
  durationMs: number
): Readonly<Record<string, string | number>> {
  const base = {
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    causationId: envelope.causationId,
    traceparent: envelope.traceparent,
    idempotencyKey: envelope.idempotencyKey,
    queue,
    attempt: envelope.attempt.count,
    durationMs
  } as const;

  return envelope.tracestate === undefined
    ? base
    : {
        ...base,
        tracestate: envelope.tracestate
      };
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
