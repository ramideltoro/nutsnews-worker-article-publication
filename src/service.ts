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
import type { PublicationMetricsSink } from "./metrics.js";
import type {
  PublicationDependencies,
  PublicationDependencyProbe,
  PublicationInboxStore
} from "./dependencies.js";
import {
  PublicationOperationDeadlineError,
  createPublicationOperationDeadline,
  type PublicationOperationDeadline
} from "./operation-deadline.js";

export interface PublicationServiceOptions {
  readonly config: PublicationConfig;
  readonly dependencies: PublicationDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: PublicationMetricsSink;
  readonly operationDeadlineFactory?: () => PublicationOperationDeadline;
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
  const createSharedProcessor = (deadlineRef: PublicationOperationDeadlineRef) => createRuntimeMessageProcessor({
    stage: "publication",
    idempotencyStore: classifyInboxStoreFailures(
      options.dependencies.inboxStore,
      () => deadlineRef.current
    ),
    clock: options.dependencies.clock,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    handler: async (context) => {
      const operationDeadline = (options.operationDeadlineFactory ?? createPublicationOperationDeadline)();
      deadlineRef.current = operationDeadline;

      try {
        return await drain.track(async () => {
          operationDeadline.assertActive();
          setInFlight(options.metrics, publicationRoute.mainQueue.name, drain.inFlight);
          const result = await options.dependencies.workHandler.handle(context, {
            assertActive: () => operationDeadline.assertActive(),
            publishBroker: (command) => guardedPublicationOperation(
              operationDeadline,
              () => broker.publish(command)
            ),
            recordBrokerOutbox: (command, receipt) => guardedPublicationOperation(
              operationDeadline,
              () => options.dependencies.brokerOutbox.record(command, receipt, operationDeadline)
            ),
            withTransaction: (operation) => guardedPublicationOperation(
              operationDeadline,
              () => options.dependencies.database.withTransaction(
                (transaction) => guardedPublicationOperation(
                  operationDeadline,
                  () => operation(transaction)
                ),
                operationDeadline
              )
            ),
            recordReadinessEvaluation: (transaction, record) => guardedPublicationOperation(
              operationDeadline,
              () => options.dependencies.database.recordReadinessEvaluation(
                transaction,
                record,
                operationDeadline
              )
            ),
            publishShadowComparison: (command) => guardedPublicationOperation(
              operationDeadline,
              () => Promise.resolve(options.dependencies.snapshotPublisher.publishShadowComparison(
                command,
                operationDeadline
              ))
            ),
            publishProductionSnapshot: (command) => guardedPublicationOperation(
              operationDeadline,
              () => Promise.resolve(options.dependencies.snapshotPublisher.publishProductionSnapshot(
                command,
                operationDeadline
              ))
            )
          });
          operationDeadline.assertActive();

          await deadlineBoundTelemetry(operationDeadline, emitRuntimeTelemetry(telemetry, {
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
          }));
          operationDeadline.assertActive();

          return result;
        });
      } finally {
        setInFlight(options.metrics, publicationRoute.mainQueue.name, drain.inFlight);
      }
    }
  });
  const processor = async (delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> => {
    const startedAtMs = options.dependencies.clock.now().getTime();
    const deadlineRef: PublicationOperationDeadlineRef = {};
    const sharedProcessor = createSharedProcessor(deadlineRef);

    try {
      return await sharedProcessor(delivery);
    } catch (error: unknown) {
      return await completeProcessorFailure(
        delivery,
        error,
        telemetry,
        options.dependencies.clock,
        startedAtMs
      );
    } finally {
      deadlineRef.current?.dispose();
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

      return observeHealthProbes(probes);
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
          await service.health.readiness();
        }
      };
      started = true;
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
      await service.health.startup();
      await service.health.readiness();
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
      consumer = undefined;
      started = false;
      await service.health.startup();
      await service.health.readiness();
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies PublicationService;

  return service;
}

interface PublicationOperationDeadlineRef {
  current?: PublicationOperationDeadline;
}

async function guardedPublicationOperation<T>(
  deadline: PublicationOperationDeadline,
  operation: () => Promise<T>
): Promise<T> {
  deadline.assertActive();
  const value = await operation();
  deadline.assertActive();
  return value;
}

async function deadlineBoundTelemetry(
  deadline: PublicationOperationDeadline,
  operation: Promise<void>
): Promise<void> {
  deadline.assertActive();

  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const onAbort = (): void => {
      if (!settled) {
        settled = true;
        reject(deadline.signal.reason instanceof Error
          ? deadline.signal.reason
          : new PublicationOperationDeadlineError());
      }
    };

    deadline.signal.addEventListener("abort", onAbort, {
      once: true
    });
    if (deadline.signal.aborted) {
      onAbort();
    }
    void operation.then(
      () => {
        if (!settled) {
          settled = true;
          deadline.signal.removeEventListener("abort", onAbort);
          resolve();
        }
      },
      () => {
        if (!settled) {
          settled = true;
          deadline.signal.removeEventListener("abort", onAbort);
          resolve();
        }
      }
    );
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
  probes: RuntimeHealthProbeSet
): RuntimeHealthProbeSet {
  const observe = async <T extends RuntimeHealthReport>(
    operation: () => Promise<T>
  ): Promise<T> => {
    return operation();
  };

  return {
    liveness: () => observe(() => probes.liveness()),
    startup: () => observe(() => probes.startup()),
    readiness: () => observe(() => probes.readiness())
  };
}

class PublicationInboxStoreError extends Error {
  readonly telemetryReason: string;

  constructor(telemetryReason: string) {
    super(telemetryReason);
    this.name = "PublicationInboxStoreError";
    this.telemetryReason = telemetryReason;
  }
}

function classifyInboxStoreFailures(
  store: PublicationInboxStore,
  getDeadline: () => PublicationOperationDeadline | undefined
): RuntimeIdempotencyStore {
  return {
    claim: async (idempotencyKey, context) => inboxStoreOperation(
      "idempotency-claim-error",
      () => store.claim(idempotencyKey, context)
    ),
    markCompleted: async (idempotencyKey, completion) => inboxStoreOperation(
      "idempotency-completion-error",
      () => guardedPublicationInboxOperation(
        getDeadline(),
        (deadline) => store.markCompleted(idempotencyKey, completion, deadline)
      )
    ),
    markFailed: async (idempotencyKey, failure) => inboxStoreOperation(
      "idempotency-failure-record-error",
      () => guardedPublicationInboxOperation(
        getDeadline(),
        (deadline) => store.markFailed(idempotencyKey, failure, deadline)
      )
    ),
    releaseClaim: async (idempotencyKey, failure) => inboxStoreOperation(
      "idempotency-release-error",
      () => guardedPublicationInboxOperation(
        getDeadline(),
        (deadline) => store.releaseClaim(idempotencyKey, failure, deadline)
      )
    )
  };
}

async function guardedPublicationInboxOperation<T>(
  deadline: PublicationOperationDeadline | undefined,
  operation: (deadline: PublicationOperationDeadline | undefined) => Promise<T>
): Promise<T> {
  deadline?.assertActive();
  const value = await operation(deadline);
  deadline?.assertActive();
  return value;
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
