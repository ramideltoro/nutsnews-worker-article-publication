import {
  WORKER_DELIVERY_BEHAVIOR,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  RUNTIME_ALLOWED_METRIC_LABELS,
  RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS,
  createBufferedRuntimeTelemetrySink,
  type RuntimeHandlerResult,
  type RuntimeMessageDelivery,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadPublicationConfig } from "../src/config.js";
import type {
  PublicationInboxStore,
  PublicationWorkHandler
} from "../src/dependencies.js";
import {
  createPublicationPrometheusTelemetrySink,
  type PublicationPrometheusTelemetrySink
} from "../src/metrics.js";
import { createPublicationService } from "../src/service.js";
import {
  LocalPublicationBrokerTransport,
  ManualPublicationClock,
  createLocalPublicationDependencies,
  createMinimalPublicationDelivery,
  createMinimalPublicationEnvelope,
  createMinimalPublicationPayload
} from "../src/test-doubles.js";

const COMPLETING_MESSAGE_EVENTS = new Set([
  "runtime.message.accepted",
  "runtime.message.duplicate",
  "runtime.message.invalid",
  "runtime.message.retry",
  "runtime.message.dlq"
]);

describe("publication lifecycle telemetry", () => {
  it("emits exactly one completion per delivery and preserves the terminal-success SLO contract", async () => {
    const context = createTelemetryContext();

    const initialMetrics = context.metrics.collect();
    expectHealthOneHot(initialMetrics, "liveness", "ok");
    expectHealthOneHot(initialMetrics, "startup", "unhealthy");
    expectHealthOneHot(initialMetrics, "readiness", "unhealthy");

    await expect(context.service.health.liveness()).resolves.toMatchObject({
      probe: "liveness",
      status: "ok"
    });
    await expect(context.service.health.startup()).resolves.toMatchObject({
      probe: "startup",
      status: "unhealthy"
    });
    const readinessBeforeStart = await context.service.health.readiness();
    expect(readinessBeforeStart).toMatchObject({
      probe: "readiness",
      status: "unhealthy"
    });
    expect(readinessBeforeStart.checks.find((check) => check.name === "rabbitmq-consumer")).toMatchObject({
      name: "rabbitmq-consumer",
      status: "unhealthy",
      details: {
        activeConsumers: 0
      }
    });

    await context.service.start();
    const startedMetrics = context.metrics.collect();
    expectHealthOneHot(startedMetrics, "liveness", "ok");
    expectHealthOneHot(startedMetrics, "startup", "ok");
    expectHealthOneHot(startedMetrics, "readiness", "unhealthy");
    expect(startedMetrics).not.toContain("nutsnews_worker_dependency_duration_ms");

    await expect(context.service.health.startup()).resolves.toMatchObject({
      probe: "startup",
      status: "ok"
    });
    const readinessAfterStart = await context.service.health.readiness();
    expect(readinessAfterStart).toMatchObject({
      probe: "readiness",
      status: "ok"
    });
    expect(readinessAfterStart.checks.find((check) => check.name === "rabbitmq-consumer")).toMatchObject({
      name: "rabbitmq-consumer",
      status: "ok",
      details: {
        activeConsumers: 1
      }
    });
    context.telemetry.clear();
    await exerciseLifecycleOutcomes(context);

    const messageEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    const started = messageEvents.filter((event) => event.name === "runtime.message.started");
    const completed = messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name));

    expect(started).toHaveLength(6);
    expect(completed).toHaveLength(started.length);
    expect(completed.map((event) => event.name)).toEqual([
      "runtime.message.accepted",
      "runtime.message.duplicate",
      "runtime.message.invalid",
      "runtime.message.retry",
      "runtime.message.dlq",
      "runtime.message.dlq"
    ]);

    const metrics = context.metrics.collect();
    expect(metricValue(metrics, "nutsnews_worker_uplift_stage_events_total", "success")).toBe(1);
    expect(metricValue(metrics, "nutsnews_worker_uplift_stage_events_total", "duplicate")).toBe(1);
    expect(metricValue(metrics, "nutsnews_worker_uplift_stage_events_total", "invalid")).toBe(1);
    expect(metricValue(metrics, "nutsnews_worker_uplift_stage_events_total", "retry")).toBe(1);
    expect(metricValue(metrics, "nutsnews_worker_uplift_stage_events_total", "dlq")).toBe(2);
    expect(sampleValue(metrics, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "+Inf"
    })).toBe(6);
    expect(sampleValue(metrics, "nutsnews_worker_uplift_stage_latency_seconds_sum")).toBe(1);
    expect(sampleValue(metrics, "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(6);
    expect(metrics).toContain('nutsnews_worker_expected_active{environment="test",service="publication"} 0');
    expect(metrics).not.toContain("nutsnews_worker_dependency_duration_ms");
    expect(context.metrics.allowedLabels).toEqual(RUNTIME_ALLOWED_METRIC_LABELS);

    for (const forbidden of RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS) {
      expect(metrics).not.toContain(`${forbidden}=`);
    }

    for (const identifier of [
      messageId(1),
      idempotencyKey(1),
      "article-001",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b4611",
      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    ]) {
      expect(metrics).not.toContain(identifier);
    }

    await context.service.stop();
    const stoppedMetrics = context.metrics.collect();
    expectHealthOneHot(stoppedMetrics, "liveness", "ok");
    expectHealthOneHot(stoppedMetrics, "startup", "unhealthy");
    expectHealthOneHot(stoppedMetrics, "readiness", "unhealthy");
  });

  it("keeps rejecting telemetry and metrics best-effort without changing exact-one lifecycle outcomes", async () => {
    const config = loadPublicationConfig({
      HOSTNAME: "publication-rejecting-telemetry-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_PUBLICATION_HTTP_PORT: "0",
      NUTSNEWS_PUBLICATION_TELEMETRY_LOGS: "silent"
    });
    const localDependencies = createLocalPublicationDependencies(config);
    const clock = localDependencies.clock as ManualPublicationClock;
    const events: RuntimeTelemetryEvent[] = [];
    const rejectingSink: PublicationPrometheusTelemetrySink = {
      allowedLabels: RUNTIME_ALLOWED_METRIC_LABELS,
      emit: (event) => {
        events.push(event);
        return Promise.reject(new Error("telemetry unavailable"));
      },
      collect: () => {
        throw new Error("metrics unavailable");
      },
      setInFlight: () => {
        throw new Error("metrics unavailable");
      },
      setShutdownDraining: () => {
        throw new Error("metrics unavailable");
      },
      setHealthProbe: () => {
        throw new Error("metrics unavailable");
      }
    };
    let handlerResult: RuntimeHandlerResult = {
      status: "ok"
    };
    const dependencies = {
      ...localDependencies,
      workHandler: {
        name: "rejecting-telemetry-publication-handler",
        handle: () => {
          clock.advance(250);
          return Promise.resolve(handlerResult);
        }
      } satisfies PublicationWorkHandler
    };
    const service = createPublicationService({
      config,
      dependencies,
      telemetry: rejectingSink,
      metrics: rejectingSink
    });
    const context: LifecycleContext = {
      broker: dependencies.brokerTransport as LocalPublicationBrokerTransport,
      setHandlerResult(value): void {
        handlerResult = value;
      }
    };

    await expect(service.start()).resolves.toBeUndefined();
    events.length = 0;
    await exerciseLifecycleOutcomes(context);

    const messageEvents = events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.accepted",
      "runtime.message.started",
      "runtime.message.duplicate",
      "runtime.message.started",
      "runtime.message.invalid",
      "runtime.message.started",
      "runtime.message.retry",
      "runtime.message.started",
      "runtime.message.dlq",
      "runtime.message.started",
      "runtime.message.dlq"
    ]);
    expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(6);
    await expect(service.stop()).resolves.toBeUndefined();
  });

  it.each([
    {
      operation: "claim" as const,
      expectedReason: "idempotency-claim-error"
    },
    {
      operation: "markCompleted" as const,
      expectedReason: "handler-error"
    },
    {
      operation: "markFailed" as const,
      expectedReason: "idempotency-failure-record-error"
    }
  ])("contains Runtime 0.5 $operation rejection as one completed retry", async ({
    operation,
    expectedReason
  }) => {
    const context = createInboxFailureContext(operation);

    await expect(context.service.processDelivery(publicationDelivery(20))).resolves.toMatchObject({
      action: "retry",
      reason: expectedReason
    });

    expectExactOneLifecycleCompletion(context.telemetry.events, "runtime.message.retry");
    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_inflight", {
      outcome: "in_flight"
    })).toBe(0);
    expect(metricValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_events_total", "retry")).toBe(1);
    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "+Inf"
    })).toBe(1);
  });

  it("contains an exhausted Runtime 0.5 inbox rejection as one completed DLQ outcome", async () => {
    const context = createInboxFailureContext("claim");
    const delivery = publicationDelivery(21, {
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-07-23T00:00:00.000Z",
        lastAttemptAt: "2026-07-23T00:05:00.000Z"
      }
    });

    await expect(context.service.processDelivery(delivery)).resolves.toMatchObject({
      action: "dlq",
      reason: "idempotency-claim-error"
    });

    expectExactOneLifecycleCompletion(context.telemetry.events, "runtime.message.dlq");
    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_inflight", {
      outcome: "in_flight"
    })).toBe(0);
    expect(metricValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_events_total", "dlq")).toBe(1);
  });
});

interface LifecycleContext {
  readonly broker: LocalPublicationBrokerTransport;
  setHandlerResult(value: RuntimeHandlerResult): void;
}

async function exerciseLifecycleOutcomes(context: LifecycleContext): Promise<void> {
  const accepted = publicationDelivery(1);
  await expect(context.broker.deliverPublication(accepted)).resolves.toMatchObject({
    action: "ack",
    reason: "handled"
  });
  await expect(context.broker.deliverPublication(accepted)).resolves.toMatchObject({
    action: "ack",
    reason: "duplicate"
  });

  await expect(context.broker.deliverPublication({
    ...publicationDelivery(2),
    envelope: createMinimalPublicationEnvelope({
      messageId: messageId(2),
      route: "persistence",
      idempotencyKey: idempotencyKey(2)
    })
  })).resolves.toMatchObject({
    action: "dlq",
    reason: "stage-mismatch"
  });

  context.setHandlerResult({
    status: "retry",
    reason: "transient-publication-error",
    retryAfterMs: 2_000
  });
  await expect(context.broker.deliverPublication(publicationDelivery(3))).resolves.toMatchObject({
    action: "retry",
    reason: "transient-publication-error"
  });

  context.setHandlerResult({
    status: "retry",
    reason: "retry-exhausted"
  });
  await expect(context.broker.deliverPublication(publicationDelivery(4, {
    attempt: {
      count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: "2026-07-23T00:00:00.000Z",
      lastAttemptAt: "2026-07-23T00:05:00.000Z"
    }
  }))).resolves.toMatchObject({
    action: "dlq",
    reason: "retry-exhausted"
  });

  context.setHandlerResult({
    status: "terminal-failure",
    reason: "terminal-publication-error"
  });
  await expect(context.broker.deliverPublication(publicationDelivery(5))).resolves.toMatchObject({
    action: "dlq",
    reason: "terminal-publication-error"
  });
}

function createTelemetryContext() {
  const config = loadPublicationConfig({
    HOSTNAME: "publication-test",
    NUTSNEWS_ENVIRONMENT: "test",
    NUTSNEWS_PUBLICATION_HTTP_PORT: "0",
    NUTSNEWS_PUBLICATION_TELEMETRY_LOGS: "silent"
  });
  const localDependencies = createLocalPublicationDependencies(config);
  const clock = localDependencies.clock as ManualPublicationClock;
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createPublicationPrometheusTelemetrySink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    },
    expectedActive: false
  });
  let handlerResult: RuntimeHandlerResult = {
    status: "ok"
  };
  const workHandler: PublicationWorkHandler = {
    name: "controllable-publication-handler",
    handle: () => {
      clock.advance(250);
      return Promise.resolve(handlerResult);
    }
  };
  const dependencies = {
    ...localDependencies,
    workHandler
  };
  const combined: RuntimeTelemetrySink = {
    emit: async (event) => {
      await telemetry.emit(event);
      await metrics.emit(event);
    }
  };
  const service = createPublicationService({
    config,
    dependencies,
    telemetry: combined,
    metrics
  });

  return {
    broker: dependencies.brokerTransport as LocalPublicationBrokerTransport,
    metrics,
    service,
    telemetry,
    setHandlerResult(value: RuntimeHandlerResult): void {
      handlerResult = value;
    }
  };
}

function createInboxFailureContext(operation: "claim" | "markCompleted" | "markFailed") {
  const config = loadPublicationConfig({
    HOSTNAME: "publication-inbox-failure-test",
    NUTSNEWS_ENVIRONMENT: "test",
    NUTSNEWS_PUBLICATION_HTTP_PORT: "0",
    NUTSNEWS_PUBLICATION_TELEMETRY_LOGS: "silent"
  });
  const localDependencies = createLocalPublicationDependencies(config);
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createPublicationPrometheusTelemetrySink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    },
    expectedActive: false
  });
  const combined: RuntimeTelemetrySink = {
    emit: async (event) => {
      await telemetry.emit(event);
      await metrics.emit(event);
    }
  };
  const inboxStore = rejectingInboxStore(localDependencies.inboxStore, operation);
  const workHandler: PublicationWorkHandler = {
    name: "inbox-failure-publication-handler",
    handle: () => Promise.resolve(operation === "markFailed"
      ? {
          status: "retry",
          reason: "controlled-handler-retry"
        }
      : {
          status: "ok"
        })
  };
  const service = createPublicationService({
    config,
    dependencies: {
      ...localDependencies,
      inboxStore,
      workHandler
    },
    telemetry: combined,
    metrics
  });

  return {
    metrics,
    service,
    telemetry
  };
}

function rejectingInboxStore(
  store: PublicationInboxStore,
  operation: "claim" | "markCompleted" | "markFailed"
): PublicationInboxStore {
  const reject = () => Promise.reject(new Error(`simulated ${operation} rejection`));

  return {
    name: `rejecting-${operation}-publication-inbox`,
    probe: () => store.probe(),
    claim: (idempotencyKey, context) => operation === "claim"
      ? reject()
      : store.claim(idempotencyKey, context),
    markCompleted: (idempotencyKey, completion) => operation === "markCompleted"
      ? reject()
      : store.markCompleted(idempotencyKey, completion),
    markFailed: (idempotencyKey, failure) => operation === "markFailed"
      ? reject()
      : store.markFailed(idempotencyKey, failure)
  };
}

function expectExactOneLifecycleCompletion(
  events: readonly RuntimeTelemetryEvent[],
  expectedCompletion: RuntimeTelemetryEvent["name"]
): void {
  const messageEvents = events.filter((event) => event.name.startsWith("runtime.message."));

  expect(messageEvents.filter((event) => event.name === "runtime.message.started")).toHaveLength(1);
  expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(1);
  expect(messageEvents.at(-1)?.name).toBe(expectedCompletion);
}

function publicationDelivery(
  sequence: number,
  envelopeOverrides: Partial<WorkerMessageEnvelope> = {}
): RuntimeMessageDelivery {
  const key = idempotencyKey(sequence);

  return {
    ...createMinimalPublicationDelivery(),
    envelope: createMinimalPublicationEnvelope({
      messageId: messageId(sequence),
      idempotencyKey: key,
      ...envelopeOverrides
    }),
    payload: createMinimalPublicationPayload({
      idempotencyKey: key
    })
  };
}

function messageId(sequence: number): string {
  return `018f1598-2dd5-7c4f-9f92-8f7a7f8b48${String(sequence).padStart(2, "0")}`;
}

function idempotencyKey(sequence: number): string {
  return `persistence:publication:telemetry-${String(sequence)}`;
}

function metricValue(output: string, metric: string, outcome: string): number {
  return sampleValue(output, metric, {
    outcome
  });
}

function sampleValue(
  output: string,
  metric: string,
  requiredLabels: Readonly<Record<string, string>> = {}
): number {
  const matches = output
    .split("\n")
    .filter((line) => line.startsWith(`${metric}{`) && Object.entries(requiredLabels).every(([name, value]) => line.includes(`${name}="${value}"`)));

  expect(matches).toHaveLength(1);
  return Number(matches[0]?.split(" ").at(-1));
}

function expectHealthOneHot(
  output: string,
  probe: "liveness" | "startup" | "readiness",
  expected: "ok" | "degraded" | "unhealthy"
): void {
  const outcomes = [
    "ok",
    "degraded",
    "unhealthy"
  ] as const;
  const values = outcomes.map((outcome) => sampleValue(output, "nutsnews_worker_health_probe", {
    probe,
    outcome
  }));

  expect(values.reduce((sum, value) => sum + value, 0)).toBe(1);
  expect(values[outcomes.indexOf(expected)]).toBe(1);
}
