import {
  createPrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySinkOptions,
  type RuntimeServiceIdentity,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

export const PUBLICATION_STAGE_LATENCY_BUCKETS_SECONDS = [
  0.005,
  0.01,
  0.025,
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2.5,
  5,
  10,
  30,
  60,
  120,
  300
] as const;

export const PUBLICATION_STAGE_OUTCOMES = [
  "success",
  "duplicate",
  "invalid",
  "retry",
  "dlq",
  "failure"
] as const;

export type PublicationStageOutcome = (typeof PUBLICATION_STAGE_OUTCOMES)[number];

export interface PublicationMetricIdentity extends RuntimeServiceIdentity {
  readonly revision?: string;
  readonly deployment?: "local" | "test" | "shadow" | "production" | "unknown";
  readonly adapter?: "in_memory" | "mixed" | "production" | "unknown";
}

export interface PublicationPrometheusTelemetrySinkOptions extends Omit<PrometheusRuntimeTelemetrySinkOptions, "identity"> {
  readonly identity: PublicationMetricIdentity;
  readonly expectedActive?: boolean;
}

export interface PublicationMetricsSink extends RuntimeTelemetrySink {
  readonly allowedLabels: PrometheusRuntimeTelemetrySink["allowedLabels"];
  collect(): string;
  setInFlight(queue: string, value: number): void;
  setShutdownDraining(draining: boolean): void;
}

export type PublicationPrometheusTelemetrySink = PublicationMetricsSink;

interface HistogramState {
  readonly buckets: number[];
  count: number;
  sum: number;
}

const PUBLICATION_STAGE_SERVICE = "publication";
const PUBLICATION_MAIN_QUEUE = "nutsnews.worker.publication.v1";
const PUBLICATION_HEALTH_CHECKS = [
  "process",
  "service-started",
  "broker-lifecycle",
  "rabbitmq-consumer",
  "publication-inbox",
  "publication-database",
  "readiness-policy",
  "snapshot-publisher",
  "feature-flag",
  "broker-outbox",
  "database-write-scope",
  "publication-write-mode"
] as const;
const MAX_LABEL_LENGTH = 96;

export function createPublicationPrometheusTelemetrySink(
  options: PublicationPrometheusTelemetrySinkOptions
): PublicationPrometheusTelemetrySink {
  const runtime = createPrometheusRuntimeTelemetrySink({
    identity: options.identity,
    ...(options.defaultQueue === undefined ? {} : {
      defaultQueue: options.defaultQueue
    }),
    cardinality: {
      ...(options.cardinality?.dependencies === undefined ? {} : {
        dependencies: options.cardinality.dependencies
      }),
      healthChecks: options.cardinality?.healthChecks ?? PUBLICATION_HEALTH_CHECKS
    },
    expectedActive: options.expectedActive === true
  });
  const environment = metricLabelValue(options.identity.environment);
  const stageCounters = new Map<PublicationStageOutcome, number>();
  const histogram: HistogramState = {
    buckets: PUBLICATION_STAGE_LATENCY_BUCKETS_SECONDS.map(() => 0),
    count: 0,
    sum: 0
  };
  return {
    allowedLabels: runtime.allowedLabels,
    async emit(event: RuntimeTelemetryEvent): Promise<void> {
      const outcome = publicationStageOutcome(event);

      if (outcome !== undefined) {
        stageCounters.set(outcome, (stageCounters.get(outcome) ?? 0) + 1);
        const durationSeconds = durationSecondsFrom(event.durationMs);

        if (durationSeconds !== undefined) {
          observeHistogram(histogram, durationSeconds);
        }
      }

      if (shouldForwardToRuntime(event)) {
        try {
          await runtime.emit(event);
          await emitRuntimeHealthTransition(runtime, event);
        } catch {
          // The compatibility sink is best effort and cannot alter worker semantics.
        }
      }

      if (event.name === "runtime.message.accepted" || event.name === "runtime.message.duplicate") {
        const timestampMs = Date.parse(event.at);

        if (Number.isFinite(timestampMs)) {
          runBestEffort(() => runtime.setLastSuccessTimestamp(timestampMs / 1_000));
        }
      }
    },
    collect(): string {
      const runtimeOutput = collectRuntimeMetrics(runtime);

      return `${[
        runtimeOutput,
        collectStageMetrics(environment, stageCounters, histogram)
      ].filter((output) => output.length > 0).join("\n")}\n`;
    },
    setInFlight(queue, value): void {
      runBestEffort(() => runtime.setInFlight(queue, value));
    },
    setShutdownDraining(draining): void {
      runBestEffort(() => runtime.setShutdownDraining(draining));
    }
  };
}

async function emitRuntimeHealthTransition(
  runtime: PrometheusRuntimeTelemetrySink,
  event: RuntimeTelemetryEvent
): Promise<void> {
  if (event.name === "runtime.broker.consumer_state_changed") {
    const active = event.outcome === "active";

    await runtime.emit(transitionHealthEvent({
      at: event.at,
      probe: "readiness",
      outcome: active ? "degraded" : "unhealthy",
      check: "rabbitmq-consumer",
      checkStatus: active ? "ok" : "unhealthy"
    }));
    return;
  }

  if (event.name !== "runtime.broker.state_changed") {
    return;
  }

  const state = typeof event.attributes?.state === "string" ? event.attributes.state : "unknown";
  const unavailable = state === "failed" || state === "closing" || state === "closed";
  const checkStatus = state === "ready" ? "ok" : unavailable ? "unhealthy" : "degraded";
  const outcome = unavailable ? "unhealthy" : "degraded";

  await runtime.emit(transitionHealthEvent({
    at: event.at,
    probe: "startup",
    outcome,
    check: "broker-lifecycle",
    checkStatus
  }));
  await runtime.emit(transitionHealthEvent({
    at: event.at,
    probe: "readiness",
    outcome,
    check: "broker-lifecycle",
    checkStatus
  }));
}

function transitionHealthEvent(input: {
  readonly at: string;
  readonly probe: "startup" | "readiness";
  readonly outcome: "degraded" | "unhealthy";
  readonly check: "broker-lifecycle" | "rabbitmq-consumer";
  readonly checkStatus: "ok" | "degraded" | "unhealthy";
}): RuntimeTelemetryEvent {
  return {
    name: "runtime.health.evaluated",
    level: input.outcome === "unhealthy" ? "error" : "warn",
    at: input.at,
    outcome: input.outcome,
    attributes: {
      probe: input.probe,
      status: input.outcome,
      checkCount: 1,
      transitionDerived: true,
      checks: [
        {
          name: input.check,
          status: input.checkStatus,
          critical: true,
          durationMs: 0
        }
      ]
    }
  };
}

function shouldForwardToRuntime(event: RuntimeTelemetryEvent): boolean {
  if (event.name !== "runtime.dependency.observed") {
    return true;
  }

  const attributeDuration = event.attributes?.durationMs;

  return (event.durationMs !== undefined && Number.isFinite(event.durationMs))
    || (typeof attributeDuration === "number" && Number.isFinite(attributeDuration));
}

function collectRuntimeMetrics(runtime: PrometheusRuntimeTelemetrySink): string {
  try {
    return runtime.collect().trimEnd();
  } catch {
    return "";
  }
}

function runBestEffort(operation: () => unknown): void {
  try {
    const result = operation();

    if (isPromiseLike(result)) {
      void result.then(undefined, () => undefined);
    }
  } catch {
    // The compatibility sink is best effort and cannot alter worker semantics.
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return typeof value === "object" && value !== null && "then" in value && typeof value.then === "function";
}

function publicationStageOutcome(event: RuntimeTelemetryEvent): PublicationStageOutcome | undefined {
  if (event.stage !== PUBLICATION_STAGE_SERVICE || event.queue !== PUBLICATION_MAIN_QUEUE) {
    return undefined;
  }

  switch (event.name) {
    case "runtime.message.accepted":
      return "success";
    case "runtime.message.duplicate":
      return "duplicate";
    case "runtime.message.invalid":
      return "invalid";
    case "runtime.message.retry":
      return "retry";
    case "runtime.message.dlq":
      return "dlq";
    case "runtime.broker.consumer_state_changed":
    case "runtime.broker.state_changed":
    case "runtime.broker.topology_asserted":
    case "runtime.dependency.observed":
    case "runtime.health.evaluated":
    case "runtime.message.started":
    case "runtime.shutdown.completed":
    case "runtime.shutdown.failed":
    case "runtime.shutdown.started":
      return undefined;
  }
}

function observeHistogram(histogram: HistogramState, durationSeconds: number): void {
  histogram.count += 1;
  histogram.sum += durationSeconds;

  for (const [index, boundary] of PUBLICATION_STAGE_LATENCY_BUCKETS_SECONDS.entries()) {
    if (durationSeconds <= boundary) {
      histogram.buckets[index] = (histogram.buckets[index] ?? 0) + 1;
    }
  }
}

function collectStageMetrics(
  environment: string,
  stageCounters: ReadonlyMap<PublicationStageOutcome, number>,
  histogram: HistogramState
): string {
  const lines = [
    "# HELP nutsnews_worker_uplift_stage_events_total Completed worker-uplift stage deliveries by bounded service and outcome.",
    "# TYPE nutsnews_worker_uplift_stage_events_total counter"
  ];

  for (const outcome of PUBLICATION_STAGE_OUTCOMES) {
    const count = stageCounters.get(outcome) ?? 0;

    lines.push(`nutsnews_worker_uplift_stage_events_total${labels({
      environment,
      service: PUBLICATION_STAGE_SERVICE,
      outcome
    })} ${formatMetricNumber(count)}`);
  }

  lines.push(
    "# HELP nutsnews_worker_uplift_stage_latency_seconds Worker-uplift stage completion latency in seconds.",
    "# TYPE nutsnews_worker_uplift_stage_latency_seconds histogram"
  );

  for (const [index, boundary] of PUBLICATION_STAGE_LATENCY_BUCKETS_SECONDS.entries()) {
    lines.push(`nutsnews_worker_uplift_stage_latency_seconds_bucket${labels({
      environment,
      service: PUBLICATION_STAGE_SERVICE,
      le: String(boundary)
    })} ${formatMetricNumber(histogram.buckets[index] ?? 0)}`);
  }

  lines.push(
    `nutsnews_worker_uplift_stage_latency_seconds_bucket${labels({
      environment,
      service: PUBLICATION_STAGE_SERVICE,
      le: "+Inf"
    })} ${formatMetricNumber(histogram.count)}`,
    `nutsnews_worker_uplift_stage_latency_seconds_sum${labels({
      environment,
      service: PUBLICATION_STAGE_SERVICE
    })} ${formatMetricNumber(histogram.sum)}`,
    `nutsnews_worker_uplift_stage_latency_seconds_count${labels({
      environment,
      service: PUBLICATION_STAGE_SERVICE
    })} ${formatMetricNumber(histogram.count)}`
  );

  return lines.join("\n");
}

function labels(values: Readonly<Record<string, string>>): string {
  return `{${Object.entries(values).map(([name, value]) => `${name}="${escapeLabelValue(value)}"`).join(",")}}`;
}

function metricLabelValue(value: string): string {
  const cleaned = value
    .trim()
    .replace(/[^A-Za-z0-9_.:-]+/gu, "_")
    .slice(0, MAX_LABEL_LENGTH);

  return cleaned.length > 0 ? cleaned : "unknown";
}

function escapeLabelValue(value: string): string {
  return value
    .replace(/\\/gu, "\\\\")
    .replace(/"/gu, "\\\"");
}

function durationSecondsFrom(value: number | undefined): number | undefined {
  return value !== undefined && Number.isFinite(value) ? Math.max(0, value) / 1_000 : undefined;
}

function formatMetricNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(6)));
}
