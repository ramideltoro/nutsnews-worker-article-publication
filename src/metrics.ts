import {
  createPrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySinkOptions,
  type RuntimeServiceIdentity,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

export const PUBLICATION_STAGE_LATENCY_BUCKETS_SECONDS = [
  0.01,
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
export type PublicationHealthProbe = (typeof HEALTH_PROBES)[number];
export type PublicationHealthOutcome = (typeof HEALTH_OUTCOMES)[number];

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

export interface PublicationPrometheusTelemetrySink extends PublicationMetricsSink {
  setHealthProbe(probe: PublicationHealthProbe, outcome: PublicationHealthOutcome): void;
}

interface HistogramState {
  readonly buckets: number[];
  count: number;
  sum: number;
}

const PUBLICATION_STAGE_SERVICE = "publication";
const PUBLICATION_MAIN_QUEUE = "nutsnews.worker.publication.v1";
const HEALTH_PROBES = [
  "liveness",
  "startup",
  "readiness"
] as const;
const HEALTH_OUTCOMES = [
  "ok",
  "degraded",
  "unhealthy"
] as const;
const MAX_LABEL_LENGTH = 96;

export function createPublicationPrometheusTelemetrySink(
  options: PublicationPrometheusTelemetrySinkOptions
): PublicationPrometheusTelemetrySink {
  const runtime = createPrometheusRuntimeTelemetrySink({
    identity: options.identity,
    ...(options.defaultQueue === undefined ? {} : {
      defaultQueue: options.defaultQueue
    })
  });
  const environment = metricLabelValue(options.identity.environment);
  const stageCounters = new Map<PublicationStageOutcome, number>();
  const histogram: HistogramState = {
    buckets: PUBLICATION_STAGE_LATENCY_BUCKETS_SECONDS.map(() => 0),
    count: 0,
    sum: 0
  };
  const health = new Map<PublicationHealthProbe, PublicationHealthOutcome>([
    [
      "liveness",
      "ok"
    ],
    [
      "startup",
      "unhealthy"
    ],
    [
      "readiness",
      "unhealthy"
    ]
  ]);

  return {
    allowedLabels: runtime.allowedLabels,
    async emit(event: RuntimeTelemetryEvent): Promise<void> {
      recordHealthEvent(health, event);
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
        } catch {
          // The compatibility sink is best effort and cannot alter worker semantics.
        }
      }
    },
    collect(): string {
      const runtimeOutput = collectRuntimeMetrics(runtime);

      return `${[
        runtimeOutput,
        collectCompatibilityIdentityMetrics(options, runtimeOutput),
        collectOwnershipMetric(environment, options.expectedActive === true, runtimeOutput),
        collectHealthProbeMetrics(environment, health),
        collectStageMetrics(environment, stageCounters, histogram)
      ].filter((output) => output.length > 0).join("\n")}\n`;
    },
    setInFlight(queue, value): void {
      runBestEffort(() => runtime.setInFlight(queue, value));
    },
    setShutdownDraining(draining): void {
      runBestEffort(() => runtime.setShutdownDraining(draining));
    },
    setHealthProbe(probe, outcome): void {
      health.set(probe, outcome);
    }
  };
}

function collectCompatibilityIdentityMetrics(
  options: PublicationPrometheusTelemetrySinkOptions,
  runtimeOutput: string
): string {
  const identity = options.identity;
  const environment = metricLabelValue(identity.environment);
  const service = metricLabelValue(identity.service);
  const lines: string[] = [];

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_build_info")) {
    lines.push(
      "# HELP nutsnews_worker_build_info Immutable worker build identity.",
      "# TYPE nutsnews_worker_build_info gauge",
      `nutsnews_worker_build_info${labels({
        environment,
        service,
        version: metricLabelValue(identity.version),
        revision: metricLabelValue(identity.revision ?? "unknown")
      })} 1`
    );
  }

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_deployment_info")) {
    lines.push(
      "# HELP nutsnews_worker_deployment_info Worker deployment ownership and dependency adapter identity.",
      "# TYPE nutsnews_worker_deployment_info gauge",
      `nutsnews_worker_deployment_info${labels({
        environment,
        service,
        deployment: metricLabelValue(identity.deployment ?? "unknown"),
        adapter: metricLabelValue(identity.adapter ?? "unknown")
      })} 1`
    );
  }

  return lines.join("\n");
}

function hasMetricFamily(output: string, metric: string): boolean {
  return output.split("\n").some((line) => line.startsWith(`# HELP ${metric} `)
    || line.startsWith(`${metric}{`)
    || line.startsWith(`${metric} `));
}

function shouldForwardToRuntime(event: RuntimeTelemetryEvent): boolean {
  if (event.name === "runtime.health.evaluated") {
    return false;
  }

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

function recordHealthEvent(
  health: Map<PublicationHealthProbe, PublicationHealthOutcome>,
  event: RuntimeTelemetryEvent
): void {
  if (event.name !== "runtime.health.evaluated") {
    return;
  }

  const probe = healthProbe(event.attributes?.probe);
  const outcome = healthOutcome(event.outcome ?? event.attributes?.status);

  if (probe !== undefined && outcome !== undefined) {
    health.set(probe, outcome);
  }
}

function healthProbe(value: unknown): PublicationHealthProbe | undefined {
  return typeof value === "string" && HEALTH_PROBES.some((probe) => probe === value)
    ? value as PublicationHealthProbe
    : undefined;
}

function healthOutcome(value: unknown): PublicationHealthOutcome | undefined {
  return typeof value === "string" && HEALTH_OUTCOMES.some((outcome) => outcome === value)
    ? value as PublicationHealthOutcome
    : undefined;
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

function collectOwnershipMetric(
  environment: string,
  expectedActive: boolean,
  runtimeOutput: string
): string {
  if (hasMetricFamily(runtimeOutput, "nutsnews_worker_expected_active")) {
    return "";
  }

  return [
    "# HELP nutsnews_worker_expected_active Whether this worker deployment is expected to own active production work.",
    "# TYPE nutsnews_worker_expected_active gauge",
    `nutsnews_worker_expected_active${labels({
      environment,
      service: PUBLICATION_STAGE_SERVICE
    })} ${expectedActive ? "1" : "0"}`
  ].join("\n");
}

function collectHealthProbeMetrics(
  environment: string,
  health: ReadonlyMap<PublicationHealthProbe, PublicationHealthOutcome>
): string {
  if (health.size === 0) {
    return "";
  }

  const lines = [
    "# HELP nutsnews_worker_health_probe Worker liveness, startup, and readiness state by bounded probe and outcome.",
    "# TYPE nutsnews_worker_health_probe gauge"
  ];

  for (const probe of HEALTH_PROBES) {
    const current = health.get(probe);

    if (current === undefined) {
      continue;
    }

    for (const outcome of HEALTH_OUTCOMES) {
      lines.push(`nutsnews_worker_health_probe${labels({
        environment,
        service: PUBLICATION_STAGE_SERVICE,
        outcome,
        probe
      })} ${current === outcome ? "1" : "0"}`);
    }
  }

  return lines.join("\n");
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
