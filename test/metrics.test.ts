import type { RuntimeTelemetryEvent } from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  PUBLICATION_STAGE_LATENCY_BUCKETS_SECONDS,
  PUBLICATION_STAGE_OUTCOMES,
  createPublicationPrometheusTelemetrySink
} from "../src/metrics.js";

describe("publication Prometheus telemetry", () => {
  it("exports fixed zero-valued canonical stage series before the first delivery", async () => {
    const metrics = createPublicationPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-publication",
        version: "0.1.0",
        environment: "shadow",
        host: "publication-zero-series"
      },
      expectedActive: false
    });
    const before = metrics.collect();
    const beforeStageEvents = metricSeries(before, "nutsnews_worker_uplift_stage_events_total");
    const beforeHistogram = metricSeries(before, "nutsnews_worker_uplift_stage_latency_seconds");

    expect(beforeStageEvents).toHaveLength(PUBLICATION_STAGE_OUTCOMES.length);
    expect(beforeHistogram).toHaveLength(PUBLICATION_STAGE_LATENCY_BUCKETS_SECONDS.length + 3);

    for (const outcome of PUBLICATION_STAGE_OUTCOMES) {
      expect(beforeStageEvents).toContain(`nutsnews_worker_uplift_stage_events_total{environment="shadow",service="publication",outcome="${outcome}"} 0`);
    }

    for (const boundary of PUBLICATION_STAGE_LATENCY_BUCKETS_SECONDS) {
      expect(before).toContain(`nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="shadow",service="publication",le="${String(boundary)}"} 0`);
    }

    expect(before).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="shadow",service="publication",le="+Inf"} 0');
    expect(before).toContain('nutsnews_worker_uplift_stage_latency_seconds_sum{environment="shadow",service="publication"} 0');
    expect(before).toContain('nutsnews_worker_uplift_stage_latency_seconds_count{environment="shadow",service="publication"} 0');

    await metrics.emit(completion("runtime.message.accepted", "success", 5));

    expect(metricSeries(metrics.collect(), "nutsnews_worker_uplift_stage_events_total")).toHaveLength(beforeStageEvents.length);
    expect(metricSeries(metrics.collect(), "nutsnews_worker_uplift_stage_latency_seconds")).toHaveLength(beforeHistogram.length);
  });

  it("records the canonical outcomes and complete fixed-bucket seconds histogram without identifier labels", async () => {
    const metrics = createPublicationPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-publication",
        version: "0.1.0",
        environment: "production",
        host: "backend.nutsnews.com"
      },
      expectedActive: false
    });
    const completions = [
      completion("runtime.message.accepted", "success", 5),
      completion("runtime.message.duplicate", "duplicate", 50),
      completion("runtime.message.invalid", "failure", 30_000),
      completion("runtime.message.retry", "retry", 30_001),
      completion("runtime.message.dlq", "dlq", 301_000)
    ] satisfies readonly RuntimeTelemetryEvent[];

    for (const event of completions) {
      await metrics.emit(started());
      await metrics.emit(event);
    }

    await metrics.emit({
      ...completion("runtime.message.accepted", "success", 1),
      stage: "persistence"
    });
    await metrics.emit({
      ...completion("runtime.message.accepted", "success", 1),
      queue: "nutsnews.worker.publication.v1.retry-30s"
    });

    const output = metrics.collect();
    const stageEvents = output
      .split("\n")
      .filter((line) => line.startsWith("nutsnews_worker_uplift_stage_events_total{"));

    expect(stageEvents).toHaveLength(PUBLICATION_STAGE_OUTCOMES.length);

    for (const outcome of PUBLICATION_STAGE_OUTCOMES) {
      const expected = outcome === "failure" ? "0" : "1";
      expect(stageEvents).toContain(`nutsnews_worker_uplift_stage_events_total{environment="production",service="publication",outcome="${outcome}"} ${expected}`);
    }

    expect(output).toContain('nutsnews_worker_expected_active{environment="production",service="nutsnews-worker-article-publication"} 0');
    expect(metricSeries(output, "nutsnews_worker_last_success_timestamp_seconds")).toEqual([
      'nutsnews_worker_last_success_timestamp_seconds{environment="production",service="nutsnews-worker-article-publication"} 1785456001'
    ]);
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="publication",le="0.005"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="publication",le="0.01"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="publication",le="0.025"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="publication",le="0.05"} 2');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="publication",le="30"} 3');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="publication",le="60"} 4');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="publication",le="300"} 4');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="publication",le="+Inf"} 5');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_count{environment="production",service="publication"} 5');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_sum{environment="production",service="publication"} 361.056');
    expect(output).not.toContain("message-id-cardinality-probe");
    expect(output).not.toContain("idempotency-cardinality-probe");
    expect(output).not.toContain("correlation-cardinality-probe");

    for (const series of stageEvents) {
      expect(metricLabelNames(series)).toEqual([
        "environment",
        "service",
        "outcome"
      ]);
    }
  });

  it("exports distinct bounded health probes and changes ownership only for protected production mode", async () => {
    const shadow = createPublicationPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-publication",
        version: "0.1.0",
        environment: "shadow environment with spaces",
        host: "publication-shadow"
      },
      expectedActive: false
    });

    const initialOutput = shadow.collect();
    expect(initialOutput).not.toContain("nutsnews_worker_health_probe");
    expect(initialOutput).not.toContain("nutsnews_worker_dependency_duration_ms");

    await shadow.emit(health("liveness", "ok"));
    await shadow.emit(health("startup", "ok"));
    await shadow.emit(health("readiness", "unhealthy"));
    await shadow.emit({
      ...health("readiness", "ok"),
      attributes: {
        probe: "unbounded-probe",
        status: "ok"
      }
    });

    const shadowOutput = shadow.collect();
    expect(shadowOutput).toContain('nutsnews_worker_expected_active{environment="shadow_environment_with_spaces",service="nutsnews-worker-article-publication"} 0');
    expectHealthOneHot(shadowOutput, "liveness", "ok");
    expectHealthOneHot(shadowOutput, "startup", "ok");
    expectHealthOneHot(shadowOutput, "readiness", "unhealthy");
    expect(shadowOutput).not.toContain("unbounded-probe");
    expect(shadowOutput.split("\n").filter((line) => line.startsWith("# HELP nutsnews_worker_health_check "))).toHaveLength(1);
    expect(shadowOutput.split("\n").filter((line) => line.startsWith("# HELP nutsnews_worker_health_check_duration_seconds "))).toHaveLength(1);
    expect(shadowOutput).toContain("nutsnews_worker_health_check{");
    expect(shadowOutput).toContain("nutsnews_worker_health_check_duration_seconds_bucket{");

    const production = createPublicationPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-publication",
        version: "0.1.0",
        environment: "production",
        host: "publication-production"
      },
      expectedActive: true
    });
    expect(production.collect()).toContain('nutsnews_worker_expected_active{environment="production",service="nutsnews-worker-article-publication"} 1');
  });

  it("does not fabricate dependency durations and exports Runtime 1.0 seconds histograms", async () => {
    const metrics = createPublicationPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-publication",
        version: "0.1.0",
        environment: "test",
        host: "publication-test"
      },
      expectedActive: false
    });

    await metrics.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-31T00:00:01.000Z",
      stage: "publication",
      queue: "nutsnews.worker.publication.v1",
      outcome: "success",
      attributes: {
        dependency: "publication-shell"
      }
    });
    const durationlessOutput = metrics.collect();
    expect(durationlessOutput).not.toContain("nutsnews_worker_dependency_duration_ms");
    expect(durationlessOutput).not.toContain("nutsnews_worker_dependency_duration_seconds");

    await metrics.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-31T00:00:02.000Z",
      stage: "publication",
      queue: "nutsnews.worker.publication.v1",
      outcome: "success",
      durationMs: 25,
      attributes: {
        dependency: "backend-api"
      }
    });
    const observedOutput = metrics.collect();
    expect(observedOutput).not.toContain("nutsnews_worker_dependency_duration_ms");
    expect(observedOutput).toContain("nutsnews_worker_dependency_duration_seconds_bucket");
    expect(observedOutput).toContain("nutsnews_worker_dependency_duration_seconds_sum");
    expect(observedOutput).toContain("nutsnews_worker_dependency_duration_seconds_count");
  });

  it("moves aggregate readiness and the consumer check together on channel loss", async () => {
    const metrics = createPublicationPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-publication",
        version: "0.1.0",
        environment: "production",
        host: "backend-vps"
      },
      expectedActive: true
    });

    await metrics.emit({
      name: "runtime.broker.consumer_state_changed",
      level: "error",
      at: "2026-08-01T00:00:00.000Z",
      stage: "publication",
      queue: "nutsnews.worker.publication.v1",
      outcome: "channel-dropped",
      attributes: {
        state: "channel-dropped",
        activeConsumers: 0
      }
    });

    const output = metrics.collect();
    expectHealthOneHot(output, "readiness", "unhealthy");
    expect(metricSampleValueByLabels(output, "nutsnews_worker_health_check", {
      probe: "readiness",
      check: "rabbitmq-consumer",
      outcome: "unhealthy"
    })).toBe(1);
    expect(output).not.toContain("checks=[]");
  });
});

function metricSeries(output: string, family: string): string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith(family));
}

function started(): RuntimeTelemetryEvent {
  return {
    name: "runtime.message.started",
    level: "info",
    at: "2026-07-31T00:00:00.000Z",
    stage: "publication",
    queue: "nutsnews.worker.publication.v1",
    outcome: "started"
  };
}

function completion(
  name: Extract<RuntimeTelemetryEvent["name"], `runtime.message.${string}`>,
  outcome: NonNullable<RuntimeTelemetryEvent["outcome"]>,
  durationMs: number
): RuntimeTelemetryEvent {
  return {
    name,
    level: outcome === "success" || outcome === "duplicate" ? "info" : "warn",
    at: "2026-07-31T00:00:01.000Z",
    stage: "publication",
    queue: "nutsnews.worker.publication.v1",
    outcome,
    durationMs,
    attempt: 1,
    messageId: "message-id-cardinality-probe",
    idempotencyKey: "idempotency-cardinality-probe",
    correlationId: "correlation-cardinality-probe"
  };
}

function health(
  probe: "liveness" | "startup" | "readiness",
  outcome: "ok" | "degraded" | "unhealthy"
): RuntimeTelemetryEvent {
  return {
    name: "runtime.health.evaluated",
    level: outcome === "ok" ? "info" : "warn",
    at: "2026-07-31T00:00:01.000Z",
    outcome,
      attributes: {
        probe,
        status: outcome,
        checkCount: 1,
        checks: [
          {
            name: `${probe}-check`,
            status: outcome,
            critical: true,
            durationMs: 5
          }
        ]
      }
  };
}

function metricLabelNames(line: string): readonly string[] {
  return [
    ...line.matchAll(/([a-z_]+)="/gu)
  ].map((match) => match[1] ?? "");
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
  const values = outcomes.map((outcome) => metricSampleValue(output, probe, outcome));

  expect(values.reduce((sum, value) => sum + value, 0)).toBe(1);
  expect(values[outcomes.indexOf(expected)]).toBe(1);
}

function metricSampleValue(
  output: string,
  probe: "liveness" | "startup" | "readiness",
  outcome: "ok" | "degraded" | "unhealthy"
): number {
  const line = output
    .split("\n")
    .find((candidate) => candidate.startsWith("nutsnews_worker_health_probe{")
      && candidate.includes(`probe="${probe}"`)
      && candidate.includes(`outcome="${outcome}"`));

  expect(line).toBeDefined();
  return Number(line?.split(" ").at(-1));
}

function metricSampleValueByLabels(
  output: string,
  metric: string,
  requiredLabels: Readonly<Record<string, string>>
): number {
  const line = output
    .split("\n")
    .find((candidate) => candidate.startsWith(`${metric}{`)
      && Object.entries(requiredLabels).every(([name, value]) => candidate.includes(`${name}="${value}"`)));

  expect(line).toBeDefined();
  return Number(line?.split(" ").at(-1));
}
