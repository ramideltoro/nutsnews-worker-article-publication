import type { RuntimeTelemetryEvent } from "@ramideltoro/nutsnews-worker-runtime";
import {
  beforeEach,
  describe,
  expect,
  it,
  vi
} from "vitest";

const runtime1Delegate = vi.hoisted(() => ({
  emittedEventNames: [] as string[],
  output: ""
}));

vi.mock("@ramideltoro/nutsnews-worker-runtime", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@ramideltoro/nutsnews-worker-runtime")>();

  return {
    ...actual,
    createPrometheusRuntimeTelemetrySink: () => ({
      allowedLabels: actual.RUNTIME_ALLOWED_METRIC_LABELS,
      emit(event: RuntimeTelemetryEvent): void {
        runtime1Delegate.emittedEventNames.push(event.name);

        if (event.name === "runtime.health.evaluated") {
          runtime1Delegate.output = [
            "# HELP nutsnews_worker_health_probe Worker liveness, startup, and readiness state by probe and outcome.",
            "# TYPE nutsnews_worker_health_probe gauge",
            'nutsnews_worker_health_probe{environment="production",host="backend-vps",service="nutsnews-worker-article-publication",version="1.0.0",outcome="degraded",probe="readiness"} 1',
            "# HELP nutsnews_worker_health_check Worker dependency health state.",
            "# TYPE nutsnews_worker_health_check gauge",
            'nutsnews_worker_health_check{environment="production",host="backend-vps",service="nutsnews-worker-article-publication",version="1.0.0",outcome="degraded",probe="readiness",check="database"} 1',
            "# HELP nutsnews_worker_health_check_duration_seconds Worker dependency health latency.",
            "# TYPE nutsnews_worker_health_check_duration_seconds histogram",
            'nutsnews_worker_health_check_duration_seconds_bucket{environment="production",host="backend-vps",service="nutsnews-worker-article-publication",version="1.0.0",probe="readiness",check="database",le="+Inf"} 1',
            'nutsnews_worker_health_check_duration_seconds_sum{environment="production",host="backend-vps",service="nutsnews-worker-article-publication",version="1.0.0",probe="readiness",check="database"} 0.005',
            'nutsnews_worker_health_check_duration_seconds_count{environment="production",host="backend-vps",service="nutsnews-worker-article-publication",version="1.0.0",probe="readiness",check="database"} 1'
          ].join("\n");
        }
      },
      collect(): string {
        return runtime1Delegate.output;
      },
      setInFlight(): void {
        // Runtime1 delegate method retained by the local publication sink contract.
      },
      setShutdownDraining(): void {
        // Runtime1 delegate method retained by the local publication sink contract.
      }
    })
  };
});

import { createPublicationPrometheusTelemetrySink } from "../src/metrics.js";

describe("Runtime1 health metric compatibility", () => {
  beforeEach(() => {
    runtime1Delegate.emittedEventNames.length = 0;
    runtime1Delegate.output = "";
  });

  it("forwards health once and leaves every health family solely owned by Runtime1", async () => {
    const metrics = createPublicationPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-publication",
        version: "1.0.0",
        environment: "production",
        host: "backend-vps",
        revision: "0123456789abcdef0123456789abcdef01234567",
        deployment: "shadow",
        adapter: "production"
      },
      expectedActive: false
    });

    await metrics.emit({
      name: "runtime.health.evaluated",
      level: "warn",
      at: "2026-08-01T00:00:00.000Z",
      outcome: "degraded",
      attributes: {
        probe: "readiness",
        status: "degraded",
        checks: [
          {
            name: "database",
            status: "degraded",
            critical: true,
            durationMs: 5
          }
        ]
      }
    });
    await metrics.emit({
      name: "runtime.message.started",
      level: "info",
      at: "2026-08-01T00:00:01.000Z",
      stage: "publication",
      queue: "nutsnews.worker.publication.v1",
      outcome: "started"
    });

    const output = metrics.collect();
    const lines = output.split("\n");
    const healthSamples = lines.filter((line) => line.startsWith("nutsnews_worker_health_probe{"));
    const healthSeries = healthSamples.map((line) => line.slice(0, line.lastIndexOf(" ")));

    expect(runtime1Delegate.emittedEventNames).toEqual([
      "runtime.health.evaluated",
      "runtime.message.started"
    ]);
    expect(lines.filter((line) => line.startsWith("# HELP nutsnews_worker_health_probe "))).toHaveLength(1);
    expect(lines.filter((line) => line === "# TYPE nutsnews_worker_health_probe gauge")).toHaveLength(1);
    expect(healthSamples).toHaveLength(1);
    expect(new Set(healthSeries).size).toBe(healthSamples.length);
    expect(lines.filter((line) => line.startsWith("# HELP nutsnews_worker_health_check "))).toHaveLength(1);
    expect(lines.filter((line) => line.startsWith("# HELP nutsnews_worker_health_check_duration_seconds "))).toHaveLength(1);
    expect(lines.filter((line) => line.startsWith("nutsnews_worker_health_check{"))).toHaveLength(1);
    expect(lines.filter((line) => line.startsWith("nutsnews_worker_health_check_duration_seconds_count{"))).toHaveLength(1);
    expect(output).toContain(
      'nutsnews_worker_health_probe{environment="production",host="backend-vps",service="nutsnews-worker-article-publication",version="1.0.0",outcome="degraded",probe="readiness"} 1'
    );
  });
});
