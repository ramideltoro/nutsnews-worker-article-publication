import {
  afterEach,
  describe,
  expect,
  it
} from "vitest";

import { loadPublicationConfig } from "../src/config.js";
import {
  createPublicationHttpServer,
  type PublicationHttpServer
} from "../src/http.js";
import { createPublicationPrometheusTelemetrySink } from "../src/metrics.js";
import type {
  PublicationReconciliationReport,
  PublicationReconciler
} from "../src/reconciliation.js";
import { createPublicationService } from "../src/service.js";
import {
  createLocalPublicationDependencies,
  createMinimalPublicationDelivery
} from "../src/test-doubles.js";

describe("createPublicationHttpServer", () => {
  let server: PublicationHttpServer | undefined;
  let service: ReturnType<typeof createPublicationService> | undefined;

  afterEach(async () => {
    await server?.close();
    await service?.stop();
  });

  it("serves health, metrics, config schema, and public status without exposing values", async () => {
    const config = loadPublicationConfig({
      NUTSNEWS_PUBLICATION_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_PUBLICATION_HTTP_PORT: "0",
      NUTSNEWS_PUBLICATION_DATABASE_URL: "postgres://example.invalid/publication",
      NUTSNEWS_PUBLICATION_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL: "https://backend.example.invalid/worker",
      NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN: "secret-not-real",
      NUTSNEWS_PUBLICATION_TELEMETRY_LOGS: "silent"
    });
    const metrics = createPublicationPrometheusTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      expectedActive: false
    });
    service = createPublicationService({
      config,
      dependencies: createLocalPublicationDependencies(config),
      telemetry: metrics,
      metrics
    });
    server = createPublicationHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await server.listen();
    await service.processDelivery(createMinimalPublicationDelivery());

    const live = await fetch(server.url("/live"));
    const startup = await fetch(server.url("/startup"));
    const ready = await fetch(server.url("/ready"));
    const metricsResponse = await fetch(server.url("/metrics"));
    const schema = await fetch(server.url("/config-schema"));
    const status = await fetch(server.url("/status"));

    expect(live.status).toBe(200);
    expect(startup.status).toBe(200);
    expect(ready.status).toBe(200);
    const metricsBody = await metricsResponse.text();
    const readyBody = await ready.json() as {
      readonly checks: readonly {
        readonly name: string;
        readonly status: string;
        readonly details?: Readonly<Record<string, unknown>>;
      }[];
    };
    expect(metricsBody).not.toContain("nutsnews_worker_dependency_duration_ms");
    expect(metricsBody).toContain("nutsnews_worker_uplift_stage_events_total");
    expect(metricsBody).toContain("nutsnews_worker_health_probe");
    expect(metricsBody).toContain('nutsnews_worker_expected_active{environment="local",service="publication"} 0');
    expect(metricsBody).toContain('outcome="ok",probe="liveness"} 1');
    expect(metricsBody).toContain('outcome="ok",probe="startup"} 1');
    expect(metricsBody).toContain('outcome="ok",probe="readiness"} 1');
    expect(readyBody.checks.find((check) => check.name === "rabbitmq-consumer")).toMatchObject({
      status: "ok",
      details: {
        activeConsumers: 1,
        queue: "nutsnews.worker.publication.v1"
      }
    });
    const schemaBody = await schema.json() as {
      readonly variables: readonly { readonly name: string; readonly sensitive: boolean }[];
    };
    const statusBody = await status.json() as Readonly<Record<string, unknown>>;

    expect(schemaBody.variables.some((variable) => variable.name === "NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN" && variable.sensitive)).toBe(true);
    expect(statusBody.writeMode).toBe("shadow_comparison");
    expect(statusBody.productionWriteConfirmationPresent).toBe(false);
    expect(JSON.stringify(schemaBody)).not.toContain("postgres://");
    expect(JSON.stringify(statusBody)).not.toContain("postgres://");
    expect(JSON.stringify(statusBody)).not.toContain("amqp://");
    expect(JSON.stringify(statusBody)).not.toContain("backend.example.invalid");
    expect(JSON.stringify(statusBody)).not.toContain("secret-not-real");
  });

  it("protects the reconciliation endpoint with bearer auth", async () => {
    const config = loadPublicationConfig({
      NUTSNEWS_PUBLICATION_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_PUBLICATION_HTTP_PORT: "0",
      NUTSNEWS_PUBLICATION_TELEMETRY_LOGS: "silent"
    });
    service = createPublicationService({
      config,
      dependencies: createLocalPublicationDependencies(config)
    });
    const reconciler: PublicationReconciler = {
      name: "test-reconciler",
      reconcile: (request) => Promise.resolve({
        service: "publication",
        mode: request.mode,
        status: "dry_run",
        requestedAt: "2026-07-23T00:00:00.000Z",
        maxItems: 1,
        minAgeSeconds: 900,
        selectedCount: 0,
        replayedCount: 0,
        failedClosedCount: 0,
        skippedCount: 0,
        writesPerformed: false,
        dryRun: true,
        productionVisibilityEnabled: false,
        legacyRuntimeRequired: false,
        protectedApplyRequired: true,
        terminalStage: true,
        candidates: [],
        errors: [],
        metrics: {
          candidateCount: 0,
          replayedCount: 0,
          failedClosedCount: 0,
          skippedCount: 0
        }
      } satisfies PublicationReconciliationReport)
    };
    server = createPublicationHttpServer({
      config,
      service,
      reconciler,
      reconciliationToken: "test-token"
    });

    await service.start();
    await server.listen();

    const unauthorized = await fetch(server.url("/reconcile/outbox"), {
      method: "POST",
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(unauthorized.status).toBe(401);

    const authorized = await fetch(server.url("/reconcile/outbox"), {
      method: "POST",
      headers: {
        authorization: "Bearer test-token"
      },
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(authorized.status).toBe(200);
    await expect(authorized.json()).resolves.toMatchObject({
      status: "dry_run",
      terminalStage: true,
      writesPerformed: false,
      productionVisibilityEnabled: false
    });
  });
});
