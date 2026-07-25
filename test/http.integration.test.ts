import {
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
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
import { createPublicationService } from "../src/service.js";
import { createLocalPublicationDependencies } from "../src/test-doubles.js";

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
    const metrics = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      }
    });
    service = createPublicationService({
      config,
      dependencies: createLocalPublicationDependencies(config),
      metrics
    });
    server = createPublicationHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await server.listen();

    const live = await fetch(server.url("/live"));
    const ready = await fetch(server.url("/ready"));
    const metricsResponse = await fetch(server.url("/metrics"));
    const schema = await fetch(server.url("/config-schema"));
    const status = await fetch(server.url("/status"));

    expect(live.status).toBe(200);
    expect(ready.status).toBe(200);
    expect(await metricsResponse.text()).toContain("nutsnews_worker_dependency_duration_ms");
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
});
