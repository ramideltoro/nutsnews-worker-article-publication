import {
  createBufferedRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadPublicationConfig } from "../src/config.js";
import { createPublicationService } from "../src/service.js";
import {
  LocalPublicationBrokerTransport,
  LocalPublicationDatabase,
  LocalPublicationSnapshotPublisher,
  createLocalPublicationDependencies,
  createMinimalPublicationDelivery,
  createMinimalPublicationEnvelope,
  createMinimalPublicationPayload
} from "../src/test-doubles.js";

describe("createPublicationService", () => {
  it("starts, consumes publication readiness, and records shadow comparison only", async () => {
    const context = createServiceContext();

    await context.service.start();

    await expect(context.broker.deliverPublication()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.database.evaluations).toHaveLength(1);
    expect(context.publisher.shadowComparisons).toHaveLength(1);
    expect(context.publisher.productionPublishes).toHaveLength(0);
    expect(context.service.isStarted).toBe(true);

    await context.service.stop();
    expect(context.service.isStarted).toBe(false);
  });

  it("acks exact replay without duplicating shadow output", async () => {
    const context = createServiceContext();
    const delivery = createMinimalPublicationDelivery();

    await context.service.start();

    await expect(context.broker.deliverPublication(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(context.broker.deliverPublication(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });

    expect(context.database.evaluations).toHaveLength(1);
    expect(context.publisher.shadowComparisons).toHaveLength(1);
    expect(context.publisher.productionPublishes).toHaveLength(0);

    await context.service.stop();
  });

  it("rejects non-publication route deliveries before work handling", async () => {
    const context = createServiceContext();

    await context.service.start();

    await expect(context.service.processDelivery({
      envelope: createMinimalPublicationEnvelope({
        route: "persistence"
      }),
      payload: createMinimalPublicationPayload(),
      receivedAt: "2026-07-23T00:00:01.000Z"
    })).resolves.toMatchObject({
      action: "dlq",
      reason: "stage-mismatch"
    });
    expect(context.database.evaluations).toHaveLength(0);
    expect(context.publisher.shadowComparisons).toHaveLength(0);

    await context.service.stop();
  });
});

function createServiceContext() {
  const config = loadPublicationConfig({
    NUTSNEWS_PUBLICATION_HTTP_PORT: "0",
    NUTSNEWS_PUBLICATION_TELEMETRY_LOGS: "silent"
  });
  const dependencies = createLocalPublicationDependencies(config);
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createPrometheusRuntimeTelemetrySink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    }
  });
  const service = createPublicationService({
    config,
    dependencies,
    telemetry,
    metrics
  });

  return {
    broker: dependencies.brokerTransport as LocalPublicationBrokerTransport,
    database: dependencies.database as LocalPublicationDatabase,
    publisher: dependencies.snapshotPublisher as LocalPublicationSnapshotPublisher,
    service
  };
}
