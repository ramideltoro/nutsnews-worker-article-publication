import {
  describe,
  expect,
  it
} from "vitest";

import { createPublicationService } from "../src/service.js";
import {
  LocalPublicationSnapshotPublisher,
  createLocalPublicationDependencies,
  createMinimalPublicationDelivery,
  createProductionCapableLocalPublicationConfig
} from "../src/test-doubles.js";

describe("publication write safety", () => {
  it("does not publish production snapshots in the default baseline", async () => {
    const config = createProductionCapableLocalPublicationConfig();
    const dependencies = createLocalPublicationDependencies(config);
    const publisher = dependencies.snapshotPublisher as LocalPublicationSnapshotPublisher;
    const service = createPublicationService({
      config,
      dependencies
    });

    await service.start();

    await expect(service.processDelivery(createMinimalPublicationDelivery())).resolves.toMatchObject({
      action: "retry",
      reason: "handler-error"
    });

    expect(publisher.shadowComparisons).toHaveLength(0);
    expect(publisher.productionPublishes).toHaveLength(0);

    await service.stop();
  });

  it("uses only the scoped backend publication command when all production gates are enabled", async () => {
    const config = createProductionCapableLocalPublicationConfig();
    const dependencies = createLocalPublicationDependencies(config);
    const publisher = dependencies.snapshotPublisher as LocalPublicationSnapshotPublisher;
    const service = createPublicationService({
      config,
      dependencies
    });

    publisher.productionWritesEnabled = true;
    publisher.singleWriterEnabled = true;
    publisher.cutoverState = "cutover-approved";

    await service.start();

    await expect(service.processDelivery(createMinimalPublicationDelivery())).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(publisher.productionPublishes).toHaveLength(1);
    expect(publisher.productionPublishes[0]).toMatchObject({
      backendOperation: "uplift-publish-articles-batch",
      providerMode: "backend_postgres_primary",
      writeMode: "production",
      requiredLanguageCodes: [
        "fr",
        "ja",
        "de-CH",
        "de",
        "el"
      ]
    });
    expect(publisher.shadowComparisons).toHaveLength(0);

    await service.stop();
  });
});
