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
});
