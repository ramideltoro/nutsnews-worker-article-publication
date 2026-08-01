import {
  describe,
  expect,
  it
} from "vitest";

import { PUBLICATION_CONFIG_SCHEMA } from "../src/config.js";
import { createPublicationPrometheusTelemetrySink } from "../src/metrics.js";

const BUILD_REVISION = "0123456789abcdef0123456789abcdef01234567";

describe("publication immutable telemetry identity", () => {
  it("exports one bounded non-unknown build and deployment series", () => {
    const output = createPublicationPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-publication",
        version: "0.1.0",
        environment: "production",
        host: "backend-vps",
        revision: BUILD_REVISION,
        deployment: "production",
        adapter: "production"
      },
      expectedActive: true
    }).collect();
    const identitySamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_build_info{")
      || line.startsWith("nutsnews_worker_deployment_info{"));
    const expectedActiveSamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_expected_active{"));

    expect(identitySamples).toHaveLength(2);
    expect(expectedActiveSamples).toEqual([
      'nutsnews_worker_expected_active{environment="production",service="nutsnews-worker-article-publication"} 1'
    ]);
    expect(identitySamples.join("\n")).toContain(`revision="${BUILD_REVISION}"`);
    expect(identitySamples.join("\n")).toContain('deployment="production"');
    expect(identitySamples.join("\n")).toContain('adapter="production"');
    expect(identitySamples.join("\n")).not.toContain("unknown");
  });

  it("declares the immutable revision as required and non-sensitive in production", () => {
    expect(PUBLICATION_CONFIG_SCHEMA.find((variable) => variable.name === "NUTSNEWS_PUBLICATION_BUILD_REVISION")).toMatchObject({
      requiredInProduction: true,
      sensitive: false
    });
  });
});
