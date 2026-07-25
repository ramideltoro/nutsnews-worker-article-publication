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
  LocalPublicationReadinessPolicy,
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
    expect(context.database.evaluations[0]?.policy.version).toBe("2026-07-23.worker-uplift-api-admin-compatibility-contract.v1");
    expect(context.database.evaluations[0]?.finalAggregateVersion).toBe(1);
    expect(context.publisher.shadowComparisons[0]).toMatchObject({
      backendOperation: "shadow-publication-comparison",
      providerMode: "backend_postgres_shadow",
      requiredLanguageCodes: [
        "fr",
        "ja",
        "de-CH",
        "de",
        "el"
      ]
    });
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

  it("blocks hold-until-complete publication when required translations are missing", async () => {
    const context = createServiceContext();

    await context.service.start();

    await expect(context.broker.deliverPublication({
      envelope: createMinimalPublicationEnvelope(),
      payload: createMinimalPublicationPayload({
        availableLanguageCodes: [
          "fr",
          "ja",
          "de"
        ],
        missingLanguageCodes: [
          "de-CH",
          "el"
        ],
        snapshotRefreshRequired: false
      }),
      receivedAt: "2026-07-23T00:00:01.000Z"
    })).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.database.evaluations[0]?.decision).toMatchObject({
      status: "blocked",
      reasons: [
        "missing-required-translations"
      ],
      missingLanguageCodes: [
        "de-CH",
        "el"
      ]
    });
    expect(context.publisher.shadowComparisons).toHaveLength(1);
    expect(context.publisher.productionPublishes).toHaveLength(0);

    await context.service.stop();
  });

  it("allows approved non-blocking backlog policy when the minimum language set is present", async () => {
    const context = createServiceContext();

    context.policy.policy = {
      ...context.policy.policy,
      holdForTranslations: false,
      minimumLanguageCodes: [
        "fr",
        "ja"
      ],
      backlogTreatment: "allow_non_blocking"
    };

    await context.service.start();

    await expect(context.broker.deliverPublication({
      envelope: createMinimalPublicationEnvelope(),
      payload: createMinimalPublicationPayload({
        availableLanguageCodes: [
          "fr",
          "ja"
        ],
        missingLanguageCodes: [
          "de-CH",
          "de",
          "el"
        ]
      }),
      receivedAt: "2026-07-23T00:00:01.000Z"
    })).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.database.evaluations[0]?.decision.status).toBe("shadow_compare_only");
    expect(context.database.evaluations[0]?.decision.reasons).toEqual(expect.arrayContaining([
      "translation-backlog-non-blocking"
    ]));
    expect(context.database.evaluations[0]?.decision.missingLanguageCodes).toEqual([
      "de-CH",
      "de",
      "el"
    ]);
    expect(context.publisher.shadowComparisons).toHaveLength(1);

    await context.service.stop();
  });

  it("rejects stale policy, mismatched required languages, and superseded content explicitly", async () => {
    const context = createServiceContext();

    context.policy.policy = {
      ...context.policy.policy,
      stale: true
    };

    await context.service.start();

    await expect(context.broker.deliverPublication()).resolves.toMatchObject({
      action: "dlq",
      reason: "stale-publication-policy"
    });
    expect(context.database.evaluations[0]?.decision.status).toBe("rejected");
    expect(context.publisher.shadowComparisons).toHaveLength(1);

    await context.service.stop();

    const invalidContext = createServiceContext();

    await invalidContext.service.start();

    await expect(invalidContext.broker.deliverPublication({
      envelope: createMinimalPublicationEnvelope({
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4812",
        idempotencyKey: "persistence:publication:article-invalid"
      }),
      payload: createMinimalPublicationPayload({
        idempotencyKey: "persistence:publication:article-invalid",
        requiredLanguageCodes: [
          "fr",
          "en"
        ],
        publicationRef: {
          processingState: "superseded"
        }
      }),
      receivedAt: "2026-07-23T00:00:01.000Z"
    })).resolves.toMatchObject({
      action: "dlq",
      reason: "payload-required-language-policy-mismatch"
    });
    expect(invalidContext.database.evaluations[0]?.decision.reasons).toEqual(expect.arrayContaining([
      "payload-required-language-policy-mismatch",
      "superseded-content"
    ]));

    await invalidContext.service.stop();
  });

  it("rejects out-of-order final aggregate versions without duplicate shadow publish", async () => {
    const context = createServiceContext();
    const current = {
      envelope: createMinimalPublicationEnvelope({
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4813",
        idempotencyKey: "persistence:publication:article-001:v2",
        aggregate: {
          type: "article",
          id: "article-001",
          version: 2
        }
      }),
      payload: createMinimalPublicationPayload({
        idempotencyKey: "persistence:publication:article-001:v2",
        publicationRef: {
          articleVersion: 2,
          currentArticleVersion: 2,
          finalAggregateVersion: 2
        }
      }),
      receivedAt: "2026-07-23T00:00:01.000Z"
    };
    const stale = {
      envelope: createMinimalPublicationEnvelope({
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4814",
        idempotencyKey: "persistence:publication:article-001:v1-late",
        aggregate: {
          type: "article",
          id: "article-001",
          version: 1
        }
      }),
      payload: createMinimalPublicationPayload({
        idempotencyKey: "persistence:publication:article-001:v1-late",
        publicationRef: {
          articleVersion: 1,
          currentArticleVersion: 1,
          finalAggregateVersion: 1
        }
      }),
      receivedAt: "2026-07-23T00:00:02.000Z"
    };

    await context.service.start();

    await expect(context.broker.deliverPublication(current)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(context.broker.deliverPublication(stale)).resolves.toMatchObject({
      action: "dlq",
      reason: "stale-final-aggregate-version"
    });

    expect(context.database.evaluations).toHaveLength(1);
    expect(context.publisher.shadowComparisons).toHaveLength(1);

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
    policy: dependencies.readinessPolicy as LocalPublicationReadinessPolicy,
    publisher: dependencies.snapshotPublisher as LocalPublicationSnapshotPublisher,
    service
  };
}
