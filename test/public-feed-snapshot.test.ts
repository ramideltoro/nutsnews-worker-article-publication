import {
  describe,
  expect,
  it
} from "vitest";

import type {
  PublicationFeatureFlagSnapshot,
  PublicationReadinessPolicySnapshot
} from "../src/dependencies.js";
import {
  BACKEND_CAPTURED_PUBLICATION_POLICY,
  evaluatePolicyDrivenPublicationReadiness
} from "../src/publication-gate.js";
import { buildPublicFeedSnapshotCompatibility } from "../src/public-feed-snapshot.js";
import { createMinimalPublicationPayload } from "../src/test-doubles.js";

describe("buildPublicFeedSnapshotCompatibility", () => {
  it("maps rank, pagination, category, localization, and legacy KV identity comparison", () => {
    const payload = createMinimalPublicationPayload({
      publicationRef: {
        publicFeedSnapshotRequest: {
          limit: 2,
          offset: 0,
          category: "World",
          languageCode: "de_ch"
        },
        publicFeedSnapshotRows: [
          publicFeedRow({
            id: "article-002",
            originalUrl: "https://example.com/article-002",
            snapshotRank: 2
          }),
          publicFeedRow({
            id: "article-001",
            originalUrl: "https://example.com/article-001",
            snapshotRank: 1
          })
        ],
        backendSnapshotRows: [
          publicFeedRow({
            id: "article-001",
            originalUrl: "https://example.com/article-001",
            snapshotRank: 1
          }),
          publicFeedRow({
            id: "article-002",
            originalUrl: "https://example.com/article-002",
            snapshotRank: 2
          })
        ],
        legacyKvSnapshotRows: [
          publicFeedRow({
            id: "article-001",
            originalUrl: "https://example.com/article-001",
            snapshotRank: 1
          }),
          publicFeedRow({
            id: "article-002",
            originalUrl: "https://example.com/article-002",
            snapshotRank: 2
          })
        ]
      }
    });
    const result = buildPublicFeedSnapshotCompatibility({
      payload,
      decision: readinessDecision(payload)
    });

    expect(result.status).toBe("compatible");
    expect(result.readModel).toBe("public.public_feed_snapshot");
    expect(result.readOperation).toBe("load-public-feed-snapshot-rows");
    expect(result.productionRefreshOperation).toBe("uplift-refresh-public-feed-snapshot");
    expect(result.orderBy).toBe("snapshot_rank asc");
    expect(result.normalizedLanguageCode).toBe("de-CH");
    expect(result.pagination).toMatchObject({
      limit: 2,
      offset: 0,
      category: "World"
    });
    expect(result.rows.map((row) => row.rank)).toEqual([
      1,
      2
    ]);
    expect(result.rows.every((row) => row.translationAvailable)).toBe(true);
    expect(result.candidateIdentityHashes.every((hash) => hash.startsWith("sha256:"))).toBe(true);
    expect(result.candidateIdentityHashes).toEqual(result.backendIdentityHashes);
    expect(result.legacyKvFallback.status).toBe("unchanged");
    expect(result.directLiveRefreshRequested).toBe(false);
    expect(result.cloudflareKvMutationRequested).toBe(false);
  });

  it("reports backend order and eligibility mismatches without live refresh", () => {
    const payload = createMinimalPublicationPayload({
      publicationRef: {
        publicFeedSnapshotRows: [
          publicFeedRow({
            id: "article-001",
            originalUrl: "https://example.com/article-001",
            snapshotRank: 1
          }),
          publicFeedRow({
            id: "article-002",
            originalUrl: "https://example.com/article-002",
            snapshotRank: 2,
            imageUrl: ""
          })
        ],
        backendSnapshotRows: [
          publicFeedRow({
            id: "article-002",
            originalUrl: "https://example.com/article-002",
            snapshotRank: 2
          }),
          publicFeedRow({
            id: "article-001",
            originalUrl: "https://example.com/article-001",
            snapshotRank: 1
          })
        ]
      }
    });
    const result = buildPublicFeedSnapshotCompatibility({
      payload,
      decision: readinessDecision(payload)
    });

    expect(result.status).toBe("mismatch");
    expect(result.reasons).toEqual(expect.arrayContaining([
      "public-snapshot-requires-image-url",
      "backend-public-snapshot-identity-or-order-mismatch"
    ]));
    expect(result.directLiveRefreshRequested).toBe(false);
    expect(result.cloudflareKvMutationRequested).toBe(false);
  });

  it("blocks snapshot compatibility when publication readiness is blocked", () => {
    const payload = createMinimalPublicationPayload({
      availableLanguageCodes: [
        "fr"
      ],
      missingLanguageCodes: [
        "ja",
        "de-CH",
        "de",
        "el"
      ]
    });
    const result = buildPublicFeedSnapshotCompatibility({
      payload,
      decision: readinessDecision(payload)
    });

    expect(result.status).toBe("blocked");
    expect(result.reasons).toEqual(expect.arrayContaining([
      "publication-not-ready-for-public-snapshot"
    ]));
    expect(result.recovery.rollbackAvailable).toBe(true);
  });
});

function readinessDecision(payload: Readonly<Record<string, unknown>>) {
  return evaluatePolicyDrivenPublicationReadiness({
    articleId: "article-001",
    envelopeArticleVersion: 1,
    payload,
    policy: {
      ...BACKEND_CAPTURED_PUBLICATION_POLICY,
      writeMode: "shadow_comparison",
      requiredChecks: []
    } satisfies PublicationReadinessPolicySnapshot,
    featureFlag: {
      flag: "worker-uplift-publication-shadow",
      enabled: true,
      writeMode: "shadow_comparison"
    } satisfies PublicationFeatureFlagSnapshot
  });
}

function publicFeedRow(overrides: Readonly<Record<string, unknown>> = {}): Readonly<Record<string, unknown>> {
  return {
    id: "article-001",
    source: "example",
    title: "Sanitized public-feed title",
    originalUrl: "https://example.com/article-001",
    imageUrl: "https://example.invalid/article.jpg",
    publishedAt: "2026-07-23T00:00:00.000Z",
    publishedOnSiteAt: "2026-07-23T00:00:00.000Z",
    aiSummary: "Sanitized public-feed summary.",
    category: "World",
    positivityScore: 0.82,
    status: "published",
    snapshotRank: 1,
    summaries: [
      {
        languageCode: "de-CH",
        title: "Sanitized Swiss German title",
        summary: "Sanitized Swiss German summary."
      }
    ],
    ...overrides
  };
}
