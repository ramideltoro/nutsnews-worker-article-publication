import { createHash } from "node:crypto";

import type {
  PublicFeedSnapshotCompatibilityResult,
  PublicFeedSnapshotStableRow,
  PublicationReadinessDecision
} from "./dependencies.js";

const DEFAULT_LANGUAGE_CODE = "en";
const SUPPORTED_LANGUAGE_CODES = new Set([
  DEFAULT_LANGUAGE_CODE,
  "fr",
  "ja",
  "de-CH",
  "de",
  "el"
]);

export const BACKEND_PUBLIC_FEED_SNAPSHOT_CONTRACT = {
  contractId: "worker-uplift-api-admin-compatibility-contract",
  contractVersion: 1,
  capturedAt: "2026-07-23T02:40:43Z",
  readModel: "public.public_feed_snapshot",
  readOperation: "load-public-feed-snapshot-rows",
  publicReadOperations: [
    "load-public-feed-snapshot",
    "load-home-feed-snapshot",
    "load-public-feed-snapshot-rows"
  ],
  productionRefreshOperation: "uplift-refresh-public-feed-snapshot",
  backendRefreshFunction: "public.refresh_public_feed_snapshot()",
  orderBy: "snapshot_rank asc"
} as const satisfies Pick<
  PublicFeedSnapshotCompatibilityResult,
  | "contractId"
  | "contractVersion"
  | "capturedAt"
  | "readModel"
  | "readOperation"
  | "publicReadOperations"
  | "productionRefreshOperation"
  | "backendRefreshFunction"
  | "orderBy"
>;

export interface PublicFeedSnapshotCompatibilityInput {
  readonly payload: Readonly<Record<string, unknown>>;
  readonly decision: PublicationReadinessDecision;
}

interface SnapshotRequest {
  readonly limit: number;
  readonly offset: number;
  readonly category: string;
  readonly requestedLanguageCode: string;
  readonly normalizedLanguageCode: string;
}

interface SnapshotSummary {
  readonly originalUrl: string;
  readonly languageCode: string;
  readonly title: string;
  readonly summary: string;
}

interface RawSnapshotRow {
  readonly id: string;
  readonly source: string;
  readonly title: string;
  readonly originalUrl: string;
  readonly imageUrl: string;
  readonly publishedAt: string;
  readonly publishedOnSiteAt: string;
  readonly aiSummary: string;
  readonly category: string;
  readonly positivityScore: number | undefined;
  readonly status: string;
  readonly snapshotRank: number;
  readonly languageCode: string | undefined;
  readonly summaries: readonly SnapshotSummary[];
}

interface NormalizedSnapshotRow extends PublicFeedSnapshotStableRow {
  readonly eligible: boolean;
  readonly reasons: readonly string[];
}

export function buildPublicFeedSnapshotCompatibility(
  input: PublicFeedSnapshotCompatibilityInput
): PublicFeedSnapshotCompatibilityResult {
  const publicationRef = recordValue(input.payload.publicationRef);
  const request = snapshotRequest(publicationRef);
  const candidateRows = normalizeCandidateRows(publicationRef, input.payload, input.decision, request);
  const expectedRows = candidateRows
    .filter((row) => row.eligible)
    .filter((row) => categoryMatches(row.category, request.category))
    .sort((left, right) => left.rank - right.rank || left.identityHash.localeCompare(right.identityHash))
    .slice(request.offset, request.offset + request.limit);
  const backendRowsProvided = Array.isArray(publicationRef.backendSnapshotRows);
  const backendRows = backendRowsProvided
    ? normalizeProvidedRows(publicationRef.backendSnapshotRows, publicationRef, input.payload, input.decision, request)
    : expectedRows;
  const legacyKvRowsProvided = Array.isArray(publicationRef.legacyKvSnapshotRows);
  const legacyKvRows = legacyKvRowsProvided
    ? normalizeProvidedRows(publicationRef.legacyKvSnapshotRows, publicationRef, input.payload, input.decision, request)
    : [];
  const expectedHashes = expectedRows.map((row) => row.identityHash);
  const backendHashes = backendRows.map((row) => row.identityHash);
  const legacyKvHashes = legacyKvRows.map((row) => row.identityHash);
  const mismatchReasons = comparisonMismatchReasons({
    decision: input.decision,
    candidateRows,
    expectedRows,
    backendRows,
    expectedHashes,
    backendHashes,
    legacyKvHashes,
    legacyKvRowsProvided,
    request
  });
  const blocked = input.decision.status === "blocked" || input.decision.status === "rejected";
  const status = blocked ? "blocked" : mismatchReasons.length > 0 ? "mismatch" : "compatible";
  const legacyKvStatus = !legacyKvRowsProvided
    ? "not_configured"
    : sameArray(expectedHashes, legacyKvHashes)
      ? "unchanged"
      : "mismatch";

  return {
    ...BACKEND_PUBLIC_FEED_SNAPSHOT_CONTRACT,
    status,
    reasons: [
      "backend-public-feed-snapshot-contract-captured",
      "public-readers-remain-on-backend-contract",
      "no-direct-snapshot-sql",
      "no-cloudflare-kv-mutation",
      ...mismatchReasons
    ],
    directLiveRefreshRequested: false,
    cloudflareKvMutationRequested: false,
    requestedLanguageCode: request.requestedLanguageCode,
    normalizedLanguageCode: request.normalizedLanguageCode,
    pagination: {
      limit: request.limit,
      offset: request.offset,
      category: request.category
    },
    candidateIdentityHashes: expectedHashes,
    backendIdentityHashes: backendHashes,
    rows: expectedRows.map(stableRow),
    legacyKvFallback: {
      status: legacyKvStatus,
      identityHashes: legacyKvHashes
    },
    cacheMetadata: {
      legacyKvFallbackObserved: legacyKvRowsProvided,
      publicReadersRemainBackendContract: true,
      cacheHeadersPreserved: true
    },
    recovery: {
      retryable: !blocked,
      rollbackAvailable: true,
      staleVersionGuarded: true,
      partialRefreshFailureObservable: true
    }
  };
}

function comparisonMismatchReasons(input: {
  readonly decision: PublicationReadinessDecision;
  readonly candidateRows: readonly NormalizedSnapshotRow[];
  readonly expectedRows: readonly NormalizedSnapshotRow[];
  readonly backendRows: readonly NormalizedSnapshotRow[];
  readonly expectedHashes: readonly string[];
  readonly backendHashes: readonly string[];
  readonly legacyKvHashes: readonly string[];
  readonly legacyKvRowsProvided: boolean;
  readonly request: SnapshotRequest;
}): readonly string[] {
  const reasons = new Set<string>();

  if (input.decision.status === "blocked" || input.decision.status === "rejected") {
    reasons.add("publication-not-ready-for-public-snapshot");
  }

  for (const row of input.candidateRows) {
    for (const reason of row.reasons) {
      reasons.add(reason);
    }
  }

  if (!sameArray(input.expectedHashes, input.backendHashes)) {
    reasons.add("backend-public-snapshot-identity-or-order-mismatch");
  }

  if (input.backendRows.some((row) => !rowHasContractShape(row))) {
    reasons.add("backend-public-snapshot-shape-mismatch");
  }

  if (
    input.request.normalizedLanguageCode !== DEFAULT_LANGUAGE_CODE
    && input.expectedRows.some((row) => !row.translationAvailable)
  ) {
    reasons.add("localized-summary-fallback-to-default-language");
  }

  if (input.legacyKvRowsProvided && !sameArray(input.expectedHashes, input.legacyKvHashes)) {
    reasons.add("legacy-kv-fallback-snapshot-mismatch");
  }

  return [...reasons];
}

function normalizeCandidateRows(
  publicationRef: Readonly<Record<string, unknown>>,
  payload: Readonly<Record<string, unknown>>,
  decision: PublicationReadinessDecision,
  request: SnapshotRequest
): readonly NormalizedSnapshotRow[] {
  const rowValues = arrayRecords(publicationRef.publicFeedSnapshotRows);

  if (rowValues.length > 0) {
    return rowValues.map((row, index) => normalizeRow(row, publicationRef, payload, decision, request, index));
  }

  return [
    normalizeRow(recordValue(publicationRef.publicFeedSnapshot), publicationRef, payload, decision, request, 0)
  ];
}

function normalizeProvidedRows(
  value: unknown,
  publicationRef: Readonly<Record<string, unknown>>,
  payload: Readonly<Record<string, unknown>>,
  decision: PublicationReadinessDecision,
  request: SnapshotRequest
): readonly NormalizedSnapshotRow[] {
  return arrayRecords(value).map((row, index) => normalizeRow(row, publicationRef, payload, decision, request, index));
}

function normalizeRow(
  row: Readonly<Record<string, unknown>>,
  publicationRef: Readonly<Record<string, unknown>>,
  payload: Readonly<Record<string, unknown>>,
  decision: PublicationReadinessDecision,
  request: SnapshotRequest,
  index: number
): NormalizedSnapshotRow {
  const raw = rawSnapshotRow(row, publicationRef, payload, decision, index);
  const localized = localizeRow(raw, publicationRef, request.normalizedLanguageCode);
  const reasons = eligibilityReasons(localized);

  return {
    identityHash: stableIdentityHash(localized.originalUrl || localized.id),
    rank: localized.snapshotRank,
    category: localized.category || "uncategorized",
    publishedAt: localized.publishedAt,
    publishedOnSiteAt: localized.publishedOnSiteAt,
    languageCode: localized.languageCode ?? DEFAULT_LANGUAGE_CODE,
    requestedLanguageCode: request.normalizedLanguageCode,
    translationAvailable: localized.languageCode === request.normalizedLanguageCode,
    shape: {
      idPresent: localized.id.trim().length > 0,
      sourcePresent: localized.source.trim().length > 0,
      titlePresent: localized.title.trim().length > 0,
      imagePresent: localized.imageUrl.trim().length > 0,
      summaryPresent: localized.aiSummary.trim().length > 0,
      categoryPresent: localized.category.trim().length > 0,
      positivityScorePresent: localized.positivityScore !== undefined,
      publishedAtPresent: localized.publishedAt.trim().length > 0,
      publishedOnSiteAtPresent: localized.publishedOnSiteAt.trim().length > 0
    },
    eligible: reasons.length === 0,
    reasons
  };
}

function rawSnapshotRow(
  row: Readonly<Record<string, unknown>>,
  publicationRef: Readonly<Record<string, unknown>>,
  payload: Readonly<Record<string, unknown>>,
  decision: PublicationReadinessDecision,
  index: number
): RawSnapshotRow {
  const originalUrl = stringField(row, "originalUrl", "original_url")
    ?? stringValue(publicationRef.originalUrl)
    ?? `shadow://article/${decision.articleId}`;
  const defaultTimestamp = stringValue(payload.producedAt) ?? "2026-07-23T00:00:00.000Z";

  return {
    id: stringField(row, "id") ?? decision.articleId,
    source: stringField(row, "source") ?? "worker-uplift-shadow",
    title: stringField(row, "title") ?? "Sanitized public-feed compatibility title",
    originalUrl,
    imageUrl: stringField(row, "imageUrl", "image_url") ?? "https://example.invalid/public-feed/article.jpg",
    publishedAt: stringField(row, "publishedAt", "published_at") ?? defaultTimestamp,
    publishedOnSiteAt: stringField(row, "publishedOnSiteAt", "published_on_site_at") ?? defaultTimestamp,
    aiSummary: stringField(row, "aiSummary", "ai_summary", "summary") ?? "Sanitized public-feed compatibility summary.",
    category: stringField(row, "category") ?? "world",
    positivityScore: numberField(row, "positivityScore", "positivity_score") ?? 0.7,
    status: stringField(row, "status") ?? "published",
    snapshotRank: positiveIntegerField(row, "snapshotRank", "snapshot_rank") ?? index + 1,
    languageCode: stringField(row, "languageCode", "language_code"),
    summaries: [
      ...summaryRows(row.summaries, originalUrl),
      ...summaryRows(publicationRef.localizedSummaries, originalUrl)
    ]
  };
}

function localizeRow(
  row: RawSnapshotRow,
  publicationRef: Readonly<Record<string, unknown>>,
  requestedLanguageCode: string
): RawSnapshotRow {
  if (requestedLanguageCode === DEFAULT_LANGUAGE_CODE) {
    return {
      ...row,
      languageCode: DEFAULT_LANGUAGE_CODE
    };
  }

  const summary = row.summaries.find((candidate) => (
    candidate.originalUrl === row.originalUrl
    && candidate.languageCode === requestedLanguageCode
    && candidate.title.trim().length > 0
    && candidate.summary.trim().length > 0
  ));

  if (summary === undefined) {
    return {
      ...row,
      languageCode: DEFAULT_LANGUAGE_CODE
    };
  }

  void publicationRef;
  return {
    ...row,
    title: summary.title,
    aiSummary: summary.summary,
    languageCode: requestedLanguageCode
  };
}

function eligibilityReasons(row: RawSnapshotRow): readonly string[] {
  const reasons: string[] = [];

  if (row.status !== "published") {
    reasons.push("public-snapshot-requires-published-status");
  }

  if (row.imageUrl.trim().length === 0) {
    reasons.push("public-snapshot-requires-image-url");
  }

  if (row.title.trim().length === 0) {
    reasons.push("public-snapshot-requires-title");
  }

  if (row.aiSummary.trim().length === 0) {
    reasons.push("public-snapshot-requires-summary");
  }

  if (row.category.trim().length === 0) {
    reasons.push("public-snapshot-requires-category");
  }

  if (row.publishedAt.trim().length === 0 || row.publishedOnSiteAt.trim().length === 0) {
    reasons.push("public-snapshot-requires-publication-timestamps");
  }

  if (row.positivityScore === undefined) {
    reasons.push("public-snapshot-requires-positivity-score");
  }

  return reasons;
}

function snapshotRequest(publicationRef: Readonly<Record<string, unknown>>): SnapshotRequest {
  const request = recordValue(publicationRef.publicFeedSnapshotRequest);
  const requestedLanguageCode = stringField(request, "languageCode", "requestedLanguageCode", "lang") ?? DEFAULT_LANGUAGE_CODE;
  const normalizedLanguageCode = normalizeLanguageCode(requestedLanguageCode);

  return {
    limit: boundedInteger(request.limit, 6, 1, 250),
    offset: boundedInteger(request.offset, 0, 0, 1_000_000),
    category: stringField(request, "category") ?? "all",
    requestedLanguageCode,
    normalizedLanguageCode
  };
}

function summaryRows(value: unknown, fallbackOriginalUrl: string): readonly SnapshotSummary[] {
  return arrayRecords(value).flatMap((row) => {
    const languageCode = normalizeLanguageCode(stringField(row, "languageCode", "language_code"));
    const title = stringField(row, "title");
    const summary = stringField(row, "summary", "aiSummary", "ai_summary");

    if (title === undefined || summary === undefined) {
      return [];
    }

    return [
      {
        originalUrl: stringField(row, "originalUrl", "original_url") ?? fallbackOriginalUrl,
        languageCode,
        title,
        summary
      }
    ];
  });
}

function stableRow(row: NormalizedSnapshotRow): PublicFeedSnapshotStableRow {
  return {
    identityHash: row.identityHash,
    rank: row.rank,
    category: row.category,
    publishedAt: row.publishedAt,
    publishedOnSiteAt: row.publishedOnSiteAt,
    languageCode: row.languageCode,
    requestedLanguageCode: row.requestedLanguageCode,
    translationAvailable: row.translationAvailable,
    shape: row.shape
  };
}

function rowHasContractShape(row: NormalizedSnapshotRow): boolean {
  return row.shape.idPresent
    && row.shape.sourcePresent
    && row.shape.titlePresent
    && row.shape.imagePresent
    && row.shape.summaryPresent
    && row.shape.categoryPresent
    && row.shape.positivityScorePresent
    && row.shape.publishedAtPresent
    && row.shape.publishedOnSiteAtPresent;
}

function categoryMatches(category: string, requestedCategory: string): boolean {
  return requestedCategory.toLowerCase() === "all"
    || category.toLowerCase().includes(requestedCategory.toLowerCase());
}

function stableIdentityHash(value: string): string {
  return `sha256:${createHash("sha256").update(value).digest("hex").slice(0, 32)}`;
}

function normalizeLanguageCode(value: string | undefined): string {
  if (value === undefined) {
    return DEFAULT_LANGUAGE_CODE;
  }

  const trimmed = value.trim();

  if (trimmed.length === 0) {
    return DEFAULT_LANGUAGE_CODE;
  }

  const lowered = trimmed.toLowerCase();

  if (lowered === "de-ch" || lowered === "de_ch" || lowered === "ch") {
    return "de-CH";
  }

  return SUPPORTED_LANGUAGE_CODES.has(lowered) ? lowered : DEFAULT_LANGUAGE_CODE;
}

function boundedInteger(value: unknown, fallback: number, minimum: number, maximum: number): number {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    return fallback;
  }

  return Math.min(Math.max(value, minimum), maximum);
}

function positiveIntegerField(row: Readonly<Record<string, unknown>>, ...keys: readonly string[]): number | undefined {
  for (const key of keys) {
    const value = row[key];

    if (typeof value === "number" && Number.isInteger(value) && value > 0) {
      return value;
    }
  }

  return undefined;
}

function numberField(row: Readonly<Record<string, unknown>>, ...keys: readonly string[]): number | undefined {
  for (const key of keys) {
    const value = row[key];

    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
  }

  return undefined;
}

function stringField(row: Readonly<Record<string, unknown>>, ...keys: readonly string[]): string | undefined {
  for (const key of keys) {
    const value = stringValue(row[key]);

    if (value !== undefined) {
      return value;
    }
  }

  return undefined;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function recordValue(value: unknown): Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Readonly<Record<string, unknown>> : {};
}

function arrayRecords(value: unknown): readonly Readonly<Record<string, unknown>>[] {
  return Array.isArray(value)
    ? value.filter((item): item is Readonly<Record<string, unknown>> => typeof item === "object" && item !== null && !Array.isArray(item))
    : [];
}

function sameArray(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}
