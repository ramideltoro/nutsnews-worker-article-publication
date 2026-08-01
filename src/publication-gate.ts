import type { PublicationWriteMode } from "./config.js";
import type {
  PublicationFeatureFlagSnapshot,
  PublicationReadinessDecision,
  PublicationReadinessInput,
  PublicationReadinessPolicySnapshot
} from "./dependencies.js";

export const BACKEND_CAPTURED_PUBLICATION_POLICY = {
  policyId: "worker-uplift-api-admin-compatibility-contract",
  version: "2026-07-23.worker-uplift-api-admin-compatibility-contract.v1",
  capturedAt: "2026-07-23T02:22:18Z",
  source: "backend-worker-api",
  requiredLanguageCodes: [
    "fr",
    "ja",
    "de-CH",
    "de",
    "el"
  ],
  holdForTranslations: true,
  minimumLanguageCodes: [],
  backlogTreatment: "block_until_recovered",
  timeoutTreatment: "block",
  stale: false,
  scopedPublicationOperation: "uplift-publish-articles-batch"
} as const satisfies Omit<PublicationReadinessPolicySnapshot, "writeMode" | "requiredChecks">;

export interface PolicyDrivenPublicationInput extends PublicationReadinessInput {
  readonly envelopeArticleVersion: number;
}

export interface NormalizedPublicationAggregate {
  readonly articleId: string;
  readonly articleVersion: number;
  readonly currentArticleVersion: number;
  readonly finalAggregateVersion: number;
  readonly canonicalIdentityHash: string | undefined;
  readonly canonicalIdentityValid: boolean;
  readonly enrichmentPolicyValid: boolean;
  readonly approvalStatus: string | undefined;
  readonly sourceSummaryPersisted: boolean;
  readonly processingState: string;
  readonly availableLanguageCodes: readonly string[];
  readonly payloadRequiredLanguageCodes: readonly string[];
  readonly missingLanguageCodes: readonly string[];
  readonly snapshotRefreshRequired: boolean;
  readonly originalUrl: string | undefined;
  readonly expectedPolicyVersion: string | undefined;
}

export function evaluatePolicyDrivenPublicationReadiness(
  input: PolicyDrivenPublicationInput
): PublicationReadinessDecision {
  const aggregate = normalizePublicationAggregate(input);
  const rejectionReasons = validateHardRequirements(input.policy, aggregate);

  if (rejectionReasons.length > 0) {
    return decision("rejected", rejectionReasons, aggregate, input.policy, input.featureFlag);
  }

  const missingRequired = missingPolicyLanguages(input.policy.requiredLanguageCodes, aggregate.availableLanguageCodes);
  const missingMinimum = missingPolicyLanguages(input.policy.minimumLanguageCodes, aggregate.availableLanguageCodes);

  if (input.policy.holdForTranslations && missingRequired.length > 0) {
    return decision("blocked", [
      "missing-required-translations"
    ], aggregate, input.policy, input.featureFlag, missingRequired);
  }

  if (!input.policy.holdForTranslations && missingMinimum.length > 0) {
    return decision("blocked", [
      "missing-minimum-translations"
    ], aggregate, input.policy, input.featureFlag, missingMinimum);
  }

  const reasons = [
    "canonical-identity-present",
    "enrichment-policy-valid",
    "approval-accepted",
    "source-summary-persisted",
    "current-article-version",
    "no-blocking-processing-state"
  ];

  if (!input.policy.holdForTranslations && missingRequired.length > 0) {
    reasons.push("translation-backlog-non-blocking");
  } else {
    reasons.push("required-translations-present");
  }

  if (input.featureFlag.writeMode === "production" && input.featureFlag.enabled) {
    return decision("ready_for_production", [
      ...reasons,
      "protected-production-mode"
    ], aggregate, input.policy, input.featureFlag, missingRequired);
  }

  return decision("shadow_compare_only", [
    ...reasons,
    "shadow-comparison-mode"
  ], aggregate, input.policy, input.featureFlag, missingRequired);
}

export function normalizePublicationAggregate(input: PolicyDrivenPublicationInput): NormalizedPublicationAggregate {
  const publicationRef = recordValue(input.payload.publicationRef);
  const articleId = stringValue(input.payload.articleId) ?? input.articleId;
  const articleVersion = positiveInteger(publicationRef.articleVersion) ?? input.envelopeArticleVersion;
  const currentArticleVersion = positiveInteger(publicationRef.currentArticleVersion) ?? articleVersion;
  const finalAggregateVersion = positiveInteger(publicationRef.finalAggregateVersion) ?? articleVersion;
  const availableLanguageCodes = uniqueStrings(input.payload.availableLanguageCodes);
  const payloadRequiredLanguageCodes = uniqueStrings(input.payload.requiredLanguageCodes);

  return {
    articleId,
    articleVersion,
    currentArticleVersion,
    finalAggregateVersion,
    canonicalIdentityHash: stringValue(publicationRef.canonicalIdentityHash),
    canonicalIdentityValid: booleanValue(publicationRef.canonicalIdentityValid, true),
    enrichmentPolicyValid: booleanValue(publicationRef.enrichmentPolicyValid, true),
    approvalStatus: stringValue(publicationRef.approvalStatus),
    sourceSummaryPersisted: booleanValue(publicationRef.sourceSummaryPersisted, publicationRef.persistedSourceSummaryRef !== undefined),
    processingState: stringValue(publicationRef.processingState) ?? "clear",
    availableLanguageCodes,
    payloadRequiredLanguageCodes,
    missingLanguageCodes: uniqueStrings(input.payload.missingLanguageCodes),
    snapshotRefreshRequired: booleanValue(input.payload.snapshotRefreshRequired, false),
    originalUrl: stringValue(publicationRef.originalUrl),
    expectedPolicyVersion: stringValue(publicationRef.policyVersion)
  };
}

function validateHardRequirements(
  policy: PublicationReadinessPolicySnapshot,
  aggregate: NormalizedPublicationAggregate
): readonly string[] {
  const reasons: string[] = [];

  if (policy.stale) {
    reasons.push("stale-publication-policy");
  }

  if (aggregate.expectedPolicyVersion !== undefined && aggregate.expectedPolicyVersion !== policy.version) {
    reasons.push("stale-policy-version");
  }

  if (policy.requiredLanguageCodes.length === 0) {
    reasons.push("missing-required-language-policy");
  }

  if (hasDuplicates(policy.requiredLanguageCodes) || policy.requiredLanguageCodes.some((language) => language.trim().length === 0)) {
    reasons.push("invalid-required-translation-policy");
  }

  if (
    aggregate.payloadRequiredLanguageCodes.length > 0
    && !sameStringSet(aggregate.payloadRequiredLanguageCodes, policy.requiredLanguageCodes)
  ) {
    reasons.push("payload-required-language-policy-mismatch");
  }

  if (aggregate.articleId.trim().length === 0) {
    reasons.push("missing-article-id");
  }

  if (aggregate.originalUrl === undefined || aggregate.originalUrl.trim().length === 0) {
    reasons.push("missing-original-url");
  } else if (!isHttpUrl(aggregate.originalUrl)) {
    reasons.push("invalid-original-url");
  }

  if (aggregate.articleVersion !== aggregate.currentArticleVersion) {
    reasons.push("non-current-article-version");
  }

  if (aggregate.finalAggregateVersion < aggregate.currentArticleVersion) {
    reasons.push("stale-final-aggregate-version");
  }

  if (aggregate.canonicalIdentityHash === undefined || aggregate.canonicalIdentityHash.trim().length === 0 || !aggregate.canonicalIdentityValid) {
    reasons.push("invalid-canonical-identity");
  }

  if (!aggregate.enrichmentPolicyValid) {
    reasons.push("invalid-enrichment-policy");
  }

  if (aggregate.approvalStatus !== "accepted") {
    reasons.push("approval-not-accepted");
  }

  if (!aggregate.sourceSummaryPersisted) {
    reasons.push("source-summary-missing");
  }

  if (aggregate.processingState !== "clear") {
    reasons.push(aggregate.processingState === "superseded" ? "superseded-content" : "blocking-processing-state");
  }

  return reasons;
}

function decision(
  status: PublicationReadinessDecision["status"],
  reasons: readonly string[],
  aggregate: NormalizedPublicationAggregate,
  policy: PublicationReadinessPolicySnapshot,
  featureFlag: PublicationFeatureFlagSnapshot,
  missingLanguages: readonly string[] = []
): PublicationReadinessDecision {
  const writeMode: PublicationWriteMode = status === "ready_for_production" ? "production" : "shadow_comparison";

  return {
    status,
    terminal: status === "rejected",
    reasons,
    articleId: aggregate.articleId,
    originalUrl: aggregate.originalUrl ?? "",
    articleVersion: aggregate.articleVersion,
    finalAggregateVersion: aggregate.finalAggregateVersion,
    policyVersion: policy.version,
    requiredLanguageCodes: policy.requiredLanguageCodes,
    availableLanguageCodes: aggregate.availableLanguageCodes,
    missingLanguageCodes: missingLanguages,
    snapshotRefreshRequired: aggregate.snapshotRefreshRequired && status !== "blocked" && status !== "rejected",
    writeMode,
    backendOperation: status === "ready_for_production" ? policy.scopedPublicationOperation : "shadow-publication-comparison",
    providerMode: status === "ready_for_production" ? "backend_postgres_primary" : "backend_postgres_shadow",
    featureFlag: featureFlag.flag
  };
}

function missingPolicyLanguages(required: readonly string[], available: readonly string[]): readonly string[] {
  const availableSet = new Set(available);

  return required.filter((language) => !availableSet.has(language));
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function isHttpUrl(value: string): boolean {
  try {
    const url = new URL(value);

    return (url.protocol === "http:" || url.protocol === "https:") && url.hostname.length > 0;
  } catch {
    return false;
  }
}

function positiveInteger(value: unknown): number | undefined {
  return typeof value === "number" && Number.isInteger(value) && value > 0 ? value : undefined;
}

function booleanValue(value: unknown, fallback: boolean): boolean {
  return typeof value === "boolean" ? value : fallback;
}

function uniqueStrings(value: unknown): readonly string[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return [...new Set(value.filter((item): item is string => typeof item === "string"))];
}

function recordValue(value: unknown): Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Readonly<Record<string, unknown>> : {};
}

function hasDuplicates(values: readonly string[]): boolean {
  return new Set(values).size !== values.length;
}

function sameStringSet(left: readonly string[], right: readonly string[]): boolean {
  if (left.length !== right.length) {
    return false;
  }

  const rightSet = new Set(right);
  return left.every((item) => rightSet.has(item));
}
