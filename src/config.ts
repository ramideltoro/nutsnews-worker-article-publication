import os from "node:os";

export const PUBLICATION_SERVICE_NAME = "nutsnews-worker-article-publication" as const;
export const PUBLICATION_SERVICE_VERSION = "0.1.0" as const;
export const PUBLICATION_PRODUCTION_CONFIRMATION = "backend-protected-publication-cutover-approved" as const;

export type PublicationDependencyMode = "test" | "production";
export type PublicationTelemetryLogMode = "stdout" | "silent";
export type PublicationWriteMode = "shadow_comparison" | "production";

export interface PublicationConfigVariable {
  readonly name: string;
  readonly description: string;
  readonly requiredInProduction: boolean;
  readonly sensitive: boolean;
  readonly defaultValue?: string;
}

export const PUBLICATION_CONFIG_SCHEMA = [
  variable("NUTSNEWS_ENVIRONMENT", "Runtime environment label for logs and metrics.", false, false, "local"),
  variable("NUTSNEWS_PUBLICATION_HTTP_HOST", "Health, status, and metrics bind host.", false, false, "0.0.0.0"),
  variable("NUTSNEWS_PUBLICATION_HTTP_PORT", "Health, status, and metrics bind port.", false, false, "8080"),
  variable("NUTSNEWS_PUBLICATION_DEPENDENCY_MODE", "Use test dependencies locally or require production dependency presence.", false, false, "test"),
  variable("NUTSNEWS_PUBLICATION_DATABASE_URL", "Worker-uplift shadow publication database connection string.", true, true),
  variable("NUTSNEWS_PUBLICATION_RABBITMQ_URL", "Private RabbitMQ connection string.", true, true),
  variable("NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL", "Scoped backend Worker API base URL.", true, true),
  variable("NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN", "Credential for the scoped backend Worker API identity.", true, true),
  variable("NUTSNEWS_PUBLICATION_BACKEND_API_COMPATIBILITY_VERSION", "Expected backend Worker API compatibility version.", false, false, "worker-api-v1"),
  variable("NUTSNEWS_PUBLICATION_SHADOW_SCHEMA_VERSION", "Expected worker-uplift shadow schema compatibility version.", false, false, "worker-uplift-shadow-v1"),
  variable("NUTSNEWS_PUBLICATION_DATABASE_ROLE", "Dedicated database role used by the publication service.", false, false, "nutsnews_worker_publication"),
  variable("NUTSNEWS_PUBLICATION_BACKEND_API_IDENTITY", "Scoped backend API identity used for publication commands.", false, false, "worker-uplift-publication"),
  variable("NUTSNEWS_PUBLICATION_POLICY_ID", "Versioned backend-owned readiness policy identifier.", false, false, "backend-publication-policy-v1"),
  variable("NUTSNEWS_PUBLICATION_FEATURE_FLAG", "Backend-owned feature flag that gates publication execution.", false, false, "worker-uplift-publication-shadow"),
  variable("NUTSNEWS_PUBLICATION_WRITE_MODE", "Publication write mode; shadow comparison is the hard default.", false, false, "shadow_comparison"),
  variable("NUTSNEWS_PUBLICATION_PRODUCTION_WRITE_CONFIRMATION", "Protected backend confirmation required for production publication writes.", true, false),
  variable("NUTSNEWS_PUBLICATION_CONCURRENCY", "Maximum concurrent publication message handlers.", false, false, "1"),
  variable("NUTSNEWS_PUBLICATION_PREFETCH", "Broker prefetch bound for publication deliveries.", false, false, "2"),
  variable("NUTSNEWS_PUBLICATION_SHUTDOWN_TIMEOUT_MS", "Graceful shutdown drain timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_PUBLICATION_TELEMETRY_LOGS", "Structured runtime log sink mode.", false, false, "stdout"),
  variable("NUTSNEWS_PUBLICATION_METRICS_ENABLED", "Expose bounded Prometheus metrics.", false, false, "true")
] as const satisfies readonly PublicationConfigVariable[];

export interface PublicationConfig {
  readonly serviceName: typeof PUBLICATION_SERVICE_NAME;
  readonly serviceVersion: typeof PUBLICATION_SERVICE_VERSION;
  readonly environment: string;
  readonly host: string;
  readonly http: {
    readonly host: string;
    readonly port: number;
  };
  readonly dependencyMode: PublicationDependencyMode;
  readonly dependencies: {
    readonly databaseConfigured: boolean;
    readonly rabbitmqConfigured: boolean;
    readonly backendApiConfigured: boolean;
    readonly backendApiCredentialConfigured: boolean;
  };
  readonly compatibility: {
    readonly backendApiVersion: string;
    readonly shadowSchemaVersion: string;
  };
  readonly security: {
    readonly databaseRole: string;
    readonly backendApiIdentity: string;
    readonly productionWriteConfirmationPresent: boolean;
  };
  readonly readiness: {
    readonly policyId: string;
    readonly featureFlag: string;
  };
  readonly writeMode: PublicationWriteMode;
  readonly concurrency: number;
  readonly prefetch: number;
  readonly shutdownTimeoutMs: number;
  readonly telemetryLogs: PublicationTelemetryLogMode;
  readonly metricsEnabled: boolean;
}

export class PublicationConfigError extends Error {
  readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`Invalid publication configuration: ${issues.join("; ")}`);
    this.name = "PublicationConfigError";
    this.issues = issues;
  }
}

export function loadPublicationConfig(env: NodeJS.ProcessEnv = process.env): PublicationConfig {
  const issues: string[] = [];
  const dependencyMode = parseDependencyMode(env.NUTSNEWS_PUBLICATION_DEPENDENCY_MODE, issues);
  const dependencies = {
    databaseConfigured: hasValue(env.NUTSNEWS_PUBLICATION_DATABASE_URL),
    rabbitmqConfigured: hasValue(env.NUTSNEWS_PUBLICATION_RABBITMQ_URL),
    backendApiConfigured: hasValue(env.NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL),
    backendApiCredentialConfigured: hasValue(env.NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN)
  };

  if (dependencyMode === "production") {
    requireConfigured("NUTSNEWS_PUBLICATION_DATABASE_URL", dependencies.databaseConfigured, issues);
    requireConfigured("NUTSNEWS_PUBLICATION_RABBITMQ_URL", dependencies.rabbitmqConfigured, issues);
    requireConfigured("NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL", dependencies.backendApiConfigured, issues);
    requireConfigured("NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN", dependencies.backendApiCredentialConfigured, issues);
  }

  const writeMode = parseWriteMode(env.NUTSNEWS_PUBLICATION_WRITE_MODE, issues);
  const productionWriteConfirmationPresent = env.NUTSNEWS_PUBLICATION_PRODUCTION_WRITE_CONFIRMATION === PUBLICATION_PRODUCTION_CONFIRMATION;
  const concurrency = parseInteger(env.NUTSNEWS_PUBLICATION_CONCURRENCY, "NUTSNEWS_PUBLICATION_CONCURRENCY", 1, 1, 8, issues);
  const prefetch = parseInteger(env.NUTSNEWS_PUBLICATION_PREFETCH, "NUTSNEWS_PUBLICATION_PREFETCH", 2, 1, 32, issues);
  const config: PublicationConfig = {
    serviceName: PUBLICATION_SERVICE_NAME,
    serviceVersion: PUBLICATION_SERVICE_VERSION,
    environment: nonEmpty(env.NUTSNEWS_ENVIRONMENT, "local"),
    host: nonEmpty(env.HOSTNAME, os.hostname()),
    http: {
      host: nonEmpty(env.NUTSNEWS_PUBLICATION_HTTP_HOST, "0.0.0.0"),
      port: parseInteger(env.NUTSNEWS_PUBLICATION_HTTP_PORT, "NUTSNEWS_PUBLICATION_HTTP_PORT", 8080, 0, 65_535, issues)
    },
    dependencyMode,
    dependencies,
    compatibility: {
      backendApiVersion: nonEmpty(env.NUTSNEWS_PUBLICATION_BACKEND_API_COMPATIBILITY_VERSION, "worker-api-v1"),
      shadowSchemaVersion: nonEmpty(env.NUTSNEWS_PUBLICATION_SHADOW_SCHEMA_VERSION, "worker-uplift-shadow-v1")
    },
    security: {
      databaseRole: nonEmpty(env.NUTSNEWS_PUBLICATION_DATABASE_ROLE, "nutsnews_worker_publication"),
      backendApiIdentity: nonEmpty(env.NUTSNEWS_PUBLICATION_BACKEND_API_IDENTITY, "worker-uplift-publication"),
      productionWriteConfirmationPresent
    },
    readiness: {
      policyId: nonEmpty(env.NUTSNEWS_PUBLICATION_POLICY_ID, "backend-publication-policy-v1"),
      featureFlag: nonEmpty(env.NUTSNEWS_PUBLICATION_FEATURE_FLAG, "worker-uplift-publication-shadow")
    },
    writeMode,
    concurrency,
    prefetch,
    shutdownTimeoutMs: parseInteger(env.NUTSNEWS_PUBLICATION_SHUTDOWN_TIMEOUT_MS, "NUTSNEWS_PUBLICATION_SHUTDOWN_TIMEOUT_MS", 30_000, 1_000, 600_000, issues),
    telemetryLogs: parseTelemetryLogMode(env.NUTSNEWS_PUBLICATION_TELEMETRY_LOGS, issues),
    metricsEnabled: parseBoolean(env.NUTSNEWS_PUBLICATION_METRICS_ENABLED, "NUTSNEWS_PUBLICATION_METRICS_ENABLED", true, issues)
  };

  if (config.prefetch < config.concurrency) {
    issues.push("NUTSNEWS_PUBLICATION_PREFETCH must be greater than or equal to NUTSNEWS_PUBLICATION_CONCURRENCY.");
  }

  if (config.writeMode === "production" && config.dependencyMode !== "production") {
    issues.push("NUTSNEWS_PUBLICATION_WRITE_MODE=production requires NUTSNEWS_PUBLICATION_DEPENDENCY_MODE=production.");
  }

  if (config.writeMode === "production" && !config.security.productionWriteConfirmationPresent) {
    issues.push("NUTSNEWS_PUBLICATION_PRODUCTION_WRITE_CONFIRMATION must match the protected backend confirmation before production writes can run.");
  }

  if (issues.length > 0) {
    throw new PublicationConfigError(issues);
  }

  return config;
}

function variable(
  name: string,
  description: string,
  requiredInProduction: boolean,
  sensitive: boolean,
  defaultValue?: string
): PublicationConfigVariable {
  return {
    name,
    description,
    requiredInProduction,
    sensitive,
    ...(defaultValue === undefined ? {} : {
      defaultValue
    })
  };
}

function nonEmpty(value: string | undefined, fallback: string): string {
  if (value === undefined) {
    return fallback;
  }

  const trimmed = value.trim();

  return trimmed.length > 0 ? trimmed : fallback;
}

function hasValue(value: string | undefined): boolean {
  return value !== undefined && value.trim().length > 0;
}

function parseDependencyMode(value: string | undefined, issues: string[]): PublicationDependencyMode {
  const normalized = nonEmpty(value, "test");

  if (normalized === "test" || normalized === "production") {
    return normalized;
  }

  issues.push("NUTSNEWS_PUBLICATION_DEPENDENCY_MODE must be test or production.");
  return "test";
}

function parseTelemetryLogMode(value: string | undefined, issues: string[]): PublicationTelemetryLogMode {
  const normalized = nonEmpty(value, "stdout");

  if (normalized === "stdout" || normalized === "silent") {
    return normalized;
  }

  issues.push("NUTSNEWS_PUBLICATION_TELEMETRY_LOGS must be stdout or silent.");
  return "stdout";
}

function parseWriteMode(value: string | undefined, issues: string[]): PublicationWriteMode {
  const normalized = nonEmpty(value, "shadow_comparison");

  if (normalized === "shadow_comparison" || normalized === "production") {
    return normalized;
  }

  issues.push("NUTSNEWS_PUBLICATION_WRITE_MODE must be shadow_comparison or production.");
  return "shadow_comparison";
}

function parseBoolean(value: string | undefined, key: string, fallback: boolean, issues: string[]): boolean {
  if (!hasValue(value)) {
    return fallback;
  }

  if (value === undefined) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();

  if (normalized === "true" || normalized === "1") {
    return true;
  }

  if (normalized === "false" || normalized === "0") {
    return false;
  }

  issues.push(`${key} must be true or false.`);
  return fallback;
}

function parseInteger(value: string | undefined, key: string, fallback: number, min: number, max: number, issues: string[]): number {
  if (!hasValue(value)) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    issues.push(`${key} must be an integer between ${String(min)} and ${String(max)}.`);
    return fallback;
  }

  return parsed;
}

function requireConfigured(key: string, configured: boolean, issues: string[]): void {
  if (!configured) {
    issues.push(`${key} is required when NUTSNEWS_PUBLICATION_DEPENDENCY_MODE=production.`);
  }
}
