import {
  describe,
  expect,
  it
} from "vitest";

import {
  PUBLICATION_PRODUCTION_CONFIRMATION,
  PublicationConfigError,
  loadPublicationConfig
} from "../src/config.js";

describe("loadPublicationConfig", () => {
  it("loads value-free local defaults with shadow comparison as the hard default", () => {
    const config = loadPublicationConfig({
      HOSTNAME: "publication-host"
    });

    expect(config).toMatchObject({
      serviceName: "nutsnews-worker-article-publication",
      dependencyMode: "test",
      buildRevision: "development",
      host: "publication-host",
      writeMode: "shadow_comparison",
      concurrency: 1,
      prefetch: 2,
      compatibility: {
        backendApiVersion: "worker-api-v1",
        shadowSchemaVersion: "worker-uplift-shadow-v1"
      },
      security: {
        databaseRole: "nutsnews_worker_publication",
        backendApiIdentity: "worker-uplift-publication",
        productionWriteConfirmationPresent: false
      },
      readiness: {
        policyId: "worker-uplift-api-admin-compatibility-contract"
      },
      dependencies: {
        databaseConfigured: false,
        rabbitmqConfigured: false,
        backendApiConfigured: false,
        backendApiCredentialConfigured: false
      }
    });
  });

  it("fails production dependency mode with secret names only", () => {
    expect(() => loadPublicationConfig({
      NUTSNEWS_PUBLICATION_DEPENDENCY_MODE: "production"
    })).toThrow(PublicationConfigError);

    try {
      loadPublicationConfig({
        NUTSNEWS_PUBLICATION_DEPENDENCY_MODE: "production"
      });
    } catch (error: unknown) {
      expect(error).toBeInstanceOf(PublicationConfigError);
      const configError = error as PublicationConfigError;

      expect(configError.issues).toEqual([
        "NUTSNEWS_PUBLICATION_DATABASE_URL is required when NUTSNEWS_PUBLICATION_DEPENDENCY_MODE=production.",
        "NUTSNEWS_PUBLICATION_RABBITMQ_URL is required when NUTSNEWS_PUBLICATION_DEPENDENCY_MODE=production.",
        "NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL is required when NUTSNEWS_PUBLICATION_DEPENDENCY_MODE=production.",
        "NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN is required when NUTSNEWS_PUBLICATION_DEPENDENCY_MODE=production.",
        "NUTSNEWS_PUBLICATION_BUILD_REVISION must be a lowercase 40-character Git commit SHA when NUTSNEWS_PUBLICATION_DEPENDENCY_MODE=production."
      ]);
      expect(configError.message).not.toContain("postgres://");
      expect(configError.message).not.toContain("amqp://");
      expect(configError.message).not.toContain("https://");
      expect(configError.message).not.toContain("secret");
    }
  });

  it("rejects production writes without backend-owned protected confirmation", () => {
    expect(() => loadPublicationConfig({
      NUTSNEWS_PUBLICATION_WRITE_MODE: "production"
    })).toThrow(PublicationConfigError);

    expect(() => loadPublicationConfig({
      NUTSNEWS_PUBLICATION_DEPENDENCY_MODE: "production",
      NUTSNEWS_PUBLICATION_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_PUBLICATION_DATABASE_URL: "postgres://example.invalid/publication",
      NUTSNEWS_PUBLICATION_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL: "https://backend.example.invalid/worker",
      NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN: "secret-not-real",
      NUTSNEWS_PUBLICATION_WRITE_MODE: "production",
      NUTSNEWS_PUBLICATION_PRODUCTION_WRITE_CONFIRMATION: "wrong"
    })).toThrow(PublicationConfigError);
  });

  it("accepts protected production mode without retaining secret values", () => {
    const config = loadPublicationConfig({
      NUTSNEWS_PUBLICATION_DEPENDENCY_MODE: "production",
      NUTSNEWS_PUBLICATION_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_PUBLICATION_DATABASE_URL: "postgres://example.invalid/publication",
      NUTSNEWS_PUBLICATION_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL: "https://backend.example.invalid/worker",
      NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN: "secret-not-real",
      NUTSNEWS_PUBLICATION_WRITE_MODE: "production",
      NUTSNEWS_PUBLICATION_PRODUCTION_WRITE_CONFIRMATION: PUBLICATION_PRODUCTION_CONFIRMATION,
      NUTSNEWS_PUBLICATION_TELEMETRY_LOGS: "silent"
    });

    expect(config.writeMode).toBe("production");
    expect(config.buildRevision).toBe("0123456789abcdef0123456789abcdef01234567");
    expect(config.security.productionWriteConfirmationPresent).toBe(true);
    expect(config.dependencies).toEqual({
      databaseConfigured: true,
      rabbitmqConfigured: true,
      backendApiConfigured: true,
      backendApiCredentialConfigured: true
    });
    expect(JSON.stringify(config)).not.toContain("postgres://example.invalid");
    expect(JSON.stringify(config)).not.toContain("amqp://example.invalid");
    expect(JSON.stringify(config)).not.toContain("backend.example.invalid");
    expect(JSON.stringify(config)).not.toContain("secret-not-real");
    expect(JSON.stringify(config)).not.toContain(PUBLICATION_PRODUCTION_CONFIRMATION);
  });

  it("rejects unsafe bounds", () => {
    expect(() => loadPublicationConfig({
      NUTSNEWS_PUBLICATION_CONCURRENCY: "4",
      NUTSNEWS_PUBLICATION_PREFETCH: "2"
    })).toThrow(PublicationConfigError);
  });
});
