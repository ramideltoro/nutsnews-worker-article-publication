import { readFile } from "node:fs/promises";

import {
  describe,
  expect,
  it
} from "vitest";

import {
  PUBLICATION_CLAIMED_OPERATION_BUDGET,
  createPublicationOperationDeadline
} from "../src/operation-deadline.js";
import { createProductionPublicationDependencies } from "../src/production.js";
import {
  ManualPublicationClock,
  createProductionCapableLocalPublicationConfig
} from "../src/test-doubles.js";

describe("publication image identity contract", () => {
  it("bakes the immutable revision and uses liveness for shadow container health", async () => {
    const [dockerfile, ciWorkflow, publishWorkflow] = await Promise.all([
      readFile(new URL("../Dockerfile", import.meta.url), "utf8"),
      readFile(new URL("../.github/workflows/ci.yml", import.meta.url), "utf8"),
      readFile(new URL("../.github/workflows/publish-container.yml", import.meta.url), "utf8")
    ]);

    expect(dockerfile).toContain("ARG BUILD_REVISION=development");
    expect(dockerfile).toContain("NUTSNEWS_PUBLICATION_BUILD_REVISION=${BUILD_REVISION}");
    expect(dockerfile).toContain("http://127.0.0.1:8080/live");
    expect(ciWorkflow).toContain("--build-arg BUILD_REVISION=${{ github.sha }}");
    expect(publishWorkflow).toContain("BUILD_REVISION=${{ github.sha }}");
  });

  it("keeps every production dependency bound inside the five-minute claim lease", async () => {
    const [production, rabbitMqTransport] = await Promise.all([
      readFile(new URL("../src/production.ts", import.meta.url), "utf8"),
      readFile(new URL("../src/rabbitmq-payload-transport.ts", import.meta.url), "utf8")
    ]);

    expect(PUBLICATION_CLAIMED_OPERATION_BUDGET).toEqual({
      leaseMs: 300_000,
      claimResponseMarginMs: 20_000,
      handlerDeadlineMs: 150_000,
      databaseConnectionTimeoutMs: 10_000,
      databaseQueryTimeoutMs: 20_000,
      backendRequestTimeoutMs: 10_000,
      brokerConfirmTimeoutMs: 5_000,
      finalTransitionDatabaseOperations: 3,
      settlementMarginMs: 90_000,
      boundedWallMs: 260_000,
      safetyMarginMs: 40_000
    });
    expect(PUBLICATION_CLAIMED_OPERATION_BUDGET.safetyMarginMs).toBeGreaterThanOrEqual(30_000);
    expect(production).toContain("query_timeout: PUBLICATION_DATABASE_QUERY_TIMEOUT_MS");
    expect(production).toContain("statement_timeout: PUBLICATION_DATABASE_STATEMENT_TIMEOUT_MS");
    expect(production).toContain("lock_timeout: PUBLICATION_DATABASE_LOCK_TIMEOUT_MS");
    expect(production).toContain("idle_in_transaction_session_timeout: PUBLICATION_DATABASE_IDLE_TRANSACTION_TIMEOUT_MS");
    expect(production).toContain("publicationRequestSignal(deadline, PUBLICATION_BACKEND_REQUEST_TIMEOUT_MS)");
    expect(production).toContain("safePublicationDatabaseUrl");
    expect(production).toContain("options: publicationDatabaseStartupOptions()");
    expect(rabbitMqTransport).toContain("DEFAULT_CONFIRM_TIMEOUT_MS = WORKER_DELIVERY_BEHAVIOR.confirmTimeoutMs");
  });

  it.each([
    "connect_timeout=0",
    "query_timeout=0",
    "statement_timeout=0",
    "lock_timeout=0",
    "idle_in_transaction_session_timeout=0",
    "options=-c%20statement_timeout%3D0"
  ])("rejects a database URL that attempts to override timeout policy with %s", (query) => {
    const config = createProductionCapableLocalPublicationConfig();

    expect(() => createProductionPublicationDependencies({
      config,
      clock: new ManualPublicationClock(),
      env: {
        NUTSNEWS_PUBLICATION_DATABASE_URL: `postgres://user:credential@example.invalid/publication?${query}`,
        NUTSNEWS_PUBLICATION_RABBITMQ_URL: "amqp://example.invalid",
        NUTSNEWS_PUBLICATION_BACKEND_API_BASE_URL: "https://backend.example.invalid/worker",
        NUTSNEWS_PUBLICATION_BACKEND_API_TOKEN: "secret-not-real"
      }
    })).toThrow(/must not override reserved parameter/u);
  });

  it("fails the monotonic deadline closed even when its timer callback has not run", () => {
    const deadline = createPublicationOperationDeadline(0);

    expect(() => deadline.assertActive()).toThrow("publication-handler-deadline-exceeded");
    expect(deadline.signal.aborted).toBe(true);
    deadline.dispose();
  });
});
