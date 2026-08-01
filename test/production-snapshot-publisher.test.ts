import type { Pool } from "pg";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import type {
  PublicationConfig
} from "../src/config.js";
import type {
  PublicationSnapshotCommand
} from "../src/dependencies.js";
import {
  PostgresPublicationSnapshotPublisher
} from "../src/production.js";
import {
  createPublicationService
} from "../src/service.js";
import {
  LocalPublicationSnapshotPublisher,
  createLocalPublicationDependencies,
  createMinimalPublicationDelivery,
  createProductionCapableLocalPublicationConfig
} from "../src/test-doubles.js";

async function productionCommand(): Promise<{
  readonly command: PublicationSnapshotCommand;
  readonly config: PublicationConfig;
}> {
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
  await service.stop();

  const command = publisher.productionPublishes[0];

  if (command === undefined) {
    throw new Error("production command fixture was not captured");
  }

  return {
    command,
    config
  };
}

function fakePool() {
  const query = vi.fn().mockResolvedValue({
    rowCount: 1,
    rows: []
  });

  return {
    pool: {
      query
    } as unknown as Pool,
    query
  };
}

function requestBody(init: RequestInit | undefined): Record<string, unknown> {
  if (typeof init?.body !== "string") {
    throw new Error("request fixture body is not JSON text");
  }

  return JSON.parse(init.body) as Record<string, unknown>;
}

describe("production snapshot publisher boundary", () => {
  it("sends the exact article and language scope and validates both receipts", async () => {
    const { command, config } = await productionCommand();
    const calls: {
      readonly input: string;
      readonly init: RequestInit | undefined;
    }[] = [];
    const fetcher = vi.fn((input: string, init?: RequestInit) => {
      calls.push({ input, init });

      if (input.endsWith("/uplift-publish-articles-batch")) {
        return Promise.resolve(Response.json({
          ok: true,
          requestedCount: 1,
          publishedCount: 1,
          blockedCount: 0,
          missingTranslations: []
        }));
      }

      return Promise.resolve(Response.json({
        refreshedAt: "2026-08-01T20:00:00Z"
      }));
    });
    const { pool, query } = fakePool();
    const publisher = new PostgresPublicationSnapshotPublisher({
      pool,
      config,
      clock: createLocalPublicationDependencies(config).clock,
      baseUrl: "https://backend.example.invalid/api/worker/db",
      token: "test-publication-token",
      fetcher
    });

    await expect(publisher.publishProductionSnapshot(command)).resolves.toMatchObject({
      accepted: true,
      mode: "production",
      snapshotRefreshRequested: true
    });

    expect(calls).toHaveLength(2);
    const publishBody = requestBody(calls[0]?.init);
    const refreshBody = requestBody(calls[1]?.init);

    expect(calls[0]?.input).toMatch(/\/uplift-publish-articles-batch$/u);
    expect(publishBody).toMatchObject({
      actorService: "worker-uplift-publication",
      providerMode: "backend_postgres_primary",
      originalUrls: ["https://example.com/article-001"],
      status: "published",
      languageCodes: [
        "fr",
        "ja",
        "de-CH",
        "de",
        "el"
      ]
    });
    expect(calls[1]?.input).toMatch(/\/uplift-refresh-public-feed-snapshot$/u);
    expect(refreshBody).not.toHaveProperty("originalUrls");
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("fails closed when the backend does not confirm one published article", async () => {
    const { command, config } = await productionCommand();
    const { pool, query } = fakePool();
    const publisher = new PostgresPublicationSnapshotPublisher({
      pool,
      config,
      clock: createLocalPublicationDependencies(config).clock,
      baseUrl: "https://backend.example.invalid/api/worker/db",
      token: "test-publication-token",
      fetcher: () => Promise.resolve(Response.json({
        ok: false,
        requestedCount: 1,
        publishedCount: 0,
        blockedCount: 1,
        missingTranslations: [{ languageCode: "fr" }]
      }))
    });

    await expect(publisher.publishProductionSnapshot(command)).rejects.toThrow(
      "did not confirm one published article"
    );
    expect(query).not.toHaveBeenCalled();
  });

  it("fails closed when snapshot refresh lacks a completion timestamp", async () => {
    const { command, config } = await productionCommand();
    const { pool, query } = fakePool();
    let requestCount = 0;
    const publisher = new PostgresPublicationSnapshotPublisher({
      pool,
      config,
      clock: createLocalPublicationDependencies(config).clock,
      baseUrl: "https://backend.example.invalid/api/worker/db",
      token: "test-publication-token",
      fetcher: () => {
        requestCount += 1;
        return Promise.resolve(requestCount === 1
          ? Response.json({
              ok: true,
              requestedCount: 1,
              publishedCount: 1,
              blockedCount: 0,
              missingTranslations: []
            })
          : Response.json({}));
      }
    });

    await expect(publisher.publishProductionSnapshot(command)).rejects.toThrow(
      "refresh did not return a completion timestamp"
    );
    expect(query).not.toHaveBeenCalled();
  });
});
