import { WORKER_DELIVERY_BEHAVIOR } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createRuntimeMessageProcessor,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure
} from "@ramideltoro/nutsnews-worker-runtime";
import type { Pool } from "pg";
import {
  describe,
  expect,
  it
} from "vitest";

import { PublicationOperationDeadlineError } from "../src/operation-deadline.js";
import { PostgresPublicationInboxStore } from "../src/production.js";
import {
  createMinimalPublicationDelivery,
  createMinimalPublicationEnvelope
} from "../src/test-doubles.js";

const RECEIVED_AT = "2026-08-01T12:00:00.000Z";
const FIRST_SEEN_AT = new Date("2026-08-01T11:45:00.000Z");
const CLAIM_TOKEN = "owned-publication-claim-token";

describe("Postgres publication inbox Runtime 1.0 conformance", () => {
  it("creates a unique opaque token with the exact five-minute production lease", async () => {
    const { pool, store } = scriptedStore([
      response(1, {
        received_at: FIRST_SEEN_AT
      })
    ]);

    const claim = await store.claim("publication:test:claim", claimContext());

    expect(claim).toMatchObject({
      status: "claimed",
      firstSeenAt: FIRST_SEEN_AT.toISOString(),
      replay: false
    });
    if (claim.status !== "claimed") {
      throw new Error("Expected a claimed publication inbox record.");
    }

    expect(claim.claimToken).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u);
    const insert = pool.calls[0];
    expect(jsonObject(insert?.values[13])).not.toHaveProperty("claimToken");
    expect(insert?.values[14]).toBe(claim.claimToken);
    expect(insert?.text).toContain("ON CONFLICT (idempotency_key) DO NOTHING");
    expect(insert?.text).toContain("statement_timestamp() + interval '300 seconds'");
    expect(insert?.text).toContain("extract(epoch FROM statement_timestamp() + interval '300 seconds')");
  });

  it("atomically reclaims failed or expired rows without stealing an active lease", async () => {
    const reclaimed = scriptedStore([
      response(0),
      response(0),
      response(1, {
        received_at: FIRST_SEEN_AT
      })
    ]);

    const claim = await reclaimed.store.claim("publication:test:reclaim", claimContext());

    expect(claim).toMatchObject({
      status: "claimed",
      firstSeenAt: FIRST_SEEN_AT.toISOString(),
      replay: true
    });
    if (claim.status !== "claimed") {
      throw new Error("Expected an atomically reclaimed publication inbox record.");
    }

    const reclaim = reclaimed.pool.calls[2];
    expect(reclaim?.text).toContain("status IN ('failed', 'parked')");
    expect(reclaim?.text).toContain("jsonb_typeof(diagnostic_metadata->'claimExpiresAtEpochMs') = 'number'");
    expect(reclaim?.text).toContain("RETURNING received_at");
    expect(reclaim?.text).toContain("<= floor(extract(epoch FROM statement_timestamp())");
    expect(reclaim?.text).toContain("statement_timestamp() + interval '300 seconds'");
    expect(reclaim?.values[2]).toBe(claim.claimToken);
    expect(reclaim?.values[3]).toBe(claimContext().envelope.messageId);
    expect(jsonObject(reclaim?.values[1])).not.toHaveProperty("claimToken");

    const active = scriptedStore([
      response(0),
      response(0),
      response(0),
      response(1, {
        status: "processing",
        received_at: FIRST_SEEN_AT,
        processed_at: null
      })
    ]);

    await expect(active.store.claim("publication:test:active", claimContext())).resolves.toEqual({
      status: "in-progress",
      firstSeenAt: FIRST_SEEN_AT.toISOString()
    });
  });

  it("gives a legacy tokenless processing row a full grace lease before reclaim", async () => {
    const legacy = scriptedStore([
      response(0),
      response(1),
      response(0),
      response(1, {
        status: "processing",
        received_at: FIRST_SEEN_AT,
        processed_at: null
      })
    ]);

    await expect(legacy.store.claim("publication:test:legacy", claimContext())).resolves.toEqual({
      status: "in-progress",
      firstSeenAt: FIRST_SEEN_AT.toISOString()
    });
    const adoption = legacy.pool.calls[1];
    expect(adoption?.text).toContain("IS DISTINCT FROM 'number'");
    expect(adoption?.text).toContain("legacyClaimObservedAt', to_jsonb(statement_timestamp())");
    expect(adoption?.text).toContain("statement_timestamp() + interval '300 seconds'");
    expect(adoption?.values).toEqual([
      "publication:test:legacy"
    ]);
  });

  it("fences completion and failure updates on both processing state and the exact claim token", async () => {
    const completed = scriptedStore([
      response(1)
    ]);
    await expect(completed.store.markCompleted(
      "publication:test:complete",
      completion()
    )).resolves.toBeUndefined();
    expect(completed.pool.calls[0]?.text).toContain("status = 'processing'");
    expect(completed.pool.calls[0]?.text).toContain("diagnostic_metadata->>'claimToken' = $4");
    expect(completed.pool.calls[0]?.text).toContain("(diagnostic_metadata->>'claimExpiresAtEpochMs')::numeric");
    expect(completed.pool.calls[0]?.text).toContain("> floor(extract(epoch FROM statement_timestamp())");
    expect(completed.pool.calls[0]?.values[3]).toBe(CLAIM_TOKEN);

    const failed = scriptedStore([
      response(1)
    ]);
    await expect(failed.store.markFailed(
      "publication:test:fail",
      failure()
    )).resolves.toBeUndefined();
    expect(failed.pool.calls[0]?.text).toContain("status = 'processing'");
    expect(failed.pool.calls[0]?.text).toContain("diagnostic_metadata->>'claimToken' = $5");
    expect(failed.pool.calls[0]?.text).toContain("(diagnostic_metadata->>'claimExpiresAtEpochMs')::numeric");
    expect(failed.pool.calls[0]?.text).toContain("> floor(extract(epoch FROM statement_timestamp())");
    expect(failed.pool.calls[0]?.values[4]).toBe(CLAIM_TOKEN);

    const notOwned = scriptedStore([
      response(0),
      response(0)
    ]);
    await expect(notOwned.store.markCompleted(
      "publication:test:not-owned-completion",
      completion()
    )).rejects.toThrow(/owned by another delivery/u);
    await expect(notOwned.store.markFailed(
      "publication:test:not-owned-failure",
      failure()
    )).rejects.toThrow(/owned by another delivery/u);
    expect(notOwned.pool.calls[1]?.text).toContain("status = 'processing'");
  });

  it("conditionally releases only its own claim and preserves completed work", async () => {
    const owned = scriptedStore([
      response(1)
    ]);
    await expect(owned.store.releaseClaim(
      "publication:test:owned-release",
      failure()
    )).resolves.toEqual({
      status: "released"
    });
    expect(owned.pool.calls[0]?.text).toContain("diagnostic_metadata->>'claimToken' = $5");
    expect(owned.pool.calls[0]?.text).toContain("(diagnostic_metadata->>'claimExpiresAtEpochMs')::numeric");
    expect(owned.pool.calls[0]?.text).toContain("> floor(extract(epoch FROM statement_timestamp())");
    expect(owned.pool.calls[0]?.values[4]).toBe(CLAIM_TOKEN);

    const completed = scriptedStore([
      response(0),
      response(1, {
        status: "processed"
      })
    ]);
    await expect(completed.store.releaseClaim(
      "publication:test:completed-release",
      failure()
    )).resolves.toEqual({
      status: "preserved-completed"
    });

    const anotherOwner = scriptedStore([
      response(0),
      response(1, {
        status: "processing"
      })
    ]);
    await expect(anotherOwner.store.releaseClaim(
      "publication:test:foreign-release",
      failure()
    )).resolves.toEqual({
      status: "not-owned"
    });
  });

  it("does not start a final inbox transition after the claimed-operation deadline", async () => {
    const { pool, store } = scriptedStore([]);
    const expiredDeadline = {
      signal: AbortSignal.abort(new PublicationOperationDeadlineError()),
      assertActive(): void {
        throw new PublicationOperationDeadlineError();
      },
      dispose(): void {}
    };

    await expect(store.markCompleted(
      "publication:test:expired-completion",
      completion(),
      expiredDeadline
    )).rejects.toThrow("publication-handler-deadline-exceeded");
    await expect(store.markFailed(
      "publication:test:expired-failure",
      failure(),
      expiredDeadline
    )).rejects.toThrow("publication-handler-deadline-exceeded");
    await expect(store.releaseClaim(
      "publication:test:expired-release",
      failure(),
      expiredDeadline
    )).rejects.toThrow("publication-handler-deadline-exceeded");
    expect(pool.calls).toHaveLength(0);
  });

  it("does not rerun a handler after an ambiguous claim response", async () => {
    const { pool, store } = scriptedStore([
      new Error("claim response lost after commit"),
      response(0),
      response(0),
      response(0),
      response(1, {
        status: "processing",
        received_at: FIRST_SEEN_AT,
        processed_at: null
      })
    ]);
    let handlerCalls = 0;
    const processor = createRuntimeMessageProcessor({
      stage: "publication",
      idempotencyStore: store,
      handler: () => {
        handlerCalls += 1;
        return {
          status: "ok"
        };
      }
    });

    await expect(processor(createMinimalPublicationDelivery())).resolves.toMatchObject({
      action: "retry",
      reason: "idempotency-claim-error"
    });
    await expect(processor(createMinimalPublicationDelivery())).resolves.toMatchObject({
      action: "retry",
      reason: "idempotency-in-progress"
    });
    expect(handlerCalls).toBe(0);
    expect(pool.calls).toHaveLength(5);
  });

  it("releases a claim when completion rejects before commit so redelivery can finish", async () => {
    const { pool, store } = scriptedStore([
      response(1, {
        received_at: FIRST_SEEN_AT
      }),
      new Error("completion rejected before commit"),
      response(1),
      response(0),
      response(0),
      response(1, {
        received_at: FIRST_SEEN_AT
      }),
      response(1)
    ]);
    let handlerCalls = 0;
    const processor = createRuntimeMessageProcessor({
      stage: "publication",
      idempotencyStore: store,
      handler: () => {
        handlerCalls += 1;
        return {
          status: "ok"
        };
      }
    });

    await expect(processor(createMinimalPublicationDelivery())).resolves.toMatchObject({
      action: "retry",
      reason: "idempotency-completion-error"
    });
    await expect(processor(createMinimalPublicationDelivery())).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(handlerCalls).toBe(2);
    expect(pool.calls[2]?.text).toContain("status = 'processing'");
    expect(pool.calls[2]?.values[4]).toBe(pool.calls[0]?.values[14]);
    expect(pool.calls[5]?.values[2]).not.toBe(pool.calls[0]?.values[14]);
    expect(pool.calls).toHaveLength(7);
  });

  it("acknowledges final-attempt work when completion committed but its response was lost", async () => {
    const { pool, store } = scriptedStore([
      response(1, {
        received_at: FIRST_SEEN_AT
      }),
      new Error("completion response lost after commit"),
      response(0),
      response(1, {
        status: "processed"
      })
    ]);
    const processor = createRuntimeMessageProcessor({
      stage: "publication",
      idempotencyStore: store,
      handler: () => ({
        status: "ok"
      })
    });

    await expect(processor(finalAttemptDelivery())).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    const claimToken = pool.calls[0]?.values[14];
    expect(pool.calls[1]?.values[3]).toBe(claimToken);
    expect(pool.calls[2]?.values[4]).toBe(claimToken);
    expect(pool.calls).toHaveLength(4);
  });
});

interface QueryCall {
  readonly text: string;
  readonly values: readonly unknown[];
}

interface QueryResponse {
  readonly rowCount: number | null;
  readonly rows: readonly Record<string, unknown>[];
}

class ScriptedPool {
  readonly calls: QueryCall[] = [];
  private readonly responses: (QueryResponse | Error)[];

  constructor(responses: readonly (QueryResponse | Error)[]) {
    this.responses = [...responses];
  }

  query(text: string, values: readonly unknown[] = []): Promise<QueryResponse> {
    this.calls.push({
      text,
      values
    });
    const next = this.responses.shift();

    if (next === undefined) {
      return Promise.reject(new Error("Unexpected publication inbox query."));
    }
    if (next instanceof Error) {
      return Promise.reject(next);
    }

    return Promise.resolve(next);
  }
}

function scriptedStore(responses: readonly (QueryResponse | Error)[]): {
  readonly pool: ScriptedPool;
  readonly store: PostgresPublicationInboxStore;
} {
  const pool = new ScriptedPool(responses);

  return {
    pool,
    store: new PostgresPublicationInboxStore(pool as unknown as Pool)
  };
}

function response(
  rowCount: number,
  ...rows: readonly Record<string, unknown>[]
): QueryResponse {
  return {
    rowCount,
    rows
  };
}

function claimContext(): RuntimeIdempotencyClaimContext {
  return {
    envelope: createMinimalPublicationEnvelope(),
    stage: "publication",
    receivedAt: RECEIVED_AT
  };
}

function completion(): RuntimeIdempotencyCompletion {
  return {
    completedAt: "2026-08-01T12:00:05.000Z",
    messageId: claimContext().envelope.messageId,
    claimToken: CLAIM_TOKEN,
    stage: "publication"
  };
}

function failure(): RuntimeIdempotencyFailure {
  return {
    failedAt: "2026-08-01T12:00:05.000Z",
    messageId: claimContext().envelope.messageId,
    claimToken: CLAIM_TOKEN,
    stage: "publication",
    reason: "idempotency-completion-error",
    retryable: true
  };
}

function finalAttemptDelivery(): ReturnType<typeof createMinimalPublicationDelivery> {
  const delivery = createMinimalPublicationDelivery();

  return {
    ...delivery,
    envelope: createMinimalPublicationEnvelope({
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-08-01T11:00:00.000Z",
        lastAttemptAt: RECEIVED_AT
      }
    })
  };
}

function jsonObject(value: unknown): Readonly<Record<string, unknown>> {
  if (typeof value !== "string") {
    throw new Error("Expected JSON query parameter.");
  }
  const parsed: unknown = JSON.parse(value);

  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("Expected a JSON object query parameter.");
  }

  return parsed as Readonly<Record<string, unknown>>;
}
