import { performance } from "node:perf_hooks";

import { WORKER_DELIVERY_BEHAVIOR } from "@ramideltoro/nutsnews-worker-contracts";

export const PUBLICATION_INBOX_CLAIM_LEASE_MS = 300_000;
export const PUBLICATION_HANDLER_DEADLINE_MS = 150_000;
export const PUBLICATION_DATABASE_CONNECTION_TIMEOUT_MS = 10_000;
export const PUBLICATION_DATABASE_QUERY_TIMEOUT_MS = 20_000;
export const PUBLICATION_DATABASE_STATEMENT_TIMEOUT_MS = 15_000;
export const PUBLICATION_DATABASE_LOCK_TIMEOUT_MS = 5_000;
export const PUBLICATION_DATABASE_IDLE_TRANSACTION_TIMEOUT_MS = 30_000;
export const PUBLICATION_BACKEND_HEALTH_TIMEOUT_MS = 5_000;
export const PUBLICATION_BACKEND_REQUEST_TIMEOUT_MS = 10_000;

const PUBLICATION_CLAIM_RESPONSE_MARGIN_MS = PUBLICATION_DATABASE_QUERY_TIMEOUT_MS;
const PUBLICATION_MAX_FINAL_TRANSITION_DATABASE_OPERATIONS = 3;
const PUBLICATION_FINAL_TRANSITION_SETTLEMENT_MS = PUBLICATION_MAX_FINAL_TRANSITION_DATABASE_OPERATIONS
  * (PUBLICATION_DATABASE_CONNECTION_TIMEOUT_MS + PUBLICATION_DATABASE_QUERY_TIMEOUT_MS);
const PUBLICATION_EXTERNAL_OPERATION_TAIL_MS = Math.max(
  PUBLICATION_FINAL_TRANSITION_SETTLEMENT_MS,
  PUBLICATION_BACKEND_REQUEST_TIMEOUT_MS,
  WORKER_DELIVERY_BEHAVIOR.confirmTimeoutMs
);
const PUBLICATION_BOUNDED_WALL_MS = PUBLICATION_CLAIM_RESPONSE_MARGIN_MS
  + PUBLICATION_HANDLER_DEADLINE_MS
  + PUBLICATION_EXTERNAL_OPERATION_TAIL_MS;

export const PUBLICATION_CLAIMED_OPERATION_BUDGET = {
  leaseMs: PUBLICATION_INBOX_CLAIM_LEASE_MS,
  claimResponseMarginMs: PUBLICATION_CLAIM_RESPONSE_MARGIN_MS,
  handlerDeadlineMs: PUBLICATION_HANDLER_DEADLINE_MS,
  databaseConnectionTimeoutMs: PUBLICATION_DATABASE_CONNECTION_TIMEOUT_MS,
  databaseQueryTimeoutMs: PUBLICATION_DATABASE_QUERY_TIMEOUT_MS,
  backendRequestTimeoutMs: PUBLICATION_BACKEND_REQUEST_TIMEOUT_MS,
  brokerConfirmTimeoutMs: WORKER_DELIVERY_BEHAVIOR.confirmTimeoutMs,
  finalTransitionDatabaseOperations: PUBLICATION_MAX_FINAL_TRANSITION_DATABASE_OPERATIONS,
  settlementMarginMs: PUBLICATION_FINAL_TRANSITION_SETTLEMENT_MS,
  boundedWallMs: PUBLICATION_BOUNDED_WALL_MS,
  safetyMarginMs: PUBLICATION_INBOX_CLAIM_LEASE_MS - PUBLICATION_BOUNDED_WALL_MS
} as const;

if (PUBLICATION_CLAIMED_OPERATION_BUDGET.safetyMarginMs <= 0) {
  throw new Error("Publication claimed-operation deadline must remain below its inbox lease.");
}

export interface PublicationOperationDeadline {
  readonly signal: AbortSignal;
  assertActive(): void;
  dispose(): void;
}

export class PublicationOperationDeadlineError extends Error {
  constructor() {
    super("publication-handler-deadline-exceeded");
    this.name = "PublicationOperationDeadlineError";
  }
}

export function createPublicationOperationDeadline(
  timeoutMs = PUBLICATION_HANDLER_DEADLINE_MS
): PublicationOperationDeadline {
  const controller = new AbortController();
  const expiresAtMs = performance.now() + timeoutMs;
  const timer = setTimeout(() => {
    controller.abort(new PublicationOperationDeadlineError());
  }, timeoutMs);
  timer.unref();

  return {
    signal: controller.signal,
    assertActive(): void {
      if (controller.signal.aborted || performance.now() >= expiresAtMs) {
        if (!controller.signal.aborted) {
          controller.abort(new PublicationOperationDeadlineError());
        }
        throw new PublicationOperationDeadlineError();
      }
    },
    dispose(): void {
      clearTimeout(timer);
    }
  };
}

export function assertPublicationOperationActive(
  deadline: PublicationOperationDeadline | undefined
): void {
  deadline?.assertActive();
}
