import {
  describe,
  expect,
  it
} from "vitest";

import { PostgresPublicationTerminalReconciler } from "../src/production.js";
import { PUBLICATION_RECONCILIATION_CONFIRMATION } from "../src/reconciliation.js";

const clock = {
  now: () => new Date("2026-07-23T00:00:00.000Z")
};

describe("publication terminal reconciliation", () => {
  it("dry-runs as a terminal no-op without enabling production visibility", async () => {
    const reconciler = new PostgresPublicationTerminalReconciler({
      clock,
      env: {}
    });

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723"
    });

    expect(report).toMatchObject({
      service: "publication",
      status: "dry_run",
      terminalStage: true,
      selectedCount: 0,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
  });

  it("requires protected apply confirmation and still performs no production writes", async () => {
    const reconciler = new PostgresPublicationTerminalReconciler({
      clock,
      env: {
        NUTSNEWS_PUBLICATION_RECONCILIATION_APPLY_ENABLED: "true"
      }
    });

    const blocked = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-20260723"
    });
    expect(blocked).toMatchObject({
      status: "failed_closed",
      writesPerformed: false,
      productionVisibilityEnabled: false
    });

    const applied = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-20260723",
      protectedConfirmation: PUBLICATION_RECONCILIATION_CONFIRMATION
    });
    expect(applied).toMatchObject({
      status: "applied",
      terminalStage: true,
      writesPerformed: false,
      productionVisibilityEnabled: false
    });
  });
});
