import * as assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as waitFor } from "node:timers/promises";
import type { Snapshot } from "../src/index.ts";
import { SnapshotService } from "../src/index.ts";

const REAL_SNAPSHOT_TEST_DURATION_MS = 60_000;
const SHOULD_RUN_REAL_SNAPSHOT_TEST = Reflect.get(globalThis, "__RUN_REAL_SNAPSHOT_TEST__") === true;
const REAL_SNAPSHOT_TEST_OPTIONS = { skip: !SHOULD_RUN_REAL_SNAPSHOT_TEST, timeout: REAL_SNAPSHOT_TEST_DURATION_MS + 15_000 };

test("SnapshotService logs live snapshots for 60 seconds", REAL_SNAPSHOT_TEST_OPTIONS, async () => {
  const snapshotService = new SnapshotService();
  const snapshotListener = (snapshot: Snapshot): void => console.log(snapshot);

  snapshotService.addSnapshotListener({ listener: snapshotListener });

  await waitFor(REAL_SNAPSHOT_TEST_DURATION_MS);

  const latestSnapshot = snapshotService.getSnapshot();
  snapshotService.removeSnapshotListener(snapshotListener);
  await snapshotService.disconnect();

  assert.notEqual(latestSnapshot, null);

  if (latestSnapshot !== null) {
    assert.ok(Object.keys(latestSnapshot).length > 2);
  }
});
