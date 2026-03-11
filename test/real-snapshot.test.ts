import * as assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as waitFor } from "node:timers/promises";

import { SnapshotService } from "../src/index.ts";
import type { Snapshot } from "../src/index.ts";

const REAL_SNAPSHOT_TEST_DURATION_MS = 60_000;
const SHOULD_RUN_REAL_SNAPSHOT_TEST = Reflect.get(globalThis, "__RUN_REAL_SNAPSHOT_TEST__") === true;

test(
  "SnapshotService logs live snapshots for 60 seconds",
  { skip: !SHOULD_RUN_REAL_SNAPSHOT_TEST, timeout: REAL_SNAPSHOT_TEST_DURATION_MS + 15_000 },
  async () => {
    const snapshotService = SnapshotService.createDefault();
    const snapshotListener = (snapshot: Snapshot): void => console.log(snapshot);

    snapshotService.addSnapshotListener({ listener: snapshotListener, assets: ["btc"], windows: ["5m"] });

    await waitFor(REAL_SNAPSHOT_TEST_DURATION_MS);

    const latestSnapshots = snapshotService.getSnapshot({ assets: ["btc"], windows: ["5m"] });
    snapshotService.removeSnapshotListener(snapshotListener);
    await snapshotService.disconnect();

    assert.ok(latestSnapshots.length > 0);
  },
);
