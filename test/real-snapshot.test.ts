import * as assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as waitFor } from "node:timers/promises";
import config from "../src/config.ts";
import type { Snapshot } from "../src/index.ts";
import { SnapshotService } from "../src/index.ts";
import type { SnapshotAsset, SnapshotWindow } from "../src/snapshot/snapshot.types.ts";

const REAL_SNAPSHOT_TEST_DURATION_MS = 60_000;
const REAL_SNAPSHOT_TEST_OPTIONS = { skip: Reflect.get(globalThis, "__RUN_REAL_SNAPSHOT_TEST__") !== true, timeout: REAL_SNAPSHOT_TEST_DURATION_MS + 15_000 };

function readSnapshotNumber(snapshot: Snapshot, columnName: string): number | null {
  const snapshotValue = snapshot[columnName];
  const columnValue = typeof snapshotValue === "number" ? snapshotValue : null;
  return columnValue;
}

function readSnapshotString(snapshot: Snapshot, columnName: string): string | null {
  const snapshotValue = snapshot[columnName];
  const columnValue = typeof snapshotValue === "string" ? snapshotValue : null;
  return columnValue;
}

function formatNumber(value: number | null, digits = 4): string {
  const formattedValue = value === null ? "-" : value.toFixed(digits);
  return formattedValue;
}

function formatPrice(value: number | null): string {
  const formattedPrice = value === null ? "-" : value.toLocaleString("en-US", { maximumFractionDigits: 2 });
  return formattedPrice;
}

function formatIso(value: number): string {
  const isoValue = new Date(value).toISOString();
  return isoValue;
}

function buildCryptoLine(snapshot: Snapshot, asset: SnapshotAsset): string {
  const cryptoLine = [
    `${asset.toUpperCase().padEnd(3)} |`,
    `binance ${formatPrice(readSnapshotNumber(snapshot, `${asset}_binance_price`)).padStart(10)}`,
    `coinbase ${formatPrice(readSnapshotNumber(snapshot, `${asset}_coinbase_price`)).padStart(10)}`,
    `kraken ${formatPrice(readSnapshotNumber(snapshot, `${asset}_kraken_price`)).padStart(10)}`,
    `okx ${formatPrice(readSnapshotNumber(snapshot, `${asset}_okx_price`)).padStart(10)}`,
    `chainlink ${formatPrice(readSnapshotNumber(snapshot, `${asset}_chainlink_price`)).padStart(10)}`,
  ].join(" ");
  return cryptoLine;
}

function buildMarketLine(snapshot: Snapshot, asset: SnapshotAsset, window: SnapshotWindow): string {
  const prefix = `${asset}_${window}`;
  const slug = readSnapshotString(snapshot, `${prefix}_slug`) ?? "-";
  const marketLine = [
    `${asset.toUpperCase()} ${window.padEnd(3)} |`,
    `slug ${slug}`,
    `| up ${formatNumber(readSnapshotNumber(snapshot, `${prefix}_up_price`)).padStart(8)}`,
    `| down ${formatNumber(readSnapshotNumber(snapshot, `${prefix}_down_price`)).padStart(8)}`,
  ].join(" ");
  return marketLine;
}

function buildChangeLines(snapshot: Snapshot, previousSnapshot: Snapshot | null): string[] {
  const changeLines: string[] = [];

  if (previousSnapshot !== null) {
    for (const asset of config.DEFAULT_SUPPORTED_ASSETS) {
      for (const window of config.DEFAULT_SUPPORTED_WINDOWS) {
        const prefix = `${asset}_${window}`;
        const previousSlug = readSnapshotString(previousSnapshot, `${prefix}_slug`);
        const currentSlug = readSnapshotString(snapshot, `${prefix}_slug`);

        if (previousSlug !== currentSlug) {
          changeLines.push(`${asset.toUpperCase()} ${window} slug: ${previousSlug ?? "-"} -> ${currentSlug ?? "-"}`);
        }
      }
    }
  }

  if (changeLines.length === 0) {
    changeLines.push("No market slug changes detected yet.");
  }

  return changeLines;
}

function buildDashboard(snapshot: Snapshot, previousSnapshot: Snapshot | null, snapshotCount: number): string {
  const cryptoLines = config.DEFAULT_SUPPORTED_ASSETS.map((asset) => buildCryptoLine(snapshot, asset));
  const marketLines = config.DEFAULT_SUPPORTED_ASSETS.flatMap((asset) =>
    config.DEFAULT_SUPPORTED_WINDOWS.map((window) => buildMarketLine(snapshot, asset, window)),
  );
  const changeLines = buildChangeLines(snapshot, previousSnapshot);
  const dashboard = [
    "=== Polymarket Snapshot Dashboard ===",
    `snapshot #${snapshotCount}`,
    `generated_at: ${snapshot.generated_at} (${formatIso(snapshot.generated_at)})`,
    "",
    "Crypto",
    ...cryptoLines,
    "",
    "Markets",
    ...marketLines,
    "",
    "Changes",
    ...changeLines,
  ].join("\n");
  return dashboard;
}

function renderDashboard(snapshot: Snapshot, previousSnapshot: Snapshot | null, snapshotCount: number): void {
  const dashboard = buildDashboard(snapshot, previousSnapshot, snapshotCount);

  if (process.stdout.isTTY) {
    process.stdout.write("\x1Bc");
  } else {
    console.log("\n--- snapshot update ---");
  }

  console.log(dashboard);
}

test("SnapshotService renders a readable live dashboard for 60 seconds", REAL_SNAPSHOT_TEST_OPTIONS, async () => {
  const snapshotService = new SnapshotService(1000);
  let latestSnapshot: Snapshot | null = null;
  let snapshotCount = 0;
  const snapshotListener = (snapshot: Snapshot): void => {
    const previousSnapshot = latestSnapshot;

    snapshotCount += 1;
    renderDashboard(snapshot, previousSnapshot, snapshotCount);
    latestSnapshot = snapshot;
  };

  snapshotService.addSnapshotListener({ listener: snapshotListener });
  await waitFor(REAL_SNAPSHOT_TEST_DURATION_MS);
  snapshotService.removeSnapshotListener(snapshotListener);
  await snapshotService.disconnect();

  assert.notEqual(latestSnapshot, null);

  if (latestSnapshot !== null) {
    assert.ok(Object.keys(latestSnapshot).length > 2);
  }
});
