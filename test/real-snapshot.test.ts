import * as assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as waitFor } from "node:timers/promises";
import config from "../src/config.ts";
import type { Snapshot } from "../src/index.ts";
import { SnapshotService } from "../src/index.ts";
import type { SnapshotAsset, SnapshotWindow } from "../src/snapshot/snapshot.types.ts";

const REAL_SNAPSHOT_TEST_DURATION_MS = 60_000;
const MAX_LOG_LINES = 8;
const REAL_SNAPSHOT_TEST_OPTIONS = { skip: Reflect.get(globalThis, "__RUN_REAL_SNAPSHOT_TEST__") !== true, timeout: REAL_SNAPSHOT_TEST_DURATION_MS + 15_000 };

type OutputRestoreSet = { restore(): void; writeDashboard(output: string): void };
type StreamWrite = typeof process.stdout.write;

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

function clampConsoleLines(consoleLines: string[]): string[] {
  const visibleConsoleLines = consoleLines.slice(-MAX_LOG_LINES);
  return visibleConsoleLines;
}

function normalizeOutputChunk(chunk: string | Uint8Array): string {
  const normalizedOutputChunk = typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
  return normalizedOutputChunk;
}

function collectOutputLines(consoleLines: string[], chunk: string): void {
  const normalizedChunk = chunk.replaceAll("\r", "");
  const splitLines = normalizedChunk.split("\n");
  const nextLines = splitLines.map((line) => line.trimEnd()).filter((line) => line.length > 0);

  if (nextLines.length > 0) {
    consoleLines.push(...nextLines);
  }
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
  const priceToBeat = formatPrice(readSnapshotNumber(snapshot, `${prefix}_price_to_beat`)).padStart(10);
  const marketStart = readSnapshotString(snapshot, `${prefix}_market_start`) ?? "-";
  const marketEnd = readSnapshotString(snapshot, `${prefix}_market_end`) ?? "-";
  const marketLine = [
    `${asset.toUpperCase()} ${window.padEnd(3)} |`,
    `slug ${slug}`,
    `| start ${marketStart}`,
    `| end ${marketEnd}`,
    `| beat ${priceToBeat}`,
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

function buildDashboard(snapshot: Snapshot, previousSnapshot: Snapshot | null, snapshotCount: number, consoleLines: string[]): string {
  const cryptoLines = config.DEFAULT_SUPPORTED_ASSETS.map((asset) => buildCryptoLine(snapshot, asset));
  const marketLines = config.DEFAULT_SUPPORTED_ASSETS.flatMap((asset) =>
    config.DEFAULT_SUPPORTED_WINDOWS.map((window) => buildMarketLine(snapshot, asset, window)),
  );
  const changeLines = buildChangeLines(snapshot, previousSnapshot);
  const visibleConsoleLines = clampConsoleLines(consoleLines);
  const dashboard = [
    "=== Polymarket Snapshot Live Dashboard ===",
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
    "",
    "Console",
    ...(visibleConsoleLines.length > 0 ? visibleConsoleLines : ["No intercepted logs."]),
  ].join("\n");
  return dashboard;
}

function renderDashboard(
  snapshot: Snapshot,
  previousSnapshot: Snapshot | null,
  snapshotCount: number,
  consoleLines: string[],
  writeDashboard: (output: string) => void,
): void {
  const dashboard = buildDashboard(snapshot, previousSnapshot, snapshotCount, consoleLines);
  const dashboardOutput = process.stdout.isTTY ? `\x1B[?25l\x1B[H\x1B[2J${dashboard}\n` : `\n--- snapshot update ---\n${dashboard}\n`;
  writeDashboard(dashboardOutput);
}

function createInterceptedWrite(options: {
  originalWrite: StreamWrite;
  consoleLines: string[];
  renderCurrentDashboard: () => void;
  readIsRendering: () => boolean;
  setIsRendering: (isRendering: boolean) => void;
}): StreamWrite {
  const interceptedWrite: StreamWrite = ((
    chunk: string | Uint8Array,
    encoding?: BufferEncoding | ((error?: Error | null) => void),
    callback?: (error?: Error | null) => void,
  ): boolean => {
    const shouldCollectOutput = !options.readIsRendering();

    if (shouldCollectOutput) {
      collectOutputLines(options.consoleLines, normalizeOutputChunk(chunk));
      options.setIsRendering(true);
      options.renderCurrentDashboard();
      options.setIsRendering(false);
    }

    let isWritePerformed = true;

    if (!shouldCollectOutput) {
      isWritePerformed = options.originalWrite(chunk, encoding as BufferEncoding, callback);
    }

    return isWritePerformed;
  }) as StreamWrite;
  return interceptedWrite;
}

function restoreOutputWriters(originalStdoutWrite: StreamWrite, originalStderrWrite: StreamWrite): void {
  process.stdout.write = originalStdoutWrite;
  process.stderr.write = originalStderrWrite;

  if (process.stdout.isTTY) {
    originalStdoutWrite("\x1B[?25h");
  }
}

function createRenderingState(): { readIsRendering(): boolean; setIsRendering(isNextRendering: boolean): void } {
  let isRendering = false;
  const readIsRendering = (): boolean => isRendering;

  function setIsRendering(isNextRendering: boolean): void {
    isRendering = isNextRendering;
  }

  return { readIsRendering, setIsRendering };
}

function createOutputRestoreSet(consoleLines: string[], renderCurrentDashboard: () => void): OutputRestoreSet {
  const originalStdoutWrite = process.stdout.write.bind(process.stdout) as StreamWrite;
  const originalStderrWrite = process.stderr.write.bind(process.stderr) as StreamWrite;
  const renderingState = createRenderingState();
  const stdoutOptions = { originalWrite: originalStdoutWrite, consoleLines, renderCurrentDashboard, ...renderingState };
  const stderrOptions = { originalWrite: originalStderrWrite, consoleLines, renderCurrentDashboard, ...renderingState };

  function writeDashboard(output: string): void {
    originalStdoutWrite(output);
  }

  function restore(): void {
    restoreOutputWriters(originalStdoutWrite, originalStderrWrite);
  }

  process.stdout.write = createInterceptedWrite(stdoutOptions);
  process.stderr.write = createInterceptedWrite(stderrOptions);
  return { writeDashboard, restore };
}

test("SnapshotService renders a readable live dashboard for 60 seconds", REAL_SNAPSHOT_TEST_OPTIONS, async () => {
  const snapshotService = new SnapshotService(1000);
  let latestSnapshot: Snapshot | null = null;
  let snapshotCount = 0;
  const consoleLines: string[] = [];
  let outputRestoreSet: OutputRestoreSet | null = null;
  const renderCurrentDashboard = (): void => {
    const snapshot = latestSnapshot;

    if (snapshot !== null && outputRestoreSet !== null) {
      renderDashboard(snapshot, snapshot, snapshotCount, consoleLines, outputRestoreSet.writeDashboard);
    }
  };
  outputRestoreSet = createOutputRestoreSet(consoleLines, renderCurrentDashboard);
  const snapshotListener = (snapshot: Snapshot): void => {
    const previousSnapshot = latestSnapshot;

    snapshotCount += 1;
    latestSnapshot = snapshot;
    renderDashboard(snapshot, previousSnapshot, snapshotCount, consoleLines, outputRestoreSet.writeDashboard);
  };

  try {
    snapshotService.addSnapshotListener({ listener: snapshotListener });
    await waitFor(REAL_SNAPSHOT_TEST_DURATION_MS);
    snapshotService.removeSnapshotListener(snapshotListener);
    await snapshotService.disconnect();
  } finally {
    outputRestoreSet.restore();
  }

  assert.notEqual(latestSnapshot, null);

  if (latestSnapshot !== null) {
    assert.ok(Object.keys(latestSnapshot).length > 2);
  }
});
