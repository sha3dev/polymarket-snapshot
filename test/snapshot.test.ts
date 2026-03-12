import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { FeedEvent, OrderBookSnapshot, PricePoint } from "@sha3/crypto";
import type { MarketEvent, OrderBook, PolymarketMarket } from "@sha3/polymarket";

import { SnapshotService } from "../src/index.ts";
import type {
  Snapshot,
  SnapshotAsset,
  SnapshotCryptoClient,
  SnapshotListener,
  SnapshotLogger,
  SnapshotMarketCatalog,
  SnapshotMarketStream,
  SnapshotScheduler,
  SnapshotWindow,
} from "../src/snapshot/snapshot.types.ts";

type ScheduledTask = {
  id: number;
  runAt: number;
  listener: () => void;
  intervalMs: number | null;
};

type FakeScheduler = SnapshotScheduler & { advanceBy(delayMs: number): void; readTaskCount(): number };

type SchedulerRuntime = {
  readNow: () => number;
  registerCallback: (listener: () => void, delayMs: number, intervalMs: number | null) => number;
  clearTask: (timer: unknown) => void;
  advanceBy: (delayMs: number) => void;
};

type SnapshotEmission = {
  sequence: number;
  receivedAt: number;
  taskCount: number;
  snapshot: Snapshot;
  loggerTail: string[];
};

type DuplicateObservation = {
  key: string;
  firstEmission: SnapshotEmission;
  secondEmission: SnapshotEmission;
  changedFields: string[];
};

type SnapshotServiceInternals = {
  emitSnapshotsAt: (generatedAt: number) => void;
};

const TIMEOUT_METHOD = "setTimeout";
const CLEAR_TIMEOUT_METHOD = "clearTimeout";
const INTERVAL_METHOD = "setInterval";
const CLEAR_INTERVAL_METHOD = "clearInterval";
const DIAGNOSTIC_LOG_TAIL_SIZE = 6;

function findNextScheduledTask(tasks: Map<number, ScheduledTask>, targetMs: number): ScheduledTask | null {
  let selectedTask: ScheduledTask | null = null;

  for (const task of tasks.values()) {
    const isDue = task.runAt <= targetMs;
    const isEarlierTask = selectedTask === null || task.runAt < selectedTask.runAt || (task.runAt === selectedTask.runAt && task.id < selectedTask.id);

    if (isDue && isEarlierTask) {
      selectedTask = task;
    }
  }

  return selectedTask;
}

function advanceScheduledTasks(tasks: Map<number, ScheduledTask>, readNow: () => number, writeNow: (nextMs: number) => void, delayMs: number): void {
  const targetMs = readNow() + delayMs;

  while (true) {
    const nextTask = findNextScheduledTask(tasks, targetMs);

    if (nextTask === null) {
      break;
    }

    writeNow(nextTask.runAt);
    tasks.delete(nextTask.id);
    nextTask.listener();

    if (nextTask.intervalMs !== null) {
      tasks.set(nextTask.id, { ...nextTask, runAt: readNow() + nextTask.intervalMs });
    }
  }

  writeNow(targetMs);
}

function createSchedulerTaskRegistrar(
  tasks: Map<number, ScheduledTask>,
  readNow: () => number,
): (listener: () => void, delayMs: number, intervalMs: number | null, nextId: number) => void {
  const registerTask = (listener: () => void, delayMs: number, intervalMs: number | null, nextId: number): void => {
    tasks.set(nextId, { id: nextId, runAt: readNow() + delayMs, listener, intervalMs });
  };
  return registerTask;
}

function createSchedulerCallbackRegistrar(
  registerTask: (listener: () => void, delayMs: number, intervalMs: number | null, nextId: number) => void,
  readNextId: () => number,
  bumpNextId: () => void,
): (listener: () => void, delayMs: number, intervalMs: number | null) => number {
  const registerCallback = (listener: () => void, delayMs: number, intervalMs: number | null): number => {
    const taskId = readNextId();

    bumpNextId();
    registerTask(listener, delayMs, intervalMs, taskId);
    return taskId;
  };
  return registerCallback;
}

function createSchedulerTaskClearer(tasks: Map<number, ScheduledTask>): (timer: unknown) => void {
  function clearTask(timer: unknown): void {
    tasks.delete(Number(timer));
  }

  return clearTask;
}

function createSchedulerAdvancer(tasks: Map<number, ScheduledTask>, readNow: () => number, writeNow: (nextMs: number) => void): (delayMs: number) => void {
  function advanceBy(delayMs: number): void {
    advanceScheduledTasks(tasks, readNow, writeNow, delayMs);
  }

  return advanceBy;
}

function createFakeSchedulerTimerApi(runtime: SchedulerRuntime): Pick<FakeScheduler, "setTimeout" | "clearTimeout" | "setInterval" | "clearInterval"> {
  const timerApi = {
    [TIMEOUT_METHOD](listener: () => void, delayMs: number): unknown {
      const taskId = runtime.registerCallback(listener, delayMs, null);
      return taskId;
    },
    [CLEAR_TIMEOUT_METHOD](timer: unknown): void {
      runtime.clearTask(timer);
    },
    [INTERVAL_METHOD](listener: () => void, delayMs: number): unknown {
      const taskId = runtime.registerCallback(listener, delayMs, delayMs);
      return taskId;
    },
    [CLEAR_INTERVAL_METHOD](timer: unknown): void {
      runtime.clearTask(timer);
    },
  } satisfies Pick<FakeScheduler, "setTimeout" | "clearTimeout" | "setInterval" | "clearInterval">;
  return timerApi;
}

function buildFakeScheduler(runtime: SchedulerRuntime): FakeScheduler {
  const timerApi = createFakeSchedulerTimerApi(runtime);
  const scheduler = {
    now(): number {
      return runtime.readNow();
    },
    ...timerApi,
    advanceBy(delayMs: number): void {
      runtime.advanceBy(delayMs);
    },
    readTaskCount(): number {
      return 0;
    },
  } satisfies FakeScheduler;
  return scheduler;
}

function createFakeSchedulerApi(
  tasks: Map<number, ScheduledTask>,
  readNow: () => number,
  writeNow: (nextMs: number) => void,
  readNextId: () => number,
  bumpNextId: () => void,
): FakeScheduler {
  const registerTask = createSchedulerTaskRegistrar(tasks, readNow);
  const registerCallback = createSchedulerCallbackRegistrar(registerTask, readNextId, bumpNextId);
  const clearTask = createSchedulerTaskClearer(tasks);
  const advanceBy = createSchedulerAdvancer(tasks, readNow, writeNow);
  const scheduler = buildFakeScheduler({ readNow, registerCallback, clearTask, advanceBy });
  scheduler.readTaskCount = (): number => tasks.size;
  return scheduler;
}

function createFakeScheduler(nowMs: number): FakeScheduler {
  const tasks = new Map<number, ScheduledTask>();
  let currentMs = nowMs;
  let nextId = 1;
  const readNow = (): number => currentMs;
  function writeNow(nextMs: number): void {
    currentMs = nextMs;
  }

  const readNextId = (): number => nextId;

  function bumpNextId(): void {
    nextId += 1;
  }

  const scheduler = createFakeSchedulerApi(tasks, readNow, writeNow, readNextId, bumpNextId);
  return scheduler;
}

function createLateExecutionSchedulerAdvancer(
  tasks: Map<number, ScheduledTask>,
  readNow: () => number,
  writeNow: (nextMs: number) => void,
): (delayMs: number) => void {
  return (delayMs: number): void => {
    const targetMs = readNow() + delayMs;

    while (true) {
      const nextTask = findNextScheduledTask(tasks, targetMs);

      if (nextTask === null) {
        break;
      }

      tasks.delete(nextTask.id);
      writeNow(targetMs);
      nextTask.listener();
    }

    writeNow(targetMs);
  };
}

function createLateExecutionScheduler(nowMs: number): FakeScheduler {
  const tasks = new Map<number, ScheduledTask>();
  let currentMs = nowMs;
  let nextId = 1;
  const readNow = (): number => currentMs;

  function writeNow(nextMs: number): void {
    currentMs = nextMs;
  }

  function readNextId(): number {
    return nextId;
  }

  function bumpNextId(): void {
    nextId += 1;
  }

  const registerTask = createSchedulerTaskRegistrar(tasks, readNow);
  const registerCallback = createSchedulerCallbackRegistrar(registerTask, readNextId, bumpNextId);
  const clearTask = createSchedulerTaskClearer(tasks);
  const advanceBy = createLateExecutionSchedulerAdvancer(tasks, readNow, writeNow);
  const scheduler = buildFakeScheduler({ readNow, registerCallback, clearTask, advanceBy });
  return scheduler;
}

function buildEmissionKey(snapshot: Snapshot): string {
  const emissionKey = `${snapshot.asset}:${snapshot.window}:${snapshot.generatedAt}`;
  return emissionKey;
}

function buildComparableSnapshotValues(snapshot: Snapshot): Record<string, number | string | null> {
  const comparableValues = {
    marketSlug: snapshot.marketSlug,
    priceToBeat: snapshot.priceToBeat,
    upEventTs: snapshot.upEventTs,
    downEventTs: snapshot.downEventTs,
    binanceEventTs: snapshot.binanceEventTs,
    coinbaseEventTs: snapshot.coinbaseEventTs,
    krakenEventTs: snapshot.krakenEventTs,
    okxEventTs: snapshot.okxEventTs,
    chainlinkEventTs: snapshot.chainlinkEventTs,
  };
  return comparableValues;
}

function buildChangedFields(firstSnapshot: Snapshot, secondSnapshot: Snapshot): string[] {
  const changedFields: string[] = [];
  const firstComparableValues = buildComparableSnapshotValues(firstSnapshot);
  const secondComparableValues = buildComparableSnapshotValues(secondSnapshot);

  for (const comparableField of Object.keys(firstComparableValues)) {
    const firstValue = firstComparableValues[comparableField];
    const secondValue = secondComparableValues[comparableField];

    if (firstValue !== secondValue) {
      changedFields.push(`${comparableField}:${firstValue ?? "null"}=>${secondValue ?? "null"}`);
    }
  }

  return changedFields;
}

function collectDuplicateObservations(emissions: SnapshotEmission[]): DuplicateObservation[] {
  const firstEmissionByKey = new Map<string, SnapshotEmission>();
  const duplicateObservations: DuplicateObservation[] = [];

  for (const emission of emissions) {
    const emissionKey = buildEmissionKey(emission.snapshot);
    const previousEmission = firstEmissionByKey.get(emissionKey) ?? null;

    if (previousEmission === null) {
      firstEmissionByKey.set(emissionKey, emission);
    }

    if (previousEmission !== null) {
      duplicateObservations.push({
        key: emissionKey,
        firstEmission: previousEmission,
        secondEmission: emission,
        changedFields: buildChangedFields(previousEmission.snapshot, emission.snapshot),
      });
    }
  }

  return duplicateObservations;
}

function buildDuplicateDiagnostic(duplicateObservation: DuplicateObservation): string {
  const diagnosticPayload = {
    key: duplicateObservation.key,
    changedFields: duplicateObservation.changedFields,
    firstEmission: buildEmissionDiagnostic(duplicateObservation.firstEmission),
    secondEmission: buildEmissionDiagnostic(duplicateObservation.secondEmission),
  };
  const diagnosticMessage = JSON.stringify(diagnosticPayload, null, 2);
  return diagnosticMessage;
}

function buildEmissionDiagnostic(emission: SnapshotEmission): Record<string, number | string | string[] | null> {
  const emissionDiagnostic = {
    sequence: emission.sequence,
    receivedAt: emission.receivedAt,
    taskCount: emission.taskCount,
    marketSlug: emission.snapshot.marketSlug,
    priceToBeat: emission.snapshot.priceToBeat,
    upEventTs: emission.snapshot.upEventTs,
    downEventTs: emission.snapshot.downEventTs,
    binanceEventTs: emission.snapshot.binanceEventTs,
    loggerTail: emission.loggerTail,
  };
  return emissionDiagnostic;
}

class FakeLogger implements SnapshotLogger {
  public readonly messages: string[];

  public constructor() {
    this.messages = [];
  }

  public debug(message: string): void {
    this.messages.push(`debug:${message}`);
  }

  public warn(message: string): void {
    this.messages.push(`warn:${message}`);
  }

  public error(message: string): void {
    this.messages.push(`error:${message}`);
  }
}

class FakeCryptoClient implements SnapshotCryptoClient {
  public connectCount: number;
  public disconnectCount: number;
  private listener: ((event: FeedEvent) => void) | null;

  public constructor() {
    this.connectCount = 0;
    this.disconnectCount = 0;
    this.listener = null;
  }

  public async connect(): Promise<void> {
    this.connectCount += 1;
  }

  public async disconnect(): Promise<void> {
    this.disconnectCount += 1;
  }

  private removeListener(): void {
    this.listener = null;
  }

  public subscribe(listener: (event: FeedEvent) => void): { unsubscribe(): void } {
    this.listener = listener;
    return { unsubscribe: this.removeListener.bind(this) };
  }

  public emit(event: FeedEvent): void {
    if (this.listener !== null) {
      this.listener(event);
    }
  }
}

class FakeMarketCatalog implements SnapshotMarketCatalog {
  public readonly slugLoads: string[];
  public readonly priceToBeatLoads: string[];
  private readonly marketsBySlug: Map<string, PolymarketMarket>;
  private readonly priceToBeatBySlug: Map<string, number | null>;
  private readonly failuresBySlug: Map<string, number>;

  public constructor(marketsBySlug: Map<string, PolymarketMarket>) {
    this.slugLoads = [];
    this.priceToBeatLoads = [];
    this.marketsBySlug = marketsBySlug;
    this.priceToBeatBySlug = new Map<string, number | null>();
    this.failuresBySlug = new Map<string, number>();
  }

  public buildCryptoWindowSlugs(options: { date: Date; window: SnapshotWindow; symbols?: SnapshotAsset[] }): string[] {
    const windowMinutes = options.window === "5m" ? 5 : 15;
    const windowMs = windowMinutes * 60 * 1000;
    const alignedMs = Math.floor(options.date.getTime() / windowMs) * windowMs;
    const alignedSeconds = Math.floor(alignedMs / 1000);
    const slugs = (options.symbols ?? []).map((symbol) => `${symbol}-updown-${options.window}-${alignedSeconds}`);
    return slugs;
  }

  public async loadMarketBySlug(options: { slug: string }): Promise<PolymarketMarket> {
    const market = this.marketsBySlug.get(options.slug) ?? null;

    this.slugLoads.push(options.slug);

    if (market === null) {
      throw new Error(`Unknown market '${options.slug}'.`);
    }

    return market;
  }

  public async getPriceToBeat(options: { market: PolymarketMarket }): Promise<number | null> {
    const remainingFailures = this.failuresBySlug.get(options.market.slug) ?? 0;
    const priceToBeat = this.priceToBeatBySlug.get(options.market.slug) ?? null;

    this.priceToBeatLoads.push(options.market.slug);

    if (remainingFailures > 0) {
      this.failuresBySlug.set(options.market.slug, remainingFailures - 1);
      throw new Error("priceToBeat temporarily unavailable");
    }

    return priceToBeat;
  }

  public setPriceToBeat(slug: string, priceToBeat: number | null): void {
    this.priceToBeatBySlug.set(slug, priceToBeat);
  }

  public setFailures(slug: string, failures: number): void {
    this.failuresBySlug.set(slug, failures);
  }
}

class FakeMarketStream implements SnapshotMarketStream {
  public connectCount: number;
  public disconnectCount: number;
  public readonly subscribedAssetIds: string[];
  public readonly unsubscribedAssetIds: string[];
  private listener: ((event: MarketEvent) => void) | null;

  public constructor() {
    this.connectCount = 0;
    this.disconnectCount = 0;
    this.subscribedAssetIds = [];
    this.unsubscribedAssetIds = [];
    this.listener = null;
  }

  public async connect(): Promise<void> {
    this.connectCount += 1;
  }

  public async disconnect(): Promise<void> {
    this.disconnectCount += 1;
  }

  public subscribe(options: { assetIds: string[] }): void {
    this.subscribedAssetIds.push(...options.assetIds);
  }

  public unsubscribe(options: { assetIds: string[] }): void {
    this.unsubscribedAssetIds.push(...options.assetIds);
  }

  public addListener(options: { listener: (event: MarketEvent) => void }): () => void {
    this.listener = options.listener;
    return (): void => {
      this.listener = null;
    };
  }

  public emit(event: MarketEvent): void {
    if (this.listener !== null) {
      this.listener(event);
    }
  }
}

function createMarket(slug: string, symbol: SnapshotAsset, startIso: string, endIso: string): PolymarketMarket {
  const market: PolymarketMarket = {
    id: `market-${slug}`,
    slug,
    question: slug,
    symbol,
    conditionId: `condition-${slug}`,
    outcomes: ["up", "down"],
    clobTokenIds: [`up-${slug}`, `down-${slug}`],
    upTokenId: `up-${slug}`,
    downTokenId: `down-${slug}`,
    orderMinSize: 1,
    orderPriceMinTickSize: "0.01",
    eventStartTime: startIso,
    endDate: endIso,
    start: new Date(startIso),
    end: new Date(endIso),
    raw: {},
  };
  return market;
}

function createPriceEvent(provider: PricePoint["provider"], symbol: SnapshotAsset, ts: number, price: number): PricePoint {
  const event: PricePoint = { type: "price", provider, symbol, ts, price };
  return event;
}

function createOrderBookEvent(provider: OrderBookSnapshot["provider"], symbol: SnapshotAsset, ts: number, price: number): OrderBookSnapshot {
  const event: OrderBookSnapshot = { type: "orderbook", provider, symbol, ts, asks: [{ price: price + 1, size: 5 }], bids: [{ price: price - 1, size: 7 }] };
  return event;
}

function createMarketPriceEvent(assetId: string, tsIso: string, price: number): MarketEvent {
  const event: MarketEvent = { type: "price", source: "polymarket", assetId, index: 1, date: new Date(tsIso), price };
  return event;
}

function createMarketBookEvent(assetId: string, tsIso: string, price: number): MarketEvent {
  const orderBook: OrderBook = { asks: [{ price: price + 0.01, size: 10 }], bids: [{ price: price - 0.01, size: 12 }] };
  const event: MarketEvent = { type: "book", source: "polymarket", assetId, index: 1, date: new Date(tsIso), asks: orderBook.asks, bids: orderBook.bids };
  return event;
}

function createServiceFixture(nowMs: number): {
  service: SnapshotService;
  scheduler: FakeScheduler;
  logger: FakeLogger;
  marketCatalog: FakeMarketCatalog;
  marketStream: FakeMarketStream;
  createdCryptoClients: FakeCryptoClient[];
} {
  const scheduler = createFakeScheduler(nowMs);
  const logger = new FakeLogger();
  const marketsBySlug = createFixtureMarkets();
  const marketCatalog = new FakeMarketCatalog(marketsBySlug);
  const marketStream = new FakeMarketStream();
  const createdCryptoClients: FakeCryptoClient[] = [];
  const service = createSnapshotService(scheduler, logger, marketCatalog, marketStream, createdCryptoClients);

  seedPriceToBeat(marketCatalog);
  return { service, scheduler, logger, marketCatalog, marketStream, createdCryptoClients };
}

function createLateExecutionServiceFixture(nowMs: number): {
  service: SnapshotService;
  scheduler: FakeScheduler;
  logger: FakeLogger;
  marketCatalog: FakeMarketCatalog;
  marketStream: FakeMarketStream;
  createdCryptoClients: FakeCryptoClient[];
} {
  const scheduler = createLateExecutionScheduler(nowMs);
  const logger = new FakeLogger();
  const marketsBySlug = createFixtureMarkets();
  const marketCatalog = new FakeMarketCatalog(marketsBySlug);
  const marketStream = new FakeMarketStream();
  const createdCryptoClients: FakeCryptoClient[] = [];
  const service = createSnapshotService(scheduler, logger, marketCatalog, marketStream, createdCryptoClients);

  seedPriceToBeat(marketCatalog);
  return { service, scheduler, logger, marketCatalog, marketStream, createdCryptoClients };
}

function createCustomServiceFixture(options: {
  nowMs: number;
  marketsBySlug: Map<string, PolymarketMarket>;
  useLateExecutionScheduler?: boolean;
}): {
  service: SnapshotService;
  scheduler: FakeScheduler;
  logger: FakeLogger;
  marketCatalog: FakeMarketCatalog;
  marketStream: FakeMarketStream;
  createdCryptoClients: FakeCryptoClient[];
} {
  const scheduler = options.useLateExecutionScheduler ? createLateExecutionScheduler(options.nowMs) : createFakeScheduler(options.nowMs);
  const logger = new FakeLogger();
  const marketCatalog = new FakeMarketCatalog(options.marketsBySlug);
  const marketStream = new FakeMarketStream();
  const createdCryptoClients: FakeCryptoClient[] = [];
  const service = createSnapshotService(scheduler, logger, marketCatalog, marketStream, createdCryptoClients);
  return { service, scheduler, logger, marketCatalog, marketStream, createdCryptoClients };
}

function createFixtureMarkets(): Map<string, PolymarketMarket> {
  const marketA = createMarket("btc-updown-5m-1767225600", "btc", "2026-01-01T00:00:00.000Z", "2026-01-01T00:05:00.000Z");
  const marketB = createMarket("btc-updown-5m-1767225900", "btc", "2026-01-01T00:05:00.000Z", "2026-01-01T00:10:00.000Z");
  const marketC = createMarket("eth-updown-15m-1767225600", "eth", "2026-01-01T00:00:00.000Z", "2026-01-01T00:15:00.000Z");
  const marketsBySlug = new Map<string, PolymarketMarket>();

  marketsBySlug.set(marketA.slug, marketA);
  marketsBySlug.set(marketB.slug, marketB);
  marketsBySlug.set(marketC.slug, marketC);
  return marketsBySlug;
}

function createDualWindowFixtureMarkets(): Map<string, PolymarketMarket> {
  const marketA = createMarket("xrp-updown-5m-1767225600", "xrp", "2026-01-01T00:00:00.000Z", "2026-01-01T00:05:00.000Z");
  const marketB = createMarket("xrp-updown-15m-1767225600", "xrp", "2026-01-01T00:00:00.000Z", "2026-01-01T00:15:00.000Z");
  const marketsBySlug = new Map<string, PolymarketMarket>();

  marketsBySlug.set(marketA.slug, marketA);
  marketsBySlug.set(marketB.slug, marketB);
  return marketsBySlug;
}

function createSnapshotService(
  scheduler: FakeScheduler,
  logger: FakeLogger,
  marketCatalog: FakeMarketCatalog,
  marketStream: FakeMarketStream,
  createdCryptoClients: FakeCryptoClient[],
): SnapshotService {
  const service = new SnapshotService({
    scheduler,
    logger,
    marketCatalogService: marketCatalog,
    marketStreamService: marketStream,
    snapshotIntervalMs: 500,
    priceToBeatInitialDelayMs: 10_000,
    priceToBeatRetryIntervalMs: 5_000,
    cryptoClientFactory: (): SnapshotCryptoClient => {
      const fakeClient = new FakeCryptoClient();

      createdCryptoClients.push(fakeClient);
      return fakeClient;
    },
  });
  return service;
}

function seedPriceToBeat(marketCatalog: FakeMarketCatalog): void {
  marketCatalog.setPriceToBeat("btc-updown-5m-1767225600", 42_000);
  marketCatalog.setPriceToBeat("btc-updown-5m-1767225900", 42_500);
  marketCatalog.setPriceToBeat("eth-updown-15m-1767225600", 2_300);
}

async function flushAsync(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

async function waitForCondition(readCondition: () => boolean): Promise<void> {
  let attempt = 0;

  while (attempt < 30 && !readCondition()) {
    await flushAsync();
    attempt += 1;
  }
}

async function advanceUntilCondition(scheduler: FakeScheduler, delayMs: number, readCondition: () => boolean): Promise<void> {
  let attempt = 0;

  while (attempt < 5 && !readCondition()) {
    scheduler.advanceBy(delayMs);
    await flushAsync();
    attempt += 1;
  }
}

test("SnapshotService emits plain snapshot objects, keeps latest values, retries priceToBeat, rotates markets, and recreates crypto clients when assets change", async () => {
  const fixture = createServiceFixture(Date.parse("2026-01-01T00:00:01.000Z"));
  const receivedSnapshots: Snapshot[] = [];

  function snapshotListener(snapshot: Snapshot): void {
    receivedSnapshots.push(snapshot);
  }

  fixture.marketCatalog.setFailures("btc-updown-5m-1767225600", 1);
  fixture.service.addSnapshotListener({ listener: snapshotListener, assets: ["btc"], windows: ["5m"] });
  const isBtcRuntimeReady = (): boolean =>
    fixture.createdCryptoClients.length === 1 &&
    fixture.marketCatalog.slugLoads.length === 1 &&
    fixture.service.getSnapshot({ assets: ["btc"], windows: ["5m"] }).length === 1;

  await waitForCondition(isBtcRuntimeReady);

  assert.equal(fixture.marketStream.connectCount, 1);
  assert.equal(fixture.createdCryptoClients.length, 1);
  assert.deepEqual(fixture.marketCatalog.slugLoads, ["btc-updown-5m-1767225600"]);

  await advanceUntilCondition(fixture.scheduler, 500, () => receivedSnapshots.length > 0);
  assert.equal(receivedSnapshots.length > 0, true);
  assert.equal(typeof receivedSnapshots[0], "object");
  assert.equal(receivedSnapshots[0]?.priceToBeat, null);
  assert.equal(receivedSnapshots[0]?.binancePrice, null);

  const firstCryptoClient = fixture.createdCryptoClients[0];
  const firstSnapshot = fixture.service.getSnapshot({ assets: ["btc"], windows: ["5m"] })[0];
  const firstUpAssetId = firstSnapshot?.upAssetId ?? "";

  firstCryptoClient?.emit(createPriceEvent("binance", "btc", Date.parse("2026-01-01T00:00:01.100Z"), 43_000));
  firstCryptoClient?.emit(createPriceEvent("binance", "btc", Date.parse("2026-01-01T00:00:01.200Z"), 43_100));
  firstCryptoClient?.emit(createOrderBookEvent("binance", "btc", Date.parse("2026-01-01T00:00:01.300Z"), 43_100));
  fixture.marketStream.emit(createMarketPriceEvent(firstUpAssetId, "2026-01-01T00:00:02.000Z", 0.61));
  fixture.marketStream.emit(createMarketPriceEvent(firstUpAssetId, "2026-01-01T00:00:02.100Z", 0.64));
  fixture.marketStream.emit(createMarketBookEvent(firstUpAssetId, "2026-01-01T00:00:02.200Z", 0.64));

  fixture.scheduler.advanceBy(500);
  const latestSnapshot = receivedSnapshots.at(-1);

  assert.equal(latestSnapshot?.binancePrice, 43_100);
  assert.equal(latestSnapshot?.binanceEventTs, Date.parse("2026-01-01T00:00:01.300Z"));
  assert.equal(latestSnapshot?.upPrice, 0.64);
  assert.equal(latestSnapshot?.upEventTs, Date.parse("2026-01-01T00:00:02.200Z"));

  fixture.scheduler.advanceBy(10_000);
  await flushAsync();
  assert.equal(fixture.marketCatalog.priceToBeatLoads.length, 1);

  fixture.scheduler.advanceBy(5_000);
  await flushAsync();
  const retriedSnapshot = fixture.service.getSnapshot({ assets: ["btc"], windows: ["5m"] })[0];

  assert.equal(fixture.marketCatalog.priceToBeatLoads.length, 2);
  assert.equal(retriedSnapshot?.priceToBeat, 42_000);

  fixture.scheduler.advanceBy(284_250);
  await flushAsync();
  const rotatedSnapshot = fixture.service.getSnapshot({ assets: ["btc"], windows: ["5m"] })[0];

  assert.equal(rotatedSnapshot?.marketSlug, "btc-updown-5m-1767225900");
  assert.equal(rotatedSnapshot?.upPrice, null);
  assert.equal(fixture.marketStream.unsubscribedAssetIds.includes("up-btc-updown-5m-1767225600"), true);

  const secondListener: SnapshotListener = (): void => {};
  fixture.service.addSnapshotListener({ listener: secondListener, assets: ["eth"], windows: ["15m"] });
  await flushAsync();
  assert.equal(fixture.createdCryptoClients.length, 2);
  assert.equal(firstCryptoClient?.disconnectCount, 1);

  fixture.service.removeSnapshotListener(snapshotListener);
  const countBeforeRemovalTick = receivedSnapshots.length;

  fixture.scheduler.advanceBy(500);
  assert.equal(receivedSnapshots.length, countBeforeRemovalTick);

  await fixture.service.disconnect();
  assert.equal(fixture.marketStream.disconnectCount, 1);
  assert.equal(fixture.createdCryptoClients[1]?.disconnectCount, 1);
});

test("SnapshotService expands omitted filters, discards out-of-range market events, and avoids duplicate subscriptions for shared listeners", async () => {
  const fixture = createServiceFixture(Date.parse("2026-01-01T00:00:01.000Z"));
  const snapshotsA: Snapshot[] = [];
  const snapshotsB: Snapshot[] = [];

  function listenerA(snapshot: Snapshot): void {
    snapshotsA.push(snapshot);
  }

  function listenerB(snapshot: Snapshot): void {
    snapshotsB.push(snapshot);
  }

  fixture.service.addSnapshotListener({ listener: listenerA, assets: ["btc"], windows: ["5m"] });
  fixture.service.addSnapshotListener({ listener: listenerB, assets: ["btc"], windows: ["5m"] });
  await waitForCondition(() => fixture.marketCatalog.slugLoads.length === 1);

  assert.equal(fixture.marketCatalog.slugLoads.length, 1);
  assert.equal(fixture.marketStream.subscribedAssetIds.filter((assetId) => assetId === "up-btc-updown-5m-1767225600").length, 1);

  const firstSnapshot = fixture.service.getSnapshot()[0];
  const firstUpAssetId = firstSnapshot?.upAssetId ?? "";

  fixture.marketStream.emit(createMarketPriceEvent(firstUpAssetId, "2025-12-31T23:59:59.999Z", 0.25));
  fixture.marketStream.emit(createMarketPriceEvent(firstUpAssetId, "2026-01-01T00:05:00.000Z", 0.75));
  fixture.scheduler.advanceBy(500);

  const ignoredSnapshot = fixture.service.getSnapshot({ assets: ["btc"], windows: ["5m"] })[0];
  assert.equal(ignoredSnapshot?.upPrice, null);

  fixture.service.removeSnapshotListener(listenerA);
  fixture.service.removeSnapshotListener(listenerB);
  await waitForCondition(() => fixture.marketStream.unsubscribedAssetIds.length > 0);
  assert.equal(fixture.marketStream.unsubscribedAssetIds.includes("up-btc-updown-5m-1767225600"), true);

  fixture.service.addSnapshotListener({ listener: listenerA });
  await waitForCondition(() => fixture.service.getSnapshot().length === 8);
  const allSnapshots = fixture.service.getSnapshot();

  assert.equal(allSnapshots.length, 8);

  await fixture.service.disconnect();
});

test("SnapshotService aligns generatedAt to the configured interval grid", async () => {
  const fixture = createServiceFixture(Date.parse("2026-01-01T00:00:01.137Z"));
  const receivedSnapshots: Snapshot[] = [];

  function snapshotListener(snapshot: Snapshot): void {
    receivedSnapshots.push(snapshot);
  }

  fixture.service.addSnapshotListener({ listener: snapshotListener, assets: ["btc"], windows: ["5m"] });
  await waitForCondition(() => fixture.service.getSnapshot({ assets: ["btc"], windows: ["5m"] }).length === 1);

  fixture.scheduler.advanceBy(362);
  await flushAsync();
  assert.equal(receivedSnapshots.length, 0);

  fixture.scheduler.advanceBy(1);
  await flushAsync();
  assert.equal(receivedSnapshots[0]?.generatedAt, Date.parse("2026-01-01T00:00:01.500Z"));
  assert.equal(receivedSnapshots[0]?.generatedAt % 500, 0);

  fixture.scheduler.advanceBy(500);
  await flushAsync();
  assert.equal(receivedSnapshots[1]?.generatedAt, Date.parse("2026-01-01T00:00:02.000Z"));
  assert.equal(receivedSnapshots[1]?.generatedAt % 500, 0);

  const latestSnapshot = fixture.service.getSnapshot({ assets: ["btc"], windows: ["5m"] })[0];
  assert.equal(latestSnapshot?.generatedAt, Date.parse("2026-01-01T00:00:02.000Z"));

  await fixture.service.disconnect();
});

test("SnapshotService avoids accumulated drift when snapshot dispatch executes late", async () => {
  const fixture = createLateExecutionServiceFixture(Date.parse("2026-01-01T00:00:01.137Z"));
  const receivedSnapshots: Snapshot[] = [];

  function snapshotListener(snapshot: Snapshot): void {
    receivedSnapshots.push(snapshot);
  }

  fixture.service.addSnapshotListener({ listener: snapshotListener, assets: ["btc"], windows: ["5m"] });
  await waitForCondition(
    () =>
      fixture.createdCryptoClients.length === 1 &&
      fixture.marketCatalog.slugLoads.length === 1 &&
      fixture.scheduler.readTaskCount() > 0 &&
      fixture.service.getSnapshot({ assets: ["btc"], windows: ["5m"] }).length === 1,
  );

  fixture.scheduler.advanceBy(363);
  await flushAsync();
  assert.equal(receivedSnapshots[0]?.generatedAt, Date.parse("2026-01-01T00:00:01.500Z"));

  fixture.scheduler.advanceBy(550);
  await flushAsync();
  assert.equal(receivedSnapshots[1]?.generatedAt, Date.parse("2026-01-01T00:00:02.000Z"));

  fixture.scheduler.advanceBy(450);
  await flushAsync();
  assert.equal(receivedSnapshots[2]?.generatedAt, Date.parse("2026-01-01T00:00:02.500Z"));

  fixture.scheduler.advanceBy(600);
  await flushAsync();
  assert.equal(receivedSnapshots[3]?.generatedAt, Date.parse("2026-01-01T00:00:03.000Z"));

  fixture.scheduler.advanceBy(400);
  await flushAsync();
  assert.equal(receivedSnapshots[4]?.generatedAt, Date.parse("2026-01-01T00:00:03.500Z"));

  await fixture.service.disconnect();
});

test("SnapshotService suppresses re-emission for the same pair and generatedAt", async () => {
  const fixture = createServiceFixture(Date.parse("2026-01-01T00:00:01.000Z"));
  const receivedSnapshots: Snapshot[] = [];
  const serviceInternals = fixture.service as unknown as SnapshotServiceInternals;

  function snapshotListener(snapshot: Snapshot): void {
    receivedSnapshots.push(snapshot);
  }

  fixture.service.addSnapshotListener({ listener: snapshotListener, assets: ["btc"], windows: ["5m"] });
  await waitForCondition(() => fixture.service.getSnapshot({ assets: ["btc"], windows: ["5m"] }).length === 1);

  serviceInternals.emitSnapshotsAt(Date.parse("2026-01-01T00:00:01.500Z"));
  serviceInternals.emitSnapshotsAt(Date.parse("2026-01-01T00:00:01.500Z"));

  assert.equal(receivedSnapshots.length, 1);
  assert.equal(receivedSnapshots[0]?.generatedAt, Date.parse("2026-01-01T00:00:01.500Z"));

  await fixture.service.disconnect();
});

test("SnapshotService logs diagnostic context when minute-boundary listener churn creates duplicate emissions", async (context) => {
  const fixture = createServiceFixture(Date.parse("2026-01-01T00:00:58.750Z"));
  const receivedEmissions: SnapshotEmission[] = [];
  let sequence = 0;

  function snapshotListener(snapshot: Snapshot): void {
    const emission: SnapshotEmission = {
      sequence,
      receivedAt: fixture.scheduler.now(),
      taskCount: fixture.scheduler.readTaskCount(),
      snapshot,
      loggerTail: fixture.logger.messages.slice(-DIAGNOSTIC_LOG_TAIL_SIZE),
    };
    receivedEmissions.push(emission);
    sequence += 1;
  }

  fixture.service.addSnapshotListener({ listener: snapshotListener, assets: ["btc"], windows: ["5m", "15m"] });
  await waitForCondition(() => fixture.service.getSnapshot({ assets: ["btc"], windows: ["5m", "15m"] }).length === 2);

  for (let minuteIndex = 0; minuteIndex < 4; minuteIndex += 1) {
    fixture.scheduler.advanceBy(59_000);
    await flushAsync();
    fixture.service.removeSnapshotListener(snapshotListener);
    fixture.service.addSnapshotListener({ listener: snapshotListener, assets: ["btc"], windows: ["5m", "15m"] });
    await flushAsync();
    fixture.scheduler.advanceBy(1_000);
    await flushAsync();
  }

  const duplicateObservations = collectDuplicateObservations(receivedEmissions);

  for (const duplicateObservation of duplicateObservations) {
    context.diagnostic(buildDuplicateDiagnostic(duplicateObservation));
  }

  assert.equal(duplicateObservations.length, 0);
  await fixture.service.disconnect();
});

test("SnapshotService logs diagnostic context when state is rebuilt for the same asset across both windows in one bucket", async (context) => {
  const nowMs = Date.parse("2026-01-01T00:00:59.700Z");
  const marketsBySlug = createDualWindowFixtureMarkets();
  const fixture = createCustomServiceFixture({ nowMs, marketsBySlug, useLateExecutionScheduler: true });
  const receivedEmissions: SnapshotEmission[] = [];
  let sequence = 0;

  fixture.marketCatalog.setPriceToBeat("xrp-updown-5m-1767225600", 2.01);
  fixture.marketCatalog.setPriceToBeat("xrp-updown-15m-1767225600", 2.02);

  function snapshotListener(snapshot: Snapshot): void {
    const emission: SnapshotEmission = {
      sequence,
      receivedAt: fixture.scheduler.now(),
      taskCount: fixture.scheduler.readTaskCount(),
      snapshot,
      loggerTail: fixture.logger.messages.slice(-DIAGNOSTIC_LOG_TAIL_SIZE),
    };
    receivedEmissions.push(emission);
    sequence += 1;
  }

  fixture.service.addSnapshotListener({ listener: snapshotListener, assets: ["xrp"], windows: ["5m", "15m"] });
  await waitForCondition(() => fixture.service.getSnapshot({ assets: ["xrp"], windows: ["5m", "15m"] }).length === 2);

  const initialSnapshots = fixture.service.getSnapshot({ assets: ["xrp"], windows: ["5m", "15m"] });
  const first5mSnapshot = initialSnapshots.find((snapshot) => snapshot.window === "5m") ?? null;
  const first15mSnapshot = initialSnapshots.find((snapshot) => snapshot.window === "15m") ?? null;
  const firstCryptoClient = fixture.createdCryptoClients[0] ?? null;

  assert.notEqual(first5mSnapshot, null);
  assert.notEqual(first15mSnapshot, null);
  assert.notEqual(firstCryptoClient, null);

  fixture.scheduler.advanceBy(300);
  await flushAsync();
  fixture.service.removeSnapshotListener(snapshotListener);
  fixture.service.addSnapshotListener({ listener: snapshotListener, assets: ["xrp"], windows: ["5m", "15m"] });
  await flushAsync();

  firstCryptoClient?.emit(createPriceEvent("binance", "xrp", Date.parse("2026-01-01T00:00:59.900Z"), 2.1));
  fixture.marketStream.emit(createMarketPriceEvent(first5mSnapshot?.upAssetId ?? "", "2026-01-01T00:00:59.910Z", 0.51));
  fixture.marketStream.emit(createMarketPriceEvent(first15mSnapshot?.upAssetId ?? "", "2026-01-01T00:00:59.920Z", 0.61));

  fixture.scheduler.advanceBy(500);
  await flushAsync();

  const duplicateObservations = collectDuplicateObservations(receivedEmissions);

  for (const duplicateObservation of duplicateObservations) {
    context.diagnostic(buildDuplicateDiagnostic(duplicateObservation));
  }

  assert.equal(duplicateObservations.length, 0);
  await fixture.service.disconnect();
});

test("SnapshotService rejects unsupported snapshot intervals", () => {
  assert.throws(
    () => {
      return new SnapshotService({ snapshotIntervalMs: 250 });
    },
    { message: "Unsupported snapshotIntervalMs '250'. Use one of 100, 200, 500, 1000." },
  );
});
