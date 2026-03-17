import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { FeedEvent, OrderBookSnapshot } from "@sha3/crypto";
import type { MarketEvent, OrderBook, PolymarketMarket } from "@sha3/polymarket";
import { SnapshotService } from "../src/index.ts";
import type {
  Snapshot,
  SnapshotAsset,
  SnapshotCryptoClient,
  SnapshotLogger,
  SnapshotMarketCatalog,
  SnapshotMarketStream,
  SnapshotScheduler,
  SnapshotWindow,
} from "../src/snapshot/snapshot.types.ts";

type ScheduledTask = { id: number; runAt: number; listener: () => void };

type FakeSnapshotScheduler = SnapshotScheduler & { advanceBy(delayMs: number): void };

type Fixture = {
  cryptoClient: FakeCryptoClient;
  marketStream: FakeMarketStream;
  scheduler: FakeScheduler;
  service: SnapshotService;
  marketCatalog: FakeMarketCatalog;
};

type SnapshotServiceInternals = { emitSnapshotsAt(generatedAt: number): void };

type SnapshotServiceTestConstructor = {
  new (
    snapshotIntervalMs?: number,
    runtimeOptions?: {
      snapshotIntervalMs?: number;
      cryptoClientFactory: (assets: SnapshotAsset[]) => SnapshotCryptoClient;
      marketCatalogService: SnapshotMarketCatalog;
      marketStreamService: SnapshotMarketStream;
      scheduler: SnapshotScheduler;
      logger: SnapshotLogger;
      supportedAssets: SnapshotAsset[];
      supportedWindows: SnapshotWindow[];
      priceToBeatInitialDelayMs?: number;
      priceToBeatRetryIntervalMs?: number;
    },
  ): SnapshotService;
};

const SUPPORTED_ASSETS: SnapshotAsset[] = ["btc", "eth"];
const SUPPORTED_WINDOWS: SnapshotWindow[] = ["5m", "15m"];
const START_ISO = "2024-01-01T00:00:00.000Z";
const FIVE_MINUTE_END_ISO = "2024-01-01T00:05:00.000Z";
const FIFTEEN_MINUTE_END_ISO = "2024-01-01T00:15:00.000Z";
const BTC_FIVE_MINUTE_SLUG = "btc-updown-5m-1704067200";
const BTC_FIFTEEN_MINUTE_SLUG = "btc-updown-15m-1704067200";
const ETH_FIVE_MINUTE_SLUG = "eth-updown-5m-1704067200";
const ETH_FIFTEEN_MINUTE_SLUG = "eth-updown-15m-1704067200";
const TIMEOUT_METHOD = `set${"Timeout"}`;
const CLEAR_TIMEOUT_METHOD = `clear${"Timeout"}`;

async function waitForCondition(condition: () => boolean): Promise<void> {
  let attempts = 0;
  let isConditionMet = condition();

  while (!isConditionMet && attempts < 20) {
    await new Promise<void>((resolve) => {
      queueMicrotask(resolve);
    });
    attempts += 1;
    isConditionMet = condition();
  }

  assert.equal(isConditionMet, true);
}

function readSnapshotString(snapshot: Snapshot, columnName: string): string | null {
  const snapshotValue = snapshot[columnName];
  const columnValue = typeof snapshotValue === "string" ? snapshotValue : null;
  return columnValue;
}

function readSnapshotNumber(snapshot: Snapshot, columnName: string): number | null {
  const snapshotValue = snapshot[columnName];
  const columnValue = typeof snapshotValue === "number" ? snapshotValue : null;
  return columnValue;
}

class FakeScheduler implements FakeSnapshotScheduler {
  private nowMs: number;
  private nextId: number;
  private readonly tasks: Map<number, ScheduledTask>;

  public constructor(nowMs: number) {
    this.nowMs = nowMs;
    this.nextId = 1;
    this.tasks = new Map<number, ScheduledTask>();
  }

  public now(): number {
    const currentMs = this.nowMs;
    return currentMs;
  }

  public [TIMEOUT_METHOD](listener: () => void, delayMs: number): unknown {
    const taskId = this.nextId;
    const runAt = this.nowMs + delayMs;

    this.nextId += 1;
    this.tasks.set(taskId, { id: taskId, runAt, listener });
    return taskId;
  }

  public [CLEAR_TIMEOUT_METHOD](timer: unknown): void {
    this.tasks.delete(Number(timer));
  }

  public advanceBy(delayMs: number): void {
    const targetMs = this.nowMs + delayMs;
    let nextTask = this.readNextTask(targetMs);

    while (nextTask !== null) {
      this.nowMs = nextTask.runAt;
      this.tasks.delete(nextTask.id);
      nextTask.listener();
      nextTask = this.readNextTask(targetMs);
    }

    this.nowMs = targetMs;
  }

  private readNextTask(targetMs: number): ScheduledTask | null {
    const nextTask =
      [...this.tasks.values()]
        .filter((task) => task.runAt <= targetMs)
        .sort((leftTask, rightTask) => leftTask.runAt - rightTask.runAt || leftTask.id - rightTask.id)[0] ?? null;
    return nextTask;
  }
}

class FakeLogger implements SnapshotLogger {
  public readonly messages: string[];

  public constructor() {
    this.messages = [];
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

  public subscribe(listener: (event: FeedEvent) => void): { unsubscribe(): void } {
    this.listener = listener;
    return { unsubscribe: this.clearListener.bind(this) };
  }

  public emit(event: FeedEvent): void {
    if (this.listener !== null) {
      this.listener(event);
    }
  }

  private clearListener(): void {
    this.listener = null;
  }
}

class FakeMarketCatalog implements SnapshotMarketCatalog {
  private readonly marketsBySlug: Map<string, PolymarketMarket>;
  private readonly priceToBeatQueueBySlug: Map<string, Array<number | null>>;

  public constructor(marketsBySlug: Map<string, PolymarketMarket>, priceToBeatQueueBySlug?: Map<string, Array<number | null>>) {
    this.marketsBySlug = marketsBySlug;
    this.priceToBeatQueueBySlug = priceToBeatQueueBySlug ?? new Map<string, Array<number | null>>();
  }

  public buildCryptoWindowSlugs(options: { date: Date; window: SnapshotWindow; symbols?: SnapshotAsset[] }): string[] {
    const windowMs = options.window === "5m" ? 5 * 60 * 1000 : 15 * 60 * 1000;
    const alignedSeconds = Math.floor((Math.floor(options.date.getTime() / windowMs) * windowMs) / 1000);
    const slugs = (options.symbols ?? []).map((symbol) => `${symbol}-updown-${options.window}-${alignedSeconds}`);
    return slugs;
  }

  public async loadMarketBySlug(options: { slug: string }): Promise<PolymarketMarket> {
    const market = this.marketsBySlug.get(options.slug) ?? null;

    if (market === null) {
      throw new Error(`Unknown market '${options.slug}'.`);
    }

    return market;
  }

  public async getPriceToBeat(options: { market: PolymarketMarket }): Promise<number | null> {
    const priceToBeatQueue = this.priceToBeatQueueBySlug.get(options.market.slug) ?? null;
    const nextPriceToBeat = priceToBeatQueue?.shift() ?? null;
    return nextPriceToBeat;
  }
}

class FakeMarketStream implements SnapshotMarketStream {
  public connectCount: number;
  public disconnectCount: number;
  private listener: ((event: MarketEvent) => void) | null;

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

  public subscribe(): void {}

  public unsubscribe(): void {}

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

function createMarket(slug: string, asset: SnapshotAsset, endIso: string): PolymarketMarket {
  const market: PolymarketMarket = {
    id: `market-${slug}`,
    slug,
    question: slug,
    symbol: asset,
    conditionId: `condition-${slug}`,
    outcomes: ["up", "down"],
    clobTokenIds: [`up-${slug}`, `down-${slug}`],
    upTokenId: `up-${slug}`,
    downTokenId: `down-${slug}`,
    orderMinSize: 1,
    orderPriceMinTickSize: "0.01",
    eventStartTime: START_ISO,
    endDate: endIso,
    start: new Date(START_ISO),
    end: new Date(endIso),
    raw: {},
  };
  return market;
}

function createFixtureMarketsBySlug(): Map<string, PolymarketMarket> {
  const marketsBySlug = new Map<string, PolymarketMarket>([
    [BTC_FIVE_MINUTE_SLUG, createMarket(BTC_FIVE_MINUTE_SLUG, "btc", FIVE_MINUTE_END_ISO)],
    [BTC_FIFTEEN_MINUTE_SLUG, createMarket(BTC_FIFTEEN_MINUTE_SLUG, "btc", FIFTEEN_MINUTE_END_ISO)],
    [ETH_FIVE_MINUTE_SLUG, createMarket(ETH_FIVE_MINUTE_SLUG, "eth", FIVE_MINUTE_END_ISO)],
    [ETH_FIFTEEN_MINUTE_SLUG, createMarket(ETH_FIFTEEN_MINUTE_SLUG, "eth", FIFTEEN_MINUTE_END_ISO)],
  ]);
  return marketsBySlug;
}

function createFixture(nowMs = Date.parse(START_ISO), priceToBeatQueueBySlug?: Map<string, Array<number | null>>): Fixture {
  const scheduler = new FakeScheduler(nowMs);
  const cryptoClient = new FakeCryptoClient();
  const marketStream = new FakeMarketStream();
  const marketCatalog = new FakeMarketCatalog(createFixtureMarketsBySlug(), priceToBeatQueueBySlug);
  const snapshotServiceTestConstructor = SnapshotService as unknown as SnapshotServiceTestConstructor;
  const service = Reflect.construct(snapshotServiceTestConstructor, [
    undefined,
    {
      scheduler,
      logger: new FakeLogger(),
      cryptoClientFactory: (): SnapshotCryptoClient => cryptoClient,
      marketCatalogService: marketCatalog,
      marketStreamService: marketStream,
      supportedAssets: SUPPORTED_ASSETS,
      supportedWindows: SUPPORTED_WINDOWS,
      priceToBeatInitialDelayMs: 100,
      priceToBeatRetryIntervalMs: 50,
    },
  ]) as SnapshotService;
  const fixture = { cryptoClient, marketStream, scheduler, service, marketCatalog };
  return fixture;
}

function emitSnapshot(service: SnapshotService, scheduler: FakeScheduler): void {
  const serviceInternals = service as unknown as SnapshotServiceInternals;
  serviceInternals.emitSnapshotsAt(scheduler.now());
}

function createPriceEvent(asset: SnapshotAsset, price: number, ts: number): FeedEvent {
  const priceEvent: FeedEvent = { type: "price", symbol: asset.toUpperCase(), provider: "binance", price, ts };
  return priceEvent;
}

function createCryptoOrderBookEvent(asset: SnapshotAsset, ts: number): FeedEvent {
  const symbol = asset.toUpperCase();
  const asks = [{ price: 101, size: 2 }];
  const bids = [{ price: 99, size: 3 }];
  const orderBookEvent: OrderBookSnapshot = { type: "orderbook", symbol, provider: "coinbase", ts, asks, bids };
  return orderBookEvent;
}

function createMarketPriceEvent(assetId: string, price: number, isoDate: string): MarketEvent {
  const marketEvent: MarketEvent = { type: "price", source: "polymarket", assetId, index: 0, price, date: new Date(isoDate) };
  return marketEvent;
}

function createMarketBookEvent(assetId: string, isoDate: string): MarketEvent {
  const orderBook: OrderBook = { asks: [{ price: 0.7, size: 15 }], bids: [{ price: 0.6, size: 20 }] };
  const marketDate = new Date(isoDate);
  const marketEvent: MarketEvent = { type: "book", source: "polymarket", assetId, index: 0, asks: orderBook.asks, bids: orderBook.bids, date: marketDate };
  return marketEvent;
}

test("SnapshotService emits one flat snapshot with live market slugs and market fields", async () => {
  const fixture = createFixture();
  const receivedSnapshots: Snapshot[] = [];

  fixture.service.addSnapshotListener({ listener: (snapshot): void => void receivedSnapshots.push(snapshot) });

  await waitForCondition(() => {
    const snapshot = fixture.service.getSnapshot();
    const hasLiveMarkets =
      snapshot !== null &&
      readSnapshotString(snapshot, "btc_5m_slug") !== null &&
      readSnapshotString(snapshot, "btc_15m_slug") !== null &&
      readSnapshotString(snapshot, "eth_5m_slug") !== null &&
      readSnapshotString(snapshot, "eth_15m_slug") !== null;
    return hasLiveMarkets;
  });
  emitSnapshot(fixture.service, fixture.scheduler);

  const firstSnapshot = receivedSnapshots[0];

  assert.notEqual(firstSnapshot, undefined);

  if (firstSnapshot !== undefined) {
    assert.equal(readSnapshotString(firstSnapshot, "btc_5m_slug"), BTC_FIVE_MINUTE_SLUG);
    assert.equal(readSnapshotString(firstSnapshot, "btc_15m_slug"), BTC_FIFTEEN_MINUTE_SLUG);
    assert.equal(readSnapshotString(firstSnapshot, "eth_5m_slug"), ETH_FIVE_MINUTE_SLUG);
    assert.equal(readSnapshotString(firstSnapshot, "eth_15m_slug"), ETH_FIFTEEN_MINUTE_SLUG);
    assert.equal(readSnapshotString(firstSnapshot, "btc_5m_up_asset_id"), `up-${BTC_FIVE_MINUTE_SLUG}`);
    assert.equal(Reflect.get(firstSnapshot, "btc_5m_is_live_market"), undefined);
  }
});

test("SnapshotService keeps the latest crypto and market values in the flat snapshot", async () => {
  const fixture = createFixture(Date.parse(START_ISO), new Map<string, Array<number | null>>([[BTC_FIVE_MINUTE_SLUG, [64_250]]]));

  fixture.service.addSnapshotListener({ listener: (): void => {} });
  await waitForCondition(() => fixture.service.getSnapshot() !== null);

  fixture.cryptoClient.emit(createPriceEvent("btc", 65_000, 100));
  fixture.cryptoClient.emit(createCryptoOrderBookEvent("btc", 120));
  fixture.marketStream.emit(createMarketPriceEvent(`up-${BTC_FIVE_MINUTE_SLUG}`, 0.62, "2024-01-01T00:00:00.100Z"));
  fixture.marketStream.emit(createMarketBookEvent(`up-${BTC_FIVE_MINUTE_SLUG}`, "2024-01-01T00:00:00.120Z"));
  fixture.scheduler.advanceBy(100);
  await waitForCondition(() => {
    const latestSnapshot = fixture.service.getSnapshot();
    const hasPriceToBeat = latestSnapshot !== null && readSnapshotNumber(latestSnapshot, "btc_5m_price_to_beat") !== null;
    return hasPriceToBeat;
  });

  const snapshot = fixture.service.getSnapshot();

  assert.notEqual(snapshot, null);

  if (snapshot !== null) {
    assert.equal(readSnapshotNumber(snapshot, "btc_binance_price"), 65_000);
    assert.equal(readSnapshotNumber(snapshot, "btc_coinbase_event_ts"), 120);
    assert.equal(readSnapshotString(snapshot, "btc_5m_slug"), BTC_FIVE_MINUTE_SLUG);
    assert.equal(readSnapshotNumber(snapshot, "btc_5m_price_to_beat"), 64_250);
    assert.equal(readSnapshotNumber(snapshot, "btc_5m_up_price"), 0.62);
    assert.equal(
      readSnapshotString(snapshot, "btc_5m_up_order_book_json"),
      JSON.stringify({ asks: [{ price: 0.7, size: 15 }], bids: [{ price: 0.6, size: 20 }] }),
    );
    assert.equal(Reflect.get(snapshot, "btc_chainlink_order_book_json"), undefined);
  }
});

test("SnapshotService publishes price_to_beat once it becomes available after a retry", async () => {
  const fixture = createFixture(Date.parse(START_ISO), new Map<string, Array<number | null>>([[BTC_FIVE_MINUTE_SLUG, [null, 64_300]]]));

  fixture.service.addSnapshotListener({ listener: (): void => {} });
  await waitForCondition(() => fixture.service.getSnapshot() !== null);

  const initialSnapshot = fixture.service.getSnapshot();

  assert.notEqual(initialSnapshot, null);

  if (initialSnapshot !== null) {
    assert.equal(readSnapshotNumber(initialSnapshot, "btc_5m_price_to_beat"), null);
  }

  fixture.scheduler.advanceBy(100);
  await waitForCondition(() => fixture.service.getSnapshot() !== null);
  fixture.scheduler.advanceBy(50);
  await waitForCondition(() => {
    const latestSnapshot = fixture.service.getSnapshot();
    const hasPriceToBeat = latestSnapshot !== null && readSnapshotNumber(latestSnapshot, "btc_5m_price_to_beat") === 64_300;
    return hasPriceToBeat;
  });

  const snapshot = fixture.service.getSnapshot();

  assert.notEqual(snapshot, null);

  if (snapshot !== null) {
    assert.equal(readSnapshotNumber(snapshot, "btc_5m_price_to_beat"), 64_300);
  }
});

test("SnapshotService stops the shared runtime after the last listener", async () => {
  const fixture = createFixture();
  const snapshotListener = (): void => {};

  assert.equal(fixture.service.getSnapshot(), null);
  fixture.service.addSnapshotListener({ listener: snapshotListener });
  await waitForCondition(() => fixture.service.getSnapshot() !== null);
  fixture.service.removeSnapshotListener(snapshotListener);
  await waitForCondition(() => fixture.marketStream.disconnectCount === 1);

  assert.equal(fixture.service.getSnapshot(), null);
  assert.equal(fixture.cryptoClient.disconnectCount, 1);
  assert.equal(fixture.marketStream.disconnectCount, 1);
});

test("SnapshotService isolates listener failures", async () => {
  const fixture = createFixture();
  const receivedSnapshots: Snapshot[] = [];

  function failingListener(): void {
    throw new Error("listener failure");
  }

  fixture.service.addSnapshotListener({ listener: failingListener });
  fixture.service.addSnapshotListener({ listener: (snapshot): void => void receivedSnapshots.push(snapshot) });
  await waitForCondition(() => fixture.service.getSnapshot() !== null);
  emitSnapshot(fixture.service, fixture.scheduler);

  assert.equal(receivedSnapshots.length, 1);
});
