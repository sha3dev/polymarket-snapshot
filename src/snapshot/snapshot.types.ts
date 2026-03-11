/**
 * @section imports:externals
 */

import type { CryptoFeedClient, CryptoProviderId, FeedEvent, OrderBookSnapshot } from "@sha3/crypto";
import type { CryptoMarketWindow, CryptoSymbol, MarketCatalogService, MarketEvent, MarketStreamService, OrderBook, PolymarketMarket } from "@sha3/polymarket";

/**
 * @section types
 */

export type SnapshotAsset = CryptoSymbol;

export type SnapshotWindow = CryptoMarketWindow;

export type ProviderSnapshot = {
  price: number | null;
  orderBook: OrderBookSnapshot | null;
  eventTs: number | null;
};

export type PolymarketOutcomeSnapshot = {
  assetId: string | null;
  price: number | null;
  orderBook: OrderBook | null;
  eventTs: number | null;
};

export type Snapshot = {
  asset: SnapshotAsset;
  window: SnapshotWindow;
  generatedAt: number;
  marketId: string | null;
  marketSlug: string | null;
  marketConditionId: string | null;
  marketStart: string | null;
  marketEnd: string | null;
  priceToBeat: number | null;
  upAssetId: string | null;
  upPrice: number | null;
  upOrderBook: OrderBook | null;
  upEventTs: number | null;
  downAssetId: string | null;
  downPrice: number | null;
  downOrderBook: OrderBook | null;
  downEventTs: number | null;
  binancePrice: number | null;
  binanceOrderBook: OrderBookSnapshot | null;
  binanceEventTs: number | null;
  coinbasePrice: number | null;
  coinbaseOrderBook: OrderBookSnapshot | null;
  coinbaseEventTs: number | null;
  krakenPrice: number | null;
  krakenOrderBook: OrderBookSnapshot | null;
  krakenEventTs: number | null;
  okxPrice: number | null;
  okxOrderBook: OrderBookSnapshot | null;
  okxEventTs: number | null;
  chainlinkPrice: number | null;
  chainlinkOrderBook: OrderBookSnapshot | null;
  chainlinkEventTs: number | null;
};

export type SnapshotListener = (snapshot: Snapshot) => void;

export type SnapshotSubscription = { unsubscribe(): void };

export type SnapshotCryptoClient = {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(listener: (event: FeedEvent) => void): SnapshotSubscription;
};

export type SnapshotMarketCatalog = {
  buildCryptoWindowSlugs(options: { date: Date; window: SnapshotWindow; symbols?: SnapshotAsset[] }): string[];
  loadMarketBySlug(options: { slug: string }): Promise<PolymarketMarket>;
  getPriceToBeat(options: { market: PolymarketMarket }): Promise<number | null>;
};

export type SnapshotMarketStream = {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(options: { assetIds: string[] }): void;
  unsubscribe(options: { assetIds: string[] }): void;
  addListener(options: { listener: (event: MarketEvent) => void }): () => void;
};

export type SnapshotScheduler = {
  now(): number;
  setTimeout(listener: () => void, delayMs: number): unknown;
  clearTimeout(timer: unknown): void;
  setInterval(listener: () => void, delayMs: number): unknown;
  clearInterval(timer: unknown): void;
};

export type SnapshotLogger = {
  debug(message: string): void;
  warn(message: string): void;
  error(message: string): void;
};

export type AddSnapshotListenerOptions = {
  listener: SnapshotListener;
  assets?: SnapshotAsset[];
  windows?: SnapshotWindow[];
};

export type GetSnapshotOptions = {
  assets?: SnapshotAsset[];
  windows?: SnapshotWindow[];
};

export type SnapshotServiceOptions = {
  snapshotIntervalMs?: number;
  supportedAssets?: SnapshotAsset[];
  supportedWindows?: SnapshotWindow[];
  priceToBeatInitialDelayMs?: number;
  priceToBeatRetryIntervalMs?: number;
  cryptoClientFactory?: (assets: SnapshotAsset[]) => SnapshotCryptoClient;
  marketCatalogService?: SnapshotMarketCatalog;
  marketStreamService?: SnapshotMarketStream;
  scheduler?: SnapshotScheduler;
  logger?: SnapshotLogger;
};

export type SnapshotRuntimeDependencies = {
  cryptoClientFactory: (assets: SnapshotAsset[]) => SnapshotCryptoClient;
  marketCatalogService: SnapshotMarketCatalog;
  marketStreamService: SnapshotMarketStream;
  scheduler: SnapshotScheduler;
  logger: SnapshotLogger;
};

export type SnapshotDefaultRuntime = {
  cryptoClientFactory: (assets: SnapshotAsset[]) => CryptoFeedClient;
  marketCatalogService: MarketCatalogService;
  marketStreamService: MarketStreamService;
};

export type PairState = {
  asset: SnapshotAsset;
  window: SnapshotWindow;
  currentMarket: PolymarketMarket | null;
  currentSlug: string | null;
  priceToBeat: number | null;
  hasResolvedPriceToBeat: boolean;
  isPriceToBeatLoading: boolean;
  priceToBeatTimer: unknown | null;
  rotationTimer: unknown | null;
  up: PolymarketOutcomeSnapshot;
  down: PolymarketOutcomeSnapshot;
};

export type PairKeyParts = { asset: SnapshotAsset; window: SnapshotWindow };

export type RuntimeState = {
  snapshotStartTimeout: unknown | null;
  snapshotInterval: unknown | null;
  marketListenerRemover: (() => void) | null;
  cryptoSubscription: SnapshotSubscription | null;
};
