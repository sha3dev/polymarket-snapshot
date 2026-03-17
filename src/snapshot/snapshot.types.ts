/**
 * @section imports:externals
 */

import type { FeedEvent } from "@sha3/crypto";
import type { CryptoMarketWindow, CryptoSymbol, MarketEvent, OrderBook, PolymarketMarket } from "@sha3/polymarket";

/**
 * @section types
 */

export type SnapshotAsset = CryptoSymbol;

export type SnapshotWindow = CryptoMarketWindow;

export type ProviderSnapshot = {
  price: number | null;
  order_book_json: string | null;
  event_ts: number | null;
};

export type PolymarketOutcomeSnapshot = {
  assetId: string | null;
  price: number | null;
  orderBook: OrderBook | null;
  eventTs: number | null;
};

export type PairSnapshot = {
  generated_at: number;
  asset: SnapshotAsset;
  window: SnapshotWindow;
  is_live_market: boolean;
  slug: string | null;
  market_start: string | null;
  market_end: string | null;
  price_to_beat: number | null;
  up_asset_id: string | null;
  up_price: number | null;
  up_order_book_json: string | null;
  up_event_ts: number | null;
  down_asset_id: string | null;
  down_price: number | null;
  down_order_book_json: string | null;
  down_event_ts: number | null;
  binance_price: number | null;
  binance_order_book_json: string | null;
  binance_event_ts: number | null;
  coinbase_price: number | null;
  coinbase_order_book_json: string | null;
  coinbase_event_ts: number | null;
  kraken_price: number | null;
  kraken_order_book_json: string | null;
  kraken_event_ts: number | null;
  okx_price: number | null;
  okx_order_book_json: string | null;
  okx_event_ts: number | null;
  chainlink_price: number | null;
  chainlink_event_ts: number | null;
};

export type Snapshot = {
  generated_at: number;
} & Record<string, number | string | null>;

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
};

export type SnapshotLogger = {
  warn(message: string): void;
  error(message: string): void;
};

export type AddSnapshotListenerOptions = {
  listener: SnapshotListener;
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
