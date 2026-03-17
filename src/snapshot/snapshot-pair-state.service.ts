/**
 * @section imports:externals
 */

import type { CryptoProviderId, FeedEvent, OrderBookSnapshot, PricePoint } from "@sha3/crypto";
import type { MarketEvent, OrderBook } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import type { PairSnapshot, PairState, PolymarketOutcomeSnapshot, ProviderSnapshot, SnapshotAsset } from "./snapshot.types.ts";

/**
 * @section class
 */

export class SnapshotPairState {
  /**
   * @section private:attributes
   */

  private readonly supportedAssets: SnapshotAsset[];

  /**
   * @section constructor
   */

  public constructor(options: { supportedAssets: SnapshotAsset[] }) {
    this.supportedAssets = [...options.supportedAssets];
  }

  /**
   * @section private:methods
   */

  private clonePricePointOrderBook(orderBook: OrderBookSnapshot): OrderBookSnapshot {
    const clonedOrderBook = { ...orderBook, asks: orderBook.asks.map((level) => ({ ...level })), bids: orderBook.bids.map((level) => ({ ...level })) };
    return clonedOrderBook;
  }

  private clonePolymarketOrderBook(orderBook: OrderBook): OrderBook {
    const clonedOrderBook = { asks: orderBook.asks.map((level) => ({ ...level })), bids: orderBook.bids.map((level) => ({ ...level })) };
    return clonedOrderBook;
  }

  private stringifyOrderBook(orderBook: OrderBookSnapshot | OrderBook | null): string | null {
    const orderBookJson = orderBook === null ? null : JSON.stringify(orderBook);
    return orderBookJson;
  }

  private buildProviderSnapshotFields(
    providerSnapshots: Record<CryptoProviderId, ProviderSnapshot>,
  ): Pick<
    PairSnapshot,
    | "binance_price"
    | "binance_order_book_json"
    | "binance_event_ts"
    | "coinbase_price"
    | "coinbase_order_book_json"
    | "coinbase_event_ts"
    | "kraken_price"
    | "kraken_order_book_json"
    | "kraken_event_ts"
    | "okx_price"
    | "okx_order_book_json"
    | "okx_event_ts"
    | "chainlink_price"
    | "chainlink_event_ts"
  > {
    const snapshotFields = {
      binance_price: providerSnapshots.binance.price,
      binance_order_book_json: providerSnapshots.binance.order_book_json,
      binance_event_ts: providerSnapshots.binance.event_ts,
      coinbase_price: providerSnapshots.coinbase.price,
      coinbase_order_book_json: providerSnapshots.coinbase.order_book_json,
      coinbase_event_ts: providerSnapshots.coinbase.event_ts,
      kraken_price: providerSnapshots.kraken.price,
      kraken_order_book_json: providerSnapshots.kraken.order_book_json,
      kraken_event_ts: providerSnapshots.kraken.event_ts,
      okx_price: providerSnapshots.okx.price,
      okx_order_book_json: providerSnapshots.okx.order_book_json,
      okx_event_ts: providerSnapshots.okx.event_ts,
      chainlink_price: providerSnapshots.chainlink.price,
      chainlink_event_ts: providerSnapshots.chainlink.event_ts,
    };
    return snapshotFields;
  }

  private getCryptoState(
    cryptoStateByAsset: Map<SnapshotAsset, Record<CryptoProviderId, ProviderSnapshot>>,
    asset: SnapshotAsset,
  ): Record<CryptoProviderId, ProviderSnapshot> {
    let providerSnapshots = cryptoStateByAsset.get(asset) ?? null;

    if (providerSnapshots === null) {
      providerSnapshots = {
        binance: { price: null, order_book_json: null, event_ts: null },
        coinbase: { price: null, order_book_json: null, event_ts: null },
        kraken: { price: null, order_book_json: null, event_ts: null },
        okx: { price: null, order_book_json: null, event_ts: null },
        chainlink: { price: null, order_book_json: null, event_ts: null },
      };
      cryptoStateByAsset.set(asset, providerSnapshots);
    }

    return providerSnapshots;
  }

  private readSnapshotAsset(symbol: string): SnapshotAsset | null {
    const normalizedSymbol = symbol.toLowerCase() as SnapshotAsset;
    const snapshotAsset = this.supportedAssets.includes(normalizedSymbol) ? normalizedSymbol : null;
    return snapshotAsset;
  }

  private applyCryptoPrice(providerSnapshots: Record<CryptoProviderId, ProviderSnapshot>, event: PricePoint): void {
    const providerSnapshot = providerSnapshots[event.provider];

    providerSnapshot.price = event.price;
    providerSnapshot.event_ts = event.ts;
  }

  private applyCryptoOrderBook(providerSnapshots: Record<CryptoProviderId, ProviderSnapshot>, event: OrderBookSnapshot): void {
    const providerSnapshot = providerSnapshots[event.provider];

    if (event.provider !== "chainlink") {
      providerSnapshot.order_book_json = this.stringifyOrderBook(this.clonePricePointOrderBook(event));
      providerSnapshot.event_ts = event.ts;
    }
  }

  private isGeneratedAtInsideCurrentMarket(pairState: PairState, generatedAt: number): boolean {
    const market = pairState.currentMarket;
    let isGeneratedAtInsideCurrentMarket = false;

    if (market !== null) {
      const marketStartMs = market.start.getTime();
      const marketEndMs = market.end.getTime();
      isGeneratedAtInsideCurrentMarket = generatedAt >= marketStartMs && generatedAt < marketEndMs;
    }

    return isGeneratedAtInsideCurrentMarket;
  }

  private isEventInsideMarket(pairState: PairState, event: MarketEvent): boolean {
    const market = pairState.currentMarket;
    let isEventInsideMarket = false;

    if (market !== null) {
      const eventMs = event.date.getTime();
      const startMs = market.start.getTime();
      const endMs = market.end.getTime();
      isEventInsideMarket = eventMs >= startMs && eventMs < endMs;
    }

    return isEventInsideMarket;
  }

  private applyPolymarketEvent(pairState: PairState, event: MarketEvent): void {
    const isUpEvent = pairState.up.assetId === event.assetId;
    const isDownEvent = pairState.down.assetId === event.assetId;

    if (isUpEvent) {
      this.applyPolymarketOutcomeEvent(pairState.up, event);
    }

    if (isDownEvent) {
      this.applyPolymarketOutcomeEvent(pairState.down, event);
    }
  }

  private applyPolymarketOutcomeEvent(outcomeSnapshot: PolymarketOutcomeSnapshot, event: MarketEvent): void {
    outcomeSnapshot.eventTs = event.date.getTime();

    if (event.type === "price") {
      outcomeSnapshot.price = event.price;
    }

    if (event.type === "book") {
      outcomeSnapshot.orderBook = this.clonePolymarketOrderBook({ asks: event.asks, bids: event.bids });
    }
  }

  private buildMarketSnapshotFields(
    pairState: PairState,
    generatedAt: number,
  ): Pick<PairSnapshot, "is_live_market" | "slug" | "market_start" | "market_end" | "price_to_beat"> {
    const isLiveMarket = this.isGeneratedAtInsideCurrentMarket(pairState, generatedAt);
    const market = pairState.currentMarket;
    const slug = isLiveMarket ? pairState.currentSlug : null;
    const marketStart = isLiveMarket && market !== null ? market.start.toISOString() : null;
    const marketEnd = isLiveMarket && market !== null ? market.end.toISOString() : null;
    const priceToBeat = isLiveMarket ? pairState.priceToBeat : null;
    const marketSnapshotFields = { is_live_market: isLiveMarket, slug, market_start: marketStart, market_end: marketEnd, price_to_beat: priceToBeat };
    return marketSnapshotFields;
  }

  private buildOutcomeSnapshotFields(
    pairState: PairState,
  ): Pick<
    PairSnapshot,
    "up_asset_id" | "up_price" | "up_order_book_json" | "up_event_ts" | "down_asset_id" | "down_price" | "down_order_book_json" | "down_event_ts"
  > {
    const outcomeSnapshotFields = {
      up_asset_id: pairState.up.assetId,
      up_price: pairState.up.price,
      up_order_book_json: this.stringifyOrderBook(pairState.up.orderBook === null ? null : this.clonePolymarketOrderBook(pairState.up.orderBook)),
      up_event_ts: pairState.up.eventTs,
      down_asset_id: pairState.down.assetId,
      down_price: pairState.down.price,
      down_order_book_json: this.stringifyOrderBook(pairState.down.orderBook === null ? null : this.clonePolymarketOrderBook(pairState.down.orderBook)),
      down_event_ts: pairState.down.eventTs,
    };
    return outcomeSnapshotFields;
  }

  private buildPairSnapshot(
    cryptoStateByAsset: Map<SnapshotAsset, Record<CryptoProviderId, ProviderSnapshot>>,
    pairState: PairState,
    generatedAt: number,
  ): PairSnapshot {
    const providerSnapshots = this.getCryptoState(cryptoStateByAsset, pairState.asset);
    const marketSnapshotFields = this.buildMarketSnapshotFields(pairState, generatedAt);
    const outcomeSnapshotFields = this.buildOutcomeSnapshotFields(pairState);
    const providerSnapshotFields = this.buildProviderSnapshotFields(providerSnapshots);
    const pairSnapshotBaseFields = { generated_at: generatedAt, asset: pairState.asset, window: pairState.window };
    const pairSnapshot: PairSnapshot = { ...pairSnapshotBaseFields, ...marketSnapshotFields, ...outcomeSnapshotFields, ...providerSnapshotFields };
    return pairSnapshot;
  }

  /**
   * @section public:methods
   */

  public readSnapshots(
    cryptoStateByAsset: Map<SnapshotAsset, Record<CryptoProviderId, ProviderSnapshot>>,
    pairStateByKey: Map<string, PairState>,
    pairKeys: string[],
    generatedAt: number,
  ): Map<string, PairSnapshot> {
    const pairSnapshotByPairKey = new Map<string, PairSnapshot>();

    for (const pairKey of pairKeys) {
      const pairState = pairStateByKey.get(pairKey) ?? null;

      if (pairState !== null) {
        pairSnapshotByPairKey.set(pairKey, this.buildPairSnapshot(cryptoStateByAsset, pairState, generatedAt));
      }
    }

    return pairSnapshotByPairKey;
  }

  public readEmittableSnapshots(
    cryptoStateByAsset: Map<SnapshotAsset, Record<CryptoProviderId, ProviderSnapshot>>,
    pairStateByKey: Map<string, PairState>,
    pairKeys: string[],
    generatedAt: number,
  ): Map<string, PairSnapshot> {
    const pairSnapshotByPairKey = new Map<string, PairSnapshot>();

    for (const pairKey of pairKeys) {
      const pairState = pairStateByKey.get(pairKey) ?? null;
      const isInsideMarket = pairState !== null ? this.isGeneratedAtInsideCurrentMarket(pairState, generatedAt) : false;

      if (pairState !== null && isInsideMarket) {
        pairSnapshotByPairKey.set(pairKey, this.buildPairSnapshot(cryptoStateByAsset, pairState, generatedAt));
      }
    }

    return pairSnapshotByPairKey;
  }

  public handleCryptoEvent(cryptoStateByAsset: Map<SnapshotAsset, Record<CryptoProviderId, ProviderSnapshot>>, event: FeedEvent): void {
    const snapshotAsset = "symbol" in event ? this.readSnapshotAsset(event.symbol) : null;
    const isDataEvent = event.type === "price" || event.type === "orderbook";

    if (snapshotAsset !== null && isDataEvent) {
      const providerSnapshots = this.getCryptoState(cryptoStateByAsset, snapshotAsset);

      if (event.type === "price") {
        this.applyCryptoPrice(providerSnapshots, event);
      }

      if (event.type === "orderbook") {
        this.applyCryptoOrderBook(providerSnapshots, event);
      }
    }
  }

  public handlePolymarketEvent(pairKeysByPolymarketAssetId: Map<string, Set<string>>, pairStateByKey: Map<string, PairState>, event: MarketEvent): void {
    const pairKeys = pairKeysByPolymarketAssetId.get(event.assetId) ?? null;

    if (pairKeys !== null) {
      for (const pairKey of pairKeys) {
        const pairState = pairStateByKey.get(pairKey) ?? null;

        if (pairState !== null && this.isEventInsideMarket(pairState, event)) {
          this.applyPolymarketEvent(pairState, event);
        }
      }
    }
  }
}
