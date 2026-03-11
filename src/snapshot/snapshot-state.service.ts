/**
 * @section imports:externals
 */

import type { CryptoProviderId, OrderBookSnapshot } from "@sha3/crypto";
import type { OrderBook } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import type { PairState, ProviderSnapshot, Snapshot, SnapshotAsset, SnapshotWindow } from "./snapshot.types.ts";

/**
 * @section consts
 */

const PROVIDER_IDS: CryptoProviderId[] = ["binance", "coinbase", "kraken", "okx", "chainlink"];

/**
 * @section public:properties
 */

export class SnapshotState {
  /**
   * @section factory
   */

  public static create(): SnapshotState {
    const service = new SnapshotState();
    return service;
  }

  /**
   * @section private:methods
   */

  private buildProviderSnapshotFields(
    providerSnapshots: Record<CryptoProviderId, ProviderSnapshot>,
  ): Pick<
    Snapshot,
    | "binancePrice"
    | "binanceOrderBook"
    | "binanceEventTs"
    | "coinbasePrice"
    | "coinbaseOrderBook"
    | "coinbaseEventTs"
    | "krakenPrice"
    | "krakenOrderBook"
    | "krakenEventTs"
    | "okxPrice"
    | "okxOrderBook"
    | "okxEventTs"
    | "chainlinkPrice"
    | "chainlinkOrderBook"
    | "chainlinkEventTs"
  > {
    const binanceSnapshot = this.cloneProviderSnapshot(providerSnapshots.binance);
    const coinbaseSnapshot = this.cloneProviderSnapshot(providerSnapshots.coinbase);
    const krakenSnapshot = this.cloneProviderSnapshot(providerSnapshots.kraken);
    const okxSnapshot = this.cloneProviderSnapshot(providerSnapshots.okx);
    const chainlinkSnapshot = this.cloneProviderSnapshot(providerSnapshots.chainlink);
    const snapshotFields = {
      binancePrice: binanceSnapshot.price,
      binanceOrderBook: binanceSnapshot.orderBook,
      binanceEventTs: binanceSnapshot.eventTs,
      coinbasePrice: coinbaseSnapshot.price,
      coinbaseOrderBook: coinbaseSnapshot.orderBook,
      coinbaseEventTs: coinbaseSnapshot.eventTs,
      krakenPrice: krakenSnapshot.price,
      krakenOrderBook: krakenSnapshot.orderBook,
      krakenEventTs: krakenSnapshot.eventTs,
      okxPrice: okxSnapshot.price,
      okxOrderBook: okxSnapshot.orderBook,
      okxEventTs: okxSnapshot.eventTs,
      chainlinkPrice: chainlinkSnapshot.price,
      chainlinkOrderBook: chainlinkSnapshot.orderBook,
      chainlinkEventTs: chainlinkSnapshot.eventTs,
    };
    return snapshotFields;
  }

  /**
   * @section public:methods
   */

  public clonePricePointOrderBook(orderBook: OrderBookSnapshot | null): OrderBookSnapshot | null {
    let clonedOrderBook: OrderBookSnapshot | null = null;

    if (orderBook !== null) {
      clonedOrderBook = { ...orderBook, asks: orderBook.asks.map((level) => ({ ...level })), bids: orderBook.bids.map((level) => ({ ...level })) };
    }

    return clonedOrderBook;
  }

  public clonePolymarketOrderBook(orderBook: OrderBook | null): OrderBook | null {
    let clonedOrderBook: OrderBook | null = null;

    if (orderBook !== null) {
      clonedOrderBook = { asks: orderBook.asks.map((level) => ({ ...level })), bids: orderBook.bids.map((level) => ({ ...level })) };
    }

    return clonedOrderBook;
  }

  public cloneProviderSnapshot(providerSnapshot: ProviderSnapshot): ProviderSnapshot {
    const clonedOrderBook = this.clonePricePointOrderBook(providerSnapshot.orderBook);
    const clonedProviderSnapshot: ProviderSnapshot = { price: providerSnapshot.price, orderBook: clonedOrderBook, eventTs: providerSnapshot.eventTs };
    return clonedProviderSnapshot;
  }

  public createProviderSnapshotRecord(): Record<CryptoProviderId, ProviderSnapshot> {
    const providerSnapshots = {} as Record<CryptoProviderId, ProviderSnapshot>;

    for (const providerId of PROVIDER_IDS) {
      providerSnapshots[providerId] = { price: null, orderBook: null, eventTs: null };
    }

    return providerSnapshots;
  }

  public createPairState(asset: SnapshotAsset, window: SnapshotWindow): PairState {
    const pairState: PairState = {
      asset,
      window,
      currentMarket: null,
      currentSlug: null,
      priceToBeat: null,
      hasResolvedPriceToBeat: false,
      isPriceToBeatLoading: false,
      priceToBeatTimer: null,
      rotationTimer: null,
      up: { assetId: null, price: null, orderBook: null, eventTs: null },
      down: { assetId: null, price: null, orderBook: null, eventTs: null },
    };
    return pairState;
  }

  public buildSnapshot(pairState: PairState, generatedAt: number, providerSnapshots: Record<CryptoProviderId, ProviderSnapshot>): Snapshot {
    const market = pairState.currentMarket;
    const providerSnapshotFields = this.buildProviderSnapshotFields(providerSnapshots);
    const snapshot: Snapshot = {
      asset: pairState.asset,
      window: pairState.window,
      generatedAt,
      marketId: market?.id ?? null,
      marketSlug: market?.slug ?? null,
      marketConditionId: market?.conditionId ?? null,
      marketStart: market?.start.toISOString() ?? null,
      marketEnd: market?.end.toISOString() ?? null,
      priceToBeat: pairState.priceToBeat,
      upAssetId: pairState.up.assetId,
      upPrice: pairState.up.price,
      upOrderBook: this.clonePolymarketOrderBook(pairState.up.orderBook),
      upEventTs: pairState.up.eventTs,
      downAssetId: pairState.down.assetId,
      downPrice: pairState.down.price,
      downOrderBook: this.clonePolymarketOrderBook(pairState.down.orderBook),
      downEventTs: pairState.down.eventTs,
      ...providerSnapshotFields,
    };
    return snapshot;
  }
}
