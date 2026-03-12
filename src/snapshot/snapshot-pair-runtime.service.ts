/**
 * @section imports:externals
 */

import type { CryptoProviderId, FeedEvent, OrderBookSnapshot, PricePoint } from "@sha3/crypto";
import type { MarketEvent, PolymarketMarket } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import config from "../config.ts";
import { SnapshotState } from "./snapshot-state.service.ts";
import type {
  PairKeyParts,
  PairState,
  PolymarketOutcomeSnapshot,
  ProviderSnapshot,
  Snapshot,
  SnapshotAsset,
  SnapshotLogger,
  SnapshotMarketCatalog,
  SnapshotMarketStream,
  SnapshotScheduler,
  SnapshotWindow,
} from "./snapshot.types.ts";

/**
 * @section consts
 */

const SNAPSHOT_STATE = SnapshotState.create();

/**
 * @section types
 */

type SnapshotPairRuntimeOptions = {
  marketCatalogService: SnapshotMarketCatalog;
  marketStreamService: SnapshotMarketStream;
  scheduler: SnapshotScheduler;
  serviceLogger: SnapshotLogger;
  supportedAssets: SnapshotAsset[];
  priceToBeatInitialDelayMs: number;
  priceToBeatRetryIntervalMs: number;
};

export class SnapshotPairRuntime {
  /**
   * @section private:properties
   */

  private readonly marketCatalogService: SnapshotMarketCatalog;
  private readonly marketStreamService: SnapshotMarketStream;
  private readonly scheduler: SnapshotScheduler;
  private readonly serviceLogger: SnapshotLogger;
  private readonly supportedAssets: SnapshotAsset[];
  private readonly priceToBeatInitialDelayMs: number;
  private readonly priceToBeatRetryIntervalMs: number;
  private readonly cryptoStateByAsset: Map<SnapshotAsset, Record<CryptoProviderId, ProviderSnapshot>>;
  private readonly pairStateByKey: Map<string, PairState>;
  private readonly pairKeysByPolymarketAssetId: Map<string, Set<string>>;
  private readonly subscriptionCountByAssetId: Map<string, number>;

  /**
   * @section constructor
   */

  public constructor(options: SnapshotPairRuntimeOptions) {
    this.marketCatalogService = options.marketCatalogService;
    this.marketStreamService = options.marketStreamService;
    this.scheduler = options.scheduler;
    this.serviceLogger = options.serviceLogger;
    this.supportedAssets = [...options.supportedAssets];
    this.priceToBeatInitialDelayMs = options.priceToBeatInitialDelayMs;
    this.priceToBeatRetryIntervalMs = options.priceToBeatRetryIntervalMs;
    this.cryptoStateByAsset = new Map<SnapshotAsset, Record<CryptoProviderId, ProviderSnapshot>>();
    this.pairStateByKey = new Map<string, PairState>();
    this.pairKeysByPolymarketAssetId = new Map<string, Set<string>>();
    this.subscriptionCountByAssetId = new Map<string, number>();
  }

  /**
   * @section private:methods
   */

  private parsePairKey(pairKey: string): PairKeyParts {
    const segments = pairKey.split(":");
    const pairKeyParts = { asset: segments[0] as SnapshotAsset, window: segments[1] as SnapshotWindow };
    return pairKeyParts;
  }

  private getCryptoState(asset: SnapshotAsset): Record<CryptoProviderId, ProviderSnapshot> {
    let providerSnapshots = this.cryptoStateByAsset.get(asset) ?? null;

    if (providerSnapshots === null) {
      providerSnapshots = SNAPSHOT_STATE.createProviderSnapshotRecord();
      this.cryptoStateByAsset.set(asset, providerSnapshots);
    }

    return providerSnapshots;
  }

  private async activateMissingPairs(activePairKeys: Set<string>): Promise<void> {
    for (const pairKey of activePairKeys) {
      const isTrackedPair = this.pairStateByKey.has(pairKey);

      if (!isTrackedPair) {
        const pairKeyParts = this.parsePairKey(pairKey);
        const pairState = SNAPSHOT_STATE.createPairState(pairKeyParts.asset, pairKeyParts.window);
        this.pairStateByKey.set(pairKey, pairState);
        await this.activatePairMarket(pairKey, pairState, new Date(this.scheduler.now()));
      }
    }
  }

  private deactivateInactivePairs(activePairKeys: Set<string>): void {
    const trackedPairKeys = [...this.pairStateByKey.keys()];

    for (const pairKey of trackedPairKeys) {
      const isStillActive = activePairKeys.has(pairKey);

      if (!isStillActive) {
        this.deactivatePair(pairKey);
      }
    }
  }

  private deactivatePair(pairKey: string): void {
    const pairState = this.pairStateByKey.get(pairKey) ?? null;

    if (pairState !== null) {
      this.clearPairTimers(pairState);
      this.detachMarketTokens(pairKey, pairState);
      this.pairStateByKey.delete(pairKey);
    }
  }

  private clearPairTimers(pairState: PairState): void {
    if (pairState.priceToBeatTimer !== null) {
      this.scheduler.clearTimeout(pairState.priceToBeatTimer);
      pairState.priceToBeatTimer = null;
    }

    if (pairState.rotationTimer !== null) {
      this.scheduler.clearTimeout(pairState.rotationTimer);
      pairState.rotationTimer = null;
    }
  }

  private buildSlug(asset: SnapshotAsset, window: SnapshotWindow, date: Date): string {
    const slugs = this.marketCatalogService.buildCryptoWindowSlugs({ date, window, symbols: [asset] });
    const slug = slugs[0] ?? "";

    if (slug.length === 0) {
      throw new Error(`Failed to build market slug for ${asset}/${window}.`);
    }

    return slug;
  }

  private resetPairMarketState(pairState: PairState, market: PolymarketMarket): void {
    pairState.currentMarket = market;
    pairState.currentSlug = market.slug;
    pairState.priceToBeat = null;
    pairState.hasResolvedPriceToBeat = false;
    pairState.isPriceToBeatLoading = false;
    pairState.up = { assetId: market.upTokenId, price: null, orderBook: null, eventTs: null };
    pairState.down = { assetId: market.downTokenId, price: null, orderBook: null, eventTs: null };
  }

  private attachMarketTokens(pairKey: string, pairState: PairState): void {
    const market = pairState.currentMarket;

    if (market !== null) {
      this.attachMarketToken(pairKey, market.upTokenId);
      this.attachMarketToken(pairKey, market.downTokenId);
    }
  }

  private attachMarketToken(pairKey: string, assetId: string): void {
    const pairKeys = this.pairKeysByPolymarketAssetId.get(assetId) ?? new Set<string>();
    const nextCount = (this.subscriptionCountByAssetId.get(assetId) ?? 0) + 1;

    pairKeys.add(pairKey);
    this.pairKeysByPolymarketAssetId.set(assetId, pairKeys);
    this.subscriptionCountByAssetId.set(assetId, nextCount);

    if (nextCount === 1) {
      this.marketStreamService.subscribe({ assetIds: [assetId] });
    }
  }

  private detachMarketTokens(pairKey: string, pairState: PairState): void {
    const market = pairState.currentMarket;

    if (market !== null) {
      this.detachMarketToken(pairKey, market.upTokenId);
      this.detachMarketToken(pairKey, market.downTokenId);
    }
  }

  private detachMarketToken(pairKey: string, assetId: string): void {
    const pairKeys = this.pairKeysByPolymarketAssetId.get(assetId) ?? null;
    const currentCount = this.subscriptionCountByAssetId.get(assetId) ?? 0;

    if (pairKeys !== null) {
      pairKeys.delete(pairKey);

      if (pairKeys.size === 0) {
        this.pairKeysByPolymarketAssetId.delete(assetId);
      }
    }

    if (currentCount > 0) {
      const nextCount = currentCount - 1;

      if (nextCount === 0) {
        this.subscriptionCountByAssetId.delete(assetId);
        this.marketStreamService.unsubscribe({ assetIds: [assetId] });
      }

      if (nextCount > 0) {
        this.subscriptionCountByAssetId.set(assetId, nextCount);
      }
    }
  }

  private scheduleMarketRotation(pairKey: string, pairState: PairState, nowMs: number): void {
    const nextBoundaryMs = this.getNextBoundaryMs(pairState.window, nowMs);
    const delayMs = Math.max(nextBoundaryMs + config.MARKET_BOUNDARY_DELAY_MS - nowMs, 0);

    if (pairState.rotationTimer !== null) {
      this.scheduler.clearTimeout(pairState.rotationTimer);
    }

    pairState.rotationTimer = this.scheduler.setTimeout((): void => {
      void this.handleMarketRotation(pairKey);
    }, delayMs);
  }

  private getNextBoundaryMs(window: SnapshotWindow, nowMs: number): number {
    const windowMinutes = window === "5m" ? 5 : 15;
    const windowMs = windowMinutes * 60 * 1000;
    const nextBoundaryMs = Math.floor(nowMs / windowMs) * windowMs + windowMs;
    return nextBoundaryMs;
  }

  private async handleMarketRotation(pairKey: string): Promise<void> {
    const pairState = this.pairStateByKey.get(pairKey) ?? null;

    if (pairState !== null) {
      await this.activatePairMarket(pairKey, pairState, new Date(this.scheduler.now()));
    }
  }

  private scheduleMarketActivationRetry(pairKey: string, pairState: PairState): void {
    if (pairState.rotationTimer !== null) {
      this.scheduler.clearTimeout(pairState.rotationTimer);
    }

    pairState.rotationTimer = this.scheduler.setTimeout((): void => {
      void this.handleMarketRotation(pairKey);
    }, config.MARKET_ACTIVATION_RETRY_INTERVAL_MS);
  }

  private schedulePriceToBeat(pairKey: string, pairState: PairState, delayMs: number): void {
    if (pairState.priceToBeatTimer !== null) {
      this.scheduler.clearTimeout(pairState.priceToBeatTimer);
    }

    pairState.priceToBeatTimer = this.scheduler.setTimeout((): void => {
      void this.loadPriceToBeat(pairKey);
    }, delayMs);
  }

  private async activatePairMarket(pairKey: string, pairState: PairState, date: Date): Promise<void> {
    const nextSlug = this.buildSlug(pairState.asset, pairState.window, date);
    const shouldReloadMarket = nextSlug !== pairState.currentSlug;

    if (shouldReloadMarket) {
      await this.reloadPairMarket(pairKey, pairState, nextSlug);
    }

    if (!shouldReloadMarket) {
      this.scheduleMarketRotation(pairKey, pairState, this.scheduler.now());
    }

    if (shouldReloadMarket && pairState.currentSlug === nextSlug) {
      this.scheduleMarketRotation(pairKey, pairState, this.scheduler.now());
    }
  }

  private async reloadPairMarket(pairKey: string, pairState: PairState, nextSlug: string): Promise<void> {
    try {
      const nextMarket = await this.marketCatalogService.loadMarketBySlug({ slug: nextSlug });
      this.detachMarketTokens(pairKey, pairState);
      this.resetPairMarketState(pairState, nextMarket);
      this.attachMarketTokens(pairKey, pairState);
      this.schedulePriceToBeat(pairKey, pairState, this.priceToBeatInitialDelayMs);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      this.serviceLogger.warn(`[SNAPSHOT] Failed to activate market ${pairKey}: ${reason}`);
      this.scheduleMarketActivationRetry(pairKey, pairState);
    }
  }

  private async loadPriceToBeat(pairKey: string): Promise<void> {
    const pairState = this.pairStateByKey.get(pairKey) ?? null;

    if (pairState !== null && !pairState.hasResolvedPriceToBeat && !pairState.isPriceToBeatLoading && pairState.currentMarket !== null) {
      pairState.isPriceToBeatLoading = true;

      try {
        const priceToBeat = await this.marketCatalogService.getPriceToBeat({ market: pairState.currentMarket });

        if (priceToBeat !== null) {
          pairState.priceToBeat = priceToBeat;
          pairState.hasResolvedPriceToBeat = true;
          pairState.priceToBeatTimer = null;
        }

        if (priceToBeat === null) {
          this.schedulePriceToBeat(pairKey, pairState, this.priceToBeatRetryIntervalMs);
        }
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        this.serviceLogger.warn(`[SNAPSHOT] Failed to load priceToBeat ${pairKey}: ${reason}`);
        this.schedulePriceToBeat(pairKey, pairState, this.priceToBeatRetryIntervalMs);
      }

      pairState.isPriceToBeatLoading = false;
    }
  }

  private readSnapshotAsset(symbol: string): SnapshotAsset | null {
    const normalizedSymbol = symbol.toLowerCase() as SnapshotAsset;
    const snapshotAsset = this.supportedAssets.includes(normalizedSymbol) ? normalizedSymbol : null;
    return snapshotAsset;
  }

  private applyCryptoPrice(providerSnapshots: Record<CryptoProviderId, ProviderSnapshot>, event: PricePoint): void {
    const providerSnapshot = providerSnapshots[event.provider];

    providerSnapshot.price = event.price;
    providerSnapshot.eventTs = event.ts;
  }

  private applyCryptoOrderBook(providerSnapshots: Record<CryptoProviderId, ProviderSnapshot>, event: OrderBookSnapshot): void {
    const providerSnapshot = providerSnapshots[event.provider];

    providerSnapshot.orderBook = SNAPSHOT_STATE.clonePricePointOrderBook(event);
    providerSnapshot.eventTs = event.ts;
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
      outcomeSnapshot.orderBook = SNAPSHOT_STATE.clonePolymarketOrderBook({ asks: event.asks, bids: event.bids });
    }
  }

  private buildSnapshot(pairState: PairState, generatedAt: number): Snapshot {
    const providerSnapshots = this.getCryptoState(pairState.asset);
    const snapshot = SNAPSHOT_STATE.buildSnapshot(pairState, generatedAt, providerSnapshots);
    return snapshot;
  }

  /**
   * @section public:methods
   */

  public async syncPairs(activePairKeys: Set<string>): Promise<void> {
    this.deactivateInactivePairs(activePairKeys);
    await this.activateMissingPairs(activePairKeys);
  }

  public stop(): void {
    for (const pairKey of [...this.pairStateByKey.keys()]) {
      this.deactivatePair(pairKey);
    }
  }

  public readTrackedPairKeys(): string[] {
    const trackedPairKeys = [...this.pairStateByKey.keys()];
    return trackedPairKeys;
  }

  public readSnapshots(pairKeys: string[], generatedAt: number): Map<string, Snapshot> {
    const snapshotByPairKey = new Map<string, Snapshot>();

    for (const pairKey of pairKeys) {
      const pairState = this.pairStateByKey.get(pairKey) ?? null;

      if (pairState !== null) {
        snapshotByPairKey.set(pairKey, this.buildSnapshot(pairState, generatedAt));
      }
    }

    return snapshotByPairKey;
  }

  public readEmittableSnapshots(pairKeys: string[], generatedAt: number): Map<string, Snapshot> {
    const snapshotByPairKey = new Map<string, Snapshot>();

    for (const pairKey of pairKeys) {
      const pairState = this.pairStateByKey.get(pairKey) ?? null;
      const isGeneratedAtInsideCurrentMarket = pairState !== null ? this.isGeneratedAtInsideCurrentMarket(pairState, generatedAt) : false;

      if (pairState !== null && isGeneratedAtInsideCurrentMarket) {
        snapshotByPairKey.set(pairKey, this.buildSnapshot(pairState, generatedAt));
      }
    }

    return snapshotByPairKey;
  }

  public handleCryptoEvent(event: FeedEvent): void {
    const snapshotAsset = "symbol" in event ? this.readSnapshotAsset(event.symbol) : null;
    const isDataEvent = event.type === "price" || event.type === "orderbook";

    if (snapshotAsset !== null && isDataEvent) {
      const providerSnapshots = this.getCryptoState(snapshotAsset);

      if (event.type === "price") {
        this.applyCryptoPrice(providerSnapshots, event);
      }

      if (event.type === "orderbook") {
        this.applyCryptoOrderBook(providerSnapshots, event);
      }
    }
  }

  public handlePolymarketEvent(event: MarketEvent): void {
    const pairKeys = this.pairKeysByPolymarketAssetId.get(event.assetId) ?? null;

    if (pairKeys !== null) {
      for (const pairKey of pairKeys) {
        const pairState = this.pairStateByKey.get(pairKey) ?? null;

        if (pairState !== null && this.isEventInsideMarket(pairState, event)) {
          this.applyPolymarketEvent(pairState, event);
        }
      }
    }
  }
}
