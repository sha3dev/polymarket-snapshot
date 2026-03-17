/**
 * @section imports:externals
 */

import type { CryptoProviderId, FeedEvent } from "@sha3/crypto";
import type { MarketEvent, PolymarketMarket } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import config from "../config.ts";
import type {
  PairKeyParts,
  PairSnapshot,
  PairState,
  ProviderSnapshot,
  SnapshotAsset,
  SnapshotLogger,
  SnapshotMarketCatalog,
  SnapshotMarketStream,
  SnapshotScheduler,
  SnapshotWindow,
} from "./snapshot.types.ts";
import { SnapshotPairState } from "./snapshot-pair-state.service.ts";

/**
 * @section types
 */

type SnapshotPairRuntimeOptions = {
  marketCatalogService: SnapshotMarketCatalog;
  marketStreamService: SnapshotMarketStream;
  scheduler: SnapshotScheduler;
  serviceLogger: SnapshotLogger;
  supportedAssets: SnapshotAsset[];
};

/**
 * @section class
 */

export class SnapshotPairRuntime {
  /**
   * @section private:attributes
   */

  private readonly marketCatalogService: SnapshotMarketCatalog;
  private readonly marketStreamService: SnapshotMarketStream;
  private readonly scheduler: SnapshotScheduler;
  private readonly serviceLogger: SnapshotLogger;
  private readonly supportedAssets: SnapshotAsset[];
  private readonly pairState: SnapshotPairState;
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
    this.pairState = new SnapshotPairState({ supportedAssets: this.supportedAssets });
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

  private async activateMissingPairs(activePairKeys: Set<string>): Promise<void> {
    for (const pairKey of activePairKeys) {
      const isTrackedPair = this.pairStateByKey.has(pairKey);

      if (!isTrackedPair) {
        const pairKeyParts = this.parsePairKey(pairKey);
        const pairState: PairState = {
          asset: pairKeyParts.asset,
          window: pairKeyParts.window,
          currentMarket: null,
          currentSlug: null,
          rotationTimer: null,
          up: { assetId: null, price: null, orderBook: null, eventTs: null },
          down: { assetId: null, price: null, orderBook: null, eventTs: null },
        };

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
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      this.serviceLogger.warn(`[SNAPSHOT] Failed to activate market ${pairKey}: ${reason}`);
      this.scheduleMarketActivationRetry(pairKey, pairState);
    }
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
    const trackedPairKeys = [...this.pairStateByKey.keys()].sort();
    return trackedPairKeys;
  }

  public readSnapshots(pairKeys: string[], generatedAt: number): Map<string, PairSnapshot> {
    const pairSnapshotByPairKey = this.pairState.readSnapshots(this.cryptoStateByAsset, this.pairStateByKey, pairKeys, generatedAt);
    return pairSnapshotByPairKey;
  }

  public readEmittableSnapshots(pairKeys: string[], generatedAt: number): Map<string, PairSnapshot> {
    const pairSnapshotByPairKey = this.pairState.readEmittableSnapshots(this.cryptoStateByAsset, this.pairStateByKey, pairKeys, generatedAt);
    return pairSnapshotByPairKey;
  }

  public handleCryptoEvent(event: FeedEvent): void {
    this.pairState.handleCryptoEvent(this.cryptoStateByAsset, event);
  }

  public handlePolymarketEvent(event: MarketEvent): void {
    this.pairState.handlePolymarketEvent(this.pairKeysByPolymarketAssetId, this.pairStateByKey, event);
  }
}
