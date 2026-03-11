/**
 * @section imports:externals
 */

import { CryptoFeedClient } from "@sha3/crypto";
import type { CryptoProviderId, FeedEvent, OrderBookSnapshot, PricePoint } from "@sha3/crypto";
import { MarketCatalogService, MarketStreamService } from "@sha3/polymarket";
import type { MarketEvent, OrderBook, PolymarketMarket } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import config from "../config.ts";
import logger from "../logger.ts";
import { SnapshotState } from "./snapshot-state.service.ts";
import type {
  AddSnapshotListenerOptions,
  GetSnapshotOptions,
  PairState,
  PolymarketOutcomeSnapshot,
  ProviderSnapshot,
  RuntimeState,
  Snapshot,
  SnapshotAsset,
  SnapshotCryptoClient,
  SnapshotListener,
  SnapshotLogger,
  SnapshotMarketCatalog,
  SnapshotMarketStream,
  SnapshotRuntimeDependencies,
  SnapshotScheduler,
  SnapshotServiceOptions,
  SnapshotWindow,
} from "./snapshot.types.ts";

/**
 * @section consts
 */

const SNAPSHOT_STATE = SnapshotState.create();

export class SnapshotService {
  /**
   * @section private:attributes
   */

  private readonly snapshotIntervalMs: number;
  private readonly priceToBeatInitialDelayMs: number;
  private readonly priceToBeatRetryIntervalMs: number;

  /**
   * @section private:properties
   */

  private readonly supportedAssets: SnapshotAsset[];
  private readonly supportedWindows: SnapshotWindow[];
  private readonly cryptoClientFactory: (assets: SnapshotAsset[]) => SnapshotCryptoClient;
  private readonly marketCatalogService: SnapshotMarketCatalog;
  private readonly marketStreamService: SnapshotMarketStream;
  private readonly scheduler: SnapshotScheduler;
  private readonly serviceLogger: SnapshotLogger;
  private readonly listenerFilters: Map<SnapshotListener, Set<string>>;
  private readonly cryptoStateByAsset: Map<SnapshotAsset, Record<CryptoProviderId, ProviderSnapshot>>;
  private readonly pairStateByKey: Map<string, PairState>;
  private readonly pairKeysByPolymarketAssetId: Map<string, Set<string>>;
  private readonly subscriptionCountByAssetId: Map<string, number>;
  private readonly runtimeState: RuntimeState;
  private cryptoClient: SnapshotCryptoClient | null;
  private activeCryptoAssetSignature: string;
  private isMarketStreamConnected: boolean;
  private isRuntimeSyncActive: boolean;
  private isRuntimeSyncQueued: boolean;

  /**
   * @section constructor
   */

  public constructor(options?: SnapshotServiceOptions) {
    const scheduler: SnapshotScheduler = options?.scheduler ?? {
      now(): number {
        const now = Date.now();
        return now;
      },
      setTimeout(listener: () => void, delayMs: number): unknown {
        const timer = globalThis.setTimeout(listener, delayMs);
        return timer;
      },
      clearTimeout(timer: unknown): void {
        clearTimeout(timer as NodeJS.Timeout);
      },
      setInterval(listener: () => void, delayMs: number): unknown {
        const timer = globalThis.setInterval(listener, delayMs);
        return timer;
      },
      clearInterval(timer: unknown): void {
        clearInterval(timer as NodeJS.Timeout);
      },
    };
    this.snapshotIntervalMs = options?.snapshotIntervalMs ?? config.DEFAULT_SNAPSHOT_INTERVAL_MS;
    this.priceToBeatInitialDelayMs = options?.priceToBeatInitialDelayMs ?? config.DEFAULT_PRICE_TO_BEAT_INITIAL_DELAY_MS;
    this.priceToBeatRetryIntervalMs = options?.priceToBeatRetryIntervalMs ?? config.DEFAULT_PRICE_TO_BEAT_RETRY_INTERVAL_MS;
    this.supportedAssets = [...(options?.supportedAssets ?? config.DEFAULT_SUPPORTED_ASSETS)];
    this.supportedWindows = [...(options?.supportedWindows ?? config.DEFAULT_SUPPORTED_WINDOWS)];
    this.cryptoClientFactory = options?.cryptoClientFactory ?? ((assets) => CryptoFeedClient.create({ symbols: assets }));
    this.marketCatalogService = options?.marketCatalogService ?? MarketCatalogService.createDefault();
    this.marketStreamService = options?.marketStreamService ?? MarketStreamService.createDefault();
    this.scheduler = scheduler;
    this.serviceLogger = options?.logger ?? logger;
    this.listenerFilters = new Map<SnapshotListener, Set<string>>();
    this.cryptoStateByAsset = new Map<SnapshotAsset, Record<CryptoProviderId, ProviderSnapshot>>();
    this.pairStateByKey = new Map<string, PairState>();
    this.pairKeysByPolymarketAssetId = new Map<string, Set<string>>();
    this.subscriptionCountByAssetId = new Map<string, number>();
    this.ensureSupportedSnapshotInterval(this.snapshotIntervalMs);
    this.runtimeState = { snapshotStartTimeout: null, snapshotInterval: null, marketListenerRemover: null, cryptoSubscription: null };
    this.cryptoClient = null;
    this.activeCryptoAssetSignature = "";
    this.isMarketStreamConnected = false;
    this.isRuntimeSyncActive = false;
    this.isRuntimeSyncQueued = false;
  }

  /**
   * @section factory
   */

  public static createDefault(options?: SnapshotServiceOptions): SnapshotService {
    const service = new SnapshotService(options);
    return service;
  }

  /**
   * @section private:methods
   */

  private normalizeAssets(assets?: SnapshotAsset[]): SnapshotAsset[] {
    const selectedAssets = assets ?? this.supportedAssets;
    const normalizedAssets: SnapshotAsset[] = [];

    for (const selectedAsset of selectedAssets) {
      const normalizedAsset = selectedAsset.toLowerCase() as SnapshotAsset;
      const isSupportedAsset = this.supportedAssets.includes(normalizedAsset);

      if (!isSupportedAsset) {
        throw new Error(`Unsupported snapshot asset '${selectedAsset}'.`);
      }

      if (!normalizedAssets.includes(normalizedAsset)) {
        normalizedAssets.push(normalizedAsset);
      }
    }

    return normalizedAssets;
  }

  private ensureSupportedSnapshotInterval(snapshotIntervalMs: number): void {
    const isSupportedInterval = config.ALLOWED_SNAPSHOT_INTERVALS_MS.some((allowedSnapshotIntervalMs) => allowedSnapshotIntervalMs === snapshotIntervalMs);

    if (!isSupportedInterval) {
      throw new Error(`Unsupported snapshotIntervalMs '${snapshotIntervalMs}'. Use one of ${config.ALLOWED_SNAPSHOT_INTERVALS_MS.join(", ")}.`);
    }
  }

  private normalizeWindows(windows?: SnapshotWindow[]): SnapshotWindow[] {
    const selectedWindows = windows ?? this.supportedWindows;
    const normalizedWindows: SnapshotWindow[] = [];

    for (const selectedWindow of selectedWindows) {
      const isSupportedWindow = this.supportedWindows.includes(selectedWindow);

      if (!isSupportedWindow) {
        throw new Error(`Unsupported snapshot window '${selectedWindow}'.`);
      }

      if (!normalizedWindows.includes(selectedWindow)) {
        normalizedWindows.push(selectedWindow);
      }
    }

    return normalizedWindows;
  }

  private parsePairKey(pairKey: string): { asset: SnapshotAsset; window: SnapshotWindow } {
    const segments = pairKey.split(":");
    const pairKeyParts = { asset: segments[0] as SnapshotAsset, window: segments[1] as SnapshotWindow };
    return pairKeyParts;
  }

  private buildPairKeys(assets?: SnapshotAsset[], windows?: SnapshotWindow[]): Set<string> {
    const normalizedAssets = this.normalizeAssets(assets);
    const normalizedWindows = this.normalizeWindows(windows);
    const pairKeys = new Set<string>();

    for (const asset of normalizedAssets) {
      for (const window of normalizedWindows) {
        pairKeys.add(`${asset}:${window}`);
      }
    }

    return pairKeys;
  }

  private getTrackedPairKeys(options?: GetSnapshotOptions): string[] {
    const filteredPairKeys = this.buildPairKeys(options?.assets, options?.windows);
    const trackedPairKeys: string[] = [];

    for (const pairKey of filteredPairKeys) {
      const isTrackedPair = this.pairStateByKey.has(pairKey);

      if (isTrackedPair) {
        trackedPairKeys.push(pairKey);
      }
    }

    trackedPairKeys.sort();
    return trackedPairKeys;
  }

  private getActivePairKeys(): Set<string> {
    const activePairKeys = new Set<string>();

    for (const pairKeys of this.listenerFilters.values()) {
      for (const pairKey of pairKeys) {
        activePairKeys.add(pairKey);
      }
    }

    return activePairKeys;
  }

  private getActiveAssets(activePairKeys: Set<string>): SnapshotAsset[] {
    const activeAssets: SnapshotAsset[] = [];

    for (const pairKey of activePairKeys) {
      const pairKeyParts = this.parsePairKey(pairKey);

      if (!activeAssets.includes(pairKeyParts.asset)) {
        activeAssets.push(pairKeyParts.asset);
      }
    }

    activeAssets.sort();
    return activeAssets;
  }

  private getCryptoState(asset: SnapshotAsset): Record<CryptoProviderId, ProviderSnapshot> {
    let providerSnapshots = this.cryptoStateByAsset.get(asset) ?? null;

    if (providerSnapshots === null) {
      providerSnapshots = SNAPSHOT_STATE.createProviderSnapshotRecord();
      this.cryptoStateByAsset.set(asset, providerSnapshots);
    }

    return providerSnapshots;
  }

  private queueRuntimeSync(): void {
    this.isRuntimeSyncQueued = true;
    void this.runRuntimeSyncLoop();
  }

  private async runRuntimeSyncLoop(): Promise<void> {
    if (!this.isRuntimeSyncActive) {
      this.isRuntimeSyncActive = true;

      while (this.isRuntimeSyncQueued) {
        this.isRuntimeSyncQueued = false;

        try {
          await this.syncRuntime();
        } catch (error) {
          const reason = error instanceof Error ? error.message : String(error);
          this.serviceLogger.error(`[SNAPSHOT] Runtime sync failed: ${reason}`);
        }
      }

      this.isRuntimeSyncActive = false;
    }
  }

  private async syncRuntime(): Promise<void> {
    const activePairKeys = this.getActivePairKeys();
    const hasListeners = activePairKeys.size > 0;

    if (!hasListeners) {
      await this.stopRuntime();
    }

    if (hasListeners) {
      await this.ensureMarketStreamStarted();
      const activeAssets = this.getActiveAssets(activePairKeys);
      const activeAssetSignature = activeAssets.join(",");
      const shouldReplaceClient = activeAssetSignature !== this.activeCryptoAssetSignature;

      if (shouldReplaceClient) {
        await this.replaceCryptoClient(activeAssets, activeAssetSignature);
      }

      await this.syncPairs(activePairKeys);
      this.ensureSnapshotInterval();
    }
  }

  private async ensureMarketStreamStarted(): Promise<void> {
    if (!this.isMarketStreamConnected) {
      const marketListener = this.handlePolymarketEvent.bind(this);

      await this.marketStreamService.connect();
      this.runtimeState.marketListenerRemover = this.marketStreamService.addListener({ listener: marketListener });
      this.isMarketStreamConnected = true;
    }
  }

  private async replaceCryptoClient(activeAssets: SnapshotAsset[], activeAssetSignature: string): Promise<void> {
    const previousSubscription = this.runtimeState.cryptoSubscription;
    const previousClient = this.cryptoClient;

    if (previousSubscription !== null) {
      previousSubscription.unsubscribe();
      this.runtimeState.cryptoSubscription = null;
    }

    if (previousClient !== null) {
      await previousClient.disconnect();
      this.cryptoClient = null;
    }

    this.activeCryptoAssetSignature = activeAssetSignature;

    if (activeAssets.length > 0) {
      const cryptoClient = this.cryptoClientFactory(activeAssets);
      await cryptoClient.connect();
      this.runtimeState.cryptoSubscription = cryptoClient.subscribe((event): void => {
        this.handleCryptoEvent(event);
      });
      this.cryptoClient = cryptoClient;
    }
  }

  private async syncPairs(activePairKeys: Set<string>): Promise<void> {
    const trackedPairKeys = [...this.pairStateByKey.keys()];

    for (const pairKey of trackedPairKeys) {
      const isStillActive = activePairKeys.has(pairKey);

      if (!isStillActive) {
        this.deactivatePair(pairKey);
      }
    }

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

  private ensureSnapshotInterval(): void {
    if (this.runtimeState.snapshotStartTimeout === null && this.runtimeState.snapshotInterval === null) {
      const nextSnapshotAtMs = this.getNextSnapshotAtMs(this.scheduler.now());
      const delayMs = Math.max(nextSnapshotAtMs - this.scheduler.now(), 0);

      this.runtimeState.snapshotStartTimeout = this.scheduler.setTimeout((): void => {
        this.runtimeState.snapshotStartTimeout = null;
        this.emitSnapshotsAt(nextSnapshotAtMs);
        this.startSnapshotInterval(nextSnapshotAtMs + this.snapshotIntervalMs);
      }, delayMs);
    }
  }

  private startSnapshotInterval(nextSnapshotAtMs: number): void {
    let nextAlignedSnapshotAtMs = nextSnapshotAtMs;

    this.runtimeState.snapshotInterval = this.scheduler.setInterval((): void => {
      this.emitSnapshotsAt(nextAlignedSnapshotAtMs);
      nextAlignedSnapshotAtMs += this.snapshotIntervalMs;
    }, this.snapshotIntervalMs);
  }

  private getAlignedSnapshotAtMs(nowMs: number): number {
    const alignedSnapshotAtMs = Math.floor(nowMs / this.snapshotIntervalMs) * this.snapshotIntervalMs;
    return alignedSnapshotAtMs;
  }

  private getNextSnapshotAtMs(nowMs: number): number {
    const alignedSnapshotAtMs = this.getAlignedSnapshotAtMs(nowMs);
    const nextSnapshotAtMs = alignedSnapshotAtMs === nowMs ? nowMs : alignedSnapshotAtMs + this.snapshotIntervalMs;
    return nextSnapshotAtMs;
  }

  private async stopRuntime(): Promise<void> {
    this.stopRuntimeState();
    await this.stopRuntimeConnections();
  }

  private stopRuntimeState(): void {
    if (this.runtimeState.snapshotStartTimeout !== null) {
      this.scheduler.clearTimeout(this.runtimeState.snapshotStartTimeout);
      this.runtimeState.snapshotStartTimeout = null;
    }

    if (this.runtimeState.snapshotInterval !== null) {
      this.scheduler.clearInterval(this.runtimeState.snapshotInterval);
      this.runtimeState.snapshotInterval = null;
    }

    for (const pairKey of [...this.pairStateByKey.keys()]) {
      this.deactivatePair(pairKey);
    }
  }

  private async stopRuntimeConnections(): Promise<void> {
    if (this.runtimeState.marketListenerRemover !== null) {
      this.runtimeState.marketListenerRemover();
      this.runtimeState.marketListenerRemover = null;
    }

    if (this.isMarketStreamConnected) {
      await this.marketStreamService.disconnect();
      this.isMarketStreamConnected = false;
    }

    if (this.runtimeState.cryptoSubscription !== null) {
      this.runtimeState.cryptoSubscription.unsubscribe();
      this.runtimeState.cryptoSubscription = null;
    }

    if (this.cryptoClient !== null) {
      await this.cryptoClient.disconnect();
      this.cryptoClient = null;
    }

    this.activeCryptoAssetSignature = "";
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
    pairState.up = { assetId: null, price: null, orderBook: null, eventTs: null };
    pairState.up.assetId = market.upTokenId;
    pairState.down = { assetId: null, price: null, orderBook: null, eventTs: null };
    pairState.down.assetId = market.downTokenId;
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

  private handleCryptoEvent(event: FeedEvent): void {
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

  private handlePolymarketEvent(event: MarketEvent): void {
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

  private buildSnapshot(pairState: PairState, generatedAt: number): Snapshot {
    const market = pairState.currentMarket;
    const providerSnapshots = this.getCryptoState(pairState.asset);
    const snapshot = SNAPSHOT_STATE.buildSnapshot(pairState, generatedAt, providerSnapshots);
    return snapshot;
  }

  private emitSnapshotsAt(generatedAt: number): void {
    const snapshotByPairKey = new Map<string, Snapshot>();

    for (const pairKey of this.getTrackedPairKeys()) {
      const pairState = this.pairStateByKey.get(pairKey) ?? null;

      if (pairState !== null) {
        snapshotByPairKey.set(pairKey, this.buildSnapshot(pairState, generatedAt));
      }
    }

    for (const [listener, pairKeys] of this.listenerFilters.entries()) {
      for (const pairKey of pairKeys) {
        const snapshot = snapshotByPairKey.get(pairKey) ?? null;

        if (snapshot !== null) {
          try {
            listener(snapshot);
          } catch (error) {
            const reason = error instanceof Error ? error.message : String(error);
            this.serviceLogger.error(`[SNAPSHOT] Listener execution failed: ${reason}`);
          }
        }
      }
    }
  }

  /**
   * @section public:methods
   */

  public addSnapshotListener(options: AddSnapshotListenerOptions): void {
    const pairKeys = this.buildPairKeys(options.assets, options.windows);

    this.listenerFilters.set(options.listener, pairKeys);
    this.queueRuntimeSync();
  }

  public removeSnapshotListener(listener: SnapshotListener): void {
    this.listenerFilters.delete(listener);
    this.queueRuntimeSync();
  }

  public getSnapshot(options?: GetSnapshotOptions): Snapshot[] {
    const trackedPairKeys = this.getTrackedPairKeys(options);
    const generatedAt = this.getAlignedSnapshotAtMs(this.scheduler.now());
    const snapshots: Snapshot[] = [];

    for (const pairKey of trackedPairKeys) {
      const pairState = this.pairStateByKey.get(pairKey) ?? null;

      if (pairState !== null) {
        snapshots.push(this.buildSnapshot(pairState, generatedAt));
      }
    }

    return snapshots;
  }

  public async disconnect(): Promise<void> {
    this.listenerFilters.clear();
    await this.stopRuntime();
  }
}
