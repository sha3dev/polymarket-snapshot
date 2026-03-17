/**
 * @section imports:externals
 */

import type { FeedEvent } from "@sha3/crypto";
import { CryptoFeedClient } from "@sha3/crypto";
import type { MarketEvent } from "@sha3/polymarket";
import { MarketCatalogService, MarketStreamService } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import config from "../config.ts";
import logger from "../logger.ts";
import type {
  AddSnapshotListenerOptions,
  PairSnapshot,
  Snapshot,
  SnapshotAsset,
  SnapshotCryptoClient,
  SnapshotLogger,
  SnapshotMarketCatalog,
  SnapshotMarketStream,
  SnapshotScheduler,
  SnapshotWindow,
} from "./snapshot.types.ts";
import { SnapshotListenerRegistry } from "./snapshot-listener-registry.service.ts";
import { SnapshotPairRuntime } from "./snapshot-pair-runtime.service.ts";
import { SnapshotTicker } from "./snapshot-ticker.service.ts";

/**
 * @section types
 */

type SnapshotRuntimeOptions = {
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
};

/**
 * @section class
 */

export class SnapshotService {
  /**
   * @section private:attributes
   */

  private readonly snapshotIntervalMs: number;
  private readonly priceToBeatInitialDelayMs: number;
  private readonly priceToBeatRetryIntervalMs: number;

  /**
   * @section private:attributes
   */

  private readonly supportedAssets: SnapshotAsset[];
  private readonly cryptoClientFactory: (assets: SnapshotAsset[]) => SnapshotCryptoClient;
  private readonly marketCatalogService: SnapshotMarketCatalog;
  private readonly marketStreamService: SnapshotMarketStream;
  private readonly scheduler: SnapshotScheduler;
  private readonly serviceLogger: SnapshotLogger;
  private readonly listenerRegistry: SnapshotListenerRegistry;
  private readonly pairRuntime: SnapshotPairRuntime;
  private readonly ticker: SnapshotTicker;
  private lastEmittedGeneratedAt: number | null;
  private marketListenerRemover: (() => void) | null;
  private cryptoSubscription: { unsubscribe(): void } | null;
  private cryptoClient: SnapshotCryptoClient | null;
  private activeCryptoAssetSignature: string;
  private isMarketStreamConnected: boolean;
  private isRuntimeSyncActive: boolean;
  private isRuntimeSyncQueued: boolean;

  /**
   * @section constructor
   */

  public constructor(snapshotIntervalMs?: number, runtimeOptions?: SnapshotRuntimeOptions) {
    const scheduler: SnapshotScheduler = {
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
    };
    this.snapshotIntervalMs = runtimeOptions?.snapshotIntervalMs ?? snapshotIntervalMs ?? config.DEFAULT_SNAPSHOT_INTERVAL_MS;
    this.priceToBeatInitialDelayMs = runtimeOptions?.priceToBeatInitialDelayMs ?? config.DEFAULT_PRICE_TO_BEAT_INITIAL_DELAY_MS;
    this.priceToBeatRetryIntervalMs = runtimeOptions?.priceToBeatRetryIntervalMs ?? config.DEFAULT_PRICE_TO_BEAT_RETRY_INTERVAL_MS;
    this.supportedAssets = [...(runtimeOptions?.supportedAssets ?? config.DEFAULT_SUPPORTED_ASSETS)];
    const supportedWindows = [...(runtimeOptions?.supportedWindows ?? config.DEFAULT_SUPPORTED_WINDOWS)];
    this.cryptoClientFactory = runtimeOptions?.cryptoClientFactory ?? ((assets): SnapshotCryptoClient => CryptoFeedClient.create({ symbols: assets }));
    this.marketCatalogService = runtimeOptions?.marketCatalogService ?? MarketCatalogService.createDefault();
    this.marketStreamService = runtimeOptions?.marketStreamService ?? MarketStreamService.createDefault();
    this.scheduler = runtimeOptions?.scheduler ?? scheduler;
    this.serviceLogger = runtimeOptions?.logger ?? logger;
    this.ensureSupportedSnapshotInterval(this.snapshotIntervalMs);
    this.listenerRegistry = new SnapshotListenerRegistry({ supportedAssets: this.supportedAssets, supportedWindows });
    this.pairRuntime = new SnapshotPairRuntime({
      marketCatalogService: this.marketCatalogService,
      marketStreamService: this.marketStreamService,
      scheduler: this.scheduler,
      serviceLogger: this.serviceLogger,
      supportedAssets: this.supportedAssets,
      priceToBeatInitialDelayMs: this.priceToBeatInitialDelayMs,
      priceToBeatRetryIntervalMs: this.priceToBeatRetryIntervalMs,
    });
    this.ticker = new SnapshotTicker({ scheduler: this.scheduler, snapshotIntervalMs: this.snapshotIntervalMs });
    this.lastEmittedGeneratedAt = null;
    this.marketListenerRemover = null;
    this.cryptoSubscription = null;
    this.cryptoClient = null;
    this.activeCryptoAssetSignature = "";
    this.isMarketStreamConnected = false;
    this.isRuntimeSyncActive = false;
    this.isRuntimeSyncQueued = false;
  }

  /**
   * @section private:methods
   */

  private ensureSupportedSnapshotInterval(snapshotIntervalMs: number): void {
    const isSupportedInterval = config.ALLOWED_SNAPSHOT_INTERVALS_MS.some((allowedSnapshotIntervalMs) => allowedSnapshotIntervalMs === snapshotIntervalMs);

    if (!isSupportedInterval) {
      throw new Error(`Unsupported snapshotIntervalMs '${snapshotIntervalMs}'. Use one of ${config.ALLOWED_SNAPSHOT_INTERVALS_MS.join(", ")}.`);
    }
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
    const activePairKeys = this.listenerRegistry.readActivePairKeys();
    const hasListeners = activePairKeys.size > 0;

    if (!hasListeners) {
      await this.stopRuntime();
    }

    if (hasListeners) {
      await this.ensureMarketStreamStarted();
      const activeAssets = this.listenerRegistry.readActiveAssets(activePairKeys);
      const activeAssetSignature = activeAssets.join(",");
      const shouldReplaceClient = activeAssetSignature !== this.activeCryptoAssetSignature;

      if (shouldReplaceClient) {
        await this.replaceCryptoClient(activeAssets, activeAssetSignature);
      }

      await this.pairRuntime.syncPairs(activePairKeys);
      this.ticker.ensureStarted(this.emitSnapshotsAt.bind(this));
    }
  }

  private async ensureMarketStreamStarted(): Promise<void> {
    if (!this.isMarketStreamConnected) {
      const marketListener = this.handlePolymarketEvent.bind(this);

      await this.marketStreamService.connect();
      this.marketListenerRemover = this.marketStreamService.addListener({ listener: marketListener });
      this.isMarketStreamConnected = true;
    }
  }

  private async replaceCryptoClient(activeAssets: SnapshotAsset[], activeAssetSignature: string): Promise<void> {
    const previousSubscription = this.cryptoSubscription;
    const previousClient = this.cryptoClient;

    if (previousSubscription !== null) {
      previousSubscription.unsubscribe();
      this.cryptoSubscription = null;
    }

    if (previousClient !== null) {
      await previousClient.disconnect();
      this.cryptoClient = null;
    }

    this.activeCryptoAssetSignature = activeAssetSignature;

    if (activeAssets.length > 0) {
      const cryptoClient = this.cryptoClientFactory(activeAssets);
      await cryptoClient.connect();
      this.cryptoSubscription = cryptoClient.subscribe((event): void => {
        this.handleCryptoEvent(event);
      });
      this.cryptoClient = cryptoClient;
    }
  }

  private async stopRuntime(): Promise<void> {
    this.ticker.stop();
    this.pairRuntime.stop();
    this.lastEmittedGeneratedAt = null;
    await this.stopRuntimeConnections();
  }

  private async stopRuntimeConnections(): Promise<void> {
    if (this.marketListenerRemover !== null) {
      this.marketListenerRemover();
      this.marketListenerRemover = null;
    }

    if (this.isMarketStreamConnected) {
      await this.marketStreamService.disconnect();
      this.isMarketStreamConnected = false;
    }

    if (this.cryptoSubscription !== null) {
      this.cryptoSubscription.unsubscribe();
      this.cryptoSubscription = null;
    }

    if (this.cryptoClient !== null) {
      await this.cryptoClient.disconnect();
      this.cryptoClient = null;
    }

    this.activeCryptoAssetSignature = "";
  }

  private handleCryptoEvent(event: FeedEvent): void {
    this.pairRuntime.handleCryptoEvent(event);
  }

  private handlePolymarketEvent(event: MarketEvent): void {
    this.pairRuntime.handlePolymarketEvent(event);
  }

  private buildPairSnapshotPrefix(pairSnapshot: PairSnapshot): string {
    const pairSnapshotPrefix = `${pairSnapshot.asset}_${pairSnapshot.window}`;
    return pairSnapshotPrefix;
  }

  private assignPairSnapshotFields(snapshot: Snapshot, pairSnapshot: PairSnapshot): void {
    const pairSnapshotPrefix = this.buildPairSnapshotPrefix(pairSnapshot);

    if (pairSnapshot.is_live_market) {
      snapshot[`${pairSnapshotPrefix}_slug`] = pairSnapshot.slug;
      snapshot[`${pairSnapshotPrefix}_market_start`] = pairSnapshot.market_start;
      snapshot[`${pairSnapshotPrefix}_market_end`] = pairSnapshot.market_end;
      snapshot[`${pairSnapshotPrefix}_price_to_beat`] = pairSnapshot.price_to_beat;
      snapshot[`${pairSnapshotPrefix}_up_asset_id`] = pairSnapshot.up_asset_id;
      snapshot[`${pairSnapshotPrefix}_up_price`] = pairSnapshot.up_price;
      snapshot[`${pairSnapshotPrefix}_up_order_book_json`] = pairSnapshot.up_order_book_json;
      snapshot[`${pairSnapshotPrefix}_up_event_ts`] = pairSnapshot.up_event_ts;
      snapshot[`${pairSnapshotPrefix}_down_asset_id`] = pairSnapshot.down_asset_id;
      snapshot[`${pairSnapshotPrefix}_down_price`] = pairSnapshot.down_price;
      snapshot[`${pairSnapshotPrefix}_down_order_book_json`] = pairSnapshot.down_order_book_json;
      snapshot[`${pairSnapshotPrefix}_down_event_ts`] = pairSnapshot.down_event_ts;
    }
  }

  private assignAssetSnapshotFields(snapshot: Snapshot, pairSnapshot: PairSnapshot): void {
    const assetPrefix = pairSnapshot.asset;

    snapshot[`${assetPrefix}_binance_price`] = pairSnapshot.binance_price;
    snapshot[`${assetPrefix}_binance_order_book_json`] = pairSnapshot.binance_order_book_json;
    snapshot[`${assetPrefix}_binance_event_ts`] = pairSnapshot.binance_event_ts;
    snapshot[`${assetPrefix}_coinbase_price`] = pairSnapshot.coinbase_price;
    snapshot[`${assetPrefix}_coinbase_order_book_json`] = pairSnapshot.coinbase_order_book_json;
    snapshot[`${assetPrefix}_coinbase_event_ts`] = pairSnapshot.coinbase_event_ts;
    snapshot[`${assetPrefix}_kraken_price`] = pairSnapshot.kraken_price;
    snapshot[`${assetPrefix}_kraken_order_book_json`] = pairSnapshot.kraken_order_book_json;
    snapshot[`${assetPrefix}_kraken_event_ts`] = pairSnapshot.kraken_event_ts;
    snapshot[`${assetPrefix}_okx_price`] = pairSnapshot.okx_price;
    snapshot[`${assetPrefix}_okx_order_book_json`] = pairSnapshot.okx_order_book_json;
    snapshot[`${assetPrefix}_okx_event_ts`] = pairSnapshot.okx_event_ts;
    snapshot[`${assetPrefix}_chainlink_price`] = pairSnapshot.chainlink_price;
    snapshot[`${assetPrefix}_chainlink_event_ts`] = pairSnapshot.chainlink_event_ts;
  }

  private buildSnapshot(pairSnapshotByPairKey: Map<string, PairSnapshot>): Snapshot {
    const pairSnapshots = [...pairSnapshotByPairKey.entries()]
      .sort(([leftPairKey], [rightPairKey]) => leftPairKey.localeCompare(rightPairKey))
      .map(([, pairSnapshot]) => pairSnapshot);
    const generatedAt = pairSnapshots[0]?.generated_at ?? 0;
    const snapshot: Snapshot = { generated_at: generatedAt };

    for (const pairSnapshot of pairSnapshots) {
      this.assignPairSnapshotFields(snapshot, pairSnapshot);
      this.assignAssetSnapshotFields(snapshot, pairSnapshot);
    }

    return snapshot;
  }

  private emitSnapshotsAt(generatedAt: number): void {
    const trackedPairKeys = this.pairRuntime.readTrackedPairKeys();
    const pairSnapshotByPairKey = this.pairRuntime.readEmittableSnapshots(trackedPairKeys, generatedAt);
    const shouldEmitSnapshot = this.lastEmittedGeneratedAt !== generatedAt && pairSnapshotByPairKey.size > 0;

    if (shouldEmitSnapshot) {
      this.lastEmittedGeneratedAt = generatedAt;
      this.listenerRegistry.dispatchSnapshot(this.buildSnapshot(pairSnapshotByPairKey), this.serviceLogger);
    }
  }

  /**
   * @section public:methods
   */

  public addSnapshotListener(options: AddSnapshotListenerOptions): void {
    this.listenerRegistry.addListener(options);
    this.queueRuntimeSync();
  }

  public removeSnapshotListener(listener: (snapshot: Snapshot) => void): void {
    this.listenerRegistry.removeListener(listener);
    this.queueRuntimeSync();
  }

  public getSnapshot(): Snapshot | null {
    const trackedPairKeys = this.pairRuntime.readTrackedPairKeys();
    const generatedAt = this.ticker.readAlignedSnapshotAtMs(this.scheduler.now());
    const pairSnapshotByPairKey = this.pairRuntime.readSnapshots(trackedPairKeys, generatedAt);
    const snapshot = pairSnapshotByPairKey.size > 0 ? this.buildSnapshot(pairSnapshotByPairKey) : null;
    return snapshot;
  }

  public async disconnect(): Promise<void> {
    this.listenerRegistry.clearListeners();
    await this.stopRuntime();
  }
}
