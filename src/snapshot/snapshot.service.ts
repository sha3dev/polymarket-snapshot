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
import { SnapshotListenerRegistry } from "./snapshot-listener-registry.service.ts";
import { SnapshotPairRuntime } from "./snapshot-pair-runtime.service.ts";
import { SnapshotTicker } from "./snapshot-ticker.service.ts";
import type {
  AddSnapshotListenerOptions,
  GetSnapshotOptions,
  Snapshot,
  SnapshotAsset,
  SnapshotCryptoClient,
  SnapshotListener,
  SnapshotLogger,
  SnapshotMarketCatalog,
  SnapshotMarketStream,
  SnapshotScheduler,
  SnapshotServiceOptions,
  SnapshotSubscription,
  SnapshotWindow,
} from "./snapshot.types.ts";

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
  private readonly listenerRegistry: SnapshotListenerRegistry;
  private readonly pairRuntime: SnapshotPairRuntime;
  private readonly ticker: SnapshotTicker;
  private readonly lastEmittedGeneratedAtByPairKey: Map<string, number>;
  private marketListenerRemover: (() => void) | null;
  private cryptoSubscription: SnapshotSubscription | null;
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
    this.ensureSupportedSnapshotInterval(this.snapshotIntervalMs);
    this.listenerRegistry = new SnapshotListenerRegistry({ supportedAssets: this.supportedAssets, supportedWindows: this.supportedWindows });
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
    this.lastEmittedGeneratedAtByPairKey = new Map<string, number>();
    this.marketListenerRemover = null;
    this.cryptoSubscription = null;
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

  private buildPendingSnapshotByPairKey(snapshotByPairKey: Map<string, Snapshot>): Map<string, Snapshot> {
    const pendingSnapshotByPairKey = new Map<string, Snapshot>();

    for (const [pairKey, snapshot] of snapshotByPairKey.entries()) {
      const lastGeneratedAt = this.lastEmittedGeneratedAtByPairKey.get(pairKey) ?? null;
      const shouldEmitSnapshot = lastGeneratedAt !== snapshot.generatedAt;

      if (shouldEmitSnapshot) {
        pendingSnapshotByPairKey.set(pairKey, snapshot);
        this.lastEmittedGeneratedAtByPairKey.set(pairKey, snapshot.generatedAt);
      }
    }

    return pendingSnapshotByPairKey;
  }

  private emitSnapshotsAt(generatedAt: number): void {
    const trackedPairKeys = this.listenerRegistry.readTrackedPairKeys(undefined, this.pairRuntime.readTrackedPairKeys());
    const snapshotByPairKey = this.pairRuntime.readSnapshots(trackedPairKeys, generatedAt);
    const pendingSnapshotByPairKey = this.buildPendingSnapshotByPairKey(snapshotByPairKey);
    this.listenerRegistry.dispatchSnapshots(pendingSnapshotByPairKey, this.serviceLogger);
  }

  /**
   * @section public:methods
   */

  public addSnapshotListener(options: AddSnapshotListenerOptions): void {
    this.listenerRegistry.addListener(options);
    this.queueRuntimeSync();
  }

  public removeSnapshotListener(listener: SnapshotListener): void {
    this.listenerRegistry.removeListener(listener);
    this.queueRuntimeSync();
  }

  public getSnapshot(options?: GetSnapshotOptions): Snapshot[] {
    const trackedPairKeys = this.listenerRegistry.readTrackedPairKeys(options, this.pairRuntime.readTrackedPairKeys());
    const generatedAt = this.ticker.readAlignedSnapshotAtMs(this.scheduler.now());
    const snapshotByPairKey = this.pairRuntime.readSnapshots(trackedPairKeys, generatedAt);
    const snapshots = [...snapshotByPairKey.values()];
    return snapshots;
  }

  public async disconnect(): Promise<void> {
    this.listenerRegistry.clearListeners();
    await this.stopRuntime();
  }
}
