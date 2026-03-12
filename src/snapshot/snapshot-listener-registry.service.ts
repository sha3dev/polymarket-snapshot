/**
 * @section imports:internals
 */

import type {
  AddSnapshotListenerOptions,
  GetSnapshotOptions,
  PairKeyParts,
  Snapshot,
  SnapshotAsset,
  SnapshotListener,
  SnapshotLogger,
  SnapshotWindow,
} from "./snapshot.types.ts";

/**
 * @section types
 */

type SnapshotListenerRegistryOptions = {
  supportedAssets: SnapshotAsset[];
  supportedWindows: SnapshotWindow[];
};

export class SnapshotListenerRegistry {
  /**
   * @section private:properties
   */

  private readonly supportedAssets: SnapshotAsset[];
  private readonly supportedWindows: SnapshotWindow[];
  private readonly listenerFilters: Map<SnapshotListener, Set<string>>;

  /**
   * @section constructor
   */

  public constructor(options: SnapshotListenerRegistryOptions) {
    this.supportedAssets = [...options.supportedAssets];
    this.supportedWindows = [...options.supportedWindows];
    this.listenerFilters = new Map<SnapshotListener, Set<string>>();
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

  private parsePairKey(pairKey: string): PairKeyParts {
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

  /**
   * @section public:methods
   */

  public addListener(options: AddSnapshotListenerOptions): void {
    const pairKeys = this.buildPairKeys(options.assets, options.windows);

    this.listenerFilters.set(options.listener, pairKeys);
  }

  public removeListener(listener: SnapshotListener): void {
    this.listenerFilters.delete(listener);
  }

  public clearListeners(): void {
    this.listenerFilters.clear();
  }

  public readActivePairKeys(): Set<string> {
    const activePairKeys = new Set<string>();

    for (const pairKeys of this.listenerFilters.values()) {
      for (const pairKey of pairKeys) {
        activePairKeys.add(pairKey);
      }
    }

    return activePairKeys;
  }

  public readActiveAssets(activePairKeys: Set<string>): SnapshotAsset[] {
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

  public readTrackedPairKeys(options: GetSnapshotOptions | undefined, trackedPairKeys: Iterable<string>): string[] {
    const filteredPairKeys = this.buildPairKeys(options?.assets, options?.windows);
    const trackedPairKeySet = new Set<string>(trackedPairKeys);
    const selectedTrackedPairKeys: string[] = [];

    for (const pairKey of filteredPairKeys) {
      const isTrackedPair = trackedPairKeySet.has(pairKey);

      if (isTrackedPair) {
        selectedTrackedPairKeys.push(pairKey);
      }
    }

    selectedTrackedPairKeys.sort();
    return selectedTrackedPairKeys;
  }

  public dispatchSnapshots(snapshotByPairKey: Map<string, Snapshot>, serviceLogger: SnapshotLogger): void {
    for (const [listener, pairKeys] of this.listenerFilters.entries()) {
      for (const pairKey of pairKeys) {
        const snapshot = snapshotByPairKey.get(pairKey) ?? null;

        if (snapshot !== null) {
          try {
            listener(snapshot);
          } catch (error) {
            const reason = error instanceof Error ? error.message : String(error);
            serviceLogger.error(`[SNAPSHOT] Listener execution failed: ${reason}`);
          }
        }
      }
    }
  }
}
