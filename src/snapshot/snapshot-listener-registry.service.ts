/**
 * @section imports:internals
 */

import type { AddSnapshotListenerOptions, Snapshot, SnapshotAsset, SnapshotListener, SnapshotLogger, SnapshotWindow } from "./snapshot.types.ts";

/**
 * @section types
 */

type SnapshotListenerRegistryOptions = {
  supportedAssets: SnapshotAsset[];
  supportedWindows: SnapshotWindow[];
};

/**
 * @section class
 */

export class SnapshotListenerRegistry {
  /**
   * @section private:attributes
   */

  private readonly supportedAssets: SnapshotAsset[];
  private readonly supportedWindows: SnapshotWindow[];
  private readonly listeners: Set<SnapshotListener>;

  /**
   * @section constructor
   */

  public constructor(options: SnapshotListenerRegistryOptions) {
    this.supportedAssets = [...options.supportedAssets];
    this.supportedWindows = [...options.supportedWindows];
    this.listeners = new Set<SnapshotListener>();
  }

  /**
   * @section private:methods
   */

  private readAssetFromPairKey(pairKey: string): SnapshotAsset {
    const asset = pairKey.split(":")[0] as SnapshotAsset;
    return asset;
  }

  /**
   * @section public:methods
   */

  public addListener(options: AddSnapshotListenerOptions): void {
    this.listeners.add(options.listener);
  }

  public removeListener(listener: SnapshotListener): void {
    this.listeners.delete(listener);
  }

  public clearListeners(): void {
    this.listeners.clear();
  }

  public hasListeners(): boolean {
    const hasListeners = this.listeners.size > 0;
    return hasListeners;
  }

  public readActivePairKeys(): Set<string> {
    const activePairKeys = new Set<string>();

    if (this.hasListeners()) {
      for (const asset of this.supportedAssets) {
        for (const window of this.supportedWindows) {
          activePairKeys.add(`${asset}:${window}`);
        }
      }
    }

    return activePairKeys;
  }

  public readActiveAssets(activePairKeys: Set<string>): SnapshotAsset[] {
    const activeAssets: SnapshotAsset[] = [];

    for (const pairKey of activePairKeys) {
      const asset = this.readAssetFromPairKey(pairKey);

      if (!activeAssets.includes(asset)) {
        activeAssets.push(asset);
      }
    }

    activeAssets.sort();
    return activeAssets;
  }

  public dispatchSnapshot(snapshot: Snapshot, serviceLogger: SnapshotLogger): void {
    for (const listener of this.listeners.values()) {
      try {
        listener(snapshot);
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        serviceLogger.error(`[SNAPSHOT] Listener execution failed: ${reason}`);
      }
    }
  }
}
