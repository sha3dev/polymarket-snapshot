/**
 * @section imports:internals
 */

import type { SnapshotScheduler } from "./snapshot.types.ts";

/**
 * @section types
 */

type SnapshotTickerOptions = {
  scheduler: SnapshotScheduler;
  snapshotIntervalMs: number;
};

export class SnapshotTicker {
  /**
   * @section private:properties
   */

  private readonly scheduler: SnapshotScheduler;
  private readonly snapshotIntervalMs: number;
  private snapshotStartTimeout: unknown | null;
  private snapshotInterval: unknown | null;

  /**
   * @section constructor
   */

  public constructor(options: SnapshotTickerOptions) {
    this.scheduler = options.scheduler;
    this.snapshotIntervalMs = options.snapshotIntervalMs;
    this.snapshotStartTimeout = null;
    this.snapshotInterval = null;
  }

  /**
   * @section private:methods
   */

  private getNextSnapshotAtMs(nowMs: number): number {
    const alignedSnapshotAtMs = this.readAlignedSnapshotAtMs(nowMs);
    const nextSnapshotAtMs = alignedSnapshotAtMs === nowMs ? nowMs : alignedSnapshotAtMs + this.snapshotIntervalMs;
    return nextSnapshotAtMs;
  }

  private getFollowingSnapshotAtMs(nowMs: number): number {
    const alignedSnapshotAtMs = this.readAlignedSnapshotAtMs(nowMs);
    const followingSnapshotAtMs = alignedSnapshotAtMs + this.snapshotIntervalMs;
    return followingSnapshotAtMs;
  }

  private scheduleNextSnapshotTick(emitSnapshotsAt: (generatedAt: number) => void): void {
    const nowMs = this.scheduler.now();
    const nextSnapshotAtMs = this.getFollowingSnapshotAtMs(nowMs);
    const delayMs = Math.max(nextSnapshotAtMs - nowMs, 0);

    this.snapshotInterval = this.scheduler.setTimeout((): void => {
      this.snapshotInterval = null;
      emitSnapshotsAt(nextSnapshotAtMs);
      this.scheduleNextSnapshotTick(emitSnapshotsAt);
    }, delayMs);
  }

  /**
   * @section public:methods
   */

  public ensureStarted(emitSnapshotsAt: (generatedAt: number) => void): void {
    if (this.snapshotStartTimeout === null && this.snapshotInterval === null) {
      const nextSnapshotAtMs = this.getNextSnapshotAtMs(this.scheduler.now());
      const delayMs = Math.max(nextSnapshotAtMs - this.scheduler.now(), 0);

      this.snapshotStartTimeout = this.scheduler.setTimeout((): void => {
        this.snapshotStartTimeout = null;
        emitSnapshotsAt(nextSnapshotAtMs);
        this.scheduleNextSnapshotTick(emitSnapshotsAt);
      }, delayMs);
    }
  }

  public stop(): void {
    if (this.snapshotStartTimeout !== null) {
      this.scheduler.clearTimeout(this.snapshotStartTimeout);
      this.snapshotStartTimeout = null;
    }

    if (this.snapshotInterval !== null) {
      this.scheduler.clearTimeout(this.snapshotInterval);
      this.snapshotInterval = null;
    }
  }

  public readAlignedSnapshotAtMs(nowMs: number): number {
    const alignedSnapshotAtMs = Math.floor(nowMs / this.snapshotIntervalMs) * this.snapshotIntervalMs;
    return alignedSnapshotAtMs;
  }
}
