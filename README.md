# @sha3/polymarket-snapshot

Capture rolling Polymarket `5m` and `15m` market snapshots as flat plain JSON objects, enriched with live crypto provider state from `@sha3/crypto`.

## TL;DR

```bash
npm install @sha3/polymarket-snapshot
```

```ts
import { SnapshotService } from "@sha3/polymarket-snapshot";

const snapshotService = SnapshotService.createDefault();

snapshotService.addSnapshotListener({
  assets: ["btc"],
  windows: ["5m"],
  listener(snapshot): void {
    console.log(snapshot.generatedAt, snapshot.asset, snapshot.window, snapshot.marketSlug);
  },
});
```

## Why

- Emits plain JSON snapshots ready for downstream persistence or model pipelines.
- Keeps the latest known crypto and Polymarket values even when no fresh update arrives during the current interval.
- Aligns `generatedAt` to the global interval grid, so `500ms` snapshots land on `...000` and `...500`.
- Rotates Polymarket market subscriptions at exact `5m` and `15m` UTC boundaries.
- Discards residual Polymarket events that arrive outside the market lifetime.
- Fetches `priceToBeat` once per market slug, with delayed retry when it is not immediately available.

## Main Capabilities

- Shared snapshot emission across multiple listeners without duplicating upstream subscriptions.
- Provider-keyed crypto state for Binance, Coinbase, Kraken, OKX, and Chainlink.
- Automatic Polymarket market rollover for `5m` and `15m` contracts.
- On-demand latest snapshot reads through `getSnapshot()`.
- Deterministic runtime injection points for tests.

## Installation

```bash
npm install @sha3/polymarket-snapshot
```

## Usage

Create one service and subscribe to one asset/window pair:

```ts
import { SnapshotService } from "@sha3/polymarket-snapshot";

const snapshotService = SnapshotService.createDefault({ snapshotIntervalMs: 500 });

snapshotService.addSnapshotListener({
  assets: ["btc"],
  windows: ["5m"],
  listener(snapshot): void {
    console.log(snapshot.binancePrice, snapshot.upPrice);
  },
});
```

Read the latest snapshot state on demand:

```ts
import { SnapshotService } from "@sha3/polymarket-snapshot";

const snapshotService = SnapshotService.createDefault();
const latestSnapshots = snapshotService.getSnapshot({ assets: ["btc"], windows: ["5m"] });

console.log(latestSnapshots[0]?.priceToBeat);
```

Stop the service cleanly:

```ts
import { SnapshotService } from "@sha3/polymarket-snapshot";

const snapshotService = SnapshotService.createDefault();

await snapshotService.disconnect();
```

## Examples

Listen to every supported asset and both windows:

```ts
import { SnapshotService } from "@sha3/polymarket-snapshot";

const snapshotService = SnapshotService.createDefault();

snapshotService.addSnapshotListener({
  listener(snapshot): void {
    console.log(snapshot.asset, snapshot.window);
  },
});
```

Remove a listener by callback reference:

```ts
import { SnapshotService, type SnapshotListener } from "@sha3/polymarket-snapshot";

const snapshotService = SnapshotService.createDefault();
const listener: SnapshotListener = (snapshot): void => {
  console.log(snapshot.generatedAt);
};

snapshotService.addSnapshotListener({ listener, assets: ["eth"], windows: ["15m"] });
snapshotService.removeSnapshotListener(listener);
```

Inject fake runtime dependencies in tests:

```ts
import { SnapshotService } from "@sha3/polymarket-snapshot";

const snapshotService = new SnapshotService({
  scheduler: fakeScheduler,
  cryptoClientFactory: () => fakeCryptoClient,
  marketCatalogService: fakeMarketCatalog,
  marketStreamService: fakeMarketStream,
  logger: fakeLogger,
});
```

## Public API

### `SnapshotService`

Main service for managing shared market subscriptions and emitting plain snapshot objects.

#### `createDefault(options?)`

Creates a ready-to-use service with the default runtime wiring.

Returns:

- `SnapshotService`

Behavior notes:

- uses `@sha3/crypto` for crypto feed aggregation
- uses `@sha3/polymarket` for market discovery, market streaming, and `priceToBeat`
- starts lazily on the first listener registration

#### `addSnapshotListener(options)`

Registers a listener for one or more `asset + window` pairs.

Returns:

- `void`

Behavior notes:

- omitted `assets` expands to all supported assets
- omitted `windows` expands to all supported windows
- each tick emits one snapshot per requested pair

#### `removeSnapshotListener(listener)`

Removes a previously registered listener by callback reference.

Returns:

- `void`

Behavior notes:

- when the last listener is removed, runtime resources are stopped

#### `getSnapshot(options?)`

Reads the latest tracked snapshot objects for the requested pairs.

Returns:

- `Snapshot[]`

Behavior notes:

- returns only tracked pairs
- returns `[]` when no matching pairs are active

#### `disconnect()`

Stops timers, stream subscriptions, and pending `priceToBeat` retries.

Returns:

- `Promise<void>`

Behavior notes:

- clears all registered listeners
- disconnects the shared crypto and Polymarket clients

### `Snapshot`

Plain JSON snapshot emitted by the service.

```ts
type Snapshot = {
  asset: SnapshotAsset;
  window: SnapshotWindow;
  generatedAt: number;
  marketId: string | null;
  marketSlug: string | null;
  marketConditionId: string | null;
  marketStart: string | null;
  marketEnd: string | null;
  priceToBeat: number | null;
  upAssetId: string | null;
  upPrice: number | null;
  upOrderBook: OrderBook | null;
  upEventTs: number | null;
  downAssetId: string | null;
  downPrice: number | null;
  downOrderBook: OrderBook | null;
  downEventTs: number | null;
  binancePrice: number | null;
  binanceOrderBook: OrderBookSnapshot | null;
  binanceEventTs: number | null;
  coinbasePrice: number | null;
  coinbaseOrderBook: OrderBookSnapshot | null;
  coinbaseEventTs: number | null;
  krakenPrice: number | null;
  krakenOrderBook: OrderBookSnapshot | null;
  krakenEventTs: number | null;
  okxPrice: number | null;
  okxOrderBook: OrderBookSnapshot | null;
  okxEventTs: number | null;
  chainlinkPrice: number | null;
  chainlinkOrderBook: OrderBookSnapshot | null;
  chainlinkEventTs: number | null;
};
```

Behavior notes:

- missing values stay `null` until first observed
- latest known values persist across ticks

### `ProviderSnapshot`

Latest provider state for one asset.

```ts
type ProviderSnapshot = {
  price: number | null;
  orderBook: OrderBookSnapshot | null;
  eventTs: number | null;
};
```

Behavior notes:

- `eventTs` tracks the latest provider timestamp for either price or order book

### `SnapshotAsset`

Supported snapshot asset symbol.

```ts
type SnapshotAsset = "btc" | "eth" | "sol" | "xrp";
```

### `SnapshotWindow`

Supported Polymarket market window.

```ts
type SnapshotWindow = "5m" | "15m";
```

### `SnapshotListener`

Callback invoked with one plain `Snapshot` object at a time.

```ts
type SnapshotListener = (snapshot: Snapshot) => void;
```

### `AddSnapshotListenerOptions`

Options for listener registration.

```ts
type AddSnapshotListenerOptions = {
  listener: SnapshotListener;
  assets?: SnapshotAsset[];
  windows?: SnapshotWindow[];
};
```

### `GetSnapshotOptions`

Options for `getSnapshot()`.

```ts
type GetSnapshotOptions = {
  assets?: SnapshotAsset[];
  windows?: SnapshotWindow[];
};
```

### `SnapshotServiceOptions`

Configuration and advanced dependency injection options for the service.

```ts
type SnapshotServiceOptions = {
  snapshotIntervalMs?: number;
  supportedAssets?: SnapshotAsset[];
  supportedWindows?: SnapshotWindow[];
  priceToBeatInitialDelayMs?: number;
  priceToBeatRetryIntervalMs?: number;
  cryptoClientFactory?: (assets: SnapshotAsset[]) => SnapshotCryptoClient;
  marketCatalogService?: SnapshotMarketCatalog;
  marketStreamService?: SnapshotMarketStream;
  scheduler?: SnapshotScheduler;
  logger?: SnapshotLogger;
};
```

Behavior notes:

- `snapshotIntervalMs` must be one of `100`, `200`, `500`, or `1000`
- emitted snapshots and `getSnapshot()` values use interval-aligned `generatedAt`
- dependency injection fields are primarily useful for deterministic tests

### `SnapshotCryptoClient`

Minimal crypto runtime contract used by `SnapshotService`.

```ts
type SnapshotCryptoClient = {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(listener: (event: FeedEvent) => void): { unsubscribe(): void };
};
```

### `SnapshotMarketCatalog`

Minimal market catalog contract used by `SnapshotService`.

```ts
type SnapshotMarketCatalog = {
  buildCryptoWindowSlugs(options: { date: Date; window: SnapshotWindow; symbols?: SnapshotAsset[] }): string[];
  loadMarketBySlug(options: { slug: string }): Promise<PolymarketMarket>;
  getPriceToBeat(options: { market: PolymarketMarket }): Promise<number | null>;
};
```

### `SnapshotMarketStream`

Minimal market stream contract used by `SnapshotService`.

```ts
type SnapshotMarketStream = {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(options: { assetIds: string[] }): void;
  unsubscribe(options: { assetIds: string[] }): void;
  addListener(options: { listener: (event: { type: "price" | "book"; assetId: string; date: Date }) => void }): () => void;
};
```

### `SnapshotScheduler`

Minimal scheduler contract used by `SnapshotService`.

```ts
type SnapshotScheduler = {
  now(): number;
  setTimeout(listener: () => void, delayMs: number): unknown;
  clearTimeout(timer: unknown): void;
  setInterval(listener: () => void, delayMs: number): unknown;
  clearInterval(timer: unknown): void;
};
```

### `SnapshotLogger`

Minimal logger contract used by `SnapshotService`.

```ts
type SnapshotLogger = {
  debug(message: string): void;
  warn(message: string): void;
  error(message: string): void;
};
```

### `PackageInfoService`

Small package metadata service exposed at the package root.

#### `createDefault()`

Creates a default package info service.

Returns:

- `PackageInfoService`

#### `readPackageInfo()`

Reads the package metadata exposed by this package.

Returns:

- `PackageInfo`

### `PackageInfo`

```ts
type PackageInfo = { packageName: string };
```

## Compatibility

- Node.js 20+
- ESM
- TypeScript with relative `.ts` imports enabled

## Configuration

The package defaults live in `src/config.ts`.

- `config.PACKAGE_NAME`: package name returned by `PackageInfoService`.
- `config.ALLOWED_SNAPSHOT_INTERVALS_MS`: supported aligned snapshot intervals.
- `config.DEFAULT_SNAPSHOT_INTERVAL_MS`: default tick interval for snapshot emission.
- `config.DEFAULT_SUPPORTED_ASSETS`: default asset list when listener filters omit `assets`.
- `config.DEFAULT_SUPPORTED_WINDOWS`: default window list when listener filters omit `windows`.
- `config.DEFAULT_PRICE_TO_BEAT_INITIAL_DELAY_MS`: delay before the first `priceToBeat` request for a new market slug.
- `config.DEFAULT_PRICE_TO_BEAT_RETRY_INTERVAL_MS`: retry cadence for `priceToBeat` after a failure or `null` response.
- `config.MARKET_BOUNDARY_DELAY_MS`: safety delay applied after a `5m` or `15m` UTC boundary before rotating markets.
- `config.MARKET_ACTIVATION_RETRY_INTERVAL_MS`: retry delay used when a market activation attempt fails.

## Scripts

- `npm run standards:check`: verifies project contract rules
- `npm run lint`: runs Biome checks
- `npm run format:check`: verifies formatting
- `npm run typecheck`: runs `tsc --noEmit`
- `npm run test`: runs `node:test`
- `npm run check`: runs standards, lint, format, typecheck, and tests
- `npm run build`: builds `dist/`

## Structure

- `src/snapshot/snapshot.service.ts`: snapshot orchestration service
- `src/snapshot/snapshot.types.ts`: public snapshot types and runtime contracts
- `src/package-info/package-info.service.ts`: package metadata service
- `src/config.ts`: default package configuration
- `src/index.ts`: package exports
- `test/snapshot.test.ts`: snapshot behavior tests
- `test/package-info.test.ts`: package info behavior test

## Troubleshooting

### `priceToBeat` stays `null`

That is expected at startup. The service waits for `config.DEFAULT_PRICE_TO_BEAT_INITIAL_DELAY_MS` before the first fetch, then retries every `config.DEFAULT_PRICE_TO_BEAT_RETRY_INTERVAL_MS` when necessary.

### A Polymarket snapshot still shows `null` prices or books

The service only populates values after the corresponding upstream events arrive. Until then, missing fields stay `null`.

### Markets appear to change every 5 or 15 minutes

That is the intended behavior. The service rotates to the new Polymarket slug at exact UTC window boundaries, with `config.MARKET_BOUNDARY_DELAY_MS` applied as a small safety buffer.

## AI Workflow

- Read `AGENTS.md`, `ai/contract.json`, and the assistant adapter before editing code.
- Do not edit managed files unless the task is an explicit standards update.
- Keep snapshot output as plain JSON objects; persistence shaping is out of scope for this package.
- Run `npm run check` before finalizing implementation work.
