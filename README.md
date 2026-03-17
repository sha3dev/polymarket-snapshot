# @sha3/polymarket-snapshot

Flat Polymarket snapshot stream for direct persistence into ClickHouse.

## TL;DR

```bash
npm install @sha3/polymarket-snapshot
```

```ts
import { SnapshotService } from "@sha3/polymarket-snapshot";

const snapshotService = new SnapshotService(500);

snapshotService.addSnapshotListener({
  listener(snapshot) {
    console.log(snapshot.generated_at, snapshot.btc_binance_price, snapshot.btc_5m_slug, snapshot.btc_5m_up_price);
  },
});
```

## Why

Use this package when you want one flat snapshot object, with snake_case columns, ready to map directly into a wide ClickHouse table.

## Main Capabilities

- Emits one flat snapshot that includes all configured assets and all configured windows.
- Keeps crypto fields asset-scoped and Polymarket fields asset-and-window-scoped.
- Fills `asset_window_price_to_beat` columns as soon as Polymarket exposes them for the active market.
- Omits market-window columns when `generated_at` is outside the market interval.

## Installation

```bash
npm install @sha3/polymarket-snapshot
```

## Usage

```ts
import { SnapshotService } from "@sha3/polymarket-snapshot";

const snapshotService = new SnapshotService(500);

snapshotService.addSnapshotListener({
  listener(snapshot) {
    console.log(snapshot.btc_binance_price, snapshot.btc_5m_slug, snapshot.btc_5m_up_price);
  },
});

const latestSnapshot = snapshotService.getSnapshot();
await snapshotService.disconnect();
```

## Examples

Read the current snapshot on demand:

```ts
import { SnapshotService } from "@sha3/polymarket-snapshot";

const snapshotService = new SnapshotService();
const snapshot = snapshotService.getSnapshot();

console.log(snapshot?.btc_5m_slug, snapshot?.btc_binance_price);
await snapshotService.disconnect();
```

## Public API

### `SnapshotService`

```ts
new SnapshotService(snapshotIntervalMs?: number)
```

Creates and maintains the live snapshot runtime.

#### `addSnapshotListener()`

- Registers a listener.
- Starts the shared runtime on first use.

#### `removeSnapshotListener()`

- Removes one listener.
- Stops the runtime after the last listener is removed.

#### `getSnapshot()`

- Returns the latest flat snapshot.
- Returns `null` before activation and after shutdown.

#### `disconnect()`

- Clears listeners.
- Closes timers, transports, and subscriptions.

### `AddSnapshotListenerOptions`

```ts
type AddSnapshotListenerOptions = { listener: SnapshotListener };
```

### `SnapshotListener`

```ts
type SnapshotListener = (snapshot: Snapshot) => void;
```

### `Snapshot`

```ts
type Snapshot = {
  generated_at: number;
} & Record<string, number | string | null>;
```

Crypto fields are asset-scoped. The same field set appears for every configured asset prefix such as `btc_*`, `eth_*`, `sol_*`, and `xrp_*`:

```ts
type CryptoFields = {
  btc_binance_price: number | null;
  btc_binance_order_book_json: string | null;
  btc_binance_event_ts: number | null;
  btc_coinbase_price: number | null;
  btc_coinbase_order_book_json: string | null;
  btc_coinbase_event_ts: number | null;
  btc_kraken_price: number | null;
  btc_kraken_order_book_json: string | null;
  btc_kraken_event_ts: number | null;
  btc_okx_price: number | null;
  btc_okx_order_book_json: string | null;
  btc_okx_event_ts: number | null;
  btc_chainlink_price: number | null;
  btc_chainlink_event_ts: number | null;
};
```

Market fields are asset-and-window-scoped. They are included only when `generated_at` is inside that market interval:

```ts
type LiveMarketFields = {
  btc_5m_slug: string | null;
  btc_5m_price_to_beat: number | null;
  btc_5m_up_asset_id: string | null;
  btc_5m_up_price: number | null;
  btc_5m_up_order_book_json: string | null;
  btc_5m_up_event_ts: number | null;
  btc_5m_down_asset_id: string | null;
  btc_5m_down_price: number | null;
  btc_5m_down_order_book_json: string | null;
  btc_5m_down_event_ts: number | null;
};
```

The same market field set can appear for every configured `asset_window` prefix such as `btc_15m_*`, `eth_5m_*`, `eth_15m_*`, and so on.

## Compatibility

- Node.js 20+
- ESM consumers
- Strict TypeScript projects

## Configuration

Configuration lives in [config.ts](/Users/jc/Documents/GitHub/polymarket-snapshot/src/config.ts).

- `PACKAGE_NAME`: package identifier used by the internal logger.
- `ALLOWED_SNAPSHOT_INTERVALS_MS`: accepted constructor values for `snapshotIntervalMs`.
- `DEFAULT_SNAPSHOT_INTERVAL_MS`: default snapshot interval when the constructor receives no value.
- `DEFAULT_SUPPORTED_ASSETS`: asset prefixes included in the snapshot by default.
- `DEFAULT_SUPPORTED_WINDOWS`: Polymarket windows included in the snapshot by default.
- `DEFAULT_PRICE_TO_BEAT_INITIAL_DELAY_MS`: initial wait before the first `price_to_beat` lookup for a newly activated market.
- `DEFAULT_PRICE_TO_BEAT_RETRY_INTERVAL_MS`: retry wait when `price_to_beat` is still unavailable.
- `MARKET_BOUNDARY_DELAY_MS`: delay applied after a market boundary before reloading the next market.
- `MARKET_ACTIVATION_RETRY_INTERVAL_MS`: retry delay after a market activation failure.

## Scripts

- `npm run standards:check`
- `npm run lint`
- `npm run format:check`
- `npm run typecheck`
- `npm run test`
- `npm run check`
- `npm run test:real-snapshot`: runs the optional live console dashboard for 60 seconds

## Structure

- `src/snapshot/`: snapshot runtime and flattening logic
- `src/config.ts`: package configuration
- `src/index.ts`: public exports
- `test/snapshot.test.ts`: deterministic snapshot behavior
- `test/real-snapshot.test.ts`: optional live integration check

## Troubleshooting

If `getSnapshot()` returns `null`, add at least one listener first. The runtime is lazy and does not start until a listener is registered.

If market columns such as `btc_5m_slug` or `btc_5m_up_price` are missing, `generated_at` is outside the current market interval for that asset/window.

If you want to inspect the live stream visually, run `npm run test:real-snapshot`. It renders a small console dashboard with crypto prices, current market slugs, and `up/down` prices.

If `npm run check` fails, run `npm run fix` and then rerun `npm run check`.

## AI Workflow

- Read `AGENTS.md`, `ai/contract.json`, and the active assistant adapter before editing code.
- Do not edit managed files under `ai/` or `prompts/` during normal feature work.
- Run `npm run standards:check` before and after implementation when feasible.
- Finish with `npm run check`.
