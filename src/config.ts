const config = {
  PACKAGE_NAME: "@sha3/polymarket-snapshot",
  ALLOWED_SNAPSHOT_INTERVALS_MS: [100, 200, 500, 1000],
  DEFAULT_SNAPSHOT_INTERVAL_MS: 500,
  DEFAULT_SUPPORTED_ASSETS: ["btc", "eth", "sol", "xrp"],
  DEFAULT_SUPPORTED_WINDOWS: ["5m", "15m"],
  MARKET_BOUNDARY_DELAY_MS: 250,
  MARKET_ACTIVATION_RETRY_INTERVAL_MS: 1_000,
} as const;

export default config;
