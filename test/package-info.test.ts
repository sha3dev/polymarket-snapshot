import * as assert from "node:assert/strict";
import { test } from "node:test";

import { PackageInfoService } from "../src/package-info/package-info.service.ts";

test("PackageInfoService exposes the configured package name", () => {
  const packageInfoService = PackageInfoService.createDefault();

  assert.deepEqual(packageInfoService.readPackageInfo(), { packageName: "@sha3/polymarket-snapshot" });
});
