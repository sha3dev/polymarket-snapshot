import { spawn } from "node:child_process";
import { readdir } from "node:fs/promises";
import path from "node:path";

const rootDir = process.cwd();
const testRoots = ["test", "src"];

async function collectTestsFrom(directoryPath, collector) {
  let entries;

  try {
    entries = await readdir(directoryPath, { withFileTypes: true });
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return;
    }

    throw error;
  }

  for (const entry of entries) {
    const absolutePath = path.join(directoryPath, entry.name);
    if (entry.isDirectory()) {
      await collectTestsFrom(absolutePath, collector);
      continue;
    }
    if (!entry.isFile()) {
      continue;
    }
    if (entry.name.endsWith(".test.ts")) {
      collector.push(path.relative(rootDir, absolutePath));
    }
  }
}

function runNodeTests(files) {
  return new Promise((resolve, reject) => {
    const args = ["--import", "tsx", "--test", ...files];
    const child = spawn(process.execPath, args, { cwd: rootDir, stdio: "inherit", shell: process.platform === "win32" });

    child.on("error", reject);
    child.on("exit", (code) => {
      resolve(code ?? 1);
    });
  });
}

async function main() {
  const testFiles = [];

  for (const root of testRoots) {
    await collectTestsFrom(path.join(rootDir, root), testFiles);
  }

  const uniqueFiles = [...new Set(testFiles)].sort((left, right) => left.localeCompare(right));
  const exitCode = await runNodeTests(uniqueFiles);
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
