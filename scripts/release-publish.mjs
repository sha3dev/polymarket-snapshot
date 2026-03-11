import { spawn } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function run(command, args, cwd, stdio = "inherit") {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd, stdio, shell: process.platform === "win32" });
    let stdout = "";
    let stderr = "";

    if (stdio === "pipe") {
      child.stdout.on("data", (chunk) => {
        stdout += String(chunk);
      });

      child.stderr.on("data", (chunk) => {
        stderr += String(chunk);
      });
    }

    child.on("error", reject);
    child.on("exit", (code) => {
      resolve({ code: code ?? 1, stdout, stderr });
    });
  });
}

function isNotFoundError(output) {
  const lower = output.toLowerCase();
  return lower.includes("e404") || lower.includes("404 not found") || lower.includes("no match found");
}

function parseSemver(version) {
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(version);
  if (!match) {
    throw new Error(`Unsupported version format: ${version}. Expected x.y.z`);
  }

  return { major: Number.parseInt(match[1], 10), minor: Number.parseInt(match[2], 10), patch: Number.parseInt(match[3], 10) };
}

function bumpMinor(version) {
  const parsed = parseSemver(version);
  return `${parsed.major}.${parsed.minor + 1}.0`;
}

async function readPackageJson(packageJsonPath) {
  const raw = await readFile(packageJsonPath, "utf8");
  return JSON.parse(raw);
}

async function writePackageJson(packageJsonPath, pkg) {
  await writeFile(packageJsonPath, `${JSON.stringify(pkg, null, 2)}\n`, "utf8");
}

async function versionExistsOnNpm(packageName, version, cwd) {
  const result = await run("npm", ["view", `${packageName}@${version}`, "version", "--json"], cwd, "pipe");
  const combined = `${result.stdout}\n${result.stderr}`;
  if (result.code === 0) {
    return true;
  }
  if (isNotFoundError(combined)) {
    return false;
  }

  throw new Error(`Unable to verify npm version ${packageName}@${version}: ${combined.trim()}`);
}

async function main() {
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const projectRoot = path.resolve(scriptDir, "..");
  const packageJsonPath = path.join(projectRoot, "package.json");
  const packageJson = await readPackageJson(packageJsonPath);
  if (typeof packageJson.name !== "string" || packageJson.name.length === 0) {
    throw new Error("package.json name is required.");
  }
  if (typeof packageJson.version !== "string" || packageJson.version.length === 0) {
    throw new Error("package.json version is required.");
  }

  const exists = await versionExistsOnNpm(packageJson.name, packageJson.version, projectRoot);
  if (exists) {
    const nextVersion = bumpMinor(packageJson.version);
    packageJson.version = nextVersion;
    await writePackageJson(packageJsonPath, packageJson);
    console.log(`Version ${packageJson.name}@${packageJson.version} already existed. Bumped to ${nextVersion}.`);
  } else {
    console.log(`Publishing new version ${packageJson.name}@${packageJson.version}.`);
  }

  const build = await run("npm", ["run", "build"], projectRoot);
  if (build.code !== 0) {
    throw new Error("npm run build failed.");
  }

  const publish = await run("npm", ["publish", "--access", "public", "--ignore-scripts"], projectRoot);
  if (publish.code !== 0) {
    throw new Error("npm publish failed.");
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
