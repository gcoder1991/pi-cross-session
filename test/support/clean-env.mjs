import fs from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Every test process gets a short private namespace (also fits Unix socket paths).
// Never inherit model/Gateway credentials, user settings, or user peer discovery.
const root = fs.mkdtempSync(path.join(process.platform === "win32" ? os.tmpdir() : fs.realpathSync("/tmp"), "pcs-test-"));
const keep = ["PATH", "SystemRoot", "WINDIR", "COMSPEC", "PATHEXT", "LANG", "LC_ALL", "TERM", "NODE_TEST_CONTEXT"];
const environment = Object.fromEntries(keep.filter((key) => process.env[key] !== undefined).map((key) => [key, process.env[key]]));
Object.assign(environment, {
  HOME: path.join(root, "home"), USERPROFILE: path.join(root, "home"),
  PI_CODING_AGENT_DIR: path.join(root, "agent"), PI_OFFLINE: "1",
  TMPDIR: path.join(root, "t"), TMP: path.join(root, "t"), TEMP: path.join(root, "t"),
  XDG_CONFIG_HOME: path.join(root, "config"), XDG_CACHE_HOME: path.join(root, "cache"),
  XDG_DATA_HOME: path.join(root, "data"), NPM_CONFIG_CACHE: path.join(root, "npm"),
  NPM_CONFIG_OFFLINE: "true", NPM_CONFIG_IGNORE_SCRIPTS: "true",
  NPM_CONFIG_AUDIT: "false", NPM_CONFIG_FUND: "false",
  PI_CROSS_TEST_PRIVATE_ROOT: root,
  NODE_OPTIONS: `--import=${JSON.stringify(fileURLToPath(new URL("./network-guard.mjs", import.meta.url)))}`,
  NPM_CONFIG_UPDATE_NOTIFIER: "false",
  GIT_CONFIG_NOSYSTEM: "1", GIT_ALLOW_PROTOCOL: "file",
});
for (const key of Object.keys(process.env)) delete process.env[key];
Object.assign(process.env, environment);
for (const key of ["HOME", "PI_CODING_AGENT_DIR", "TMPDIR", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "NPM_CONFIG_CACHE"]) fs.mkdirSync(process.env[key], { recursive: true, mode: 0o700 });
process.on("exit", () => {
  // Only this fixture namespace, and only if product cleanup left it empty.
  if (process.platform !== "win32") {
    const namespace = createHash("sha256").update(environment.PI_CODING_AGENT_DIR).digest("hex").slice(0, 12);
    try { fs.rmdirSync(`/tmp/pi-peers-${process.getuid?.() ?? 0}-${namespace}`); } catch { /* retain nonempty failure evidence */ }
  }
  fs.rmSync(root, { recursive: true, force: true });
});

await import("./network-guard.mjs");
await import("node:test");
