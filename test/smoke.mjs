// Headless smoke test: launch pi in RPC mode with this package loaded via -e,
// verify the extension registers its commands, then shut down.
// Requires `pi` on PATH. Run: npm run smoke
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const pkgDir = process.env.PI_EXT_SOURCE || fileURLToPath(new URL("..", import.meta.url));
const agentDir = mkdtempSync(join(tmpdir(), "pi-pkg-smoke-"));
const piBin = process.env.PI_BIN || "pi";

const child = spawn(piBin, ["--mode", "rpc", "--no-session", "-e", pkgDir], {
  env: { ...process.env, PI_CODING_AGENT_DIR: agentDir },
  stdio: ["pipe", "pipe", "pipe"],
});

let out = "";
let err = "";
child.stdout.on("data", (chunk) => (out += chunk));
child.stderr.on("data", (chunk) => (err += chunk));

const fail = (msg) => {
  console.error(`SMOKE FAIL: ${msg}`);
  console.error("--- stdout ---\n" + out.slice(0, 4000));
  console.error("--- stderr ---\n" + err.slice(0, 2000));
  child.kill("SIGKILL");
  process.exit(1);
};

const timer = setTimeout(() => fail("timed out after 30s"), 30_000);

function shutdown(passed) {
  clearTimeout(timer);
  child.kill("SIGTERM");
  const done = () => {
    rmSync(agentDir, { recursive: true, force: true });
    if (passed) {
      console.log("SMOKE PASS: extension autoloaded, peers + list-pi registered");
      process.exit(0);
    }
  };
  child.on("exit", done);
  setTimeout(done, 3000);
}

// Give the agent a moment to boot and load extensions, then ask for commands.
setTimeout(() => {
  child.stdin.write(JSON.stringify({ id: "smoke", type: "get_commands" }) + "\n");
}, 2500);

const waitFor = setInterval(() => {
  for (const line of out.split("\n")) {
    if (!line.includes('"command":"get_commands"')) continue;
    clearInterval(waitFor);
    const hasPeers = /"name"\s*:\s*"peers"/.test(out);
    const hasListPi = /"name"\s*:\s*"list-pi"/.test(out);
    if (hasPeers && hasListPi) return shutdown(true);
    return fail(`get_commands response missing extension commands (peers=${hasPeers}, list-pi=${hasListPi})`);
  }
  if (child.exitCode !== null) return fail(`pi exited early with code ${child.exitCode}`);
}, 250);

process.on("exit", () => rmSync(agentDir, { recursive: true, force: true }));
