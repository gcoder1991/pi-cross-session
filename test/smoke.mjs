// Real installed CLI load/registration/EOF cleanup smoke (not mutual IPC test).
// Run via test/run-clean.sh run smoke; never resolves a global pi binary.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const repo = fileURLToPath(new URL('../', import.meta.url));
const root = process.env.PI_CROSS_TEST_PRIVATE_ROOT;
assert.ok(root && process.env.PI_CODING_AGENT_DIR.startsWith(root + '/'), 'Run with isolated test/run-clean.sh');
const cwd = path.join(root, 'cli-cwd'); fs.mkdirSync(cwd, { recursive: true });
const child = spawn(process.execPath, [path.join(repo, 'node_modules/@earendil-works/pi-coding-agent/dist/cli.js'), '--mode', 'rpc', '--no-session', '--no-extensions', '--no-skills', '--no-prompt-templates', '--no-themes', '--no-context-files', '-e', path.join(repo, 'extensions/cross-session.ts')], { cwd, env: { ...process.env }, stdio: ['pipe', 'pipe', 'pipe'] });
let out = '', err = '';
child.stdout.on('data', data => out += data); child.stderr.on('data', data => err += data);
const exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
const timer = setTimeout(() => child.kill('SIGKILL'), 15000);
try {
  child.stdin.write(JSON.stringify({ id: 'smoke', type: 'get_commands' }) + '\n');
  let response, peer;
  const end = Date.now() + 10000;
  while (Date.now() < end && child.exitCode === null) {
    response = out.split('\n').filter(Boolean).map(line => { try { return JSON.parse(line); } catch { return {}; } }).find(r => r.type === 'response' && r.id === 'smoke' && r.command === 'get_commands');
    const dir = path.join(process.env.PI_CODING_AGENT_DIR, 'peers');
    const file = fs.existsSync(dir) && fs.readdirSync(dir).find(f => /^[0-9a-f]{32}\.json$/.test(f));
    if (file) peer = JSON.parse(fs.readFileSync(path.join(dir, file), 'utf8'));
    if (response && peer) break;
    await new Promise(r => setTimeout(r, 20));
  }
  assert.ok(response?.success, out + err);
  assert.ok(response.data.commands.some(c => c.name === 'peers'));
  assert.ok(response.data.commands.some(c => c.name === 'list-pi'));
  assert.ok(peer && fs.existsSync(peer.socketPath), 'actual registered inbox must exist (commands alone are insufficient)');
  child.stdin.end();
  assert.deepEqual(await exited, { code: 0, signal: null }, out + err);
  assert.ok(!fs.existsSync(peer.socketPath), 'EOF must remove endpoint');
  assert.ok(!fs.existsSync(path.join(process.env.PI_CODING_AGENT_DIR, 'peers', peer.instanceId + '.json')), 'EOF must remove registration');
  console.log('SMOKE PASS: installed SDK CLI, commands + actual inbox, stdin EOF, normal exit0, endpoint/registration cleanup (no model/network)');
} finally { clearTimeout(timer); if (child.exitCode === null) { child.kill('SIGKILL'); await exited; } }
