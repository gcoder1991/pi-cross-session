import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { component, wire } from './support/component.mjs';
import { make, registrations, until } from './support/sdk.mjs';
// Install a fixture-only I/O delay BEFORE loading the product module, whose
// transpiled imports otherwise retain the original fs methods. No SDK patch.
const original = fsp.writeFile;
let hook = async () => {};
fsp.writeFile = async (...args) => { await hook(...args); return original(...args); };
syncBuiltinESMExports();
test('component/I/O fault: serialized registration write fenced by shutdown; independent/idempotent cleanup', async () => {
  const a = await component('a'), b = await component('b');
  let release, entered = false;
  const gate = new Promise(r => release = r);
  hook = async file => { if (String(file).includes('.' + b.peer.instanceId + '.')) { entered = true; await gate; } };
  try {
    const writing = b.emit('session_info_changed'); await until(() => entered);
    const closing = b.emit('session_shutdown'); release(); await writing; await closing; await b.emit('session_shutdown');
    assert.ok(!registrations().some(p => p.instanceId === b.peer.instanceId)); assert.ok(!fs.existsSync(b.peer.socketPath));
    assert.equal((await wire(undefined, a, null)).status, 'ready');
  } finally { release(); hook = async () => {}; await b.close(); await a.close(); }
});
test('component: overlapping starts/shutdown do not resurrect registrations; listener count returns to baseline', async () => {
  const baseline = process.listenerCount('beforeExit'); const b = await component('racer');
  const first = b.emit('session_start'), second = b.emit('session_start'), close = b.emit('session_shutdown');
  await Promise.all([first, second, close]);
  assert.ok(!registrations().some(p => p.name === 'racer'));
  assert.equal(process.listenerCount('beforeExit'), baseline);
  await b.close();
});

for (const initial of [undefined, '/fixture/pre-existing-parent.sock']) test(`real SDK Hosts/managed Bash leave endpoint environment untouched: ${initial ?? 'unset'}`, { timeout: 15000 }, async () => {
  const key = 'PI_CROSS_MESSAGING_ENDPOINT', previous = process.env[key];
  let a, b, managed;
  if (initial === undefined) delete process.env[key]; else process.env[key] = initial;
  try {
    a = await make('env-a'); b = await make('env-b'); managed = await make('env-managed', { managed: true });
    assert.notEqual(a.peer.socketPath, b.peer.socketPath);
    assert.equal(managed.peer, undefined);
    const check = async x => {
      assert.equal(process.env[key], initial, 'Cross must not advertise one Host as the process-wide endpoint');
      const result = await x.session.executeBash('printf "%s" "${PI_CROSS_MESSAGING_ENDPOINT-<unset>}"');
      assert.equal(result.exitCode, 0); assert.equal(result.output, initial ?? '<unset>');
    };
    for (const x of [a, b, managed]) await check(x);
    await b.close(); await check(a); await check(managed);
    assert.equal((await wire(undefined, a, null)).status, 'ready', 'A inbox remains usable without a global export');
    const external = initial === undefined ? '/fixture/external.sock' : undefined;
    if (external === undefined) delete process.env[key]; else process.env[key] = external;
    await managed.close(); await a.close(); await a.close();
    assert.equal(process.env[key], external, 'shutdown must preserve external replacement/deletion');
  } finally {
    try { await managed?.close(); await b?.close(); await a?.close(); }
    finally { if (previous === undefined) delete process.env[key]; else process.env[key] = previous; }
  }
});
