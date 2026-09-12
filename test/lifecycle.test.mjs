import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { component, wire } from './support/component.mjs';
import { registrations, until } from './support/sdk.mjs';
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
