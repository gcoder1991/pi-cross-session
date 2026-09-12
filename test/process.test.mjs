import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import { repo, until, registrations } from './support/sdk.mjs';
function worker(name) {
  const child = spawn(process.execPath, [repo + '/test/support/worker.mjs', name], { cwd: process.env.PI_CROSS_TEST_PRIVATE_ROOT, env: { ...process.env }, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  const messages = []; let error = '';
  child.on('message', message => messages.push(message)); child.stderr.on('data', data => error += data);
  const exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
  return { child, messages, exited, async wait(type) { await until(() => messages.some(m => m.type === type || m.type === 'failure') || child.exitCode !== null, name + ':' + type); const failure = messages.find(m => m.type === 'failure'); assert.ok(!failure, failure?.error); const result = messages.find(m => m.type === type); assert.ok(result, error); return result; } };
}
test('two OS processes: actual SDK/local provider, mutual Unix sends/history/responses, shutdown and normal EOF cleanup', { timeout: 20000 }, async () => {
  const a = worker('process-A'), b = worker('process-B');
  try {
    const A = await a.wait('ready'), B = await b.wait('ready'); assert.notEqual(A.pid, B.pid); assert.notEqual(A.pid, process.pid);
    a.child.send({ type: 'send', target: B.peer.instanceId, text: 'A_TO_B' }); assert.equal((await a.wait('sent')).details.status, 'submitted');
    b.child.send({ type: 'send', target: A.peer.instanceId, text: 'B_TO_A' }); assert.equal((await b.wait('sent')).details.status, 'submitted');
    for (const [x, text] of [[a, 'B_TO_A'], [b, 'A_TO_B']]) {
      x.child.send({ type: 'inspect' }); const evidence = await x.wait('evidence');
      assert.equal(evidence.calls.length, 1); assert.ok(evidence.calls[0].completed); assert.match(JSON.stringify(evidence.calls[0].messages), new RegExp(text));
      assert.ok(evidence.entries.some(e => e.type === 'custom_message' && e.details?.text === text)); assert.deepEqual(evidence.errors, []);
    }
    a.child.send({ type: 'close' }); assert.deepEqual(await a.exited, { code: 0, signal: null });
    assert.ok(!fs.existsSync(A.peer.socketPath)); assert.ok(fs.existsSync(B.peer.socketPath), 'independent process resources');
    b.child.send({ type: 'eof' }); assert.deepEqual(await b.exited, { code: 0, signal: null });
    for (const p of [A.peer, B.peer]) { assert.ok(!fs.existsSync(p.socketPath)); assert.ok(!registrations().some(r => r.instanceId === p.instanceId)); }
    console.log(JSON.stringify({ evidence: 'two OS processes/SDK 0.84.4/Unix IPC', pids: [A.pid, B.pid], calls: 2, responses: 2, normalExits: 2, cleanup: true }));
  } finally { for (const x of [a, b]) if (x.child.exitCode === null) { x.child.kill('SIGKILL'); await x.exited; } }
});

test('SIGKILL: no cleanup promise; discovery reclaims only verified-dead fixture PID', { timeout: 15000 }, async () => {
  const x = worker('killed-fixture'); let host;
  try {
    const ready = await x.wait('ready'); x.child.kill('SIGKILL');
    assert.deepEqual(await x.exited, { code: null, signal: 'SIGKILL' });
    assert.ok(registrations().some(p => p.instanceId === ready.peer.instanceId), 'SIGKILL leaves registration');
    const { component } = await import('./support/component.mjs'); host = await component('reaper');
    await host.tool('list_pi', {});
    await until(() => !registrations().some(p => p.instanceId === ready.peer.instanceId));
    assert.ok(!fs.existsSync(ready.peer.socketPath));
  } finally { if (host) await host.close(); if (x.child.exitCode === null && x.child.signalCode === null) { x.child.kill('SIGKILL'); await x.exited; } }
});
