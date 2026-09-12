import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import net from 'node:net';
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { component, wire, message, hello, info } from './support/component.mjs';
import { repo, sleep, registrations, until } from './support/sdk.mjs';
async function fixtures(fn, options = {}) { const a = await component('sender'), b = await component('receiver', options); try { await fn(a, b); } finally { await b.close(); await a.close(); } }

test('list_pi: resolves current Git worktree, branch and HEAD; non-Git cwd is null', async () => fixtures(async (a, b) => {
  a.ctx.cwd = b.ctx.cwd = repo; await a.emit('session_info_changed'); await b.emit('session_info_changed');
  const [expectedRoot, expectedHead, expectedBranch] = execFileSync('git', ['-C', repo, 'rev-parse', '--show-toplevel', 'HEAD', '--abbrev-ref', 'HEAD'], { encoding: 'utf8' }).trim().split('\n');
  const listing = await a.tool('list_pi', {});
  assert.deepEqual(listing.details.self.git, { worktree: expectedRoot, branch: expectedBranch === 'HEAD' ? null : expectedBranch, head: expectedHead });
  assert.deepEqual(listing.details.peers.find(peer => peer.instanceId === b.peer.instanceId).git, listing.details.self.git);
  assert.ok(listing.content[0].text.includes(`@${expectedHead.slice(0, 8)}) — session `));
  const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-cross-nongit-'));
  try {
    b.ctx.cwd = outside; await b.emit('session_info_changed');
    const refreshed = await a.tool('list_pi', {});
    assert.equal(refreshed.details.peers.find(peer => peer.instanceId === b.peer.instanceId).git, null);
    assert.match(refreshed.content[0].text, / — git none — session /);
  } finally { fs.rmSync(outside, { recursive: true, force: true }); }
}));

test('component/IPC: legacy idle submitted, legacy busy refuses BEFORE SDK; new busy queues; duplicate ID', async () => fixtures(async (a, b) => {
  const m = message('legacy-idle'); assert.equal((await wire(a, b, m, { safe: false })).status, 'submitted'); assert.equal(b.calls.length, 1);
  assert.equal((await wire(a, b, message('preflight-second'))).status, 'busy'); assert.equal(b.calls.length, 1);
  await b.emit('agent_start'); // Unknown signal => cannot queue even if SDK says idle.
  assert.equal((await wire(a, b, message('unknown'))).status, 'busy');
  await b.settled();
  assert.equal((await wire(a, b, message('unknown completion stays closed'))).status, 'stopped');
  await b.command('cross-session-resume'); await b.busy();
  assert.equal((await wire(a, b, message('legacy-busy'), { safe: false })).status, 'busy');
  const queued = message('new-busy'); assert.equal((await wire(a, b, queued)).status, 'queued');
  assert.equal((await wire(a, b, { ...queued, text: 'changed text same ID' })).status, 'duplicate');
  assert.equal(b.calls.length, 1);
  await b.settled(); assert.equal(b.calls.length, 2);
}));

test('component/IPC: authentication, null/empty IDs, invalid text, UTF8, multi-frame, byte cap and endpoint mode', async () => fixtures(async (a, b) => {
  for (const mutate of [h => h.token = '0'.repeat(64), h => h.target.id = 'wrong', h => h.target.instanceId = '0'.repeat(32), h => h.from.token = '0'.repeat(64), h => h.from = null, h => h.requestId = '', h => h.target.id = '']) {
    const h = hello(a, b); mutate(h); assert.equal((await wire(a, b, null, { hello: h })).status, 'authentication_failed');
  }
  for (const m of [message(' '), message('x', { messageId: '' }), message('x', { text: null }), message('x', { requestId: '' }), message('x', { summary: '界'.repeat(201) })]) assert.ok(!(await wire(a, b, m)).ok);
  assert.equal((await wire(a, b, message(), { raw: Buffer.from([0xff, 0x0a]) })).status, 'invalid_frame');
  assert.equal((await wire(a, b, message(), { raw: Buffer.from('{}\n{}\n{}\n') })).status, 'invalid_frame');
  assert.equal((await wire(a, b, message(), { raw: Buffer.alloc(1_048_577, 65) })).status, 'message_too_large');
  assert.equal((await wire(a, b, message('你好 😀'), { split: 1 })).status, 'submitted');
  assert.equal(b.calls.length, 1); assert.ok(b.calls[0].message.content.includes('你好 😀'));
  fs.chmodSync(b.peer.socketPath, 0o666);
  await assert.rejects(a.tool('send_pi_message', { target: b.peer.instanceId, message: 'unsafe mode' }), e => e.code === 'not_found');
  fs.chmodSync(b.peer.socketPath, 0o600);
}));

test('component/IPC: exact serialized UTF8 frame boundary', async () => fixtures(async (a, b) => {
  const m = message(''); const overhead = Buffer.byteLength(JSON.stringify(m) + '\n');
  const remaining = 1_048_576 - overhead;
  m.text = '界'.repeat(Math.floor(remaining / 3)) + 'x'.repeat(remaining % 3);
  assert.equal(Buffer.byteLength(JSON.stringify(m) + '\n'), 1_048_576);
  assert.equal((await wire(a, b, m)).status, 'submitted');
  await b.settled();
  m.text += 'x'; assert.equal((await wire(a, b, m)).status, 'message_too_large');
}));

test('component/IPC: three sessions ambiguity, old ref, invalid tool parameters, refused policy', async () => {
  const a = await component('sender'), b = await component('same'), c = await component('same', { inbound: 'refuse' });
  try {
    await assert.rejects(a.tool('send_pi_message', { target: 'same', message: 'x' }), e => e.code === 'ambiguous');
    await assert.rejects(a.tool('send_pi_message', { target: c.peer.instanceId, message: 'x' }), e => e.code === 'refused');
    for (const params of [{ target: null, message: 'x' }, { target: b.peer.instanceId, message: null }, { target: b.peer.instanceId, message: 'x', summary: 3 }, { target: '', message: 'x' }]) await assert.rejects(a.tool('send_pi_message', params), e => /^invalid_/.test(e.code));
    const old = b.peer.instanceId; await b.emit('session_start');
    assert.ok(!registrations().some(p => p.instanceId === old));
    await assert.rejects(a.tool('send_pi_message', { target: `same [${old.slice(0, 8)}]`, message: 'x' }), e => e.code === 'not_found');
  } finally { await c.close(); await b.close(); await a.close(); }
});

test('component/IPC: queue MAX_PENDING=50, short rate, status and total non-refilling budget=256', { timeout: 20000 }, async () => {
  const a = await component('sender1'), c = await component('sender2'), b = await component('receiver', { rpc: true });
  try {
    await b.busy();
    for (let i = 0; i < 30; i++) assert.equal((await wire(a, b, message('q' + i))).status, 'queued');
    assert.equal((await wire(a, b, message('rate'))).status, 'rate_limited');
    for (let i = 0; i < 20; i++) assert.equal((await wire(c, b, message('q' + i))).status, 'queued');
    assert.equal((await wire(c, b, message('full'))).status, 'queue_full');
    const queued = info(b).states[0]; assert.equal(queued.state, 'queued');
    assert.equal((await wire(a, b, { v: 1, type: 'status', requestId: randomUUID(), messageId: queued.messageId })).status, 'queued');
    b.setIdle(true); b.ctx.signal = undefined; // Discarded stub run: SDK idle has no active signal.
    await b.emit('session_start'); // new incarnation resets only here, not at settled
    b.peer = registrations().find(p => p.name === 'receiver');
    b.bus.on('cross-session:received', e => e.reply({ handled: true }));
    const realNow = Date.now; let now = realNow(); Date.now = () => now;
    try {
      for (let i = 0; i < 256; i++) { now += 2100; assert.equal((await wire(a, b, message('budget' + i))).status, 'accepted'); if (i % 30 === 0) { await b.busy(); await b.settled(); } }
      assert.equal(info(b).remainingBudget, 0);
      await b.busy(); await b.settled();
      assert.equal((await wire(a, b, message('over-budget'))).status, 'budget_exhausted');
      assert.equal(info(b).states.length, 256);
    } finally { Date.now = realNow; }
  } finally { await b.close(); await c.close(); await a.close(); }
});

test('component/IPC: queued TTL and shutdown/heartbeat synchronous getter failure (real 31s)', { timeout: 40000 }, async () => fixtures(async (a, b) => {
  await b.busy(); const m = message('expire'); assert.equal((await wire(a, b, m)).status, 'queued');
  a.runtime.getSessionName = () => { throw Error('stale synchronous getter'); };
  await sleep(31_100);
  assert.equal((await wire(undefined, b, null)).status, 'ready');
  assert.equal((await wire(b, b, { v: 1, type: 'status', requestId: randomUUID(), messageId: m.messageId })).status, 'unknown', 'status source-qualified');
  assert.equal(info(b).states[0].state, 'expired');
  assert.ok(!registrations().some(p => p.instanceId === a.peer.instanceId)); assert.ok(!fs.existsSync(a.peer.socketPath));
  assert.equal(b.calls.length, 0);
}, { rpc: true }));

test('component/IPC: new sender refuses old receiver capability BEFORE message frame', async () => fixtures(async (a, b) => {
  await b.emit('session_shutdown');
  fs.writeFileSync(process.env.PI_CODING_AGENT_DIR + '/peers/' + b.peer.instanceId + '.json', JSON.stringify(b.peer), { mode: 0o600 });
  let messages = 0;
  const server = net.createServer(socket => { let buf = ''; socket.on('data', data => { buf += data; let n; while ((n = buf.indexOf('\n')) >= 0) { const f = JSON.parse(buf.slice(0, n)); buf = buf.slice(n + 1); if (f.type === 'message') messages++; socket.write(JSON.stringify({ v: 1, type: 'response', requestId: f.requestId, ok: true, status: 'ready', peer: { id: b.peer.id, instanceId: b.peer.instanceId, pid: process.pid } }) + '\n'); } }); });
  await new Promise(r => server.listen(b.peer.socketPath, r)); fs.chmodSync(b.peer.socketPath, 0o600);
  try { await assert.rejects(a.tool('send_pi_message', { target: b.peer.instanceId, message: 'unsafe fallback forbidden' }), e => e.code === 'unsupported'); assert.equal(messages, 0); }
  finally { await new Promise(r => server.close(r)); fs.rmSync(process.env.PI_CODING_AGENT_DIR + '/peers/' + b.peer.instanceId + '.json', { force: true }); }
}));

test('component/IPC: receipt loss/absolute timeout are unknown, never auto-retried; shutdown cancels outstanding exchange', { timeout: 15000 }, async () => fixtures(async (a, b) => {
  await b.emit('session_shutdown');
  const registration = process.env.PI_CODING_AGENT_DIR + '/peers/' + b.peer.instanceId + '.json';
  fs.writeFileSync(registration, JSON.stringify(b.peer), { mode: 0o600 });
  let messages = 0, mode = 'loss'; const sockets = new Set();
  const server = net.createServer(socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); let buf = ''; socket.on('data', data => { buf += data; let n; while ((n = buf.indexOf('\n')) >= 0) { const f = JSON.parse(buf.slice(0, n)); buf = buf.slice(n + 1); if (f.type === 'message') { messages++; if (mode === 'loss') socket.destroy(); } else socket.write(JSON.stringify({ v: 1, type: 'response', requestId: f.requestId, ok: true, status: 'ready', capabilities: ['cancel-safe-queue-v1'], peer: { id: b.peer.id, instanceId: b.peer.instanceId, pid: process.pid } }) + '\n'); } }); });
  await new Promise(r => server.listen(b.peer.socketPath, r)); fs.chmodSync(b.peer.socketPath, 0o600);
  try {
    await assert.rejects(a.tool('send_pi_message', { target: b.peer.instanceId, message: 'lost' }), e => e.code === 'connection_closed' && e.state === 'receipt_unknown' && !e.retryable); assert.equal(messages, 1);
    mode = 'timeout';
    await assert.rejects(a.tool('send_pi_message', { target: b.peer.instanceId, message: 'timeout' }), e => e.code === 'timeout' && e.state === 'receipt_unknown'); assert.equal(messages, 2);
    const sending = a.tool('send_pi_message', { target: b.peer.instanceId, message: 'shutdown' });
    const rejected = assert.rejects(sending, e => e.code === 'cancelled' && e.state === 'receipt_unknown');
    await until(() => messages === 3); await a.emit('session_shutdown'); await rejected;
    await until(() => sockets.size === 0); assert.equal(messages, 3);
  } finally { for (const s of sockets) s.destroy(); await new Promise(r => server.close(r)); fs.rmSync(registration, { force: true }); }
}));
