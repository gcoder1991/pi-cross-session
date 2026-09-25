import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import net from 'node:net';
import { syncBuiltinESMExports } from 'node:module';
import { randomUUID } from 'node:crypto';
import { component, wire, message, hello, info } from './support/component.mjs';
import { registrations, runtimeDir, until, sleep, hold } from './support/sdk.mjs';
// Patch BEFORE the first loader/jiti evaluation; hit assertions below are mandatory.
const originalStat = fsp.lstat, originalWrite = fsp.writeFile;
let statHook = async () => {}, writeHook = async () => {};
fsp.lstat = async (...args) => { await statHook(...args); return originalStat(...args); };
fsp.writeFile = async (...args) => { await writeHook(...args); return originalWrite(...args); };
syncBuiltinESMExports();
const files = () => [process.env.PI_CODING_AGENT_DIR + '/peers', runtimeDir].flatMap(d => fs.existsSync(d) ? fs.readdirSync(d).map(f => d + '/' + f) : []).sort();

for (const fault of ['idle', 'mode', 'tui', 'partial-rpc', 'write-notify-getter', 'write-notify-method']) test(`review start cleanup: ${fault}, private resources/listeners baseline and other Host intact`, async () => {
  const endpoint = process.env.PI_CROSS_MESSAGING_ENDPOINT;
  const a = await component('independent-host'); await sleep(30);
  const baseline = files(), listeners = process.listenerCount('beforeExit'); let hits = 0, subscriptions = 0, b;
  writeHook = async (file, data) => { if (fault.startsWith('write') && String(data).includes('fault-host')) { hits++; throw Error('fixture start I/O failure'); } };
  try {
    b = await component('fault-host', { rpc: true, configure(ctx, bus) {
      const on = bus.on.bind(bus); bus.on = (topic, handler) => { if (fault === 'partial-rpc' && topic === 'cross-session:rpc:send') { hits++; throw Error('fixture second RPC acquisition'); } const off = on(topic, handler); subscriptions++; return () => { subscriptions--; off(); }; };
      if (fault === 'idle') ctx.isIdle = () => { hits++; throw Error('fixture initial idle getter'); };
      if (fault === 'mode') Object.defineProperty(ctx, 'mode', { get() { hits++; throw Error('fixture mode getter'); } });
      if (fault === 'tui') { ctx.mode = 'tui'; ctx.ui.addAutocompleteProvider = () => { hits++; throw Error('fixture TUI acquisition'); }; }
      if (fault === 'write-notify-getter') Object.defineProperty(ctx.ui, 'notify', { get() { hits++; throw Error('fixture notify getter'); } });
      if (fault === 'write-notify-method') ctx.ui.notify = () => { hits++; throw Error('fixture notify method'); };
    } });
    assert.ok(hits >= (fault.startsWith('write') ? 2 : 1), 'fault must actually be entered');
    assert.equal(subscriptions, 0); assert.equal(info(b), undefined);
    assert.deepEqual(files(), baseline); assert.equal(process.listenerCount('beforeExit'), listeners);
    assert.equal((await wire(undefined, a, null)).status, 'ready');
    assert.equal(process.env.PI_CROSS_MESSAGING_ENDPOINT, endpoint, 'failed startup must not change process-wide endpoint environment');
    console.log(JSON.stringify({ evidence: 'start fault hit and cleaned', fault, hits, subscriptions }));
  } finally { writeHook = async () => {}; await b?.close(); await a.close(); }
});

test('review dedup: sender-qualified A/B/A across settled, same ID tombstones and no budget refill', async () => {
  const a = await component('dedup-a'), c = await component('dedup-c'), b = await component('dedup-b', { rpc: true });
  b.bus.on('cross-session:received', e => e.reply({ handled: true }));
  try {
    const first = message('A'); assert.equal((await wire(a, b, first)).status, 'accepted');
    assert.equal((await wire(a, b, message('B'))).status, 'accepted');
    await b.busy(); await b.emit('message_end', { message: { role: 'assistant', stopReason: 'stop' } }); await b.settled();
    assert.equal((await wire(a, b, message('A'))).status, 'duplicate');
    assert.equal((await wire(c, b, message('A'))).status, 'accepted');
    assert.equal(info(b).remainingBudget, 253);
    const now = Date.now; Date.now = () => now() + 31000;
    try { assert.equal((await wire(a, b, { ...first, text: 'changed ID replay', sentAt: Date.now() })).status, 'duplicate'); assert.equal((await wire(a, b, message('A'))).status, 'accepted'); }
    finally { Date.now = now; }
    assert.equal(info(b).remainingBudget, 252);
  } finally { await b.close(); await c.close(); await a.close(); }
});

async function fakePeer(a, b, handle, fn) {
  await b.emit('session_shutdown');
  const registration = process.env.PI_CODING_AGENT_DIR + '/peers/' + b.peer.instanceId + '.json';
  fs.writeFileSync(registration, JSON.stringify(b.peer), { mode: 0o600 });
  const sockets = new Set(); let connects = 0, sends = 0;
  const server = net.createServer(s => { connects++; sockets.add(s); s.on('error', () => {}); s.on('close', () => sockets.delete(s)); let buffer = '', helloId;
    s.on('data', data => { buffer += data; let n; while ((n = buffer.indexOf('\n')) >= 0) { const f = JSON.parse(buffer.slice(0, n)); buffer = buffer.slice(n + 1);
      if (f.type === 'hello') { helloId = f.requestId; s.write(JSON.stringify({ v: 1, type: 'response', requestId: f.requestId, ok: true, status: 'ready', capabilities: ['cancel-safe-queue-v1'], peer: { id: b.peer.id, instanceId: b.peer.instanceId, pid: process.pid } }) + '\n'); }
      else { sends++; handle(s, f, helloId); }
    } });
  });
  await new Promise(r => server.listen(b.peer.socketPath, r)); fs.chmodSync(b.peer.socketPath, 0o600);
  try { await fn(() => ({ connects, sends })); }
  finally { for (const s of sockets) s.destroy(); await new Promise(r => server.close(r)); fs.rmSync(registration, { force: true }); }
}
for (const bad of ['wrong-id', 'old-hello', 'invalid-metadata']) test(`review negative receipt ${bad}: correlate before refusal, one send, unknown/no retry`, async () => {
  const a = await component('receipt-a'), b = await component('receipt-b');
  try { await fakePeer(a, b, (s, f, helloId) => s.write(JSON.stringify({ v: 1, type: 'response', requestId: bad === 'old-hello' ? helloId : bad === 'invalid-metadata' ? f.requestId : randomUUID(), ok: false, status: 'busy', code: 'busy', retryable: true, next: bad === 'invalid-metadata' ? { unsafe: 'remote metadata' } : 'automatically resend' }) + '\n'), async counts => {
    await assert.rejects(a.tool('send_pi_message', { target: b.peer.instanceId, message: 'correlation test' }), e => { console.log(JSON.stringify({ evidence: 'negative receipt', bad, code: e.code, state: e.state, retryable: e.retryable })); return e.state === 'receipt_unknown' && !e.retryable; });
    await sleep(50); assert.equal(counts().sends, 1);
  }); } finally { await b.close(); await a.close(); }
});

test('review ordinary send deadline includes HIT delayed exchange lstat; late release cannot connect/send', { timeout: 12000 }, async () => {
  const a = await component('deadline-a'), b = await component('deadline-b'); await sleep(50);
  const gate = hold(); let hits = 0, entered = false;
  try { await fakePeer(a, b, s => s.destroy(), async counts => {
    statHook = async file => { if (String(file) === b.peer.socketPath && ++hits === 2) { entered = true; await gate.promise; } };
    // First socket lstat is livePeers' probe; second is ordinary message exchange, NOT RPC.
    const start = performance.now(); let result, settledMs;
    const sending = a.tool('send_pi_message', { target: b.peer.instanceId, message: 'delayed vetSocket' }).then(v => { settledMs = performance.now() - start; result = { value: v }; }, e => { settledMs = performance.now() - start; result = { error: e }; });
    try {
      await until(() => entered, 'actual second lstat entered');
      await sleep(5300);
      console.log(JSON.stringify({ evidence: 'exchange lstat deadline', entered, hits, elapsed: performance.now() - start, settledMs, settled: !!result, ...counts() }));
      assert.ok(result, 'caller must return before releasing outstanding lstat'); assert.ok(settledMs >= 4900 && settledMs < 5300);
      assert.equal(result.error?.code, 'timeout'); assert.equal(result.error.state, 'receipt_unknown'); assert.equal(result.error.retryable, false);
      const before = counts(); gate.release(); await sending; await sleep(100);
      assert.deepEqual(counts(), before, 'late lstat must not connect or send'); assert.equal(before.sends, 0);
      console.log(JSON.stringify({ evidence: 'late lstat released', entered, hits, ...counts(), noConnectionGrowth: true }));
    } finally { gate.release(); await sending; statHook = async () => {}; }
  }); } finally { gate.release(); statHook = async () => {}; await b.close(); await a.close(); }
});

async function pipeline(a, b, bytes, split, warmup = 0) {
  const s = net.createConnection({ path: b.peer.socketPath }), replies = []; let buffer = '';
  s.on('error', () => {}); s.on('data', data => { buffer += data; let n; while ((n = buffer.indexOf('\n')) >= 0) { replies.push(JSON.parse(buffer.slice(0, n))); buffer = buffer.slice(n + 1); } });
  await new Promise((r, j) => { s.once('connect', r); s.once('error', j); });
  try {
    if (warmup) { s.write(bytes.subarray(0, warmup)); await sleep(30); bytes = bytes.subarray(warmup); }
    // No ready wait: exercise authentication's pending async I/O window.
    for (let i = 0; i < bytes.length; i += split ?? bytes.length) s.write(bytes.subarray(i, i + (split ?? bytes.length)));
    await until(() => s.destroyed || replies.some(r => r.status !== 'ready'), 'pipeline response');
    return replies;
  } finally { s.destroy(); }
}
for (const scenario of ['coalesced-near-hello', 'fragmented-near-message', 'BOM-raw-exact', 'BOM-raw-over', 'partial-multibyte-over', 'valid-then-invalid-utf8', 'valid-then-third']) test(`review raw per-frame IPC pipeline: ${scenario}`, async () => {
  const a = await component('frame-a'), b = await component('frame-b', { rpc: true });
  b.bus.on('cross-session:received', e => e.reply({ handled: true }));
  try {
    let h = Buffer.from(JSON.stringify(hello(a, b))), m = message('界😀 pipeline');
    if (scenario === 'coalesced-near-hello') m.text += 'z'.repeat(32000);
    let tail = Buffer.alloc(0), prefix = Buffer.alloc(0), split;
    if (scenario === 'coalesced-near-hello' || scenario.startsWith('BOM-raw')) {
      h = Buffer.concat([h, Buffer.alloc((scenario.startsWith('BOM-raw') ? (scenario === 'BOM-raw-exact' ? 1048572 : 1048575) : 1048475) - h.length, 32)]);
      if (scenario.startsWith('BOM-raw')) prefix = Buffer.from([0xef, 0xbb, 0xbf]);
    }
    if (scenario === 'fragmented-near-message') {
      const spare = 1048576 - Buffer.byteLength(JSON.stringify(m) + '\n');
      m.text += '界'.repeat(Math.floor(spare / 3)) + 'x'.repeat(spare % 3); split = 4093;
    }
    if (scenario === 'valid-then-invalid-utf8') tail = Buffer.from([0xff, 0x0a]);
    if (scenario === 'valid-then-third') tail = Buffer.from('{}\n');
    const bytes = scenario === 'partial-multibyte-over' ? Buffer.concat([h, Buffer.from('\n'), Buffer.alloc(1048575, 32), Buffer.from([0xe7, 0x95])]) : Buffer.concat([prefix, h, Buffer.from('\n' + JSON.stringify(m) + '\n'), tail]);
    const replies = await pipeline(a, b, bytes, split, scenario === 'coalesced-near-hello' ? 17000 : 0);
    console.log(JSON.stringify({ evidence: 'raw pipelined frames', scenario, bytes: bytes.length, statuses: replies.map(r => r.status), admitted: info(b).states.length }));
    if (scenario === 'BOM-raw-over' || scenario === 'partial-multibyte-over') { assert.ok(replies.some(r => r.status === 'message_too_large')); assert.equal(info(b).states.length, 0); }
    else { assert.ok(replies.some(r => r.status === 'accepted')); assert.equal(info(b).states.length, 1, 'later malformed/extra frame cannot revoke earlier processed admission'); }
  } finally { await b.close(); await a.close(); }
});

test('review component: unknown/toolUse/error ending drops queue; internal cancelled/peer continuations never gain tool authority', async () => {
  const a = await component('evidence-a'), b = await component('evidence-b', { rpc: true });
  try {
    for (const stopReason of [undefined, 'toolUse', 'error', 'length']) {
      await b.busy(); assert.equal((await wire(a, b, message('drop ' + stopReason))).status, 'queued');
      if (stopReason) await b.emit('message_end', { message: { role: 'assistant', stopReason } });
      await b.settled(false); assert.ok(info(b).stopped); assert.equal(b.calls.length, 0);
      assert.ok(info(b).states.every(s => s.state === 'dropped_cancelled'));
      await b.command('cross-session-resume');
    }
    // Explicit reopen starts a new peer turn, not the old cancelled authority.
    assert.equal((await wire(a, b, message('new peer after reopen'))).status, 'submitted');
    b.ctx.signal = new AbortController().signal; b.setIdle(false); await b.emit('agent_start');
    for (const iteration of [0, 1]) {
      if (iteration) { await b.emit('before_agent_start'); await b.emit('agent_start'); }
      for (const [toolName, input] of [['Agent', {}], ['send_subagent', { agent_id: 'fixture', message: 'x' }], ['mesh', { action: 'resume' }], ['mesh_control', { action: 'grow' }]]) assert.equal((await b.emit('tool_call', { toolName, input })).block, true);
    }
    assert.equal((await wire(a, b, message('queue after reopen'))).status, 'queued');
    await b.settled(); assert.equal(b.calls.length, 2, 'new peer successful turn can flush, old cancelled flag does not poison it');
    await b.emit('message_end', { message: { role: 'assistant', stopReason: 'aborted' } });
    await b.emit('agent_start'); assert.equal((await b.emit('tool_call', { toolName: 'Agent', input: {} })).block, true);
    await b.settled(false);
    await b.emit('input', { source: 'extension' }); await b.emit('agent_start');
    assert.equal((await b.emit('tool_call', { toolName: 'send_subagent', input: {} })).block, true);
    assert.equal((await wire(a, b, message('unknown continuation cannot reopen'))).status, 'stopped');
  } finally { await b.close(); await a.close(); }
});
