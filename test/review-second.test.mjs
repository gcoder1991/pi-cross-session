import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { Type } from 'typebox';
import { make, send, hold, until, command, registrations } from './support/sdk.mjs';
import { component, info, wire, message } from './support/component.mjs';

// Patch the real registration callee before the FIRST product loader evaluation.
// Only armed private incarnation writes fail; no timer/SDK/mock cleanup shortcut.
const originalWrite = fsp.writeFile, originalChmod = fsp.chmod;
const faults = new Map();
let publicationFault;
fsp.writeFile = async (...args) => {
  for (const [id, fault] of faults) if (String(args[0]).includes('.' + id + '.')) { fault.hits++; throw Error('fixture heartbeat registration write fault'); }
  if (publicationFault && String(args[1]).includes('"name":"scope-publication"')) publicationFault.peer = JSON.parse(args[1]);
  return originalWrite(...args);
};
fsp.chmod = async (...args) => {
  await originalChmod(...args);
  if (publicationFault?.peer && String(args[0]) === process.env.PI_CODING_AGENT_DIR + '/peers/' + publicationFault.peer.instanceId + '.json') {
    publicationFault.hits++; await publicationFault.gate.promise; throw Error('fixture post-publication startup failure');
  }
};
syncBuiltinESMExports();
const reason = 'Peer-only/cancelled turn cannot authorize task creation, resume, growth or policy changes; ask the local user';
const call = name => ({ name, arguments: {} });
const inert = (name, count) => pi => pi.registerTool({ name, label: 'Inert fixture', description: 'No spawn, peer or platform tools', parameters: Type.Object({}), async execute() { count.executions++; return { content: [{ type: 'text', text: 'inert execution' }] }; } });
const result = (b, name) => b.session.agent.state.messages.filter(m => m.role === 'toolResult' && m.toolName === name).at(-1);

test('second F1 real SDK: heartbeat WRITE cleanup cannot grant peer Agent/send_subagent; fresh user works with inbox gone', { timeout: 45000 }, async () => {
  const a = await make('heartbeat-sender'), rows = [];
  const baseline = process.listenerCount('beforeExit');
  try {
    for (const name of ['Agent', 'send_subagent']) {
      const count = { executions: 0 }, gate = hold();
      const b = await make('heartbeat-' + name, { rpc: true, retry: true, tools: [name], extra: inert(name, count) });
      rows.push({ b, name, count, gate });
      b.session.setActiveToolsByName([name]); b.setToolCall(call(name)); b.setGate(gate);
      assert.equal((await send(a, b, 'peer authority must survive inbox IO failure ' + name)).details.status, 'submitted');
      await until(() => b.calls.length === 1);
      assert.equal(b.session.isIdle, false); assert.equal(b.calls[0].signal.aborted, false);
      faults.set(b.peer.instanceId, { hits: 0 });
    }
    await until(() => rows.every(({ b }) => faults.get(b.peer.instanceId).hits === 1 && !fs.existsSync(b.peer.socketPath) && !registrations().some(p => p.instanceId === b.peer.instanceId)), 'actual 30s heartbeat fault and endpoint/registration cleanup', 35000);
    assert.equal(process.listenerCount('beforeExit'), baseline);
    for (const { b, gate } of rows) {
      assert.equal(info(b), undefined, 'RPC listeners removed');
      assert.equal(b.session.isIdle, false); assert.equal(b.calls[0].signal.aborted, false, 'inbox abort is NOT SDK retraction');
      gate.release();
    }
    await until(() => rows.every(({ b }) => b.calls.length === 2 && b.session.isIdle));
    console.log(JSON.stringify({ evidence: 'F1 after HIT heartbeat cleanup', rows: rows.map(({ b, name, count }) => ({ name, hits: faults.get(b.peer.instanceId).hits, executions: count.executions, result: result(b, name), calls: b.calls.length })) }));
    for (const { b, name, count } of rows) {
      assert.equal(count.executions, 0); assert.equal(result(b, name)?.isError, true); assert.ok(JSON.stringify(result(b, name).content).includes(reason));
      b.setToolCall(call(name)); await command(b, 'new genuine user, no inbox reopen');
      assert.equal(count.executions, 1); assert.equal(b.calls.length, 4); assert.deepEqual(b.errors, []);
      assert.equal(info(b), undefined); assert.ok(!fs.existsSync(b.peer.socketPath)); assert.ok(!registrations().some(p => p.instanceId === b.peer.instanceId));
      await assert.rejects(send(a, b, 'closed peer inbox'), e => e.code === 'not_found');
    }
    console.log(JSON.stringify({ evidence: 'F1 user positive controls inbox still gone', calls: 8, peerExecutions: [0, 0], userExecutions: [1, 1], errors: [] }));
  } finally { faults.clear(); for (const { b, gate } of rows) { gate.release(); await b.close(); } await a.close(); }
});

for (const first of ['interactive', 'extension']) test(`second F2 real SDK downstream handled ${first}: next idle input replaces stale source, NOT inbound stop`, async () => {
  const count = { executions: 0 }, observed = [];
  const a = await make('handled-sender'), b = await make('handled-' + first, { rpc: true, retry: true, tools: ['Agent'], extra: pi => {
    inert('Agent', count)(pi);
    pi.on('input', (event, ctx) => { observed.push({ text: event.text, source: event.source, idle: ctx.isIdle(), signal: !!ctx.signal }); if (event.text === 'consume without agent') return { action: 'handled' }; });
  } });
  const gate = hold(); let prompt;
  try {
    b.session.setActiveToolsByName(['Agent']);
    if (first === 'extension') {
      b.setGate(gate); prompt = command(b, 'user run to cancel'); await until(() => b.calls.length === 1);
      await b.session.abort(); gate.release(); await prompt; assert.equal(info(b).stopped, true);
    }
    const eventsBefore = b.events.length, callsBefore = b.calls.length;
    if (first === 'interactive') await command(b, 'consume without agent'); else await b.session.sendUserMessage('consume without agent');
    assert.equal(b.session.isIdle, true); assert.equal(b.calls.length, callsBefore);
    assert.ok(!b.events.slice(eventsBefore).some(e => ['agent_start', 'agent_settled'].includes(e)));
    // Cross has seen preflight, but there was no SDK run or settled callback.
    assert.equal(observed.at(-1).source, first); assert.equal(observed.at(-1).idle, true); assert.equal(observed.at(-1).signal, false);
    b.setToolCall(call('Agent'));
    if (first === 'interactive') await b.session.sendUserMessage('independent extension request'); else await command(b, 'new genuine user WITHOUT resume');
    console.log(JSON.stringify({ evidence: 'F2 actual downstream handled', first, observed, executions: count.executions, result: result(b, 'Agent'), calls: b.calls.length, stopped: info(b).stopped }));
    assert.equal(count.executions, first === 'interactive' ? 0 : 1);
    if (first === 'interactive') { assert.equal(result(b, 'Agent')?.isError, true); assert.ok(JSON.stringify(result(b, 'Agent').content).includes(reason)); }
    assert.equal(info(b).stopped, true); await assert.rejects(send(a, b, 'peer still refused'), e => e.code === 'stopped');
    assert.equal(b.calls.length, first === 'interactive' ? 2 : 3); assert.deepEqual(b.errors, []);
  } finally { gate.release(); await b.session.abort(); await prompt?.catch(() => {}); await b.close(); await a.close(); }
});

test('second authority scope: closed peer/unknown/cancelled continuations gated; idle/active/owned preflight cannot be relabelled; managed and never-bound unaffected', async () => {
  const a = await component('scope-sender'), b = await component('scope-host');
  const sensitive = [['Agent', {}], ['send_subagent', {}], ['mesh', { action: 'resume' }], ['mesh_control', { action: 'grow' }]];
  async function blocked() { for (const [toolName, input] of sensitive) assert.deepEqual(await b.emit('tool_call', { toolName, input }), { block: true, reason }); }
  try {
    await wire(a, b, message('owned preflight'));
    await b.emit('input', { source: 'interactive' }); await blocked(); // isIdle=true but Cross submission owns preflight.
    await b.busy('interactive'); await blocked(); // active peer run cannot be relabelled.
    await b.emit('session_shutdown');
    await b.emit('input', { source: 'interactive' }); await blocked();
    await b.emit('before_agent_start'); await b.emit('agent_start'); await blocked();
    await b.settled();
    await b.busy('extension'); await blocked(); await b.settled();
    b.ctx.signal = new AbortController().signal; await b.emit('input', { source: 'interactive' }); await blocked();
    b.ctx.signal = undefined; b.setIdle(false); await b.emit('input', { source: 'interactive' }); await blocked(); b.setIdle(true);
    await b.busy(); assert.equal(await b.emit('tool_call', { toolName: 'Agent', input: {} }), undefined);
    b.controller.abort(); await b.emit('agent_start'); await blocked(); await b.settled(false);
    // Rebinding the SAME extension object must not reuse previous Session user authority.
    await b.emit('session_start'); await b.busy();
    b.ctx.sessionManager.getSessionId = () => 'replacement-fixture-session'; await b.emit('session_start');
    await b.emit('before_agent_start'); await b.emit('agent_start'); await blocked();
    const child = await component('scope-managed', { managed: true });
    try { await child.busy('extension'); assert.equal(await child.emit('tool_call', { toolName: 'Agent', input: {} }), undefined); assert.equal(child.peer, undefined); }
    finally { await child.close(); }
    // Same handlers with an unrelated/unbound Session must not inherit this Host's restrictions.
    const ctx = { ...b.ctx, sessionManager: { getSessionId: () => 'unrelated-never-bound' } };
    for (const h of b.ext.handlers.get('tool_call')) assert.equal(await h({ type: 'tool_call', toolName: 'Agent', input: {} }, ctx), undefined);
    // Registration rename is public before startup's final chmod await returns.
    // A peer submitted in that window must retain authority even if start fails.
    publicationFault = { hits: 0, gate: hold() }; let c;
    const starting = component('scope-publication');
    try {
      await until(() => publicationFault.hits === 1, 'real post-rename chmod callee');
      const peer = registrations().find(p => p.instanceId === publicationFault.peer.instanceId); assert.ok(peer);
      assert.equal((await wire(a, { peer }, message('peer before startup await finishes'))).status, 'submitted');
      publicationFault.gate.release(); c = await starting;
      assert.equal(c.calls.length, 1); assert.equal(c.peer, undefined); assert.ok(!fs.existsSync(peer.socketPath));
      await c.emit('agent_start');
      const results = [];
      for (const [toolName, input] of sensitive) results.push(await c.emit('tool_call', { toolName, input }));
      console.log(JSON.stringify({ evidence: 'published startup peer authority', hits: publicationFault.hits, submissions: c.calls.length, results }));
      assert.deepEqual(results, sensitive.map(() => ({ block: true, reason })));
    } finally { publicationFault.gate.release(); c ??= await starting; await c.close(); publicationFault = undefined; }
  } finally { await b.close(); await a.close(); }
});
