import test from 'node:test';
import assert from 'node:assert/strict';
import { Type } from 'typebox';
import { make, send, hold, until, sleep, hasPeer, command } from './support/sdk.mjs';
import { info } from './support/component.mjs';

test('review real SDK DEFAULT retry: abort in actual backoff drops two queued peers without replay', { timeout: 15000 }, async () => {
  const a = await make('retry-abort-A'), b = await make('retry-abort-B', { retry: true, rpc: true });
  const gate = hold(); b.setGate(gate); b.setError('503 overloaded fixture'); let prompt;
  try {
    assert.equal(b.session.autoRetryEnabled, true);
    prompt = command(b, 'real user starts erroring request');
    await until(() => b.calls.length === 1);
    for (const text of ['DROP_RETRY_ONE', 'DROP_RETRY_TWO']) assert.equal((await send(a, b, text)).details.status, 'queued');
    gate.release(); await until(() => b.events.includes('auto_retry_start') && b.session.isRetrying, 'actual SDK retry backoff');
    await b.session.abort(); await prompt; await sleep(150);
    console.log(JSON.stringify({ evidence: 'default retry backoff abort', callsAfterAbort: b.calls.length, oldSignalAborted: b.calls[0].signal.aborted, states: info(b).states.map(s => s.state) }));
    assert.equal(b.calls[0].signal.aborted, false, 'SDK old agent signal is NOT the retry controller');
    assert.equal(b.calls.length, 1, 'zero automatic successor provider calls');
    assert.ok(info(b).stopped); assert.deepEqual(info(b).states.map(s => s.state), ['dropped_cancelled', 'dropped_cancelled']);
    await command(b, 'new genuine user WITHOUT reopen');
    await assert.rejects(send(a, b, 'still closed'), e => e.code === 'stopped');
    await command(b, '/cross-session-resume'); await command(b, 'another user after reopen'); await sleep(100);
    assert.equal(b.calls.length, 3);
    for (const text of ['DROP_RETRY_ONE', 'DROP_RETRY_TWO']) { assert.equal(hasPeer(b, text), false); assert.ok(!JSON.stringify(b.calls.slice(1)).includes(text)); }
    assert.deepEqual(b.errors, []);
    console.log(JSON.stringify({ evidence: 'retry abort no replay with user/reopen', calls: b.calls.length, errors: 1, successors: 0 }));
  } finally { gate.release(); await b.session.abort(); await prompt?.catch(() => {}); await b.close(); await a.close(); }
});

for (const name of ['Agent', 'send_subagent']) {
  const parameters = name === 'Agent' ? {} : { agent_id: 'inert-fixture', message: 'fixture direction' };
  function fixture(counter) { return pi => pi.registerTool({ name, label: 'Inert fixture', description: 'No children or actual Agent execution', parameters: name === 'Agent' ? Type.Object({}) : Type.Object({ agent_id: Type.String(), message: Type.String() }), async execute() { counter.executions++; return { content: [{ type: 'text', text: 'inert fixture executed' }] }; } }); }
  test(`review real SDK DEFAULT retry: peer ${name} remains explicitly blocked; genuine user executes`, { timeout: 15000 }, async () => {
    const count = { executions: 0 }, a = await make('retry-tool-A'), b = await make('retry-tool-B', { retry: true, tools: [name], extra: fixture(count) });
    try {
      b.session.setActiveToolsByName([name]); b.setError('503 overloaded fixture');
      await send(a, b, 'peer asks for authority across retry');
      await until(() => b.events.includes('auto_retry_start') && b.session.isRetrying, 'SDK retry');
      b.setToolCall({ name, arguments: parameters });
      await until(() => b.calls.length === 3 && b.session.isIdle, 'retry tool completion');
      const result = b.session.agent.state.messages.find(m => m.role === 'toolResult' && m.toolName === name);
      console.log(JSON.stringify({ evidence: 'retry peer tool', name, executions: count.executions, result }));
      assert.equal(count.executions, 0); assert.equal(result?.isError, true); assert.match(JSON.stringify(result.content), /Peer-only\/cancelled turn cannot authorize/);
      b.setToolCall({ name, arguments: parameters }); await command(b, 'Explicit genuine user fixture request');
      assert.equal(count.executions, 1); assert.equal(b.calls.length, 5); assert.deepEqual(b.errors, []);
      console.log(JSON.stringify({ evidence: 'retry tool control', name, calls: 5, peerExecutions: 0, userExecutions: 1 }));
    } finally { await b.close(); await a.close(); }
  });
  test(`review real SDK: abort latch stays inbound-only; new user ${name} works WITHOUT resume`, { timeout: 15000 }, async () => {
    const count = { executions: 0 }, a = await make('stopped-tool-A'), b = await make('stopped-tool-B', { rpc: true, tools: [name], extra: fixture(count) });
    const gate = hold(); b.setGate(gate); let prompt;
    try {
      b.session.setActiveToolsByName([name]); prompt = command(b, 'user busy'); await until(() => b.calls.length === 1);
      assert.equal((await send(a, b, 'drop cancelled queued peer')).details.status, 'queued');
      await b.session.abort(); gate.release(); await prompt;
      assert.ok(info(b).stopped);
      b.setToolCall({ name, arguments: parameters }); await command(b, 'new genuine user fixture request, do not reopen inbound');
      console.log(JSON.stringify({ evidence: 'inbound stop vs user authority', name, executions: count.executions, calls: b.calls.length, stopped: info(b).stopped }));
      assert.equal(count.executions, 1); assert.equal(b.calls.length, 3); assert.ok(info(b).stopped);
      assert.equal(hasPeer(b, 'drop cancelled queued peer'), false);
      await assert.rejects(send(a, b, 'peer still refused'), e => e.code === 'stopped');
      assert.deepEqual(b.errors, []);
    } finally { gate.release(); await b.session.abort(); await prompt?.catch(() => {}); await b.close(); await a.close(); }
  });
}

test('review real SDK DEFAULT retry success: terminal stop evidence flushes two peers one-at-a-time', { timeout: 15000 }, async () => {
  const a = await make('retry-success-A'), b = await make('retry-success-B', { retry: true, rpc: true });
  const first = hold(), retry = hold(); b.setGate(first); b.setError('503 overloaded fixture'); let prompt;
  try {
    prompt = command(b, 'user request will retry successfully'); await until(() => b.calls.length === 1);
    for (const text of ['RETRY_SUCCESS_ONE', 'RETRY_SUCCESS_TWO']) assert.equal((await send(a, b, text)).details.status, 'queued');
    first.release(); await until(() => b.events.includes('auto_retry_start') && b.session.isRetrying);
    b.setGate(retry); await until(() => b.calls.length === 2);
    assert.deepEqual(info(b).states.map(s => s.state), ['queued', 'queued']); assert.equal(b.session.agent.hasQueuedMessages(), false);
    retry.release(); await prompt; await until(() => b.calls.length === 4 && b.session.isIdle);
    assert.deepEqual(b.calls.map(c => c.response.stopReason), ['error', 'stop', 'stop', 'stop']);
    assert.deepEqual(info(b).states.map(s => s.state), ['submitted', 'submitted']); assert.equal(info(b).stopped, false);
    assert.deepEqual(b.errors, []);
    console.log(JSON.stringify({ evidence: 'successful retry followed by serial extension queue', calls: 4, errors: 1, successfulResponses: 3 }));
  } finally { first.release(); retry.release(); await b.session.abort(); await prompt?.catch(() => {}); await b.close(); await a.close(); }
});

test('review real SDK known send_subagent: direct peer blocked without retry; genuine user positive control', async () => {
  let executions = 0;
  const a = await make('known-entry-A'), b = await make('known-entry-B', { tools: ['send_subagent'], extra: pi => pi.registerTool({
    name: 'send_subagent', label: 'Inert current Mesh entry', description: 'Fixture does not contact any agent', parameters: Type.Object({ agent_id: Type.String(), message: Type.String() }),
    async execute() { executions++; return { content: [{ type: 'text', text: 'inert execution' }] }; },
  }) });
  const call = { name: 'send_subagent', arguments: { agent_id: 'fixture', message: 'fixture-only' } };
  try {
    b.session.setActiveToolsByName(['send_subagent']); b.setToolCall(call); await send(a, b, 'peer attempts terminal continuation');
    await until(() => b.calls.length === 2 && b.session.isIdle);
    assert.equal(executions, 0);
    const result = b.session.agent.state.messages.find(m => m.role === 'toolResult' && m.toolName === 'send_subagent');
    assert.equal(result?.isError, true); assert.match(JSON.stringify(result.content), /Peer-only\/cancelled turn cannot authorize/);
    b.setToolCall(call); await command(b, 'genuine user explicitly asks fixture');
    assert.equal(executions, 1); assert.equal(b.calls.length, 4); assert.deepEqual(b.errors, []);
    console.log(JSON.stringify({ evidence: 'known send_subagent no retry', calls: 4, peerExecutions: 0, userExecutions: 1 }));
  } finally { await b.close(); await a.close(); }
});
