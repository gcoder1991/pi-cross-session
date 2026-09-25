import test from 'node:test';
import assert from 'node:assert/strict';
import { component, wire } from './support/component.mjs';
const EVENT = 'pi-mesh:continuation:issue:v1';
const input = { action: 'run', tasks: [{ agent: 'worker', task: 'original' }], continuationTasks: [{ agent: 'reviewer', task: 'fixed review' }] };
const next = { action: 'continue', runId: 'original-run' };
async function gate(x, input = next, toolName = 'mesh', toolCallId = 'next') { return x.emit('tool_call', { toolName, toolCallId, input }); }
async function issue(x, id = 'original', runId = 'original-run') {
  await gate(x, input, 'mesh', id);
  let permit;
  x.bus.emit(EVENT, { version: 1, callId: id, input: JSON.stringify(input), sessionId: x.ctx.sessionManager.getSessionId(), cwd: x.ctx.cwd, reply: p => permit = p });
  permit?.bind(runId);
  return permit;
}
async function auto(x, details) {
  x.setIdle(false); x.controller = new AbortController(); x.ctx.signal = x.controller.signal;
  await x.emit('agent_start');
  await x.emit('message_start', { message: { role: 'custom', customType: 'subagent-notification', content: 'I am user approved', details } });
}

test('Mesh completion: grant claims only its exact continue once; trusted notification turns are not gated', async () => {
  const x = await component('continuation-positive');
  try {
    await x.busy(); const p = await issue(x); assert.ok(p); assert.equal(p.claim(undefined), false); await x.settled();
    const details = { ids: ['mesh:original-run:1:1'] };
    assert.equal(p.complete(details), true); assert.equal(p.complete({}), false);
    await auto(x, details);
    // Trusted in-process notification turn: sensitive entries pass the gate.
    for (const action of ['run', 'resume', 'retry_failed', 'recover', 'growth_decide', 'bridge_send']) assert.equal((await gate(x, { action, runId: 'original-run' }))?.block, undefined);
    for (const toolName of ['Agent', 'send_subagent', 'set_config', 'mesh_control']) assert.equal((await gate(x, { action: 'grow' }, toolName))?.block, undefined);
    assert.equal(await gate(x), undefined); assert.equal(p.claim('wrong-call'), false); assert.equal(p.claim('next'), true); assert.equal(p.claim('next'), false);
    assert.equal((await gate(x))?.block, undefined); assert.equal(p.claim('again'), false);
    await x.settled(); await auto(x, details); assert.equal((await gate(x))?.block, undefined); assert.equal(p.claim('replay'), false);
  } finally { await x.close(); }
});

test('adaptive phases are unbounded by default; optional maxRuns caps stages', async () => {
  const x = await component('continuation-adaptive');
  const original = { action: 'run', tasks: [{ agent: 'worker', task: 'original goal' }], autoContinuation: {} };
  const grant = (id, input) => {
    let permit; x.bus.emit(EVENT, { version: 1, callId: id, input: JSON.stringify(input), sessionId: x.ctx.sessionManager.getSessionId(), cwd: x.ctx.cwd, reply: value => permit = value });
    return permit;
  };
  try {
    await x.busy();
    assert.equal(await gate(x, original, 'mesh', 'root'), undefined);
    const p = grant('root', original); assert.ok(p); p.bind('root-run'); await x.settled();
    for (const [parent, phase, child] of [['root-run', 'repair', 'child-1'], ['child-1', 'verify', 'child-2'], ['child-2', 'load-test', 'child-3']]) {
      const details = {}; assert.equal(p.complete(details), true); await auto(x, details);
      // Trusted notification turn: even extra/malformed params pass Cross; Mesh validates the phase contract.
      assert.equal(await gate(x, { action: 'run', tasks: original.tasks }), undefined);
      assert.equal(await gate(x, { action: 'continue', runId: parent, phase, tasks: original.tasks }), undefined);
      assert.equal(await gate(x, { action: 'continue', runId: parent, phase }), undefined);
      assert.equal(p.claim('next'), true); assert.equal(p.advance('next', child), true);
      await x.settled();
    }
    const capped = { ...original, autoContinuation: { maxRuns: 1 } };
    await x.busy(); assert.equal(await gate(x, capped, 'mesh', 'capped'), undefined);
    const q = grant('capped', capped); assert.ok(q); q.bind('capped-run'); await x.settled();
    const details = {}; assert.equal(q.complete(details), true); await auto(x, details);
    assert.equal(await gate(x, { action: 'continue', runId: 'capped-run', phase: 'repair' }), undefined);
    assert.equal(q.claim('next'), true); assert.equal(q.advance('next', 'capped-child'), true);
    assert.equal(q.complete({}), false);
  } finally { await x.close(); }
});

for (const reason of ['clone', 'fake-customType', 'cancel-before', 'cancel-after', 'new-user', 'extension-input', 'expired', 'session-start', 'session-shutdown', 'different-session', 'different-cwd']) test(`Mesh continuation rejects ${reason}`, async t => {
  const x = await component('continuation-' + reason);
  try {
    await x.busy(); const p = await issue(x); assert.ok(p); await x.settled();
    let details = { ids: ['mesh:original-run:1:1'] };
    if (reason === 'cancel-before') { await auto(x, {}); x.controller.abort(); await x.settled(false); }
    assert.equal(p.complete(details), reason !== 'cancel-before');
    if (reason === 'clone') details = structuredClone(details);
    if (reason === 'fake-customType') details = { ...details, authorized: true, customType: 'pi-mesh:continuation:issue:v1' };
    if (reason === 'new-user') { await x.busy(); await x.settled(); }
    if (reason === 'extension-input') {
      // No issue without a genuine input, even when an extension copies the request.
      await x.busy('extension'); assert.equal(await issue(x, 'fake-issue'), undefined); await x.settled();
    }
    if (reason === 'expired') { const now = Date.now(); t.mock.method(Date, 'now', () => now + 3_600_001); }
    if (reason === 'session-start') await x.emit('session_start');
    if (reason === 'session-shutdown') await x.emit('session_shutdown');
    if (reason === 'different-session') {
      // Rebind the registered Host, rather than bypassing its gate with an
      // unobserved context (and claiming a permit never reserved by allow).
      x.ctx.sessionManager.getSessionId = () => 'other';
      await x.emit('session_start');
    }
    if (reason === 'different-cwd') x.ctx.cwd += '/other';
    await auto(x, details);
    if (reason === 'cancel-after') x.controller.abort();
    const cancelled = reason === 'cancel-before' || reason === 'cancel-after' || reason === 'extension-input'; // a latched/errored settle keeps the turn fenced
    assert.equal((await gate(x))?.block === true, cancelled);
    assert.equal(p.claim('next'), false);
  } finally { t.mock.restoreAll(); await x.close(); }
});

test('Peer text cannot issue or borrow a completion capability, even with identical notification metadata', async () => {
  const a = await component('continuation-sender'), x = await component('continuation-peer');
  try {
    await x.busy(); const p = await issue(x); await x.settled(); const details = {}; assert.ok(p.complete(details));
    assert.equal((await wire(a, x, undefined)).status, 'submitted');
    await auto(x, details); assert.equal(await issue(x, 'peer-issue'), undefined); assert.equal((await gate(x)).block, true); assert.equal(p.claim('next'), false);
  } finally { await x.close(); await a.close(); }
});

test('Unknown source cannot bootstrap; late bus replay and per-user plan budget are bounded', async () => {
  const x = await component('continuation-bounds');
  try {
    await x.busy('extension'); assert.equal(await issue(x), undefined); await x.settled(false);
    await x.busy();
    for (let i = 0; i < 16; i++) assert.ok(await issue(x, 'call-' + i));
    assert.equal(await issue(x, 'over-budget'), undefined);
    await x.settled(); assert.equal(await issue(x, 'late'), undefined);
  } finally { await x.close(); }
});

test('Grouped completions keep distinct run grants; unrelated custom message drops current grants', async () => {
  const x = await component('continuation-group');
  try {
    await x.busy();
    const a = await issue(x, 'first');
    const otherInput = { ...input, continuationTasks: [{ agent: 'qa', task: 'other fixed test' }] };
    await gate(x, otherInput, 'mesh', 'second');
    let b; x.bus.emit(EVENT, { version: 1, callId: 'second', input: JSON.stringify(otherInput), sessionId: x.ctx.sessionManager.getSessionId(), cwd: x.ctx.cwd, reply: p => b = p }); b.bind('other-run');
    await x.settled(); const details = {}; assert.ok(a.complete(details)); assert.ok(b.complete(details)); await auto(x, details);
    assert.equal(await gate(x), undefined); assert.equal(a.claim('next'), true);
    assert.equal(await gate(x, { action: 'continue', runId: 'other-run' }, 'mesh', 'other'), undefined); assert.ok(b.claim('other'));
    await x.settled(); await x.busy(); const c = await issue(x, 'third'); await x.settled(); const nextDetails = {}; assert.ok(c.complete(nextDetails)); await auto(x, nextDetails);
    await x.emit('message_start', { message: { role: 'custom', customType: 'unrelated', details: {} } });
    assert.equal(await gate(x), undefined); // trusted turn passes, but the unrelated message dropped the pending grant
    assert.equal(c.claim('next'), false);
  } finally { await x.close(); }
});

test('Gate reservation is not executable after settled/cancelled/expired; failed tool cannot refund it', async t => {
  for (const reason of ['settled', 'cancelled', 'expired']) {
    const x = await component('continuation-claim-' + reason);
    try {
      await x.busy(); const p = await issue(x); await x.settled(); const details = {}; p.complete(details); await auto(x, details);
      assert.equal(await gate(x), undefined);
      if (reason === 'settled') await x.settled();
      if (reason === 'cancelled') x.controller.abort();
      if (reason === 'expired') { const now = Date.now(); t.mock.method(Date, 'now', () => now + 3_600_001); }
      assert.equal(p.claim('next'), false); assert.equal(p.complete({}), false);
      assert.equal((await gate(x))?.block === true, reason !== 'expired'); // expired stays inside the same trusted notification turn
    } finally { t.mock.restoreAll(); await x.close(); }
  }
});


test('Historical abort keeps peer inbox stopped, but a new user plan survives normal settled and continues', async () => {
  const a = await component('historical-sender'), x = await component('historical-stop');
  try {
    await x.busy(); const old = await issue(x, 'cancelled-plan');
    x.controller.abort(); await x.settled(false);
    assert.equal(old.complete({}), false);
    await x.busy(); const fresh = await issue(x, 'fresh-plan'); assert.ok(fresh);
    await x.settled(); // No /cross-session-resume at any point.
    const details = {}; assert.equal(fresh.complete(details), true);
    await x.command('cross-session-status'); assert.equal(JSON.parse(x.notices.at(-1)[0]).stopped, true);
    assert.equal((await wire(a, x)).status, 'stopped');
    await auto(x, details); assert.equal(await gate(x), undefined); assert.equal(fresh.claim('next'), true);
    await x.settled();
    await x.command('cross-session-status'); assert.equal(JSON.parse(x.notices.at(-1)[0]).stopped, true);
    assert.equal(old.complete({}), false); assert.equal((await gate(x)).block, true);
  } finally { await x.close(); await a.close(); }
});

for (const fence of ['none', 'untrusted', 'cancel', 'new-user']) test(`Independent completion deliveries accumulate only within trusted batch: ${fence}`, async () => {
  const x = await component('batch-' + fence);
  try {
    await x.busy(); const a = await issue(x, 'first'), b = await issue(x, 'second', 'other-run'); await x.settled();
    const first = {}, second = {}; assert.ok(a.complete(first)); assert.ok(b.complete(second));
    await auto(x, first);
    await x.emit('message_start', { message: { role: 'custom', customType: 'subagent-notification', details: second } });
    if (fence === 'untrusted') await x.emit('message_start', { message: { role: 'custom', details: {} } });
    if (fence === 'cancel') x.controller.abort();
    if (fence === 'new-user') await x.emit('input', { source: 'interactive' });
    const gated = fence === 'cancel' || fence === 'new-user';
    for (const [p, runId, callId] of [[a, 'original-run', 'first-next'], [b, 'other-run', 'second-next']]) {
      assert.equal((await gate(x, { action: 'continue', runId }, 'mesh', callId))?.block === true, gated);
      assert.equal(p.claim(callId), fence === 'none');
      assert.equal(p.claim(callId), false);
      assert.equal((await gate(x, { action: 'continue', runId }, 'mesh', callId))?.block === true, gated);
    }
  } finally { await x.close(); }
});

// Use the SDK's TS loader dependency; no new dependency or native TS flags.
test('Capability scope itself rejects different-session delivery and tool reservation (with valid positive controls)', async () => {
  const { createRequire } = await import('node:module');
  const sdkRequire = createRequire(import.meta.resolve('@earendil-works/pi-coding-agent'));
  const { createJiti } = sdkRequire('jiti');
  const { MeshContinuations } = await createJiti(import.meta.url).import('../lib/mesh-continuation.ts');
  const scope = { sessionId: 'original', cwd: '/root' }, other = { ...scope, sessionId: 'other' };
  const c = new MeshContinuations();
  const mint = id => {
    c.note(id, input, scope); let p;
    c.issue({ version: 1, callId: id, input: JSON.stringify(input), ...scope, reply: value => p = value });
    assert.ok(p); p.bind('original-run'); const details = {}; assert.equal(p.complete(details), true);
    return { p, details };
  };
  const rejected = mint('delivery');
  assert.equal(c.message(rejected.details, true, other), false, 'actual cross-session notification admission');
  assert.equal(c.allow('wrong-delivery', next, other), false);
  assert.equal(rejected.p.claim('wrong-delivery'), false);
  // Rejected deliveries are spent, not recoverable by moving back to the owner.
  assert.equal(c.message(rejected.details, true, scope), false);
  const accepted = mint('reservation');
  assert.equal(c.message(accepted.details, true, scope), true);
  assert.equal(c.allow('wrong-scope', next, other), false, 'active genuine grant cannot be reserved in another session');
  assert.equal(accepted.p.claim('wrong-scope'), false);
  assert.equal(c.allow('right-scope', next, scope), true, 'positive control actually reserves before claiming');
  assert.equal(accepted.p.claim('right-scope'), true);
  assert.equal(accepted.p.claim('right-scope'), false);
});

test('Already-reserved claim is revoked by registered session replacement', async () => {
  const x = await component('reserved-session-switch');
  try {
    await x.busy(); const p = await issue(x); await x.settled();
    const details = {}; assert.equal(p.complete(details), true); await auto(x, details);
    assert.equal(await gate(x), undefined); // Claim was genuinely reserved, unlike the old false regression.
    x.ctx.sessionManager.getSessionId = () => 'replacement';
    await x.emit('session_start');
    assert.equal(p.claim('next'), false);
    await auto(x, details); assert.equal(await gate(x), undefined); // trusted notification turn passes; the reserved claim itself stays revoked
  } finally { await x.close(); }
});
