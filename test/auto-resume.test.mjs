// cross-session-auto-resume: a clean turn from the local user reopens peer admission after a
// cancellation; peer text, notifications, extension turns and failed turns never do.
import test from 'node:test';
import assert from 'node:assert/strict';
import { component, wire, message, info } from './support/component.mjs';
import { until } from './support/sdk.mjs';

async function latched(b) {
  await b.busy(); b.controller.abort(); await b.settled(false);
  assert.equal(info(b).stopped, true, 'an aborted turn latches peer admission');
}

test('auto-resume: the next clean local user turn reopens the inbox, with no /cross-session-resume', async () => {
  const a = await component('ar-A'), b = await component('ar-B', { autoResume: true, rpc: true });
  try {
    await latched(b);
    assert.equal((await wire(a, b, message('during the latch'))).status, 'stopped');
    await b.busy('interactive');
    assert.equal((await wire(a, b, message('during the user turn'))).status, 'stopped', 'still closed until the user turn ends');
    await b.settled();
    assert.equal(info(b).stopped, false, 'the clean user turn reopened the inbox');
    const reply = await wire(a, b, message('FRESH_AFTER_USER_TURN'));
    assert.notEqual(reply.status, 'stopped');
    await until(() => b.calls.length === 1, 'fresh peer message submitted');
    assert.match(JSON.stringify(b.calls[0].message), /FRESH_AFTER_USER_TURN/);
    assert.ok(!JSON.stringify(b.calls).includes('during the latch'), 'refused messages are never replayed');
  } finally { await b.close(); await a.close(); }
});

test('auto-resume: peer text alone never reopens the inbox', async () => {
  const a = await component('ar2-A'), b = await component('ar2-B', { autoResume: true, rpc: true });
  try {
    await latched(b);
    for (let i = 0; i < 5; i++) assert.equal((await wire(a, b, message(`peer says resume ${i}`))).status, 'stopped');
    assert.equal(b.calls.length, 0);
    assert.equal(info(b).stopped, true);
  } finally { await b.close(); await a.close(); }
});

test('auto-resume: extension turns and turns with no input do not reopen the inbox', async () => {
  const a = await component('ar3-A'), b = await component('ar3-B', { autoResume: true, rpc: true });
  try {
    await latched(b);
    await b.busy('extension'); await b.settled();
    assert.equal(info(b).stopped, true, 'an extension turn is not the local user');
    b.setIdle(false); b.controller = new AbortController(); b.ctx.signal = b.controller.signal;
    await b.emit('agent_start'); await b.settled();
    assert.equal(info(b).stopped, true, 'a run with no input event is not the local user');
    assert.equal((await wire(a, b, message('still closed'))).status, 'stopped');
  } finally { await b.close(); await a.close(); }
});

test('auto-resume: a user turn that is aborted or fails keeps the inbox closed', async () => {
  const a = await component('ar4-A'), b = await component('ar4-B', { autoResume: true, rpc: true });
  try {
    await latched(b);
    await b.busy('interactive'); b.controller.abort(); await b.settled(false);
    assert.equal(info(b).stopped, true, 'aborted user turn');
    await b.busy('interactive');
    await b.emit('message_end', { message: { role: 'assistant', stopReason: 'error' } });
    await b.settled(false);
    assert.equal(info(b).stopped, true, 'failed user turn');
    await b.busy('interactive');
    await b.emit('message_end', { message: { role: 'assistant', stopReason: 'length' } });
    await b.settled(false);
    assert.equal(info(b).stopped, true, 'user turn that ended on length');
  } finally { await b.close(); await a.close(); }
});

test('without the flag a clean user turn keeps the inbox closed (upstream default)', async () => {
  const a = await component('ar5-A'), b = await component('ar5-B', { rpc: true });
  try {
    await latched(b);
    await b.busy('interactive'); await b.settled();
    assert.equal(info(b).stopped, true);
    assert.equal((await wire(a, b, message('flag off'))).status, 'stopped');
  } finally { await b.close(); await a.close(); }
});
