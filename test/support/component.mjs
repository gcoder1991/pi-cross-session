// Component fixture: actual SDK extension loader + product + Unix IPC;
// ExtensionContext and sendMessage are controlled stubs (NOT real model execution).
import assert from 'node:assert/strict';
import net from 'node:net';
import { randomUUID } from 'node:crypto';
import { repo, createEventBus, registrations, sleep, until } from './sdk.mjs';
const { loadExtensions } = await import(repo + '/node_modules/@earendil-works/pi-coding-agent/dist/core/extensions/loader.js');
export async function component(name, options = {}) {
  const bus = createEventBus(), calls = [], notices = [];
  if (options.managed) bus.on('pi-mesh:runtime:identity:query', q => q.reply(Object.freeze({ version: 1, managed: true, agentId: name })));
  const { extensions, runtime, errors } = await loadExtensions([repo + '/extensions/cross-session.ts'], process.env.PI_CROSS_TEST_PRIVATE_ROOT, bus);
  assert.deepEqual(errors, []); const ext = extensions[0];
  runtime.flagValues.set('cross-session-rpc', !!options.rpc);
  runtime.flagValues.set('cross-session-inbound', options.inbound ?? 'accept');
  runtime.getSessionName = () => name;
  runtime.sendMessage = (message, opts) => { calls.push({ message, opts }); };
  const ctx = { cwd: process.env.PI_CROSS_TEST_PRIVATE_ROOT, mode: 'rpc', signal: undefined, isIdle: () => idle, sessionManager: { getSessionId: () => sessionId }, ui: { notify: (...args) => notices.push(args) } };
  let idle = true, sessionId = randomUUID();
  async function emit(type, value = {}) { let result; for (const h of ext.handlers.get(type) ?? []) result = await h({ type, ...value }, ctx); return result; }
  options.configure?.(ctx, bus, runtime);
  await emit('session_start');
  const x = { bus, runtime, ctx, ext, calls, notices, emit, peer: registrations().find(p => p.id === sessionId),
    command: (name, args = '') => ext.commands.get(name).handler(args, ctx),
    tool: (name, params) => ext.tools.get(name).definition.execute(randomUUID(), params, undefined, undefined, ctx),
    async busy(source = 'interactive') { await emit('input', { source }); idle = false; this.controller = new AbortController(); ctx.signal = this.controller.signal; await emit('agent_start'); },
    async settled(success = true) { if (success) await emit('message_end', { message: { role: 'assistant', stopReason: 'stop' } }); idle = true; await emit('agent_settled'); ctx.signal = undefined; await sleep(10); },
    setIdle(value) { idle = value; },
    async close() { await emit('session_shutdown'); runtime.invalidate(); },
  };
  return x;
}
export function hello(a, b, safe = true) { return { v: 1, type: 'hello', requestId: randomUUID(), token: b.peer.token, target: { id: b.peer.id, instanceId: b.peer.instanceId }, ...(a && { from: { id: a.peer.id, instanceId: a.peer.instanceId, token: a.peer.token } }), ...(safe && { capabilities: ['cancel-safe-queue-v1'] }) }; }
export function message(text = 'hello', overrides = {}) { return { v: 1, type: 'message', requestId: randomUUID(), messageId: randomUUID(), text, summary: 'fixture', sentAt: Date.now(), ...overrides }; }
export async function wire(a, b, frame = message(), options = {}) {
  const socket = net.createConnection({ path: b.peer.socketPath });
  const replies = []; let buffer = '';
  socket.setEncoding('utf8'); socket.on('error', () => {});
  socket.on('data', data => { buffer += data; let i; while ((i = buffer.indexOf('\n')) >= 0) { replies.push(JSON.parse(buffer.slice(0, i))); buffer = buffer.slice(i + 1); } });
  await new Promise((r, j) => { socket.once('connect', r); socket.once('error', j); });
  try {
    socket.write(JSON.stringify(options.hello ?? hello(a, b, options.safe !== false)) + '\n');
    await until(() => replies.length, 'hello');
    if (!replies[0].ok || frame === null) return replies[0];
    const payload = options.raw ?? Buffer.from(JSON.stringify(frame) + '\n');
    if (options.split) { for (let i = 0; i < payload.length; i += options.split) socket.write(payload.subarray(i, i + options.split)); }
    else socket.write(payload);
    await until(() => replies.length > 1 || socket.destroyed, 'wire response');
    return replies[1] ?? { status: 'connection_closed' };
  } finally { socket.destroy(); }
}
export function info(x) { const requestId = randomUUID(); let response; const off = x.bus.on(`cross-session:rpc:info:reply:${requestId}`, data => response = data); x.bus.emit('cross-session:rpc:info', { version: 1, requestId }); off(); return response; }
