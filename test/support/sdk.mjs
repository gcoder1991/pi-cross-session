import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, randomUUID } from 'node:crypto';
export const repo = fileURLToPath(new URL('../../', import.meta.url));
const sdkRoot = path.join(repo, 'node_modules/@earendil-works/pi-coding-agent');
export const sdk = await import(`${sdkRoot}/dist/index.js`);
const { createAssistantMessageEventStream } = await import(`${sdkRoot}/node_modules/@earendil-works/pi-ai/dist/index.js`);
const { emitSessionShutdownEvent } = await import(`${sdkRoot}/dist/core/extensions/runner.js`);
export const { createEventBus } = await import(`${sdkRoot}/dist/core/event-bus.js`);
export const agentDir = process.env.PI_CODING_AGENT_DIR;
assert.ok(agentDir.startsWith(process.env.PI_CROSS_TEST_PRIVATE_ROOT + '/'), 'isolated fixture required');
export const runtimeDir = `/tmp/pi-peers-${process.getuid()}-${createHash('sha256').update(agentDir).digest('hex').slice(0, 12)}`;
export const sleep = ms => new Promise(r => setTimeout(r, ms));
export async function until(fn, label = 'condition', ms = 7000) { const end = Date.now() + ms; while (Date.now() < end) { if (await fn()) return; await sleep(10); } throw Error('TIMEOUT: ' + label); }
export function hold() { let release; const promise = new Promise(r => release = r); return { promise, release }; }
export function registrations() { return fs.existsSync(agentDir + '/peers') ? fs.readdirSync(agentDir + '/peers').filter(f => /^[0-9a-f]{32}\.json$/.test(f)).map(f => JSON.parse(fs.readFileSync(agentDir + '/peers/' + f, 'utf8'))) : []; }
let totalCalls = 0;
export async function make(name, { inbound = 'accept', rpc = false, managed = false, extra, tools = [], retry = false } = {}) {
  const { ModelRuntime, SettingsManager, DefaultResourceLoader, createAgentSession, SessionManager } = sdk;
  const cwd = path.join(process.env.PI_CROSS_TEST_PRIVATE_ROOT, 'cwd'); fs.mkdirSync(cwd, { recursive: true });
  const runtime = await ModelRuntime.create({ authPath: agentDir + '/' + randomUUID() + '-auth.json', modelsPath: null, modelsStorePath: agentDir + '/' + randomUUID() + '-models.json', allowModelNetwork: false, refreshOnCreate: false });
  let nextGate, nextToolCall, nextError;
  const calls = [], errors = [], events = [];
  runtime.registerProvider('test-local', {
    name: 'Offline local fixture', baseUrl: 'file://fixture', apiKey: 'fixture', api: 'openai-completions',
    models: [{ id: 'only', name: 'Local', reasoning: false, input: ['text'], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 32000, maxTokens: 1024 }],
    streamSimple(model, request, options) {
      assert.ok(++totalCalls <= 100, 'hard local provider budget');
      const gate = nextGate; nextGate = undefined;
      const error = nextError; nextError = undefined;
      const row = { messages: structuredClone(request.messages), signal: options.signal, held: !!gate, completed: false, aborted: false }; calls.push(row);
      const stream = createAssistantMessageEventStream();
      const message = { role: 'assistant', content: [{ type: 'text', text: `LOCAL_${name}_${calls.length}_OK` }], api: model.api, provider: model.provider, model: model.id, usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, stopReason: 'stop', timestamp: Date.now() };
      if (nextToolCall) { message.content = [{ type: 'toolCall', id: 'fixture-call-' + totalCalls, ...nextToolCall }]; message.stopReason = 'toolUse'; nextToolCall = undefined; }
      if (error) { message.content = []; message.stopReason = 'error'; message.errorMessage = error; }
      let settled = false;
      const abort = () => { if (settled) return; settled = true; row.aborted = true; stream.push({ type: 'error', reason: 'aborted', error: { ...message, content: [], stopReason: 'aborted', errorMessage: 'fixture cancellation' } }); stream.end(); };
      options.signal?.addEventListener('abort', abort, { once: true });
      queueMicrotask(async () => { if (gate) await gate.promise; if (settled) return; if (options.signal?.aborted) return abort(); settled = true; row.completed = true; row.response = message; stream.push({ type: 'start', partial: { ...message, content: [] } }); if (error) stream.push({ type: 'error', reason: 'error', error: message }); else stream.push({ type: 'done', reason: message.stopReason, message }); stream.end(); options.signal?.removeEventListener('abort', abort); });
      return stream;
    },
  });
  const settings = SettingsManager.inMemory({ compaction: { enabled: false }, ...(retry ? {} : { retry: { enabled: false } }) });
  const bus = createEventBus();
  if (managed) bus.on('pi-mesh:runtime:identity:query', q => q.reply(Object.freeze({ version: 1, managed: true, agentId: name })));
  const loader = new DefaultResourceLoader({ cwd, agentDir, eventBus: bus, settingsManager: settings, noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true, systemPrompt: 'Offline fixture. No external tools.', additionalExtensionPaths: [path.join(repo, 'extensions/cross-session.ts')], extensionFactories: extra ? [{ name: 'fixture', factory: extra }] : [] });
  await loader.reload(); assert.deepEqual(loader.getExtensions().errors, []);
  loader.getExtensions().runtime.flagValues.set('cross-session-inbound', inbound);
  loader.getExtensions().runtime.flagValues.set('cross-session-rpc', rpc);
  const { session } = await createAgentSession({ cwd, agentDir, modelRuntime: runtime, model: runtime.getModel('test-local', 'only'), thinkingLevel: 'off', tools, resourceLoader: loader, sessionManager: SessionManager.inMemory(cwd), settingsManager: settings });
  session.setSessionName(name);
  session.subscribe(e => events.push(e.type));
  await session.bindExtensions({ mode: 'rpc', onError: e => errors.push(e) });
  session.setActiveToolsByName([]);
  const peer = registrations().find(p => p.id === session.sessionManager.getSessionId());
  assert.equal(!!peer, !managed);
  let closed = false;
  return {
    name, session, loader, bus, peer, calls, errors, events, setGate: gate => nextGate = gate, setToolCall: call => nextToolCall = call, setError: error => nextError = error,
    async close() { if (closed) return; closed = true; await session.abort(); await emitSessionShutdownEvent(session.extensionRunner, { type: 'session_shutdown', reason: 'quit' }); session.dispose(); assert.ok(!peer || !fs.existsSync(peer.socketPath)); assert.ok(!peer || !registrations().some(p => p.instanceId === peer.instanceId)); },
  };
}
export function tool(x, name) { return x.session.extensionRunner.getAllRegisteredTools().find(t => t.definition.name === name).definition; }
export async function send(a, b, text) { return tool(a, 'send_pi_message').execute(randomUUID(), { target: b.peer.instanceId, message: text }, undefined, undefined, a.session.extensionRunner.createContext()); }
export const hasPeer = (x, text) => x.session.agent.state.messages.some(m => m.role === 'custom' && m.details?.text === text);
export async function command(x, text) { await x.session.prompt(text, { source: 'interactive' }); }
