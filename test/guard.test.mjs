import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import http from 'node:http';
import https from 'node:https';
import dns from 'node:dns';
test('sandbox network-only guard: 12 denials before I/O; descendants inherit guard', () => {
  const cases = [() => http.get('http://fixture.invalid'), () => https.get('https://fixture.invalid'), () => fetch('http://fixture.invalid'), () => dns.lookup('fixture.invalid', () => {}), () => dns.resolve4('fixture.invalid', () => {}), () => dns.promises.resolveTxt('fixture.invalid'), () => new dns.Resolver().resolve6('fixture.invalid', () => {}), () => new dns.promises.Resolver().resolveMx('fixture.invalid'), () => new net.Socket().connect({ host: '127.0.0.1', port: 9 }), () => new net.Socket().connect({ path: '/tmp/foreign.sock' }), () => net.createServer().listen(0), () => net.createServer().listen('/tmp/foreign.sock')];
  for (const fn of cases) assert.throws(fn, /TEST_NETWORK_DENIED/);
  assert.match(process.env.NODE_OPTIONS, /network-guard.mjs/);
  assert.equal(process.env.NPM_CONFIG_UPDATE_NOTIFIER, 'false');
});
