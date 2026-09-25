import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
const sdkRequire = createRequire(import.meta.resolve('@earendil-works/pi-coding-agent'));
const { createJiti } = sdkRequire('jiti');
const { socketPathFor } = await createJiti(import.meta.url).import('../lib/ipc-path.ts');
const id = 'f'.repeat(32), ns = 'abc123456789';

test('win32: preserve 2.2.0 named pipes regardless of launch username environment', () => {
  const old = { USERNAME: process.env.USERNAME, USER: process.env.USER };
  const expected = `\\\\.\\pipe\\pi-peer-${ns}-${id}`;
  try {
    for (const [USERNAME, USER] of [['Alice', 'alice-shell'], [undefined, 'alice-shell'], ['', 'alice-shell'], [undefined, undefined], ['用户', 'changed']]) {
      for (const [key, value] of Object.entries({ USERNAME, USER })) {
        if (value === undefined) delete process.env[key]; else process.env[key] = value;
      }
      assert.equal(socketPathFor('win32', '/unused', ns, id), expected);
    }
    assert.notEqual(socketPathFor('win32', '', ns, 'e'.repeat(32)), expected, 'instance ID isolates incarnations');
    assert.notEqual(socketPathFor('win32', '', 'different', id), expected, 'agent directory namespace is retained');
  } finally {
    for (const [key, value] of Object.entries(old)) {
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    }
  }
});

test('posix: explicit platform uses POSIX separators even on a Windows test runner', () => {
  assert.equal(socketPathFor('darwin', `/tmp/pi-peers-501-${ns}`, ns, id), `/tmp/pi-peers-501-${ns}/${id}.sock`);
  assert.equal(socketPathFor('linux', `/tmp/pi-peers-0-${ns}`, ns, id), `/tmp/pi-peers-0-${ns}/${id}.sock`);
  assert.equal(socketPathFor('linux', '/tmp/x', ns, id), `/tmp/x/${id}.sock`);
});
