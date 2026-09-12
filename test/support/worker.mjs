// Independent OS SDK Host fixture, not a managed child agent.
import { make, tool, until } from './sdk.mjs';
const x = await make(process.argv[2]);
const { token: _token, ...peer } = x.peer;
process.send({ type: 'ready', peer, pid: process.pid });
process.on('message', async command => {
  try {
    if (command.type === 'send') {
      const receipt = await tool(x, 'send_pi_message').execute('fixture-send', { target: command.target, message: command.text });
      process.send({ type: 'sent', details: receipt.details });
    } else if (command.type === 'inspect') {
      await until(() => x.calls.length === 1 && x.session.isIdle);
      process.send({ type: 'evidence', pid: process.pid, calls: x.calls, entries: x.session.sessionManager.getEntries(), errors: x.errors });
    } else if (command.type === 'close') {
      await x.close(); process.disconnect();
    } else if (command.type === 'eof') {
      // Exercise private-resource beforeExit fallback without SDK shutdown.
      process.disconnect();
    }
  } catch (error) { process.send({ type: 'failure', error: error.stack }); process.exitCode = 1; process.disconnect(); }
});
