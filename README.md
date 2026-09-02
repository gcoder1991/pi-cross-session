# pi-cross-session

Pi ↔ Pi same-machine cross-session messaging. Lets independent [pi](https://www.npmjs.com/package/@earendil-works/pi-coding-agent) coding-agent sessions on one machine discover each other and exchange plain-text peer messages over per-instance IPC (Unix domain socket on macOS/Linux, named pipe on Windows) — no daemon, no database, zero runtime dependencies beyond Node's standard library.

Inspired by Claude Code's cross-session messaging; copies its product semantics (accept/refuse inbound policy, `name [ref]` disambiguation, collapsed message UI, "a peer message is never user approval"), not its wire protocol.

## Install

```bash
pi install npm:pi-cross-session
```

Project-local instead of global:

```bash
pi install -l npm:pi-cross-session
```

Try it without installing:

```bash
pi -e npm:pi-cross-session
```

## Usage

Each running Pi session automatically registers itself and listens on a private IPC endpoint.

| Entry point | Kind | What it does |
|---|---|---|
| `/peers`, `/list-pi` | command | List other live Pi sessions (name, busy/idle, cwd, ref) |
| `list_pi` | tool | Same listing, structured output for the model |
| `send_message` | tool | Send plain text by exact name, session id, runtime id, or `name [ref]` |
| `--cross-session-inbound=accept\|refuse` | flag | Control inbound messages (default `accept`; invalid values fall back to `refuse`) |

Just talk to Pi: *"tell the backend session the order API moved to /v2"* — the model calls `send_message` itself. On the receiving side, messages are injected with `deliverAs: "steer"` + `triggerTurn: true`: busy sessions get them between tool calls without interruption, idle sessions start a new turn. Messages render as a single folded line; `Ctrl+O` expands.

```
› Message from @frontend: API 升版通知 (Ctrl+O to expand)
```

## How it works

- **Discovery plane**: each runtime instance writes a `0600` JSON file under `~/.pi/agent/peers/<instanceId>.json` (name, status, cwd, pid, socket path, bearer token). Refreshed every 30s; removed on shutdown.
- **Data plane**: each instance owns a `0600` Unix socket (named pipe on Windows) under a `0700` per-UID runtime directory, namespace-isolated by a hash of the agent dir.
- **Protocol**: two-phase JSONL — `hello` (mutual bearer-token authentication, timing-safe comparison) → one `message` frame → `submitted` receipt. Paths are built only from random instance IDs; persistent session IDs never enter any path.
- **Admission control**: 1 MiB frame cap, 5s send timeout, 30s first-line deadline, 30-cap/0.5-per-second token bucket, 30s duplicate window, 50-message pending queue. Rate/dedup state is transactional — a rejected or failed injection never consumes quota.
- **Lifecycle**: registration on `session_start`, busy/idle tracking, dead-endpoint reaping (only when the PID is confirmed dead), full cleanup on `session_shutdown`.

## Security model

The trust boundary is the OS user. Bearer tokens, file modes, owner checks, and symlink rejection defend against other OS users, stale endpoints, path substitution, and accidental clients. A malicious process **running as the same UID** can read registration tokens and impersonate a sender — this is an explicit, documented boundary, not an oversight.

Messages from peers are plain text from another agent, never user intent: they cannot grant permissions, approve actions, execute slash commands, or change configuration. Every injected message ends with a statement saying so, and the receiving session's own permission system remains the final defense.

The send receipt `submitted` means "validated and synchronously submitted to the Pi extension API" — it is **not** a durable delivery acknowledgement, and senders do not auto-retry after timeouts (to avoid duplicates).

## Limitations (by design)

- Same machine only — no relay, no cloud routing, no offline queueing.
- No `hold` approval queue — inbound is either `accept` or `refuse`.
- Windows named-pipe branch is code-reviewed but not yet verified on real Windows.

## Development

```bash
npm install
npm run typecheck   # tsc --noEmit
npm run smoke       # headless pi --mode rpc: extension loads, /peers commands registered
```

Design docs (reverse-engineering of Claude Code v2.1.224–252, v1 audit, v2 architecture) live in [`docs/`](./docs).

## License

[MIT](./LICENSE)
