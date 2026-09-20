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

Each independent Host session registers itself and listens on a private IPC endpoint. Before registration, the extension queries the child-local Mesh runtime identity contract; a frozen `managed: true` reply disables registration and discovery for that extension instance, without affecting other Sessions in the same PID. Older/foreign runtimes without that contract cannot be identified automatically. Node **>=22.19.0** is required.

| Entry point | Kind | What it does |
|---|---|---|
| `/peers`, `/list-pi` | command | List other live Pi sessions (name, busy/idle, cwd, Git worktree/branch/HEAD, ref) |
| `list_pi` | tool | Same listing, structured output for the model (`git: null` outside Git) |
| `send_pi_message` | tool | Send plain text by exact name, session id, runtime id, or `name [ref]` |
| `--cross-session-inbound=accept\|refuse` | flag | Control inbound messages (default `accept`; invalid values fall back to `refuse`) |
| `/cross-session-status` | command | Bounded local queue/submission/drop diagnostics and remaining budget |
| `/cross-session-resume` | command | Explicit local-user reopening after observed cancellation; never replays dropped messages or refills budget |
| `--cross-session-rpc` | flag | Enable trusted Host-only EventBus send/info/received contracts (default **off**) |

Just talk to Pi: *"tell the backend session the order API moved to /v2"* — the model calls `send_pi_message` itself. If the target is not explicit, the model uses `list_pi` and chooses from the known responsibility, exact session name, and working directory; busy/idle is delivery state, not a routing preference. In the TUI, type `@` to select a live session as `@name [ref]` and make the target explicit.

Pi may also send a concrete finding, decision, question, or status when another independent session needs it mid-task. It should not send routine progress, guess an uncertain recipient, or delegate work it can handle itself.

On the receiving side, safe idle messages are synchronously submitted to the SDK and may start a turn. **Busy messages stay in a bounded extension-memory queue, never the SDK steering queue.** One message is submitted after an observed assistant terminal `stop` and non-cancelled, source/signal-confirmed `agent_settled` gate. Errors revoke success evidence: retry-backoff cancellation or an unknown terminal outcome latches/drops rather than treating a stale non-aborted signal as success. This intentionally no longer steers between tools of the current turn: SDK abort can retain such queued custom steering messages. Unknown/preflight states without a confirmed active signal and turn source refuse admission rather than guessing.

An observed abort (including unknown/internal abort) latches reception closed and drops all not-yet-submitted messages. Only the local `/cross-session-resume` command reopens it; peer text and ordinary prompts cannot. A new genuine user turn can still use its own tools without reopening inbound peers; old cancelled/peer/unknown continuations stay restricted. Submitted work is not retractable by this extension. Messages render as a single folded line; `Ctrl+O` expands.

```
› Message from @frontend: API 升版通知 (Ctrl+O to expand)
```

## How it works

- **Discovery plane**: each runtime instance writes a `0600` JSON file under `~/.pi/agent/peers/<instanceId>.json` (name, status, cwd, pid, socket path, bearer token). Refreshed every 30s; removed on shutdown. Listings resolve Git worktree, branch and HEAD live from each cwd, so stale Git metadata is never registered.
- **Data plane**: each instance owns a `0600` Unix socket (named pipe on Windows) under a `0700` per-UID runtime directory, namespace-isolated by a hash of the agent dir.
- **Protocol**: two-phase JSONL v1 — authenticated `hello` → one `message` (or `status`) frame → receipt. `cancel-safe-queue-v1` is negotiated in hello/ready. Old senders retain safe-idle compatibility but get actionable `busy` refusal instead of an unknown queued receipt. New senders refuse old receivers lacking the capability, before sending any message. Paths use random instance IDs, never persistent session IDs.
- **Bounds**: 1 MiB raw UTF-8 per-frame cap (including LF/BOM and incomplete tails, not a combined chunk), 5s exchange deadline including endpoint vetting/lstat, 30s connection deadline, 64 incoming connections, 30-cap/0.5-per-second per-sender token bucket, 30s sender-qualified hashed same-text suppression (including A/B/A across settled), 50 pending messages, 30s queue/message TTL. IDs are deduplicated per authenticated sender incarnation for the recipient incarnation. A **shared 256-unit incarnation budget** covers inbound admissions and outbound exchange attempts, including bridges; settled/user-resume never replenishes it. Admission reserves identity/quota before trusted event callbacks; later drops/failures retain their status and reservation. No automatic retry.
- **Lifecycle**: serialized/fenced registration and heartbeat writes; awaitable idempotent shutdown/reload cleanup; instance-private `beforeExit` fallback for normal Node EOF. SIGKILL cannot run cleanup: discovery reaps an unavailable endpoint only after confirming its PID is dead. This is not a filesystem transaction or PID-reuse-proof lease.

## Security model

The trust boundary is the OS user. Bearer tokens, file modes, owner checks, and symlink rejection defend against other OS users, stale endpoints, path substitution, and accidental clients. A malicious process **running as the same UID** can read registration tokens and impersonate a sender — this is an explicit, documented boundary, not an oversight.

Messages from peers are plain text from another agent, never user intent: they cannot grant permissions, approve actions, execute slash commands, or change configuration. In addition to the displayed statement, a `tool_call` gate blocks known task/growth/resume/configuration entry points on peer-only, unknown-source or currently cancelled logical turns, preserved across retry/continuation (including `Agent`, `send_subagent`, authority-changing `mesh` actions and `mesh_control grow`). It does **not** classify arbitrary Bash, wrap every third-party tool, or sandbox trusted extensions. Existing user-directed busy turns and new genuine user turns retain their authority. This does not clear Direct/Mesh tools’ own user-cancel locks. Inbox I/O cleanup removes endpoints/listeners, not SDK work or its turn authority: the registered Session keeps observing cancellation/continuation and gating known tools even with its inbox gone. A downstream `input` handler returning `handled` need not emit any agent lifecycle events; the next observable input replaces its stale pending source only when the SDK is idle, has no active signal, and Cross owns no peer submission. Busy retry/continuation and peer preflight cannot be relabelled this way. Managed/never-registered contexts are not enrolled in this Host gate, and session rebinding cannot inherit a prior Session's user authority. The receiving Host's permission system remains the final defense.

### Fixed Mesh continuation (trusted EventBus, not peer RPC)

An updated Mesh can predeclare `continuationTasks` during a genuine local user
`mesh run` call. Cross records that exact call and issues a process-local
capability through `pi-mesh:continuation:issue:v1`. Mesh binds its original
run/epoch and associates the capability with the exact notification `details`
object at flush. Cross recognizes that live object on `message_start`; neither
JSON copies, peer text, customType names nor serialized history are credentials.
The resulting restricted `mesh` provenance is **not** `user` provenance.

Only one `mesh continue` for that exact parent run, with no extra parameters,
can be reserved and claimed. The Mesh implementation owns the immutable plan:
1–4 sequential fixed tasks, concurrency 1, ten minutes each, no retries or
recursive continuation. Cross bounds issuance to 16 plans per genuine-user
generation and expires them after one hour. Abort/unsafe settlement, new
interactive/RPC input, session startup/replacement, reload and shutdown revoke
old permits. A claim cannot cross a settled turn. Duplicate deliveries and
failed executions do not refund authorization; expiry/restart never restores it.

This does not whitelist all Mesh actions, grant arbitrary `run`, growth,
resume/configuration/Direct permissions, clear cancellation locks, enable peer
reception, or depend on the opt-in bridge RPC flag. Mesh verifies original-epoch
all-first-attempt success and its own current Host/root/cancel fences before
creation. Unplanned follow-up tasks still require a new local user instruction.
Both updated extensions must be loaded; an absent capability always fails closed.
Trusted extensions sharing the EventBus remain outside the security sandbox,
as in the existing Cross contract. SDKs that stop preserving live message
`details` identity fail closed rather than falling back to serialized tokens.

Receipts are deliberately limited:

- `queued`: accepted only into volatile extension memory; may expire or be dropped on abort/shutdown/crash.
- `accepted`: claimed synchronously by a trusted Host event listener; **no SDK submission or spool/storage acknowledgement**.
- `submitted`: `pi.sendMessage` returned synchronously. It does **not** mean history insertion, provider invocation, assistant response, or business completion.

The SDK API returns `void`; asynchronous `send_message` errors remain on the SDK Host error channel. Local status retains correlated `message_end` evidence/assistant-error observations, or an unconfirmed-submission diagnostic after 5s. All positive/negative responses must correlate with the current phase/request ID; remote `code`/`next` are diagnostics, not authority. A timeout, mismatched/lost receipt or cancelled outgoing wait is **receipt unknown**, not evidence of failure/delivery: inspect the recipient's `/cross-session-status` or source-qualified wire status; never auto-retry.

## Limitations (by design)

- Same machine only — no relay, no cloud routing, no offline queueing.
- No durable/offline/approval queue. The busy admission queue is volatile and strictly bounded.
- No global SDK patch and no `clearQueue` API assumption. An arbitrary abort before a public active signal exists is not observable/retractable; the extension does not claim a universal pre-agent abort fix.
- Default-off Host RPC is transport/admission only, not an AgentBus or task engine. Reserved bridge text is **never** submitted to the model: a synchronous trusted handler must claim it or receive `no_handler`/`unsupported`. Mesh mapping, spool acknowledgements, generations and user approvals belong to the next Host adapter batch.
- Windows named-pipe branch is code-reviewed but not yet verified on real Windows.

## Development

```bash
# Use the existing installation tree; these commands do not install dependencies.
./test/run-clean.sh test
./test/run-clean.sh run typecheck
./test/run-clean.sh run smoke
# Override CROSS_TEST_NODE with an explicit Node >=22.19 path on other machines.
```

The wrapper starts the fixed installation's npm-cli under `env -i` and preloads the standalone sandbox **in the npm parent**. HOME/agentDir/XDG/cache/tmp are private, notifier/network are disabled, and Node/Pi descendants inherit a network-only guard without losing fixture arguments. Do not bypass it with an unguarded npm/npx/global Pi invocation. Tests distinguish stubbed extension-context components, real SDK/local-provider + Unix IPC, and two actual OS processes. Smoke uses this repo's CLI, checks a real registered endpoint, ends stdin, requires normal exit0, and checks cleanup; it does not claim mutual communication or a real model.

See **[HANDOFF.md](./HANDOFF.md)** for frozen EventBus/wire schemas, adapter constraints, executed counts and remaining boundaries. `lib/contract.ts` provides the bounded `requestRpc` helper; `on()` unsubscriptions and abort/timeout cleanup are mandatory.

Design docs (reverse-engineering of Claude Code v2.1.224–252, v1 audit, v2 architecture) live in [`docs/`](./docs).

## License

[MIT](./LICENSE)
