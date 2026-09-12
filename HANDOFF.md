# Cross-session core / Host RPC handoff

范围：只改 `pi-cross-session`。当前第二轮修复 **F1/F2 两项 P1 + 跨 SDK RPC 测试契约**，最终证据见第 7 节（待主审复核）；第 5/6 节保留历史批次证据，不代表当前 SDK 矩阵。基线 HEAD `d2eb1bc`，原仓库安装树 SDK **0.84.4**，另以 fresh 私有 source-hash fixture 验证已装 **0.83.0**，未改原版本声明或依赖。
没有提交、push、stash、安装、联网、依赖版本变更、真实用户 session/peer/凭据访问；没有启动子代理。
**Mesh Host adapter、映射、spool、权限 UI 与两份原报告均未修改。** 本文不是 Mesh 集成验收。

## 1. 已冻结的忙碌收件协议

保留 registration `version:2, protocol:1` 和 JSONL `v:1`，以 hello/ready capability 做最薄协商：

```ts
hello = {
  v: 1, type: "hello", requestId, token: recipientToken,
  target: { id: recipientSessionId, instanceId: recipientInstanceId },
  from: { id: senderSessionId, instanceId: senderInstanceId, token: senderToken },
  capabilities: ["cancel-safe-queue-v1"]
}
ready = {
  v: 1, type: "response", requestId, ok: true, status: "ready",
  capabilities: ["cancel-safe-queue-v1"], peer: { id, instanceId, pid },
  code: "ready", state: "ready", retryable: false, next: string
}
message = {
  v: 1, type: "message", requestId, messageId,
  text, summary, sentAt: epochMilliseconds
}
```

- `from` 可省略的 hello **只能探测**；发 message/status 必须认证 sender registration。
- requestId/messageId：`[A-Za-z0-9_-]{1,128}`；instanceId：32 位小写十六进制。
- capabilities：至多 8 个字符串，各至多 64 字符。禁止把字符串误当数组协商成功。
- sessionId 非空、至多 512；text 非空、well-formed Unicode、至多 1,000,000 UTF-16 code units；summary 非空、至多 200 Unicode code points。发送工具的可选 summary 先验证（非空、至多 400 code units），再正规化。
- UTF-8 fatal streaming decoder；每帧（包括 LF、首帧 BOM、未完整多字节尾部）按原始字节上限 1,048,576 bytes。合并 chunk 不作为一帧；后续坏帧的处理排在此前合法帧之后。连接只允许 hello + 一条 message/status；第三帧拒绝。违规第三帧不能撤回之前已处理的合法帧。
- 每次发送 exchange 从入口即有 5s 单调时钟绝对 deadline，含 endpoint vetSocket/lstat（迟到结果不得 connect/send）；发现阶段的 registration I/O 不在这一次 exchange 内，RPC 另有外层 5s timer；接收有 30s 连接 deadline、hello 后 5s idle timeout、最多 64 入站连接。超时不重试。

| 发送方 / 接收方 | 冻结行为 |
|---|---|
| new / new idle | 安全 idle gate 后 `submitted` |
| new / new busy | 已确认当前有效 signal + turn 来源时，进入扩展内存队列，`queued` |
| old / new busy 或 preflight | **SDK 提交前** `busy` / `ok:false`，要求等待 confirmed idle 后由调用方明确重试 |
| old / new safe idle | 保留 v1 `submitted` 兼容；不能用 handled 返回 old sender 不认识的 accepted |
| new / old | hello/ready 无 capability 即 `unsupported`，要求升级，**不发送 message** |
| old / old | 未改变旧产品；不把原有不安全 steering 行为称为本次修复 |

`submitted` **仅**表示 `pi.sendMessage` 同步返回；不是 history/model/reply/business 成功。
`queued` 仅表示扩展接受到非 durable 内存；`accepted` 仅表示受信 Host listener 同步 claim，无 SDK 提交，也不是实际 spool 存储收据。

### 队列、取消和来源

- 每 incarnation 最多 50 条待提交消息，TTL 至多 30s（同时受发送时间约束，未来 clock skew 最多 5s）。
- 不再把 busy 消息交给 SDK steer：只在最近一次 low-level run 观测到 assistant `message_end.stopReason="stop"`、非取消、signal/来源已确认的 `agent_settled` 后，下一个 event-loop gate 重验 idle/epoch，再提交 **一条**。
- 从同步提交到 agent_start 的窗口设为 preflight，不能因 `isIdle()` 暂 true 把第二条漏入 SDK queue。
- agent_start 捕获 signal 并注册 abort listener；signal abort 或 assistant aborted 证据都会 stop latch，并立刻丢弃未提交队列。unknown/internal abort 也保守 latch。
- admission 的同步受信事件回调后再次验证取消、incarnation 与同一 signal，覆盖 listener 内发生 abort 的重入。
- 未确认 signal/来源的忙碌或 preflight 状态拒绝（不自动猜测执行）。error 会撤销终止成功资格，每次 low-level agent_start 都重新等待成功证据。错误/backoff 取消、缺失成功证据、toolUse/length 尾态或未知来源的 settled 均保守 latch/drop；实际 retry 后成功 stop 才可恢复该逻辑 turn 的安全 flush 资格。
- 只有本地用户 `/cross-session-resume` 命令可以重新打开；需要 SDK idle 且无 active signal。普通用户 prompt、peer 文本、工具参数均不解锁收件；但 idle 后新的 genuine interactive/rpc 用户 turn 可正常使用自己的工具，不需要先重开 peer 收件。命令不重放 dropped，也不补充预算。
- 每 incarnation **共享 256 单位**收发预算：每次 inbound admission、每次 outbound exchange attempt 各扣一；桥接同样扣；agent_settled/resume 不补满。新 incarnation 才重置，绝不能由 adapter 自动 reload 来续预算。
- per-sender token bucket 30 / refill 0.5s⁻¹；sender-qualified SHA-256 摘要 → admission 时间 Map 抑制同 sender 同文本 30s 内 A/B/A，不仅上一条；不随 settled 清空，最多 256 个 committed 摘要键，由总预算约束。
- 去重键是 `authenticatedSenderInstanceId:messageId`；ID 在整个 receiver incarnation 保留，不因 30s 窗口到期重复注入。总预算保证状态有界（最多 256 条）。
- admission 先保留 ID/额度再发受信事件；之后的 no_handler、SDK 同步失败、取消/TTL drop 不回滚到可自动重试状态。

### 有界可见状态 / 不确定收据

接收者 `/cross-session-status`、启用 RPC 的 info，或同一认证 sender 的 wire status 查询：

```ts
{ v:1, type:"status", requestId, messageId } // hello 后的第二帧
// response.status / state:
// unknown | accepted | queued | submitted | expired | dropped |
// dropped_cancelled | dropped_shutdown | dropped_user_reset | injection_failed
```

status 查询不会消费消息预算，不可查询其他 sender 的消息。`unknown` 不表示未送达：重启/失联可能丢失整个内存状态。
通过工具发送时，exchange 错误会带 messageId、recipient instanceId、code/state/retryable/next，避免收据丢失后连查询 ID 都不知道。
正/负 response 一律先按当前 hello/message phase 的 requestId 关联，错 ID/旧 hello 负 receipt 不得分类为 busy/refused。可选 code/state 限 64 字符、next 限 2048、error 限 4096、retryable 为 boolean；这些是远端诊断数据，不是执行许可，发送端 next/retryable 由本地错误分类产生。
`timeout/connection_closed/cancelled/transport_error/invalid_response/invalid_receipt` 保守为 `receipt_unknown`，不得自动重试。

SDK `sendMessage` 返回 void；不能 `await void` 假装等待注入。底层异步错误仍由 SDK Host 的 `onError`/`send_message` 通道报告。
本扩展保留相关 custom `message_end` 观测、assistant error 诊断；5s 内没有 message_end 时标记 unconfirmed submission；如果连 agent_start 都未观测到则保守 latch。状态仍是 submitted，不伪造 injected/completed。

## 2. 默认 Host-only，默认关闭的事件 RPC

开启 flag：**`--cross-session-rpc`**（boolean，默认 false）。不支持通过 peer 文本或 RPC 请求修改 flag。

注册 endpoint 前同步发送 child-local：

```ts
pi.events.emit("pi-mesh:runtime:identity:query", { version:1, reply(identity) { ... } })
// 仅同步、Object.isFrozen(identity)、version:1、managed:true 的响应抑制注册。
```

不设置进程全局环境，不把所有 SDK/RPC 模式当 managed child。同 PID 的合法 Host 不受影响。
旧/foreign runtime 未实现此同步 frozen identity 契约时，不能自动识别其 child；不是通用 managed-process 侦测器。

### RPC send（复用原 send/exchange，无 executeTool）

topic：`cross-session:rpc:send`
reply：`cross-session:rpc:send:reply:<requestId>`

```ts
type SendRequest = {
  version: 1;
  requestId: string;                  // 上述 bounded ID
  local: { sessionId: string; instanceId: string }; // 必须精确匹配当前 Host
  remoteInstanceId: string;           // 必须精确 32hex；绝不按同名 peer 解析
  messageId: string;                  // 由 adapter 提供稳定关联 ID
  text: string;
  summary?: string;
};
// reply
{ version:1, requestId, local, ok:true, messageId,
  target: PublicPeer, receipt: WireResponse }
// 或
{ version:1, requestId, local, ok:false, code, state, retryable, next, error? }
```

- 每个 send requestId 在 incarnation 内最多一次发送和一次 reply；重复请求忽略，不能触发第二次发送/回复。
- 至多 256 个 send requestId（包括 invalid requests）；容量耗尽不分配新状态，调用方 bounded wait 得到 receipt_unknown。info 的 `remainingRpcRequests` 可预检。
- 5s server deadline；超时/关闭取消 outgoing wait 和 socket。已经到达远端的 admission 不能因此撤回，返回不确定收据而非失败/成功承诺。
- shutdown/reload 注销所有 RPC listeners、结束 pending wait、fence 旧 incarnation。
- 默认关闭 / 没有 listener / 请求 ID 重复：调用方必须有限等待，不可无限 await 或盲目重发。

### RPC info（用于 bootstrap 真实 incarnation，不从 metadata 猜）

topic：`cross-session:rpc:info`
reply：`cross-session:rpc:info:reply:<requestId>`

```ts
{ version:1, requestId }
// 同步快照，每次 query 一条 reply：
{ version:1, requestId, ok, local: {sessionId,instanceId} | null,
  capability:"cancel-safe-queue-v1", stopped, remainingBudget, remainingRpcRequests,
  states:[{messageId, source: senderInstanceId, state, updatedAt, diagnostic?}] }
```

`lib/contract.ts` 导出 topic 常量、`SendRequest` / `ReceivedEvent` / `BridgeEnvelope`、`requestRpc(bus, topic, request, timeoutMs=5000, signal?)`。
helper 先 on 再 emit，timeout 1..5000ms，首个合法 version/requestId reply 后清理；abort/timeout 同样 unsubscribe/清 timer。`on` 的返回值就是 unsubscribe。
调用方 abort 只停止等待，绝不是远端撤回证明。valid requestId 的缺 listener/timeout 异常具有 `code/state=receipt_unknown, retryable:false, next`。

### 收件事件 / 只展示 Host，不额外触发模型

仅 RPC enabled Host 在认证 + admission 后发送 `cross-session:received`：

```ts
{
  version:1, local:{sessionId,instanceId},
  source: PublicPeer, // 冻结、无 token；来自接受连接的已认证 registration
  messageId, text, summary, sentAt,
  bridge?: BridgeEnvelope,
  canHandle: boolean, // legacy sender=false，不可改变其 idle submitted 语义
  reply({handled:true}): boolean
}
```

事件和 source 是 frozen；source 包含 v2 的 `id/instanceId/name/cwd/pid/status/inbound/socketPath/startedAt/updatedAt/protocol/version/ref`，**没有 token**。
正文里 origin/from/agentId 不是认证源，更不是用户权限。

`reply({handled:true})` **仅第一个同步 claim 返回 true**；之后/await 后/legacy sender 均 false。
adapter 必须先检查 claim 返回 true，才开始自己的异步存储/展示工作；其他 listener 不得重复处理。
claim 将 cross receipt 设为 accepted，禁止此条 SDK submission；**并不证明 adapter 已存入 spool**。
异步 spool 成败及关联回复由 adapter 独立记录。不能把 accepted 当 stored；需要 crash 去重时使用 Mesh 已有 evidence/spool，不加 cross outbox。

## 3. 冻结的 bridge envelope（此批不实现 Mesh 映射）

text 使用以下保留前缀，之后是一个 JSON object：

```ts
"cross-session:bridge:v1\n" + JSON.stringify({
  version:1,
  origin: string,        // bounded ID，Host-transcribed 来源，不是 child 认证
  correlationId: string,
  expiresAt: number,     // safe integer，now < expiresAt <= now+30s
  hops: number,          // integer 0..4
  budget: number,        // integer 1..256
  payload: unknown      // Mesh adapter 冻结自己的映射/代际 schema
})
```

认证的 `source` 就是当前 relay Host；无另一个可冒充身份的 relay 字段。Mesh 必须在转发时增加 hops、扣 budget，保留 correlation/origin/TTL；本地总预算另有硬上限。
保留前缀的消息 **永不由 cross 自动进入 SDK/model/history**：关闭 RPC/无 capability 为 unsupported；无同步 claim 为 no_handler；非法 JSON/version/TTL/hop/budget 为 invalid_bridge。

下批 Mesh adapter 必须做：

1. 显式用户设置的精确 peer incarnation → **已存在** run/node/attempt 或 Direct ID/generation 映射。
2. 默认 mailbox/Host 展示；如用户启用 active steer，只能匹配当前正在运行 execution；不能调用 terminal resume/spawn/retry/growth approval。
3. 固定目的地、stale generation/attempt、用户取消、hop/TTL/budget、qualified message identity 与 restart dedup 校验。
4. Cross accepted/queued/submitted、Mesh actual stored/steered、业务回复分别记证据，不混成一个成功状态。
5. 不从 payload/peer 文本授权新任务、修改权限、批准增长或解除 stop。

本仓库额外在 peer-only/未知来源/当前逻辑 turn 已取消时的 **已知** `tool_call` 入口阻断 `Agent/agent/subagent/steer_subagent/send_subagent/send_user_message/set_active_tools/set_config`，以及非只读/ack `mesh` action、`mesh_control grow`。
权限来源跨逻辑 turn 的 retry/continuation 保留，不因内部 agent_start 的 inputSource=unknown 而放宽；这与 queue 的成功终止证据分离。全局 stopped 仅是 inbound latch，不封锁 idle 后新 genuine user turn 的工具。跨两个仓库现有 inventory 核对：`send_subagent` 可请求 terminal continuation，故加入门控；`get_subagent_result` 仍是只读查询。本插件不修改 Direct/Mesh 的 user/unknown cancel 锁，允许用户调用工具也不等于清锁。
不是任意 Bash/别名工具识别器，也不能抵抗同 UID 恶意进程或恶意受信扩展。

## 4. 生命周期与文件安全

- 保留 owner/mode/token + sessionId/instanceId 检查；额外验证 Unix socket mode。
- per-incarnation 写入队列：写临时文件前、rename 前重验 fence；关闭等待既有 writes，再删注册/endpoint。
- start/reload/shutdown 序列化，入口立即 fence/abort outgoing；identity query、RPC/idle/mode/TUI、文件和 socket 资源获取都在同一 try/cleanup 边界；诊断 notify getter/method 异常不得跳过 cleanup。共享可等待 cleanup promise，重复 cleanup 无副作用。
- heartbeat 包含同步 getter 异常，失败触发 cleanup，不留下 stale timer。
- beforeExit fallback 只管理本扩展实例的私有文件/socket/timer/listener；不退出别的 Session、不清其文件。
- SIGKILL/断电不能执行 cleanup。发现仅在 endpoint 不可用且 PID 检查明确 ESRCH 时回收；EPERM/其他未知错误不当作已证实死亡。
- PID 重用、同 UID 对抗、文件系统失败后的强事务清理不在承诺内。

## 5. 可重跑测试与证据

所有执行从 cross repo 的 `test/run-clean.sh` 进入。默认 Node **22.23.1**，可显式设置 `CROSS_TEST_NODE` 指向其他 >=22.19 Node。
wrapper 对 npm **父进程**使用 env -i + `--import test/support/clean-env.mjs` + 该 Node 安装的固定 npm-cli.js；不会解析 global Pi/npx，不安装依赖。
子进程继承 network-only guard，不重复 sanitize 掉 fixture argv/IPC 参数。
helper 不依赖兄弟仓库；可随 package 分发。沙箱不是 OS sandbox，但阻断 TCP/HTTP/HTTPS/fetch/DNS/foreign Unix/listen；只允许私有 fixture namespace IPC。

```sh
cd /Users/relvf/ai/pi-cross-session
./test/run-clean.sh test
./test/run-clean.sh run typecheck
./test/run-clean.sh run smoke
git diff --check
```

日志目录：`/private/tmp/pi-mesh-optimization-20260909/cross/`。
最终结果在本文末尾更新。早期失败日志保留：guard 的 /tmp canonical-path 差异；I/O fault fixture 的 import 捕获时机；busy restart fixture 未回 idle；TS lib declaration；惰性工具 allowlist 导致 fixture Agent not found。最后一项先用真实用户执行的反向控制捕获，再明确允许**惰性 fixture 工具**并断言阻断 reason，不能把 “tool not found” 当权限 PASS。
没有调用平台真实 Agent/list_pi/send_pi_message；跨会话工具调用只针对这些隔离实例/定义，fixture Agent 从不创建子代理。

### 诚实保留的边界

- 不宣称修复 API 不可观测的任意 pre-agent abort；已同步交给 SDK 的工作无法由本扩展 clearQueue/retract。此批没有 SDK monkeypatch。
- 本地 provider 是确定性替身，SDK/Unix IPC/进程与 history 是真实的；没有真实模型服务/生产 UI 验证。
- SDK **0.83 runtime 未执行**，不把 Host0.83 + imports0.84 混合称纯0.83；可留主审最终 version-isolated 矩阵。
- 已执行真实 SDK reload；完整 RuntimeHost switch/newSession/UI TUI Escape 路径未单独执行。component 则执行 incarnation replacement/start race/shutdown fencing。
- Windows named pipe 未实机验证；PID 重用、敌对同 UID、安全分类任意 Bash 不在验收声称内。
- 没有端到端 Mesh 双 Host mapping/spool 桥接：这是明确留给下一批的 adapter 产品工作，不以 cross 事件 claim 测试冒充完成。

### 上一批归档结果（27 项，不验证真实默认 retry 安全）

| 检查 | 实际结果 | 稳定日志 |
|---|---|---|
| 完整 npm test | **27/27 pass，0 fail / skip / cancelled**，54.62s | `npm-test-acceptance.log` |
| npm run typecheck | exit0，无 TS 诊断 | `typecheck-acceptance.log` |
| npm run smoke | 固定本 repo CLI，真实 inbox，stdin EOF，正常 exit0，注册/endpoint 删除 | `smoke-acceptance.log` |
| git diff --check | PASS | `diff-check-acceptance.log` |

27 项的证据层次：**16 component/I/O/IPC 项、10 涉及真实 SDK 的集成项、1 network guard 项**。component 使用真实扩展 loader/产品/Unix IPC，但 ExtensionContext/sendMessage 是 stub；不能将这 16 项称真实模型执行。

真实 SDK provider 调用合计 **16 次：14 个完整 provider stream response（含 2 个 tool-call assistant response），2 次受控 abort**。全部为本地确定性 provider，未调用外部模型。

- 三条 stop 对照分别是 2 / 1 / 3 次 provider 调用；两个 abort 场景自动后继调用为 **0**，explicit resume 后旧 dropped 文本不进新 history/context。
- SDK preflight + 两条 extension queue 串行 flush：3 次调用 / 3 个 response。
- SDK Host RPC bridge：claimed bridge 无 SDK history/模型调用；另一次普通 RPC send 真正入 history 并完成 1 次调用。
- 真实 SDK `tool_call`：惰性 fixture Agent，peer-only 执行次数 0，明确用户执行次数 1；4 次 provider 调用。没有创建子代理。
- 两独立 OS SDK Host 相互 Unix 发送：2 次调用 / 2 个 response；两进程均正常 exit0；一个标准 shutdown、一个正常 EOF/beforeExit fallback；分别验证独立资源清理。
- 另一个 fixture OS SDK Host 被明确 SIGKILL，证明不能运行 cleanup；真实发现回收在 PID 已死后删除残留。
- 真实 SDK managed identity、同 PID Host 独立性、reload/ref 替换另有断言；这些测试不额外调用 provider。
- queue MAX_PENDING / rate / 256 总预算 / duplicate / wrong identity / exact32-name collision / UTF-8 分段和精确 1MiB 边界 / TTL / 31s heartbeat getter fault / FS write race / receipt loss / absolute timeout / RPC duplicate/缺 listener/销毁均有对应测试。

Node 的原生 TS helper 导入可能输出 `MODULE_TYPELESS_PACKAGE_JSON` 的模块推断 warning；不代表 SDK 出错，没有为了消除它改变依赖或强制 package 全局 module 语义。
最终源码/测试/运行版本 hash manifest：`acceptance-manifest.json`。上述日志均位于本节之前给出的 cross 日志目录。


## 6. 本批 review-fixes 证据（待主审核对，不是 checklist 完成声明）

新日志目录：`/private/tmp/pi-mesh-optimization-20260909/cross-review-fixes/`。
改前已保存 `HANDOFF.baseline.md`、`acceptance-manifest.baseline.json` 与 `cross-session.baseline.ts`；未覆盖上批 cross 日志/manifest 或两个历史 HTML 报告。
本批产品改动仅在 extension（以及 README/HANDOFF 说明），测试复用现有 loader/SDK/IPC helpers；`lib/contract.ts`、package/lock、run-clean guard 未改变。
RPC/send/info/received 的 topic、字段、独占同步 claim、bridge 保留前缀和字段形状全部保留；没有 Host adapter、outbox、daemon、DB、AgentBus 或 SDK monkeypatch。

### 先写回归与旧实现实际失败

| 故障 | 改前证据 | 修复后断言 |
|---|---|---|
| retry backoff abort | `sdk-before.log`：默认 retry 开启，真实 auto_retry_start/isRetrying，旧 agent signal.aborted=false；abort 后 calls=3，两条旧队列变 submitted | 原始 calls=1，自动后继=0；两条 dropped_cancelled，普通 user 和显式 resume 后都不重放 |
| peer retry 权限 | 同日志：惰性 fixture Agent/send_subagent 各实际执行 1 次、toolResult.isError=false | 各执行 0，明确 Peer-only/cancelled 权限 reason；genuine user 各执行 1，绝不以 not found/参数错充数 |
| stopped 错封新 user | 同日志：新 genuine user Agent 执行 0，仍被全局 stopped 阻断 | 不先 resume，新 user Agent/send_subagent 各执行 1，inbound 仍 stopped、peer 仍 refused |
| 部分 start + notify 故障 | `io-before.log`：初始 idle/mode/TUI throw、write fail 后 notify getter/method throw 越过清理边界 | 另加第二 RPC listener 获取失败；6 种注入均 hit，RPC subscriptions=0，beforeExit/files/socket 回基线，独立 Host ready |
| 负收据错关联 | 同日志：错 ID/旧 hello 的 busy 负响应被当作 state=refused, retryable=true | invalid_receipt / receipt_unknown / retryable=false，一次 message send；非法远端 metadata 也是 unknown |
| A/B/A 去重 | 同日志：跨 settled 的 A/B/A 第三条 accepted | duplicate；另一 sender 的 A 可 accepted；30s 后新 ID 可接受，旧 ID 墓碑和 budget 不重置 |
| raw frame 窗口 | `frame-coalesced-before-shifted.log`：先写 17000 bytes 错开 OS chunk 边界，再合并余下 hello+message，不等 ready，合法每帧被 message_too_large；同日志目录 io-before 还显示末尾坏 UTF-8/第三帧使前面合法 admission 丢失 | 合并/分块 pipeline、精确 1MiB 多字节、BOM raw exact/over、未完整多字节 over、合法帧后坏 UTF-8/第三帧都按每帧处理 |
| lstat 在 timer 前 | `io-before.log`：fixture-only 第二 socket lstat entered=true/hits=2，5.31s 后仍未返回，未释放 I/O | 普通 send（非 RPC）约 5s 返回 timeout/unknown；释放迟到 I/O 后连接数保持 1（仅 discovery probe）、message sends=0 |
| 当前 send_subagent 漏表 | SDK retry 旧实现实际执行；公开 inventory 确认其可 terminal continuation，并非仅 steer | 另有独立无 retry peer 测试：显式 reason、执行 0，genuine user 执行 1；不触碰 Direct/Mesh 自己的 cancel 锁 |

最初对齐 OS chunk 的近界 pipeline 旧实现碰巧通过，保留在 `frame-coalesced-before.log`，**没有把未命中当修复证据**；错位 warmup 回归才实际复现。
I/O patch 在首个产品 loader/jiti 评估前安装并 syncBuiltinESMExports，过滤到 fixture 路径，全部断言 hit；不访问真实 user session/peer/凭据。
最初全量修复后日志 `npm-test-first.log` 为 44/46，旧 component 的 settled stub 未提供成功终止事件；现 helper 正常 settled 明确发 stop，未知 settled 用 false，并断言 stop latch。同期 TS catch 返回 Socket 的诊断也已修正；失败日志保留。

### 最终实际 acceptance run

| 检查 | 实际结果 | 日志 |
|---|---|---|
| 完整 npm test | **53/53 pass，0 fail / skip / cancelled**，69.13s | `npm-test-acceptance.log` |
| npm run typecheck | exit0，无 TS 诊断 | `typecheck-acceptance.log` |
| npm run smoke | 固定本 repo CLI、实际 inbox、stdin EOF、正常 exit0、独立 endpoint/registration 清理 | `smoke-acceptance.log` |
| git diff --check | PASS | `diff-check-acceptance.log` |

53 项 = **35 component/I/O/IPC + 17 涉及真实 SDK 的集成 + 1 network guard**；其中新增 26 项 = 19 component/I/O/IPC + 7 SDK。
新增 SDK 测试使用已启用的 lazy fixture 工具，不创建子代理、不调用真实平台工具。4 个 retry 场景 **不设置 retry 配置**，使用安装 SDK 0.84.4 的实际默认 `enabled=true, maxRetries=3, baseDelayMs=2000`，并等待真实 retry 事件/状态；其余原 helper 场景仍显式 retry=false，不能拿它们单独证明默认 retry。

本次完整 npm test 的 provider 调用共 **43 次**：**35 次非错误完整 response（含 10 次 tool-call assistant response）、4 次受控 retryable error response、4 次受控 abort**。全部是本地模拟 provider，不是线上模型。
新增 7 个 SDK 场景 calls 依次为 **3 / 5 / 3 / 5 / 3 / 4 / 4 = 27**（21 非错误 response、4 error、2 abort），加上原 16 次。自动后继为 0 的 backoff abort 测试在后续两次显式用户请求后 calls 才增加到 3；不把后续用户请求算成自动重放。
成功 retry 的反向对照为 4 次 calls（error → stop → 两条串行 peer stop），证明不是简单永久禁用所有 queue flush。另有 component 对未知/toolUse/error/length 终态保守 drop、peer/internal cancelled/extension-source continuation 权限和显式 reopen 后新 peer 队列的检查。

只读参考：本机 Ponytail public README / `.agents/rules/ponytail.md`（先理解调用链、复用、标准库、最小正确改动）；安装 SDK public `docs/extensions.md` 的 input/agent_start/agent_settled/signal/tool_call/sendMessage、`docs/sdk.md`、`docs/settings.md` 的 retry 默认值；并核对本地 SDK agent-session retry/abort 实现。只读核对 Mesh `compat-extension.ts`、`control-extension.ts`、`extension.ts`、`session-agents.ts` 的现有入口与 terminal send/cancel guard；未运行或改动 Mesh。

**公开安全语义收紧提示给未来 Host adapter：** 只有实际 terminal stop + 已确认 signal/来源 + 非取消 settled 才能自动 flush；错误/未知终态 latch，不因旧 signal 未 abort 而成功。普通新 user 工具权限与 inbound stopped 分离。新加入 send_subagent/unknown-turn 门控；远端 response metadata 有类型/长度校验。其余 accepted≠stored/processing、RPC 默认 off、old/new capability、Host-only managed identity、收据 unknown/拒绝不自动重试、256 总预算不 refill 均不变。

剩余范围仍是前述不可观测任意 pre-agent abort、任意 bash/未知 alias、同 UID 对抗、纯 SDK0.83、Win、生产 TUI Escape/完整 RuntimeHost switch、真实模型以及 Mesh 双 Host mapping/spool。TUI acquisition throw component 不是实机 TUI 资源生命周期验收；SDK addAutocompleteProvider 的公开返回值是 void，不新增私有 TUI disposer/SDK patch。无权把此批测试称这些范围已验证。
最终本批 hashes、版本、日志摘要见新 `acceptance-manifest.json`；由主审判断是否接收上述修复。

## 7. 第二轮 F1/F2 + 跨 SDK 测试契约（当前结果，待主审复核）

权威 findings：`/private/tmp/pi-mesh-optimization-20260909/cross-review-second-findings.md`。
本批新证据根目录 **`/private/tmp/pi-mesh-optimization-20260909/cross-review-second/`**（以下相对路径均从这里起）。
先读 Ponytail README/rules、两版 SDK 公开 extensions/sdk/settings 文档与涉及源码，再写正确行为回归，在旧产品源码上真实 FAIL 后才修复。没有其它产品写者或子代理；Mesh 只读取已安装依赖与公开参考，不改 Mesh 产品/adapter。

### 最小产品改动与公开语义

- **F1 — inbox resource cleanup ≠ SDK turn 撤回。** `authoritySessionId` 在成功注册、或本扩展实际提交 peer turn 前建立该 Session 的逻辑来源观察；heartbeat I/O cleanup 仍关闭 endpoint、registration、RPC、队列/收据 timer 和 outgoing wait，但不删除已提交 SDK turn 的来源/取消观察。`input/before_agent_start/agent_start/message_end/agent_settled/tool_call` 不再以 `current/shuttingDown` 作为权限旁路。peer、unknown、cancelled 的已知敏感工具与 continuation 继续被阻断；只有安全 idle 后新的 genuine interactive/rpc input 可以获得 user 权限，不要求恢复 inbox。signal listener 在 settled 或新 session_start 解除，不把 `incarnationAbort` 当 SDK abort/retract。
- 新 `session_start` 立即重置旧 authority scope、watch 与 pending source；新注册重新从 unknown 开始，不借用上一 Session 的 user 权。managed identity 与未绑定/从未成功注册的无关 Session 不自动加入本 Host gate；未注册上下文的 `list_pi` 等仍诚实显示不可用。这不是全局 child sandbox。
- **F2 — downstream handled 可没有任何 agent lifecycle。** 新的可观测 `input` 只有在 `phase !== busy`、SDK `isIdle()`、`ctx.signal` 与已观察 active signal 均为空、没有 Cross-owned `submittedKey` 时才替换待启动来源。这样 user-handled 后的 extension 请求不借 user 权，extension-handled 后 genuine user 不被旧 unknown 错挡。busy retry/continuation、活跃 signal 和 Cross peer preflight 不可被覆盖。不是任意不可观测 pre-agent abort 保证。
- **跨 SDK RPC 测试契约。** 0.83 的原始 EventBus 在 runtime invalidation 后仍可返回精确、带 requestId/local 关联的 shutdown `ok:false, code/state=receipt_unknown, retryable:false`；0.84 stale facade 不可 emit，helper 在有限等待后 reject。测试只接受上述完整精确 receipt，或限定 code/state/retryable/next/timeout-message 的 bounded rejection；`ok:true`、别的失败形状或错关联均不能通过。仍验证远端 SDK stub 注入 0、RPC 监听/本地资源清理和独立 Host ready。不是用 diagnostic probe 代替原 suite PASS。

`lib/contract.ts` **未改**，无扩展契约的必要。RPC/send/info/received topics/字段、source frozen/token-free、独占同步 claim、bridge prefix/envelope 形状均保持。accepted 仍不是 stored，submitted 仍不是 model/history/reply/business 成功；预算不 refill，unknown 不自动 retry，RPC default-off 与 managed child-local 同步 frozen identity 查询保留。无 clearQueue/executeTool、SDK patch、AgentBus、daemon、outbox 或 Mesh adapter。

### 先归档 / 真实 FAIL → PASS

| 场景 | 本批改前真实证据 | 修复后实际证据 |
|---|---|---|
| 原 53，SDK0.84.4 | `sdk084-baseline53.log`：53/53，69.59s | 最终 57/57（原 53 全保留） |
| 原 53，pure SDK0.83.0 | `baseline083-fixture` 的 26 文件逐 SHA 与本批改前 Cross 相同；`sdk083-baseline53.log`：52/53，69.87s；仅原 RPC hard `assert.rejects` 失败 | 最终原 suite 加回归 57/57，不再仅是 QA probe PASS |
| F1 实际 heartbeat write fault | `regressions-old-source.log`：两个真实 SDK peer provider 均先 hold；真正 30s heartbeat writeFile 各 hits=1，socket/registration/RPC 已清；释放 tool-call 后 **Agent/send_subagent 各执行1、isError=false** | 两版最终各 hits=1；旧 SDK signal.aborted=false、run 仍 busy（非撤回）；释放后 **各执行0 + 完整 gate reason**；新 genuine user 各执行1，calls=8，inbox 仍不存在 |
| F2 user-handled → extension | 同 old-source 日志：downstream input 返回 handled，SDK idle/无 signal、无 start/settled；next extension **错误执行1** | 明确 gate reason、执行0、calls=2；peer 仍 stopped/refused |
| F2 extension-handled → genuine user | 同 old-source 日志：先真实 abort 留 stopped；extension handled 后 next genuine user **错误执行0** | 不 resume 的 user 执行1、calls=3（其中1受控 abort）；inbound 仍 stopped，peer 仍 refused |
| logical authority scope / continuation | 同 old-source 日志：cleanup 后 tool gate 返回 undefined | component 覆盖 closed peer/unknown/cancelled、privileged mesh、continuation、active signal/SDK busy/owned peer preflight 不可 relabel、替换 Session scope、managed/never-bound 不误挡 |
| shutdown RPC | 本批 0.83 原 53 仍真实 FAIL；没有篡改历史 fixture | 0.83 **0.53ms structured unknown**；0.84 **50.97ms bounded missing-reply reject**；各 remote injection0/cleanup true |

`regressions-old-source.log` 是 **0/4 PASS、4 FAIL**，`regressions-fixed-first.log` 为 4/4。旧源码和回归执行记录均保留。
收尾另核对 startup 发布窗口：registration rename 已公开，但最后 chmod await 尚未返回时可能收到合法 peer。第一轮 scope 只在注册 await 完成后建立，会漏掉这种已提交工作；现在在 `submit` 同步 SDK 调用之前也建立 authority。`publication-before-fixture` 保留第一轮产品 hash `c2bfdf35…`；`publication-before.log` 真实 callee hits=1、submitted stub=1，随后 startup fault cleanup，四个敏感入口原返回 undefined，**1 FAIL**；`publication-after.log` 为 **1 PASS**。这段放在既有第4个 component 回归内，测试总数仍57。没有拿先前两版57 PASS抵消这个后来发现的窗口；最终为修正后新 hash 另建 `acceptance/` fresh fixture，重新完整执行两版 suite/types/CLI。旧根目录 sdk083-final/sdk084-final 矩阵、publication-before fixture 与全部日志保留，**不验证最终代码**。

首次全量 `sdk084-full-first.log` 为 **56/57**：原 component budget fixture 新 incarnation 虽设置 idle，却残留被丢弃 run 的 `ctx.signal`，被新严格 idle gate 正确拒绝。只在该 fixture 清除 stale signal，使其符合 SDK idle/no-active-run；没有放松产品 gate、关闭 retry/allowlist、删除 50/256/budget 断言。`focused-idle-signal.log` 两项通过；随后两版完整原项全部通过。

### 最终完整矩阵与证据分层

| 检查 | 当前 repo SDK **0.84.4** | fresh private SDK **0.83.0** |
|---|---|---|
| 完整原 npm test（含新4项） | **57 pass / 0 fail / 0 cancelled / 0 skipped**；104.09s | **57 pass / 0 fail / 0 cancelled / 0 skipped**；104.28s |
| 原 npm run typecheck | exit0 | exit0；另有原 tsc 的 traceResolution 指向真实 Mesh0.83 声明 |
| 实际 CLI smoke | exit0；commands + inbox + stdin EOF + 注册/socket 删除 | 同样 exit0；不是只查命令列表 |
| 固定安装路径 CLI --version | **0.84.4** | **0.83.0** |
| git diff --check | exit0 | 私有 fixture 不带 .git；校验原 repo diff，并逐源 hash 比较 |

每版日志分别在 `acceptance/sdk084-final/` 与 `acceptance/sdk083-final/`：`npm-test.log`、`typecheck.log`、`smoke.log`、`cli-version.log`；都有独立 `.exit`。最终原 repo diff 日志在 `acceptance/diff-check.log/.exit`。
**57 = 36 component/I/O/IPC + 20 涉及真实 SDK 集成 + 1 guard**。新增4项为3 SDK测试（F1含两个 SDK receiver）+1 component；原53没有删除。component loader/Unix 是真实的，但 ctx/sendMessage 是 stub，不算模型执行。

每版最终全套确定性本地 provider **56 calls = 47 非错误完整 responses（含16个 tool-call assistant responses）+ 4 retryable error responses + 5 受控 abort**。原43 + 新13：F1 两 receiver 共8、F2 两方向2/3。新 fixture 均启用 lazy inert Agent/send_subagent、工具 allowlist 明确保留；它们不创建或联系任何 agent。原4个 retry 场景仍等待 SDK 默认 enabled=true/maxRetries=3/baseDelayMs=2000 的真实 backoff/事件，成功 retry 后两条队列串行 flush、backoff abort 自动后继0均保留；新场景也不禁 retry，但没有把没有触发 error 的运行说成 retry 测试。RPC bridge claim 的模型调用仍0；其它 probe/CLI smoke 无 provider 调用。

### 环境、纯版本与完整性

- 所有执行经**原仓库未改的 `test/run-clean.sh` / clean-env / network-guard**。Node **22.23.1，darwin arm64**；env-i、私有 HOME/agentDir/TMP/XDG/cache。0.84 npm cwd 是原 repo（wrapper 的明确 cd），0.83 npm cwd 是 private fixture；SDK Session、OS worker 与 CLI 的工作目录均是 helper 创建的私有 cwd。没有把 repo cwd 说成私有 npm cwd。
- 最终完整 npm 父进程另加载只读 resolver observer 与 `guard-proof.mjs`，然后仍运行原 `npm test` 脚本。父进程9类网络拒绝、private Unix handshake 正控；后代3类拒绝、argv sentinel、同一 private root、只继承 network guard 不重复 sanitize；原 suite 的12拒绝也通过。两版 `guard-proof-result.json` 保存 actual argv/env。cooperative Node guard 不是恶意同 UID 的 OS sandbox。
- `acceptance/sdk083-final/fixture` 是本批修复后的 **fresh 27-file source-hash snapshot**，不是旧 QA fixture。只链接5个 package 已声明、已安装包及其 bin。coding-agent 来自 Mesh顶层0.83；ai/agent-core/tui 来自它已有 nested0.83。TypeScript5.9.3、@types/node24.13.3、typebox1.3.25 使用已有声明依赖。Cross 的 dev 声明 **仍 ^0.84.4**；0.83 是受控兼容实验，不是符合该 semver 的 registry install 或扩展发布支持承诺。
- 0.83 `resolution-manifest.json`：85个 importer/direct dependency 检查、136个包、289条 runtime/optional 边；4个 SDK family 全为 Mesh0.83；**不包含/不依赖 pi-client/pi-protocol/pi-telemetry**，没有借084补齐。8个其它平台 clipboard optional variant 未安装，本机 variant 已装、无必需依赖缺失。`loader-identity-result.json` 实际经 DefaultResourceLoader 比较 bare-import 与 native 对象 identity；tsc 另证声明解析，CLI另证实际版本。
- 0.83 observer 有 **17,477条记录 / 390个不同 SDK文件**；0.84有 **16,983条 / 437个文件**，均检查 nearest named package、真实路径、版本和 entry SHA256。记录覆盖该版 suite/worker/CLI及所执行 probe，不把 observer 去重后的记录数当所有 resolve 调用次数，更不说每次解析都执行模块。原旧 tests 少数静态标题写 SDK0.84.4；0.83版本判定依据实际 paths/version/identity，不依据标题。
- `source-before.json`、`HANDOFF.before.md`、`cross-session.before.ts` 保存本批前状态；`acceptance/source-final-test-snapshot.json` 与 fixture-before/after 保存实际被测源码。最后只更新本 HANDOFF 报告文字；fixture 原封不动保留，最终产品/测试与被测 snapshot 逐 SHA 相同，文档差异单列 `acceptance/integrity-result.json`，不谎称最终 HANDOFF 字节在运行前已存在。两 repo 安装树前后 SHA/size/mode/link inventory 不变；详见 cross/mesh-modules-before/after 和 acceptance/integrity-result。原 package/lock、contract、guard、两份历史 HTML 报告哈希不变，旧 cross/review-fixes/sdk083-cross fixtures/logs 均未覆盖。
- 完整逐命令、退出码、分层 counts/calls、依赖与源码 hash、真实误差/失败索引见根目录 `COMMANDS.md`、`acceptance/acceptance-manifest.json` 与两版 `counts.json`。新证据包含所有本批失败，不拿诊断/聚焦 PASS 抵消最终全套 FAIL。

### 当前仍有的真实边界 / 未完成范围

当前 F1/F2 及跨 SDK 测试契约无已知未解决影响；两版当前全 suite/types/CLI smoke 实际通过。但 submitted SDK 工作仍不可撤回；不可观测任意 pre-agent abort、任意 Bash/未知别名、同 UID 对抗、Windows、生产 TUI/Escape、完整 RuntimeHost switch/newSession、真实模型/真实 sessions/peers/平台工具均未验证，不能由这里的新测试替代。scope replacement 的 component 是受控 ctx 重绑，不是实际 RuntimeHost switch。SDK 真实 reload 仍由原测试覆盖。

**Mesh adapter/mapping/spool/双Host bridge 产品没有实现**，accepted 不代表已存储。这是下一批产品工作，不把本批标 checklist 完成。没有 install/联网/依赖升级/commit/push/stash/子代理/真实凭据访问；原报告与历史日志完整保留。
