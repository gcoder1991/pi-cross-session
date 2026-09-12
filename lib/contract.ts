// Thin Host EventBus contract. No tool execution, persistence, mapping, or retries.
export const CAPABILITY = "cancel-safe-queue-v1";
export const RPC_SEND = "cross-session:rpc:send";
export const RPC_INFO = "cross-session:rpc:info";
export const RECEIVED = "cross-session:received";
export const BRIDGE_PREFIX = "cross-session:bridge:v1\n";
export type Identity = { sessionId: string; instanceId: string };
export type BridgeEnvelope = {
  version: 1;
  origin: string;
  correlationId: string;
  expiresAt: number;
  hops: number;
  budget: number;
  payload: unknown; // Mesh owns exact mapping/generation validation; never authority.
};
export type SendRequest = {
  version: 1; requestId: string; local: Identity; remoteInstanceId: string;
  messageId: string; text: string; summary?: string;
};
export type EventBus = {
  emit(topic: string, data: unknown): void;
  on(topic: string, handler: (data: unknown) => void): () => void;
};
export type ReceivedEvent = {
  readonly version: 1;
  readonly local: Identity;
  readonly source: { id: string; instanceId: string; name: string; ref: string; cwd: string; pid: number };
  readonly messageId: string;
  readonly text: string;
  readonly summary: string;
  readonly sentAt: number;
  readonly bridge?: BridgeEnvelope;
  readonly canHandle: boolean;
  // True only for the first synchronous claim. Claim before starting async work.
  reply(result: { handled: true }): boolean;
};
export const validId = (value: unknown): value is string => typeof value === "string" && /^[A-Za-z0-9_-]{1,128}$/.test(value);
export const validInstance = (value: unknown): value is string => typeof value === "string" && /^[0-9a-f]{32}$/.test(value);
export function bridgeEnvelope(text: string): BridgeEnvelope | undefined {
  if (!text.startsWith(BRIDGE_PREFIX)) return undefined;
  const v = JSON.parse(text.slice(BRIDGE_PREFIX.length));
  if (!v || v.version !== 1 || !validId(v.origin) || !validId(v.correlationId) ||
    !Number.isSafeInteger(v.expiresAt) || v.expiresAt <= Date.now() || v.expiresAt > Date.now() + 30_000 ||
    !Number.isInteger(v.hops) || v.hops < 0 || v.hops > 4 ||
    !Number.isInteger(v.budget) || v.budget < 1 || v.budget > 256 || !("payload" in v)) {
    throw new Error("Invalid/expired bridge envelope; refresh mapping, TTL, hop and budget at the Host");
  }
  return v;
}
// Callers subscribe before emitting. Missing listener/default-off -> bounded timeout.
// Cancel only stops waiting; it cannot retract a remote admission. Never retry blindly.
export function requestRpc(bus: EventBus, topic: typeof RPC_SEND | typeof RPC_INFO, request: { version: 1; requestId: string }, timeoutMs = 5_000, signal?: AbortSignal): Promise<unknown> {
  if (!validId(request.requestId) || !Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 5_000) return Promise.reject(new Error("invalid_request"));
  return new Promise((resolve, reject) => {
    let done = false;
    let unsubscribe = () => {};
    const finish = (error?: Error, result?: unknown) => {
      if (done) return;
      done = true; clearTimeout(timer); unsubscribe(); signal?.removeEventListener("abort", abort);
      if (error) reject(error); else resolve(result);
    };
    const unknownReceipt = (reason: string) => Object.assign(new Error(`receipt_unknown: ${reason}; query status, do not auto-retry`), {
      code: "receipt_unknown", state: "receipt_unknown", retryable: false,
      next: "Inspect receiver /cross-session-status or same-incarnation wire status; never auto-retry",
    });
    const abort = () => finish(unknownReceipt("aborted waiting"));
    const timer = setTimeout(() => finish(unknownReceipt("missing listener or timeout")), timeoutMs);
    try {
      unsubscribe = bus.on(`${topic}:reply:${request.requestId}`, data => {
        const reply = data as { version?: unknown; requestId?: unknown } | null;
        if (reply?.version === 1 && reply.requestId === request.requestId) finish(undefined, data);
      });
    } catch (error) { finish(error instanceof Error ? error : new Error(String(error))); return; }
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) return abort();
    try { bus.emit(topic, request); } catch (error) { finish(error instanceof Error ? error : new Error(String(error))); }
  });
}
