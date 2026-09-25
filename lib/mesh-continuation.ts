// Trusted in-process EventBus protocol, not a wire/RPC or customType authority.
// No capabilities are persisted or exposed to the model. Mesh owns the fixed
// plan and verifies the exact successful run epoch; Cross owns turn authority.
export const MESH_CONTINUATION = "pi-mesh:continuation:issue:v1";
const TTL = 60 * 60 * 1000;
const LIMIT = 16;
type Grant = { valid: () => boolean; runId: string; scope: Scope; adaptive: boolean; consume: (id: string) => void };
type Scope = { sessionId: string; cwd: string };
export type MeshPermit = { bind(runId: string): void; complete(details: object): boolean; claim(callId: string): boolean; advance(callId: string, runId: string): boolean };
export class MeshContinuations {
  private generation = 0;
  private issued = 0;
  private turn = 0;
  private calls = new Map<string, { input: string; scope: Scope }>();
  private deliveries = new WeakMap<object, Grant[]>();
  private active: Grant[] = [];
  revoke(): void { this.generation++; this.issued = 0; this.calls.clear(); this.deliveries = new WeakMap(); this.active = []; }
  settle(): void { this.turn++; this.active = []; this.calls.clear(); }
  note(callId: string, input: Record<string, unknown>, scope: Scope): void {
    const adaptive = input.autoContinuation;
    if (typeof callId !== "string" || !callId || input.action !== "run" || input.async === false || this.calls.size >= LIMIT ||
      !(Array.isArray(input.continuationTasks) && input.continuationTasks.length && !adaptive ||
        adaptive && !input.continuationTasks && typeof adaptive === "object" && ((adaptive as any).maxRuns === undefined || Number.isInteger((adaptive as any).maxRuns) && (adaptive as any).maxRuns >= 1) &&
        Array.isArray(input.tasks) && input.tasks.length === 1 && typeof (input.tasks[0] as any)?.task === "string" && (input.tasks[0] as any).task.length <= 8192)) return;
    this.calls.set(callId, { input: JSON.stringify(input), scope: { ...scope } });
  }
  issue(request: any): void {
    const call = this.calls.get(request?.callId);
    if (!call) return;
    this.calls.delete(request.callId);
    if (request.version !== 1 || typeof request.reply !== "function" || request.input !== call.input || request.sessionId !== call.scope.sessionId || request.cwd !== call.scope.cwd || this.issued >= LIMIT) return;
    this.issued++;
    const requested = JSON.parse(call.input);
    const adaptive = !!requested.autoContinuation;
    const generation = this.generation, expires = adaptive ? Infinity : Date.now() + TTL;
    let remaining = adaptive ? requested.autoContinuation.maxRuns ?? Infinity : 1;
    let delivered = false, used = false, execution: string | undefined, executionTurn: number | undefined, claimed: string | undefined;
    const valid = () => generation === this.generation && Date.now() < expires && !used && remaining > 0;
    const grant = { valid, runId: "", scope: call.scope, adaptive, consume: (id: string) => { used = true; execution = id; executionTurn = this.turn; } };
    request.reply(Object.freeze({
      bind: (runId: string) => { if (valid() && !grant.runId && typeof runId === "string") grant.runId = runId; },
      complete: (details: object) => {
        if (!valid() || !grant.runId || delivered || !details || typeof details !== "object") return false;
        delivered = true;
        this.deliveries.set(details, [...(this.deliveries.get(details) ?? []), grant]);
        return true;
      },
      claim: (id: string) => {
        if (!used || execution === undefined || executionTurn !== this.turn || generation !== this.generation || Date.now() >= expires || execution !== id) return false;
        execution = undefined; claimed = id;
        return true;
      },
      advance: (id: string, runId: string) => {
        if (!adaptive || claimed !== id || generation !== this.generation || Date.now() >= expires || typeof runId !== "string" || !runId || runId === grant.runId) return false;
        claimed = undefined; remaining--; grant.runId = runId; delivered = false; used = false;
        return true;
      },
    } satisfies MeshPermit));
  }
  message(details: unknown, eligible: boolean, scope: Scope): boolean {
    this.turn++;
    const grants = details && typeof details === "object" ? this.deliveries.get(details) ?? [] : [];
    if (details && typeof details === "object") this.deliveries.delete(details); // One SDK delivery, including rejected deliveries.
    const admitted = eligible ? grants.filter(grant => grant.valid() && grant.scope.sessionId === scope.sessionId && grant.scope.cwd === scope.cwd) : [];
    // SDK followUpMode=all emits independent messages before the next model call.
    // Accumulate only genuine deliveries; unrelated/replayed/rejected messages
    // still fence the whole active batch, as do settle/revoke/new user input.
    this.active = admitted.length ? [...this.active.filter(grant => grant.valid() && grant.scope.sessionId === scope.sessionId && grant.scope.cwd === scope.cwd), ...admitted] : [];
    return admitted.length > 0;
  }
  allow(callId: string, input: Record<string, unknown>, scope: Scope): boolean {
    const grant = this.active.find(grant => grant.runId === input.runId);
    if (typeof callId !== "string" || !callId || !grant?.valid() || grant.scope.sessionId !== scope.sessionId || grant.scope.cwd !== scope.cwd || input.action !== "continue" || input.runId !== grant.runId) return false;
    if (grant.adaptive ? !["repair", "verify", "load-test"].includes(String(input.phase)) || Object.keys(input).some(k => !["action", "runId", "phase"].includes(k)) : Object.keys(input).some(k => k !== "action" && k !== "runId")) return false;
    this.active = this.active.filter(item => item !== grant);
    grant.consume(callId); // Charge before any other extension/tool can fail; no retry/refund.
    return true;
  }
}
