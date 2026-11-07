import { randomUUID } from "node:crypto";
import { and, eq, like, sql } from "drizzle-orm";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import {
  agents, agentWakeupRequests, authUsers, companies, companyMemberships, createDb, heartbeatRuns, issues,
} from "@paperclipai/db";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "../../__tests__/helpers/embedded-postgres.js";
import { resolveExternalTestDatabaseUrl } from "../../__tests__/helpers/external-test-database.js";
import { createHeartbeatQueueTestDependencies } from "../../__tests__/helpers/heartbeat-queue.js";
import { HttpError } from "../../errors.js";
import { instanceSettingsService } from "../instance-settings.js";
import { createHeartbeatQueue } from "./queue.js";
import {
  createTaskOperationWakeDispatcher, insertTaskOperationWakeIntent, taskOperationDispatchKey,
  type TaskOperationWakeIntentInput,
} from "./task-operation-wake-intents.js";

const external = resolveExternalTestDatabaseUrl("PAPERCLIP_TASK_OPERATION_WAKE_TEST_DATABASE_URL");
const support = external ? { supported: true, reason: undefined } : await getEmbeddedPostgresTestSupport();
if (!support.supported) console.warn(`Task-operation wake database unavailable: ${support.reason}`);

(support.supported ? describe : describe.skip)("task-operation wake intents", () => {
  vi.setConfig({ testTimeout: 60_000 });
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | undefined;
  let db: ReturnType<typeof createDb>;
  const userId = "board-operator";

  beforeAll(async () => {
    if (external) db = createDb(external);
    else {
      temporary = await startEmbeddedPostgresTestDatabase("paperclip-task-operation-wake-");
      db = createDb(temporary.connectionString);
    }
    await db.insert(authUsers).values({ id: userId, name: "Operator", email: `${userId}@example.test`,
      createdAt: new Date(), updatedAt: new Date() });
  });
  afterAll(async () => { await temporary?.cleanup(); });
  beforeEach(() => { vi.restoreAllMocks(); });

  function createQueue() {
    const deps = createHeartbeatQueueTestDependencies(db);
    deps.getAgent.mockImplementation(async id => (await db.select().from(agents).where(eq(agents.id, id)))[0] ?? null);
    deps.instanceSettings.getExperimental.mockImplementation(() => instanceSettingsService(db).getExperimental());
    return { deps, queue: createHeartbeatQueue(db, deps) };
  }

  async function fixture() {
    const [company] = await db.insert(companies).values({ name: "Task operations", issuePrefix: `TO${randomUUID().slice(0, 6)}`,
      defaultResponsibleUserId: userId, requireBoardApprovalForNewAgents: false }).returning();
    await db.insert(companyMemberships).values({ companyId: company.id, principalType: "user", principalId: userId,
      status: "active", membershipRole: "owner" });
    const newAgent = async (name: string) => (await db.insert(agents).values({ companyId: company.id, name,
      adapterType: "process", status: "idle", adapterConfig: { command: process.execPath, args: ["-e", "0"] },
      runtimeConfig: { heartbeat: { enabled: true, intervalSec: 0, wakeOnDemand: true } } }).returning())[0];
    const previous = await newAgent("Previous assignee");
    const agent = await newAgent("New assignee");
    const [issue] = await db.insert(issues).values({ companyId: company.id, title: "Reassigned work", status: "todo",
      assigneeAgentId: previous.id, responsibleUserId: userId }).returning();
    return { company, agent, previous, issue };
  }

  type Fixture = Awaited<ReturnType<typeof fixture>>;

  /** The future CAS caller: transition the task and record its wake in one transaction. */
  async function reassign(f: Fixture, overrides: Partial<TaskOperationWakeIntentInput> = {}, options: { fail?: boolean } = {}) {
    return db.transaction(async tx => {
      const [updated] = await tx.update(issues).set({ assigneeAgentId: f.agent.id,
        statusVersion: sql`${issues.statusVersion} + 1` }).where(eq(issues.id, f.issue.id)).returning();
      const result = await insertTaskOperationWakeIntent(tx as never, {
        companyId: f.company.id, issueId: f.issue.id, operation: "reassign", operationId: randomUUID(),
        expectedAssigneeAgentId: f.agent.id, expectedStatus: "todo", expectedStatusVersion: updated.statusVersion,
        requestedBy: { actorType: "user", actorId: userId }, ...overrides,
      });
      if (options.fail) throw new Error("caller rolled back after the intent write");
      return { ...result, statusVersion: updated.statusVersion };
    });
  }

  const intentRow = async (id: string) => (await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, id)))[0];
  const downstreamRows = (f: Fixture, intentId: string) => db.select().from(agentWakeupRequests).where(and(
    eq(agentWakeupRequests.companyId, f.company.id), eq(agentWakeupRequests.idempotencyKey, taskOperationDispatchKey(intentId))));
  const runsFor = (f: Fixture) => db.select().from(heartbeatRuns).where(eq(heartbeatRuns.companyId, f.company.id));

  it("records the intent only when the caller transaction commits, then dispatches it once with original provenance", async () => {
    const f = await fixture();
    await expect(reassign(f, {}, { fail: true })).rejects.toThrow("caller rolled back");
    expect(await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.companyId, f.company.id))).toEqual([]);
    expect((await db.select().from(issues).where(eq(issues.id, f.issue.id)))[0].assigneeAgentId).toBe(f.previous.id);

    const { intent, statusVersion } = await reassign(f);
    expect(intent).toMatchObject({ status: "queued", agentId: f.agent.id, reason: "task_operation_reassign",
      requestedByActorType: "user", requestedByActorId: userId, runId: null,
      payload: { issueId: f.issue.id, taskOperation: { expectedStatusVersion: statusVersion, expectedStatus: "todo" } } });

    // A fresh queue instance stands in for a restarted process: discovery is
    // a database scan, not process memory.
    const { queue } = createQueue();
    expect(await queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id })).toMatchObject({ dispatched: 1 });
    const [downstream] = await downstreamRows(f, intent.id);
    expect(downstream).toMatchObject({ requestedByActorType: "user", requestedByActorId: userId, reason: "task_operation_reassign" });
    expect(downstream.runId).toBeTruthy();
    expect(await intentRow(intent.id)).toMatchObject({ status: "coalesced", runId: downstream.runId });
    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, downstream.runId!));
    expect(run.contextSnapshot).toMatchObject({ source: "task_operation", taskOperationIntentId: intent.id,
      taskOperation: "reassign", issueId: f.issue.id, taskOperationExpectedStatusVersion: statusVersion });
    expect(run.contextSnapshot?.statusDecisionSource).toBeUndefined();
    expect(run.contextSnapshot?.nativeStatusWakeIntentId).toBeUndefined();

    expect(await createQueue().queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id })).toMatchObject({ scanned: 0 });
    expect(await runsFor(f)).toHaveLength(1);
  });

  it("replays an identical operation after response loss and later state changes without a second wake", async () => {
    const f = await fixture();
    const operationId = randomUUID();
    const first = await reassign(f, { operationId });
    const input: TaskOperationWakeIntentInput = { companyId: f.company.id, issueId: f.issue.id, operation: "reassign",
      operationId, expectedAssigneeAgentId: f.agent.id, expectedStatus: "todo", expectedStatusVersion: first.statusVersion,
      requestedBy: { actorType: "user", actorId: userId } };
    // State evolves after the lost response; the replay still reconciles.
    await db.update(issues).set({ status: "in_progress", statusVersion: sql`${issues.statusVersion} + 1` }).where(eq(issues.id, f.issue.id));
    const replayed = await db.transaction(tx => insertTaskOperationWakeIntent(tx as never, input));
    expect(replayed).toMatchObject({ created: false, intent: { id: first.intent.id } });
    for (const changed of [
      { requestedBy: { actorType: "user" as const, actorId: "someone-else" } },
      { operation: "resume_blocked" as const },
      { expectedStatusVersion: first.statusVersion + 1 },
      { expectedAssigneeAgentId: f.previous.id },
    ]) {
      await expect(db.transaction(tx => insertTaskOperationWakeIntent(tx as never, { ...input, ...changed })))
        .rejects.toMatchObject({ status: 409 });
    }
    // The original intent is now stale: it terminates skipped, and replaying
    // it afterwards returns the skipped intent rather than a second wake.
    await createQueue().queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id });
    expect(await intentRow(first.intent.id)).toMatchObject({ status: "skipped", runId: null });
    expect(await db.transaction(tx => insertTaskOperationWakeIntent(tx as never, input)))
      .toMatchObject({ created: false, intent: { id: first.intent.id, status: "skipped" } });
    expect(await createQueue().queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id })).toMatchObject({ scanned: 0 });
    expect(await runsFor(f)).toHaveLength(0);
    expect(await downstreamRows(f, first.intent.id)).toHaveLength(0);
  });

  it("keeps both task-operation key namespaces unique across skipped and failed rows", async () => {
    const f = await fixture();
    for (const key of [`task-operation:${randomUUID()}`, `task-operation-dispatch:${randomUUID()}`]) {
      await db.insert(agentWakeupRequests).values({ companyId: f.company.id, agentId: f.agent.id, source: "assignment",
        status: "skipped", idempotencyKey: key });
      await expect(db.insert(agentWakeupRequests).values({ companyId: f.company.id, agentId: f.agent.id,
        source: "assignment", status: "queued", idempotencyKey: key })).rejects.toThrow();
      await db.update(agentWakeupRequests).set({ status: "failed" }).where(eq(agentWakeupRequests.idempotencyKey, key));
      await expect(db.insert(agentWakeupRequests).values({ companyId: f.company.id, agentId: f.agent.id,
        source: "assignment", status: "queued", idempotencyKey: key })).rejects.toThrow();
    }
  });

  it("rejects intents that do not match the committed task or that borrow reserved provenance", async () => {
    const f = await fixture();
    const other = await fixture();
    await expect(reassign(f, { expectedStatusVersion: 999 })).rejects.toMatchObject({ status: 409 });
    await expect(reassign(f, { expectedStatus: "in_progress" })).rejects.toMatchObject({ status: 409 });
    await expect(reassign(f, { expectedStatus: "blocked" as never })).rejects.toMatchObject({ status: 422 });
    await expect(reassign(f, { companyId: other.company.id })).rejects.toMatchObject({ status: 409 });
    await expect(reassign(f, { requestedBy: { actorType: "user", actorId: "native-status-committer" } })).rejects.toMatchObject({ status: 422 });
    await expect(reassign(f, { requestedBy: { actorType: "user", actorId: "task-operation-wake-dispatch:x" } })).rejects.toMatchObject({ status: 422 });
    await expect(reassign(f, { requestedBy: { actorType: "system", actorId: "heartbeat" } as never })).rejects.toMatchObject({ status: 422 });
    await expect(reassign(f, { requestedBy: { actorType: "agent", actorId: other.agent.id } })).rejects.toMatchObject({ status: 409 });
    await expect(reassign(f, { extra: true } as never)).rejects.toMatchObject({ status: 422 });
    expect(await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.companyId, f.company.id))).toEqual([]);
    // An agent requester from the same company is valid attribution.
    expect((await reassign(f, { requestedBy: { actorType: "agent", actorId: f.previous.id } })).intent)
      .toMatchObject({ requestedByActorType: "agent", requestedByActorId: f.previous.id });
  });

  it.each([
    ["status version", async (f: Fixture) => { await db.update(issues).set({ statusVersion: sql`${issues.statusVersion} + 1` }).where(eq(issues.id, f.issue.id)); }],
    ["status", async (f: Fixture) => { await db.update(issues).set({ status: "cancelled" }).where(eq(issues.id, f.issue.id)); }],
    ["assignee", async (f: Fixture) => { await db.update(issues).set({ assigneeAgentId: f.previous.id }).where(eq(issues.id, f.issue.id)); }],
    ["paused agent", async (f: Fixture) => { await db.update(agents).set({ status: "paused" }).where(eq(agents.id, f.agent.id)); }],
    ["errored agent", async (f: Fixture) => { await db.update(agents).set({ status: "error" }).where(eq(agents.id, f.agent.id)); }],
    ["unknown agent status", async (f: Fixture) => { await db.update(agents).set({ status: "hibernating" }).where(eq(agents.id, f.agent.id)); }],
    ["unready lifecycle", async (f: Fixture) => { await db.update(agents).set({ lifecycleState: "preparing" }).where(eq(agents.id, f.agent.id)); }],
    ["paused company", async (f: Fixture) => { await db.update(companies).set({ status: "paused" }).where(eq(companies.id, f.company.id)); }],
  ] as const)("terminates a stale or ineligible intent as skipped without a run: %s", async (_name, mutate) => {
    const f = await fixture();
    const { intent } = await reassign(f);
    await mutate(f);
    const result = await createQueue().queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id });
    expect(result).toMatchObject({ skipped: 1, dispatched: 0 });
    expect(await intentRow(intent.id)).toMatchObject({ status: "skipped", runId: null });
    expect(await runsFor(f)).toHaveLength(0);
  });

  it("skips while the task tree is held and treats an unavailable invokability check as ineligible", async () => {
    const f = await fixture();
    const held = await reassign(f);
    const { deps, queue } = createQueue();
    deps.treeControlSvc.getActivePauseHoldGate.mockResolvedValue({ holdId: randomUUID() } as never);
    await queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id });
    expect(await intentRow(held.intent.id)).toMatchObject({ status: "skipped", error: expect.stringContaining("hold") });

    const g = await fixture();
    const unavailable = await reassign(g);
    const second = createQueue();
    second.deps.getAgentInvokability.mockResolvedValue({ invokable: false, message: "agent unavailable" } as never);
    await second.queue.dispatchPendingTaskOperationWakeups({ companyId: g.company.id });
    expect(await intentRow(unavailable.intent.id)).toMatchObject({ status: "skipped", error: "agent unavailable" });
    expect([...await runsFor(f), ...await runsFor(g)]).toHaveLength(0);
  });

  it("waits behind the task's current execution instead of deferring a wake that skips the version check", async () => {
    const f = await fixture();
    const { intent } = await reassign(f);
    const [current] = await db.insert(heartbeatRuns).values({ companyId: f.company.id, agentId: f.previous.id,
      status: "running", contextSnapshot: { issueId: f.issue.id }, responsibleUserId: userId }).returning();
    await db.update(issues).set({ executionRunId: current.id }).where(eq(issues.id, f.issue.id));
    const deadlines: number[] = [];
    const { queue } = createQueue();
    expect(await queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id, onDeferredDeadline: at => deadlines.push(at) }))
      .toMatchObject({ deferred: 1, dispatched: 0 });
    expect(deadlines).toHaveLength(1);
    expect(await intentRow(intent.id)).toMatchObject({ status: "queued", claimedAt: null, error: expect.stringContaining("current execution") });
    expect(await downstreamRows(f, intent.id)).toHaveLength(0);
    // After the backoff, a finished execution lets the same intent dispatch.
    await db.update(heartbeatRuns).set({ status: "succeeded", finishedAt: new Date() }).where(eq(heartbeatRuns.id, current.id));
    await db.update(issues).set({ executionRunId: null }).where(eq(issues.id, f.issue.id));
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 31_000) }).where(eq(agentWakeupRequests.id, intent.id));
    expect(await queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id })).toMatchObject({ dispatched: 1 });
    expect((await runsFor(f)).filter(run => run.contextSnapshot?.taskOperationIntentId === intent.id)).toHaveLength(1);
  });

  it("withdraws a downstream admission deferred behind another execution and never runs it", async () => {
    const f = await fixture();
    const { intent } = await reassign(f);
    await db.update(agentWakeupRequests).set({ status: "claimed", claimedAt: new Date(Date.now() - 120_000) })
      .where(eq(agentWakeupRequests.id, intent.id));
    const [deferred] = await db.insert(agentWakeupRequests).values({ companyId: f.company.id, agentId: f.agent.id,
      source: "assignment", reason: "task_operation_reassign", status: "deferred_issue_execution",
      requestedByActorType: "user", requestedByActorId: userId, idempotencyKey: taskOperationDispatchKey(intent.id),
      payload: { issueId: f.issue.id } }).returning();
    await createQueue().queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id });
    expect(await intentRow(deferred.id)).toMatchObject({ status: "skipped", runId: null });
    expect(await intentRow(intent.id)).toMatchObject({ status: "skipped", runId: null });
    expect(await runsFor(f)).toHaveLength(0);
  });

  it("does not report delivery for a downstream admission that has no run yet", async () => {
    const f = await fixture();
    const { intent } = await reassign(f);
    const [pending] = await db.insert(agentWakeupRequests).values({ companyId: f.company.id, agentId: f.agent.id,
      source: "assignment", status: "queued", requestedByActorType: "user", requestedByActorId: userId,
      idempotencyKey: taskOperationDispatchKey(intent.id), payload: { issueId: f.issue.id } }).returning();
    const result = await createQueue().queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id });
    expect(result).toMatchObject({ dispatched: 0, deferred: 1 });
    expect(await intentRow(intent.id)).toMatchObject({ status: "queued", runId: null });
    expect(await runsFor(f)).toHaveLength(0);
    // Once that one admission binds a run, the intent links to it.
    const [run] = await db.insert(heartbeatRuns).values({ companyId: f.company.id, agentId: f.agent.id, status: "queued",
      contextSnapshot: { issueId: f.issue.id }, responsibleUserId: userId }).returning();
    await db.update(agentWakeupRequests).set({ runId: run.id }).where(eq(agentWakeupRequests.id, pending.id));
    await createQueue().queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id });
    expect(await intentRow(intent.id)).toMatchObject({ status: "coalesced", runId: run.id });
  });

  it("recovers a crash after the downstream wake was written but before the intent was linked", async () => {
    const f = await fixture();
    const { intent } = await reassign(f);
    const { queue } = createQueue();
    // Crash window: admission committed, then the process died before linking.
    const crashed = createTaskOperationWakeDispatcher(db, {
      enqueueWakeup: async (agentId, opts) => {
        await queue.enqueueWakeup(agentId, opts);
        throw new Error("process died after admission");
      },
      getAgentInvokability: async () => ({ invokable: true }),
      getActivePauseHoldGate: async () => null,
    });
    // Lost response: the same pass links the admission it wrote via the dispatch key.
    await crashed.dispatch({ companyId: f.company.id });
    expect((await intentRow(intent.id)).status).toBe("coalesced");
    // Process crash: the admission row exists but the intent was never linked.
    await db.update(agentWakeupRequests).set({ status: "claimed", runId: null, claimedAt: new Date(Date.now() - 120_000),
      finishedAt: null }).where(eq(agentWakeupRequests.id, intent.id));
    const restarted = createQueue().queue;
    expect(await restarted.dispatchPendingTaskOperationWakeups({ companyId: f.company.id })).toMatchObject({ recovered: 1, dispatched: 0 });
    const [downstream] = await downstreamRows(f, intent.id);
    expect(await intentRow(intent.id)).toMatchObject({ status: "coalesced", runId: downstream.runId });
    expect(await runsFor(f)).toHaveLength(1);
  });

  it("dispatches once under concurrent dispatchers", async () => {
    const f = await fixture();
    const { intent } = await reassign(f);
    const results = await Promise.all([createQueue(), createQueue(), createQueue()]
      .map(({ queue }) => queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id })));
    expect(results.reduce((sum, result) => sum + result.dispatched, 0)).toBe(1);
    expect(await downstreamRows(f, intent.id)).toHaveLength(1);
    expect(await runsFor(f)).toHaveLength(1);
    expect((await intentRow(intent.id)).status).toBe("coalesced");
  });

  it("fences a stale dispatcher after a claim takeover so it cannot admit or overwrite the outcome", async () => {
    const f = await fixture();
    const { intent } = await reassign(f);
    const { queue } = createQueue();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let entered!: () => void;
    const started = new Promise<void>(resolve => { entered = resolve; });
    const stale = createTaskOperationWakeDispatcher(db, {
      enqueueWakeup: async (agentId, opts) => { entered(); await gate; return queue.enqueueWakeup(agentId, opts); },
      getAgentInvokability: async () => ({ invokable: true }),
      getActivePauseHoldGate: async () => null,
    });
    const staleRun = stale.dispatch({ companyId: f.company.id });
    await started;
    const staleClaim = await intentRow(intent.id);
    expect(staleClaim.status).toBe("claimed");
    await new Promise(resolve => setTimeout(resolve, 1_100));
    const takeover = await createQueue().queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id, staleClaimMs: 1_000 });
    expect(takeover).toMatchObject({ dispatched: 1 });
    const settled = await intentRow(intent.id);
    release();
    await staleRun;
    expect(await intentRow(intent.id)).toMatchObject({ status: "coalesced", runId: settled.runId, error: null });
    expect(await downstreamRows(f, intent.id)).toHaveLength(1);
    expect(await runsFor(f)).toHaveLength(1);
  });

  it("skips on a deterministic admission refusal and fails after bounded transient errors", async () => {
    const f = await fixture();
    const refused = await reassign(f);
    const refusing = createTaskOperationWakeDispatcher(db, {
      enqueueWakeup: async () => { throw new HttpError(409, "budget blocked"); },
      getAgentInvokability: async () => ({ invokable: true }),
      getActivePauseHoldGate: async () => null,
    });
    await refusing.dispatch({ companyId: f.company.id });
    expect(await intentRow(refused.intent.id)).toMatchObject({ status: "skipped", error: expect.stringContaining("budget blocked") });

    const g = await fixture();
    const transient = await reassign(g);
    const errors: unknown[] = [];
    const flaky = createTaskOperationWakeDispatcher(db, {
      enqueueWakeup: async () => { throw new Error("database briefly unavailable"); },
      getAgentInvokability: async () => ({ invokable: true }),
      getActivePauseHoldGate: async () => null,
    });
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      await flaky.dispatch({ companyId: g.company.id, onError: error => errors.push(error) });
      const row = await intentRow(transient.intent.id);
      if (attempt < 5) {
        expect(row).toMatchObject({ status: "queued", error: "database briefly unavailable" });
        // A second pass inside the backoff does not retry early.
        await flaky.dispatch({ companyId: g.company.id });
        expect(errors).toHaveLength(attempt);
        await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 31_000) })
          .where(eq(agentWakeupRequests.id, transient.intent.id));
      } else expect(row).toMatchObject({ status: "failed" });
    }
    expect(await createQueue().queue.dispatchPendingTaskOperationWakeups({ companyId: g.company.id })).toMatchObject({ scanned: 0 });
    expect([...await runsFor(f), ...await runsFor(g)]).toHaveLength(0);
  });

  it("fails a malformed row in the reserved namespace and rejects generic callers that spoof task-operation origin", async () => {
    const f = await fixture();
    const [forged] = await db.insert(agentWakeupRequests).values({ companyId: f.company.id, agentId: f.agent.id,
      source: "assignment", reason: "task_operation_reassign", status: "queued", requestedByActorType: "system",
      requestedByActorId: "native-status-committer", idempotencyKey: `task-operation:${randomUUID()}`,
      payload: { issueId: f.issue.id } }).returning();
    const { queue } = createQueue();
    await queue.dispatchPendingTaskOperationWakeups({ companyId: f.company.id });
    expect(await intentRow(forged.id)).toMatchObject({ status: "failed", runId: null });

    const { intent } = await reassign(f);
    const generic = { source: "on_demand" as const, reason: "manual", requestedByActorType: "user" as const,
      requestedByActorId: userId, payload: { issueId: f.issue.id } };
    await expect(queue.enqueueWakeup(f.agent.id, { ...generic, idempotencyKey: `task-operation:${randomUUID()}` }))
      .rejects.toMatchObject({ status: 409 });
    await expect(queue.enqueueWakeup(f.agent.id, { ...generic, idempotencyKey: taskOperationDispatchKey(intent.id) }))
      .rejects.toMatchObject({ status: 409 });
    await expect(queue.enqueueWakeup(f.agent.id, { ...generic, contextSnapshot: { source: "task_operation" } }))
      .rejects.toMatchObject({ status: 409 });
    // Even the internal marker needs a claimed, matching intent.
    await expect(queue.enqueueWakeup(f.agent.id, { ...generic, taskOperationIntentId: intent.id,
      idempotencyKey: taskOperationDispatchKey(intent.id),
      contextSnapshot: { source: "task_operation", taskOperationIntentId: intent.id } })).rejects.toMatchObject({ status: 409 });
    expect(await db.select().from(agentWakeupRequests).where(and(eq(agentWakeupRequests.companyId, f.company.id),
      like(agentWakeupRequests.idempotencyKey, "task-operation-dispatch:%")))).toHaveLength(0);
    expect(await runsFor(f)).toHaveLength(0);
    expect(await intentRow(intent.id)).toMatchObject({ status: "queued" });
  });
});
