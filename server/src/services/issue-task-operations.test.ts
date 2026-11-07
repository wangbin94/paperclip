import { randomUUID } from "node:crypto";
import { and, eq } from "drizzle-orm";
import {
  activityLog,
  agents,
  agentWakeupRequests,
  approvals,
  companies,
  createDb,
  heartbeatRuns,
  issueApprovals,
  issues,
  issueTaskOperationReceipts,
  issueTreeHolds,
} from "@paperclipai/db";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { startEmbeddedPostgresTestDatabase } from "../__tests__/helpers/embedded-postgres.js";
import { issueTaskOperationService, taskOperationWakeKey } from "./issue-task-operations.js";

describe("issue task operations: reassign unstarted task", () => {
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  beforeAll(async () => {
    temporary = await startEmbeddedPostgresTestDatabase("issue-task-operations-");
    db = createDb(temporary.connectionString);
  });
  afterAll(async () => { await temporary?.cleanup(); });

  const actor = { actorType: "user" as const, actorId: "board-user-1" };

  /** Saves the wake the way queue admission does, so the unique key index applies. */
  function recordingWakeup(options: { failTimes?: number; delayMs?: number } = {}) {
    const calls: string[] = [];
    let failures = options.failTimes ?? 0;
    const wakeup = async (agentId: string, opts: { idempotencyKey: string; payload: Record<string, unknown>;
      reason: string; source: string; requestedByActorType: string; requestedByActorId: string }) => {
      calls.push(opts.idempotencyKey);
      if (options.delayMs) await new Promise(resolve => setTimeout(resolve, options.delayMs));
      if (failures > 0) {
        failures -= 1;
        throw new Error("simulated lost admission");
      }
      const [company] = await db.select({ companyId: agents.companyId }).from(agents).where(eq(agents.id, agentId));
      const [row] = await db.insert(agentWakeupRequests).values({
        companyId: company!.companyId, agentId, source: opts.source, reason: opts.reason, payload: opts.payload,
        status: "queued", requestedByActorType: opts.requestedByActorType,
        requestedByActorId: opts.requestedByActorId, idempotencyKey: opts.idempotencyKey,
      }).returning();
      return row;
    };
    return { calls, wakeup };
  }

  async function fixture(input: { status?: string; agentStatus?: string; lifecycleState?: string } = {}) {
    const companyId = randomUUID();
    await db.insert(companies).values({ id: companyId, name: "CAS", issuePrefix: `C${companyId.slice(0, 6)}` });
    const [from] = await db.insert(agents).values({ companyId, name: "From", status: "idle" }).returning();
    const [to] = await db.insert(agents).values({ companyId, name: "To", status: input.agentStatus ?? "idle",
      lifecycleState: (input.lifecycleState ?? "ready") as "ready" }).returning();
    const [parent] = await db.insert(issues).values({ companyId, title: "Parent", status: "todo" }).returning();
    const [issue] = await db.insert(issues).values({ companyId, title: "Task", status: input.status ?? "todo",
      assigneeAgentId: from!.id, parentId: parent!.id }).returning();
    return { companyId, fromId: from!.id, toId: to!.id, parentId: parent!.id, issue: issue! };
  }

  function request(f: Awaited<ReturnType<typeof fixture>>, overrides: Record<string, unknown> = {}) {
    return { requestId: `req-${randomUUID()}`, assigneeAgentId: f.toId, expectedStatusVersion: f.issue.statusVersion,
      expectedAssigneeAgentId: f.fromId, ...overrides } as {
      requestId: string; assigneeAgentId: string; expectedStatusVersion: number; expectedAssigneeAgentId: string | null };
  }

  const wakesFor = (companyId: string, issueId: string) => db.select().from(agentWakeupRequests).where(and(
    eq(agentWakeupRequests.companyId, companyId)));
  const receiptsFor = (issueId: string) =>
    db.select().from(issueTaskOperationReceipts).where(eq(issueTaskOperationReceipts.issueId, issueId));
  const issueRow = async (issueId: string) => (await db.select().from(issues).where(eq(issues.id, issueId)))[0]!;

  it("reassigns a todo task, admits exactly one start wake, and replays the identical receipt", async () => {
    const f = await fixture();
    const wake = recordingWakeup();
    const svc = issueTaskOperationService(db, { wakeup: wake.wakeup as never });
    const body = request(f);
    const first = await svc.reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(first.replayed).toBe(false);
    expect(first.receipt).toMatchObject({ operation: "reassign_unstarted", fromAssigneeAgentId: f.fromId,
      toAssigneeAgentId: f.toId, issueStatus: "todo", wakeRequired: true,
      wakeIdempotencyKey: taskOperationWakeKey(first.receipt.receiptId) });
    expect(first.wake.state).toBe("admitted");
    expect((await issueRow(f.issue.id)).assigneeAgentId).toBe(f.toId);

    // Lost reply: the caller repeats the request. No second wake is queued.
    const replay = await svc.reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(replay.replayed).toBe(true);
    expect(replay.receipt).toEqual(first.receipt);
    expect(replay.wake).toEqual(first.wake);
    expect(wake.calls).toHaveLength(1);
    expect(await wakesFor(f.companyId, f.issue.id)).toHaveLength(1);

    // Status evolves afterwards; a replay still returns the persisted receipt.
    await db.update(issues).set({ status: "in_progress", statusVersion: 7 }).where(eq(issues.id, f.issue.id));
    const late = await svc.reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(late.receipt).toEqual(first.receipt);
    expect(wake.calls).toHaveLength(1);
    expect(await receiptsFor(f.issue.id)).toHaveLength(1);
    const activity = await db.select().from(activityLog).where(and(eq(activityLog.entityId, f.issue.id),
      eq(activityLog.action, "issue.task_operation.reassigned")));
    expect(activity).toHaveLength(1);
  });

  it("does not wake a backlog task", async () => {
    const f = await fixture({ status: "backlog" });
    const wake = recordingWakeup();
    const result = await issueTaskOperationService(db, { wakeup: wake.wakeup as never })
      .reassignUnstarted(f.companyId, f.issue.id, actor, request(f));
    expect(result.receipt).toMatchObject({ issueStatus: "backlog", wakeRequired: false, wakeIdempotencyKey: null });
    expect(result.wake.state).toBe("not_required");
    expect(wake.calls).toHaveLength(0);
  });

  it("reports an uncertain wake honestly and completes it once on replay", async () => {
    const f = await fixture();
    const wake = recordingWakeup({ failTimes: 1 });
    const svc = issueTaskOperationService(db, { wakeup: wake.wakeup as never });
    const body = request(f);
    const first = await svc.reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(first.wake).toMatchObject({ state: "not_admitted", error: "simulated lost admission" });
    const second = await svc.reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(second.receipt).toEqual(first.receipt);
    expect(second.wake.state).toBe("admitted");
    const third = await svc.reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(third.wake).toEqual(second.wake);
    expect(wake.calls).toHaveLength(2);
    expect(await wakesFor(f.companyId, f.issue.id)).toHaveLength(1);
  });

  it("does not re-admit a lost wake once the task has moved on", async () => {
    const f = await fixture();
    const wake = recordingWakeup({ failTimes: 1 });
    const svc = issueTaskOperationService(db, { wakeup: wake.wakeup as never });
    const body = request(f);
    await svc.reassignUnstarted(f.companyId, f.issue.id, actor, body);
    await db.update(issues).set({ status: "blocked", statusVersion: f.issue.statusVersion + 1 })
      .where(eq(issues.id, f.issue.id));
    const replay = await svc.reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(replay.wake.state).toBe("not_admitted");
    expect(wake.calls).toHaveLength(1);
  });

  it("rejects a reused requestId with a different payload", async () => {
    const f = await fixture();
    const svc = issueTaskOperationService(db, { wakeup: recordingWakeup().wakeup as never });
    const body = request(f);
    await svc.reassignUnstarted(f.companyId, f.issue.id, actor, body);
    await expect(svc.reassignUnstarted(f.companyId, f.issue.id, actor, { ...body, expectedAssigneeAgentId: null }))
      .rejects.toMatchObject({ status: 409, details: { code: "request_id_conflict" } });
    // The receipt is per actor: another actor's identical requestId is a new decision, judged on its own guards.
    await expect(svc.reassignUnstarted(f.companyId, f.issue.id, { actorType: "user", actorId: "other" }, body))
      .rejects.toMatchObject({ status: 409, details: { code: "stale_status_version" } });
  });

  it("fails stale version and stale assignee with 409 and changes nothing", async () => {
    const f = await fixture();
    const wake = recordingWakeup();
    const svc = issueTaskOperationService(db, { wakeup: wake.wakeup as never });
    await expect(svc.reassignUnstarted(f.companyId, f.issue.id, actor,
      request(f, { expectedStatusVersion: f.issue.statusVersion + 1 })))
      .rejects.toMatchObject({ status: 409, details: { code: "stale_status_version" } });
    await expect(svc.reassignUnstarted(f.companyId, f.issue.id, actor, request(f, { expectedAssigneeAgentId: null })))
      .rejects.toMatchObject({ status: 409, details: { code: "stale_assignee" } });
    expect((await issueRow(f.issue.id)).assigneeAgentId).toBe(f.fromId);
    expect(await receiptsFor(f.issue.id)).toHaveLength(0);
    expect(wake.calls).toHaveLength(0);
  });

  it.each(["in_progress", "in_review", "done", "blocked", "cancelled"])("refuses %s work", async (status) => {
    const f = await fixture({ status });
    await expect(issueTaskOperationService(db, { wakeup: recordingWakeup().wakeup as never })
      .reassignUnstarted(f.companyId, f.issue.id, actor, request(f)))
      .rejects.toMatchObject({ status: 409, details: { code: "issue_not_unstarted" } });
  });

  it("refuses tasks with unsettled runs, pending wakes, approvals, holds, review state, or claims", async () => {
    const cases: Array<[string, (f: Awaited<ReturnType<typeof fixture>>) => Promise<unknown>]> = [
      ["issue_run_unsettled", f => db.insert(heartbeatRuns).values({ companyId: f.companyId, agentId: f.fromId,
        status: "scheduled_retry", contextSnapshot: { issueId: f.issue.id } })],
      ["issue_run_unsettled", f => db.insert(heartbeatRuns).values({ companyId: f.companyId, agentId: f.fromId,
        status: "interrupted", issueId: f.issue.id })],
      ["issue_wake_pending", f => db.insert(agentWakeupRequests).values({ companyId: f.companyId, agentId: f.fromId,
        source: "assignment", status: "deferred_issue_execution", payload: { issueId: f.issue.id } })],
      ["issue_approval_pending", async f => {
        const [approval] = await db.insert(approvals).values({ companyId: f.companyId, type: "hire_agent",
          payload: {}, status: "pending" }).returning();
        await db.insert(issueApprovals).values({ companyId: f.companyId, issueId: f.issue.id, approvalId: approval!.id });
      }],
      ["issue_tree_held", f => db.insert(issueTreeHolds).values({ companyId: f.companyId, rootIssueId: f.parentId,
        mode: "pause", status: "active" })],
      ["issue_review_or_approval_state", f => db.update(issues).set({ executionState: { stage: "review" } })
        .where(eq(issues.id, f.issue.id))],
      ["issue_execution_started", f => db.update(issues).set({ startedAt: new Date() }).where(eq(issues.id, f.issue.id))],
    ];
    for (const [code, arrange] of cases) {
      const f = await fixture();
      await arrange(f);
      const wake = recordingWakeup();
      await expect(issueTaskOperationService(db, { wakeup: wake.wakeup as never })
        .reassignUnstarted(f.companyId, f.issue.id, actor, request(f)))
        .rejects.toMatchObject({ status: 409, details: { code } });
      expect((await issueRow(f.issue.id)).assigneeAgentId).toBe(f.fromId);
      expect(await receiptsFor(f.issue.id)).toHaveLength(0);
      expect(wake.calls).toHaveLength(0);
    }
  });

  it("refuses paused, unready, and other-company assignees", async () => {
    for (const options of [{ agentStatus: "paused" }, { agentStatus: "error" }, { lifecycleState: "pausing" },
      { lifecycleState: "pending_approval" }]) {
      const f = await fixture(options);
      await expect(issueTaskOperationService(db, { wakeup: recordingWakeup().wakeup as never })
        .reassignUnstarted(f.companyId, f.issue.id, actor, request(f)))
        .rejects.toMatchObject({ status: 409, details: { code: "assignee_unavailable" } });
    }
    const f = await fixture();
    const other = await fixture();
    await expect(issueTaskOperationService(db, { wakeup: recordingWakeup().wakeup as never })
      .reassignUnstarted(f.companyId, f.issue.id, actor, request(f, { assigneeAgentId: other.toId })))
      .rejects.toMatchObject({ status: 422, details: { code: "assignee_not_found" } });
    // A task of another company is not found, even with valid guards.
    await expect(issueTaskOperationService(db, { wakeup: recordingWakeup().wakeup as never })
      .reassignUnstarted(other.companyId, f.issue.id, actor, request(f)))
      .rejects.toMatchObject({ status: 404 });
  });

  it("serializes concurrent identical requests into one decision and one wake", async () => {
    const f = await fixture();
    const wake = recordingWakeup({ delayMs: 20 });
    const svc = issueTaskOperationService(db, { wakeup: wake.wakeup as never });
    const body = request(f);
    const results = await Promise.all(Array.from({ length: 6 }, () =>
      svc.reassignUnstarted(f.companyId, f.issue.id, actor, body)));
    expect(results.filter(r => !r.replayed)).toHaveLength(1);
    expect(new Set(results.map(r => r.receipt.receiptId)).size).toBe(1);
    expect(results.every(r => r.wake.state === "admitted")).toBe(true);
    expect(await receiptsFor(f.issue.id)).toHaveLength(1);
    expect(await wakesFor(f.companyId, f.issue.id)).toHaveLength(1);
  });

  it("lets exactly one of two competing reassignments win the compare-and-set", async () => {
    const f = await fixture();
    const [third] = await db.insert(agents).values({ companyId: f.companyId, name: "Third", status: "idle" }).returning();
    const svc = issueTaskOperationService(db, { wakeup: recordingWakeup().wakeup as never });
    const outcomes = await Promise.allSettled([
      svc.reassignUnstarted(f.companyId, f.issue.id, actor, request(f)),
      svc.reassignUnstarted(f.companyId, f.issue.id, actor, request(f, { assigneeAgentId: third!.id })),
    ]);
    expect(outcomes.filter(o => o.status === "fulfilled")).toHaveLength(1);
    const rejected = outcomes.find(o => o.status === "rejected") as PromiseRejectedResult;
    expect(rejected.reason).toMatchObject({ status: 409 });
    expect(["stale_status_version", "stale_assignee", "issue_wake_pending"]).toContain(rejected.reason.details.code);
    expect(await receiptsFor(f.issue.id)).toHaveLength(1);
    expect(await wakesFor(f.companyId, f.issue.id)).toHaveLength(1);
  });

  it("never re-admits a skipped wake on replay of one receipt", async () => {
    const f = await fixture();
    const wake = recordingWakeup();
    const svc = issueTaskOperationService(db, { wakeup: wake.wakeup as never });
    const body = request(f);
    const first = await svc.reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(first.wake.state).toBe("admitted");
    await db.update(agentWakeupRequests).set({ status: "skipped" })
      .where(eq(agentWakeupRequests.idempotencyKey, first.receipt.wakeIdempotencyKey!));
    const replay = await svc.reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(replay.replayed).toBe(true);
    expect(replay.wake).toMatchObject({ state: "not_admitted", status: "skipped" });
    expect(wake.calls).toHaveLength(1);
    expect(await wakesFor(f.companyId, f.issue.id)).toHaveLength(1);
  });

  it("keeps a duplicate admission of the same wake key out of the queue", async () => {
    const f = await fixture();
    const key = taskOperationWakeKey(randomUUID());
    const values = { companyId: f.companyId, agentId: f.toId, source: "assignment", status: "queued",
      idempotencyKey: key, payload: { issueId: f.issue.id } };
    await db.insert(agentWakeupRequests).values(values);
    await expect(db.insert(agentWakeupRequests).values(values)).rejects.toThrow();
    // A skipped wake still consumes this request's one permitted admission.
    // Reuse requires a new explicit operation, not a replay of the same receipt.
    await db.update(agentWakeupRequests).set({ status: "skipped" }).where(eq(agentWakeupRequests.idempotencyKey, key));
    await expect(db.insert(agentWakeupRequests).values(values)).rejects.toThrow();
  });
});
