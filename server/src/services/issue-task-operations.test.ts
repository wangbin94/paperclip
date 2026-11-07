import { randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
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
import { issueTaskOperationService } from "./issue-task-operations.js";

describe("issue task operations: reassign unstarted task", () => {
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  beforeAll(async () => {
    temporary = await startEmbeddedPostgresTestDatabase("issue-task-operations-");
    db = createDb(temporary.connectionString);
  });
  afterAll(async () => { await temporary?.cleanup(); });

  const actor = { actorType: "user" as const, actorId: "board-user-1" };
  const svc = () => issueTaskOperationService(db);

  async function fixture(input: { status?: string; agentStatus?: string; lifecycleState?: string } = {}) {
    const companyId = randomUUID();
    await db.insert(companies).values({ id: companyId, name: "CAS", issuePrefix: `C${companyId.slice(0, 6)}` });
    const [from] = await db.insert(agents).values({ companyId, name: "From", status: "idle" }).returning();
    const [to] = await db.insert(agents).values({ companyId, name: "To", status: input.agentStatus ?? "idle",
      lifecycleState: (input.lifecycleState ?? "ready") as "ready" }).returning();
    const [parent] = await db.insert(issues).values({ companyId, title: "Parent", status: "todo" }).returning();
    const [issue] = await db.insert(issues).values({ companyId, title: "Task", status: input.status ?? "backlog",
      assigneeAgentId: from!.id, parentId: parent!.id }).returning();
    return { companyId, fromId: from!.id, toId: to!.id, parentId: parent!.id, issue: issue! };
  }

  function request(f: Awaited<ReturnType<typeof fixture>>, overrides: Record<string, unknown> = {}) {
    return { requestId: `req-${randomUUID()}`, assigneeAgentId: f.toId, expectedStatusVersion: f.issue.statusVersion,
      expectedAssigneeAgentId: f.fromId, ...overrides } as {
      requestId: string; assigneeAgentId: string; expectedStatusVersion: number; expectedAssigneeAgentId: string | null };
  }

  const receiptsFor = (issueId: string) =>
    db.select().from(issueTaskOperationReceipts).where(eq(issueTaskOperationReceipts.issueId, issueId));
  const issueRow = async (issueId: string) => (await db.select().from(issues).where(eq(issues.id, issueId)))[0]!;
  const activityFor = (issueId: string) => db.select().from(activityLog).where(and(eq(activityLog.entityId, issueId),
    eq(activityLog.action, "issue.task_operation.reassigned")));
  /** The operation never queues or starts work: no wake intents and no runs in the company. */
  async function expectNoExecution(companyId: string) {
    expect(await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.companyId, companyId))).toHaveLength(0);
    expect(await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.companyId, companyId))).toHaveLength(0);
  }
  async function expectUnchanged(f: Awaited<ReturnType<typeof fixture>>) {
    const row = await issueRow(f.issue.id);
    expect(row.assigneeAgentId).toBe(f.fromId);
    expect(row.statusVersion).toBe(f.issue.statusVersion);
    expect(await receiptsFor(f.issue.id)).toHaveLength(0);
    expect(await activityFor(f.issue.id)).toHaveLength(0);
  }

  it("reassigns a backlog task without any wake and replays the identical receipt", async () => {
    const f = await fixture();
    const body = request(f);
    const first = await svc().reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(first.replayed).toBe(false);
    expect(first.receipt).toMatchObject({ operation: "reassign_unstarted", fromAssigneeAgentId: f.fromId,
      toAssigneeAgentId: f.toId, issueStatus: "backlog", startWake: "not_required", requestId: body.requestId });
    const row = await issueRow(f.issue.id);
    expect(row).toMatchObject({ assigneeAgentId: f.toId, status: "backlog", statusVersion: first.receipt.statusVersion });
    await expectNoExecution(f.companyId);

    // Lost reply: the caller repeats the request and gets the stored decision.
    const replay = await svc().reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(replay).toEqual({ receipt: first.receipt, replayed: true });

    // Status evolves afterwards (even to todo); a replay still returns the
    // persisted receipt and does not re-decide or wake anyone.
    await db.update(issues).set({ status: "todo" }).where(eq(issues.id, f.issue.id));
    const late = await svc().reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(late).toEqual({ receipt: first.receipt, replayed: true });
    expect(await receiptsFor(f.issue.id)).toHaveLength(1);
    expect(await activityFor(f.issue.id)).toHaveLength(1);
    await expectNoExecution(f.companyId);
  });

  it("fails closed for todo tasks instead of committing without a durable start wake", async () => {
    const f = await fixture({ status: "todo" });
    const body = request(f);
    await expect(svc().reassignUnstarted(f.companyId, f.issue.id, actor, body))
      .rejects.toMatchObject({ status: 422, details: { code: "todo_reassign_unsupported" } });
    await expectUnchanged(f);
    await expectNoExecution(f.companyId);
    // A retry of the same request is judged again and still refused; nothing was recorded to replay.
    await expect(svc().reassignUnstarted(f.companyId, f.issue.id, actor, body))
      .rejects.toMatchObject({ status: 422, details: { code: "todo_reassign_unsupported" } });
    await expectUnchanged(f);
  });

  it("rolls back the assignee change when the receipt cannot be saved", async () => {
    const f = await fixture();
    const body = request(f, { requestId: `fail-${randomUUID()}` });
    await db.execute(sql.raw(`
      create or replace function test_fail_task_operation_receipt() returns trigger language plpgsql as $$
      begin
        if new.request_id like 'fail-%' then raise exception 'simulated receipt failure'; end if;
        return new;
      end $$;
      drop trigger if exists test_fail_task_operation_receipt on issue_task_operation_receipts;
      create trigger test_fail_task_operation_receipt before insert on issue_task_operation_receipts
        for each row execute function test_fail_task_operation_receipt();`));
    try {
      await expect(svc().reassignUnstarted(f.companyId, f.issue.id, actor, body)).rejects.toThrow();
      await expectUnchanged(f);
      await expectNoExecution(f.companyId);
    } finally {
      await db.execute(sql.raw("drop trigger if exists test_fail_task_operation_receipt on issue_task_operation_receipts"));
    }
    // The same request succeeds once the fault is gone: no partial state blocked it.
    const ok = await svc().reassignUnstarted(f.companyId, f.issue.id, actor, body);
    expect(ok.replayed).toBe(false);
  });

  it("rejects a reused requestId with a different payload", async () => {
    const f = await fixture();
    const body = request(f);
    await svc().reassignUnstarted(f.companyId, f.issue.id, actor, body);
    await expect(svc().reassignUnstarted(f.companyId, f.issue.id, actor, { ...body, expectedAssigneeAgentId: null }))
      .rejects.toMatchObject({ status: 409, details: { code: "request_id_conflict" } });
    // The receipt is per actor: another actor's identical requestId is a new decision, judged on its own guards.
    await expect(svc().reassignUnstarted(f.companyId, f.issue.id, { actorType: "user", actorId: "other" }, body))
      .rejects.toMatchObject({ status: 409, details: { code: "stale_status_version" } });
    expect(await receiptsFor(f.issue.id)).toHaveLength(1);
  });

  it("fails stale version and stale assignee with 409 and changes nothing", async () => {
    const f = await fixture();
    await expect(svc().reassignUnstarted(f.companyId, f.issue.id, actor,
      request(f, { expectedStatusVersion: f.issue.statusVersion + 1 })))
      .rejects.toMatchObject({ status: 409, details: { code: "stale_status_version" } });
    await expect(svc().reassignUnstarted(f.companyId, f.issue.id, actor, request(f, { expectedAssigneeAgentId: null })))
      .rejects.toMatchObject({ status: 409, details: { code: "stale_assignee" } });
    await expectUnchanged(f);
    await expectNoExecution(f.companyId);
  });

  it.each(["in_progress", "in_review", "done", "blocked", "cancelled"])("refuses %s work", async (status) => {
    const f = await fixture({ status });
    await expect(svc().reassignUnstarted(f.companyId, f.issue.id, actor, request(f)))
      .rejects.toMatchObject({ status: 409, details: { code: "issue_not_unstarted" } });
    await expectUnchanged(f);
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
      await expect(svc().reassignUnstarted(f.companyId, f.issue.id, actor, request(f)))
        .rejects.toMatchObject({ status: 409, details: { code } });
      await expectUnchanged(f);
    }
  });

  it("refuses paused, unready, and other-company assignees", async () => {
    for (const options of [{ agentStatus: "paused" }, { agentStatus: "error" }, { agentStatus: "terminated" },
      { lifecycleState: "pausing" }, { lifecycleState: "paused" }, { lifecycleState: "pending_approval" },
      { lifecycleState: "terminated" }]) {
      const f = await fixture(options);
      await expect(svc().reassignUnstarted(f.companyId, f.issue.id, actor, request(f)))
        .rejects.toMatchObject({ status: 409, details: { code: "assignee_unavailable" } });
      await expectUnchanged(f);
    }
    const f = await fixture();
    const other = await fixture();
    await expect(svc().reassignUnstarted(f.companyId, f.issue.id, actor, request(f, { assigneeAgentId: other.toId })))
      .rejects.toMatchObject({ status: 422, details: { code: "assignee_not_found" } });
    // A task of another company is not found, even with valid guards.
    await expect(svc().reassignUnstarted(other.companyId, f.issue.id, actor, request(f)))
      .rejects.toMatchObject({ status: 404 });
    await expectUnchanged(f);
    await expectUnchanged(other);
  });

  it("serializes concurrent identical requests into one decision", async () => {
    const f = await fixture();
    const body = request(f);
    const results = await Promise.all(Array.from({ length: 6 }, () =>
      svc().reassignUnstarted(f.companyId, f.issue.id, actor, body)));
    expect(results.filter(r => !r.replayed)).toHaveLength(1);
    expect(new Set(results.map(r => JSON.stringify(r.receipt))).size).toBe(1);
    expect(await receiptsFor(f.issue.id)).toHaveLength(1);
    expect(await activityFor(f.issue.id)).toHaveLength(1);
    await expectNoExecution(f.companyId);
  });

  it("lets exactly one of two competing reassignments win the compare-and-set", async () => {
    const f = await fixture();
    const [third] = await db.insert(agents).values({ companyId: f.companyId, name: "Third", status: "idle" }).returning();
    const outcomes = await Promise.allSettled([
      svc().reassignUnstarted(f.companyId, f.issue.id, actor, request(f)),
      svc().reassignUnstarted(f.companyId, f.issue.id, actor, request(f, { assigneeAgentId: third!.id })),
    ]);
    expect(outcomes.filter(o => o.status === "fulfilled")).toHaveLength(1);
    const rejected = outcomes.find(o => o.status === "rejected") as PromiseRejectedResult;
    expect(rejected.reason).toMatchObject({ status: 409 });
    expect(["stale_status_version", "stale_assignee"]).toContain(rejected.reason.details.code);
    const winner = (outcomes.find(o => o.status === "fulfilled") as PromiseFulfilledResult<{
      receipt: { toAssigneeAgentId: string } }>).value;
    expect((await issueRow(f.issue.id)).assigneeAgentId).toBe(winner.receipt.toAssigneeAgentId);
    expect(await receiptsFor(f.issue.id)).toHaveLength(1);
    await expectNoExecution(f.companyId);
  });
});
