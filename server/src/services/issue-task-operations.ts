import { createHash } from "node:crypto";
import { and, eq, notInArray, sql } from "drizzle-orm";
import {
  agents,
  agentWakeupRequests,
  approvals,
  heartbeatRuns,
  issueApprovals,
  issueTaskOperationReceipts,
  issues,
  issueTreeHolds,
  type Db,
} from "@paperclipai/db";
import type {
  IssueTaskOperationWakeState,
  IssueTaskReassignDecision,
  IssueTaskReassignResult,
  ReassignUnstartedIssue,
} from "@paperclipai/shared";
import { isUniqueViolation } from "../db-errors.js";
import { conflict, notFound, unprocessable } from "../errors.js";
import { logActivity } from "./activity-log.js";
import { getExecutionBlocker } from "./execution-blocker.js";

export const TASK_OPERATION_WAKE_INDEX = "agent_wakeup_requests_task_operation_wake_uq";
const RECEIPT_REQUEST_INDEX = "issue_task_operation_receipts_request_uq";

/** Only work that has never started may be reassigned by this operation. */
const UNSTARTED_STATUSES = ["backlog", "todo"] as const;
/** Settled heartbeat runs. Every other status (queued, running, scheduled
 * retry, interrupted, or an unknown future status) keeps the task owned. */
const SETTLED_RUN_STATUSES = ["succeeded", "failed", "cancelled", "timed_out"];
/** Settled wake requests. Anything else is admitted or pending work. */
const SETTLED_WAKE_STATUSES = ["coalesced", "skipped", "completed", "failed", "cancelled"];
const SETTLED_APPROVAL_STATUSES = ["approved", "rejected", "cancelled"];
/** Statuses of an agent that can accept a newly assigned task. */
const ASSIGNABLE_AGENT_STATUSES = new Set(["active", "idle", "running"]);
const MAX_HOLD_ANCESTOR_DEPTH = 64;

export type TaskOperationActor = {
  actorType: "user" | "agent";
  actorId: string;
  agentId?: string | null;
  runId?: string | null;
  agentApiKeyId?: string | null;
};

type Wakeup = (agentId: string, opts: {
  source: "assignment";
  triggerDetail: "system";
  reason: string;
  payload: Record<string, unknown>;
  requestedByActorType: TaskOperationActor["actorType"];
  requestedByActorId: string;
  idempotencyKey: string;
  contextSnapshot: Record<string, unknown>;
}) => Promise<unknown>;

export function taskOperationWakeKey(receiptId: string) {
  return `task-operation:${receiptId}`;
}

function digestReassign(issueId: string, input: ReassignUnstartedIssue) {
  // Fixed field order: the digest names the exact requested decision.
  const canonical = JSON.stringify([
    "reassign_unstarted",
    issueId,
    input.assigneeAgentId,
    input.expectedStatusVersion,
    input.expectedAssigneeAgentId,
  ]);
  return `sha256:${createHash("sha256").update(canonical).digest("hex")}`;
}

function denied(code: string, message: string, details: Record<string, unknown> = {}) {
  return conflict(message, { code, ...details });
}

/**
 * Provider-owned compare-and-set operations on existing tasks. Each operation
 * decides under the issue row lock in one transaction and persists a decision
 * receipt keyed by issue, actor, and caller request id. Route-level company and
 * permission checks remain with the caller; this service re-checks company
 * ownership of every row it touches.
 */
export function issueTaskOperationService(db: Db, deps: { wakeup: Wakeup }) {
  async function findReceipt(tx: Db, companyId: string, issueId: string, actor: TaskOperationActor, requestId: string) {
    const [row] = await tx.select().from(issueTaskOperationReceipts).where(and(
      eq(issueTaskOperationReceipts.companyId, companyId),
      eq(issueTaskOperationReceipts.issueId, issueId),
      eq(issueTaskOperationReceipts.actorType, actor.actorType),
      eq(issueTaskOperationReceipts.actorId, actor.actorId),
      eq(issueTaskOperationReceipts.requestId, requestId),
    )).limit(1);
    return row ?? null;
  }

  function replayOf(row: typeof issueTaskOperationReceipts.$inferSelect, payloadDigest: string) {
    if (row.operation !== "reassign_unstarted" || row.payloadDigest !== payloadDigest) {
      throw denied("request_id_conflict", "requestId was already used for a different task operation", {
        receiptId: row.id,
      });
    }
    return row.decision as unknown as IssueTaskReassignDecision;
  }

  async function hasActiveTreeHold(tx: Db, companyId: string, issueId: string) {
    const result = await tx.execute(sql`
      with recursive chain(id, parent_id, depth) as (
        select ${issues.id}, ${issues.parentId}, 0 from ${issues}
          where ${issues.companyId} = ${companyId}::uuid and ${issues.id} = ${issueId}::uuid
        union all
        select i.id, i.parent_id, chain.depth + 1 from issues i
          join chain on i.id = chain.parent_id
          where i.company_id = ${companyId}::uuid and chain.depth < ${MAX_HOLD_ANCESTOR_DEPTH}
      )
      select 1 from ${issueTreeHolds}
        where ${issueTreeHolds.companyId} = ${companyId}::uuid and ${issueTreeHolds.status} = 'active'
          and ${issueTreeHolds.rootIssueId} in (select id from chain)
      limit 1`);
    const rows = (result as unknown as { rows?: unknown[] }).rows ?? (result as unknown as unknown[]);
    return rows.length > 0;
  }

  /** Every guard that keeps this a reassignment of untouched work. */
  async function assertReassignable(tx: Db, issue: typeof issues.$inferSelect, input: ReassignUnstartedIssue) {
    if (!(UNSTARTED_STATUSES as readonly string[]).includes(issue.status)) {
      throw denied("issue_not_unstarted", "Only backlog or todo tasks can be reassigned", { status: issue.status });
    }
    if (issue.statusVersion !== input.expectedStatusVersion) {
      throw denied("stale_status_version", "Task status changed since it was read", {
        expectedStatusVersion: input.expectedStatusVersion, statusVersion: issue.statusVersion,
      });
    }
    if (issue.assigneeAgentId !== input.expectedAssigneeAgentId) {
      throw denied("stale_assignee", "Task assignee changed since it was read", {
        expectedAssigneeAgentId: input.expectedAssigneeAgentId, assigneeAgentId: issue.assigneeAgentId,
      });
    }
    if (issue.assigneeUserId) throw denied("issue_assigned_to_user", "Task is assigned to a user");
    if (issue.assigneeAgentId === input.assigneeAgentId) {
      throw denied("assignee_unchanged", "Task is already assigned to this agent");
    }
    if (issue.startedAt || issue.checkoutRunId || issue.executionRunId || issue.executionLockedAt) {
      throw denied("issue_execution_started", "Task has started or holds an execution claim");
    }
    if (issue.executionWorkspaceId || issue.workspacePendingSelection) {
      throw denied("issue_workspace_bound", "Task already owns an execution workspace");
    }
    if (issue.conversationAgentId) throw denied("issue_is_conversation", "Chat conversations cannot be reassigned");
    if (issue.executionState) {
      throw denied("issue_review_or_approval_state", "Task has review or approval execution state");
    }
    const [run] = await tx.select({ id: heartbeatRuns.id, status: heartbeatRuns.status }).from(heartbeatRuns).where(and(
      eq(heartbeatRuns.companyId, issue.companyId),
      sql`(${heartbeatRuns.issueId} = ${issue.id}::uuid or ${heartbeatRuns.contextSnapshot}->>'issueId' = ${issue.id})`,
      notInArray(heartbeatRuns.status, SETTLED_RUN_STATUSES),
    )).limit(1);
    if (run) throw denied("issue_run_unsettled", "Task has an unsettled run", { runId: run.id, runStatus: run.status });
    const [wake] = await tx.select({ id: agentWakeupRequests.id, status: agentWakeupRequests.status })
      .from(agentWakeupRequests).where(and(
        eq(agentWakeupRequests.companyId, issue.companyId),
        sql`${agentWakeupRequests.payload}->>'issueId' = ${issue.id}`,
        notInArray(agentWakeupRequests.status, SETTLED_WAKE_STATUSES),
      )).limit(1);
    if (wake) throw denied("issue_wake_pending", "Task has a pending wake", { wakeupRequestId: wake.id, wakeStatus: wake.status });
    const [approval] = await tx.select({ id: approvals.id }).from(issueApprovals)
      .innerJoin(approvals, eq(approvals.id, issueApprovals.approvalId))
      .where(and(
        eq(issueApprovals.companyId, issue.companyId), eq(issueApprovals.issueId, issue.id),
        notInArray(approvals.status, SETTLED_APPROVAL_STATUSES),
      )).limit(1);
    if (approval) throw denied("issue_approval_pending", "Task has an unresolved approval", { approvalId: approval.id });
    if (await hasActiveTreeHold(tx, issue.companyId, issue.id)) {
      throw denied("issue_tree_held", "Task is under an active pause or stop hold");
    }
    const blocker = await getExecutionBlocker(tx, issue.companyId, issue.id);
    if (blocker) throw denied("issue_execution_blocked", "Task has unresolved execution recovery", { cause: blocker.cause });

    // Lock the target so a concurrent pause or termination serializes with this decision.
    const [target] = await tx.select({ id: agents.id, companyId: agents.companyId, status: agents.status,
      lifecycleState: agents.lifecycleState }).from(agents)
      .where(eq(agents.id, input.assigneeAgentId)).for("share").limit(1);
    if (!target || target.companyId !== issue.companyId) {
      throw unprocessable("Assignee agent was not found in this company", { code: "assignee_not_found" });
    }
    if (target.lifecycleState !== "ready" || !ASSIGNABLE_AGENT_STATUSES.has(target.status)) {
      throw denied("assignee_unavailable", "Assignee agent cannot accept work", {
        agentStatus: target.status, lifecycleState: target.lifecycleState,
      });
    }
  }

  /** Reads the admitted start wake for a receipt, if one was saved. */
  async function readWake(companyId: string, key: string) {
    const [row] = await db.select({ id: agentWakeupRequests.id, status: agentWakeupRequests.status })
      .from(agentWakeupRequests).where(and(
        eq(agentWakeupRequests.companyId, companyId), eq(agentWakeupRequests.idempotencyKey, key),
      )).limit(1);
    return row ?? null;
  }

  /**
   * Admits the start wake owed by a todo reassignment, at most once. The unique
   * key index makes a concurrent or repeated admission a no-op; a replay only
   * re-attempts while the task is still exactly as the receipt left it.
   */
  async function admitWake(decision: IssueTaskReassignDecision, actor: TaskOperationActor):
    Promise<IssueTaskReassignResult["wake"]> {
    if (!decision.wakeRequired || !decision.wakeIdempotencyKey) {
      return { state: "not_required", wakeupRequestId: null, status: null };
    }
    const key = decision.wakeIdempotencyKey;
    const existing = await readWake(decision.companyId, key);
    if (existing) return { state: existing.status === "skipped" ? "not_admitted" : "admitted",
      wakeupRequestId: existing.id, status: existing.status };
    const [issue] = await db.select({ status: issues.status, assigneeAgentId: issues.assigneeAgentId,
      statusVersion: issues.statusVersion }).from(issues)
      .where(and(eq(issues.companyId, decision.companyId), eq(issues.id, decision.issueId))).limit(1);
    if (!issue || issue.status !== "todo" || issue.assigneeAgentId !== decision.toAssigneeAgentId ||
        issue.statusVersion !== decision.statusVersion) {
      // The task moved on; whoever moved it owns starting it.
      return { state: "not_admitted", wakeupRequestId: null, status: null };
    }
    const admissionError = await deps.wakeup(decision.toAssigneeAgentId, {
      source: "assignment",
      triggerDetail: "system",
      reason: "issue_assigned",
      payload: { issueId: decision.issueId, mutation: "reassign", taskOperationReceiptId: decision.receiptId },
      requestedByActorType: actor.actorType,
      requestedByActorId: actor.actorId,
      idempotencyKey: key,
      contextSnapshot: {
        issueId: decision.issueId,
        taskId: decision.issueId,
        source: "issue.task_operation.reassign",
        wakeReason: "issue_assigned",
        taskOperationReceiptId: decision.receiptId,
      },
    }).then(() => null, (error: unknown) => {
      // Another admission already saved this receipt's wake; it owns the work.
      if (isUniqueViolation(error, TASK_OPERATION_WAKE_INDEX)) return null;
      // The decision is committed. Report the wake as not admitted so the
      // caller replays the same request instead of assuming work started.
      return error instanceof Error ? error.message : String(error);
    });
    const saved = await readWake(decision.companyId, key);
    return saved
      ? { state: saved.status === "skipped" ? "not_admitted" : "admitted",
        wakeupRequestId: saved.id, status: saved.status }
      : { state: "not_admitted" as IssueTaskOperationWakeState, wakeupRequestId: null, status: null,
        ...(admissionError ? { error: admissionError } : {}) };
  }

  async function decideReassign(companyId: string, issueId: string, actor: TaskOperationActor,
    input: ReassignUnstartedIssue, payloadDigest: string) {
    return db.transaction(async (tx) => {
      const [issue] = await tx.select().from(issues)
        .where(and(eq(issues.companyId, companyId), eq(issues.id, issueId))).for("update").limit(1);
      if (!issue) throw notFound("Issue not found");
      const prior = await findReceipt(tx, companyId, issueId, actor, input.requestId);
      if (prior) return { decision: replayOf(prior, payloadDigest), replayed: true };

      await assertReassignable(tx, issue, input);
      const [updated] = await tx.update(issues).set({ assigneeAgentId: input.assigneeAgentId, updatedAt: new Date() })
        .where(and(
          eq(issues.companyId, companyId), eq(issues.id, issueId),
          eq(issues.statusVersion, input.expectedStatusVersion),
          input.expectedAssigneeAgentId === null
            ? sql`${issues.assigneeAgentId} is null`
            : eq(issues.assigneeAgentId, input.expectedAssigneeAgentId),
        )).returning({ status: issues.status, statusVersion: issues.statusVersion });
      if (!updated) throw denied("stale_status_version", "Task changed since it was read");

      const [receipt] = await tx.insert(issueTaskOperationReceipts).values({
        companyId, issueId, operation: "reassign_unstarted", actorType: actor.actorType, actorId: actor.actorId,
        requestId: input.requestId, payloadDigest, decision: {},
      }).returning({ id: issueTaskOperationReceipts.id, createdAt: issueTaskOperationReceipts.createdAt });
      const wakeRequired = updated.status === "todo";
      const decision: IssueTaskReassignDecision = {
        operation: "reassign_unstarted",
        receiptId: receipt!.id,
        companyId,
        issueId,
        requestId: input.requestId,
        payloadDigest,
        fromAssigneeAgentId: issue.assigneeAgentId,
        toAssigneeAgentId: input.assigneeAgentId,
        issueStatus: updated.status as IssueTaskReassignDecision["issueStatus"],
        statusVersion: updated.statusVersion,
        wakeRequired,
        wakeIdempotencyKey: wakeRequired ? taskOperationWakeKey(receipt!.id) : null,
        decidedAt: receipt!.createdAt.toISOString(),
      };
      await tx.update(issueTaskOperationReceipts).set({
        decision: decision as unknown as Record<string, unknown>, wakeIdempotencyKey: decision.wakeIdempotencyKey,
      }).where(eq(issueTaskOperationReceipts.id, receipt!.id));
      await logActivity(tx, {
        companyId, actorType: actor.actorType, actorId: actor.actorId, agentId: actor.agentId ?? null,
        runId: actor.runId ?? null, agentApiKeyId: actor.agentApiKeyId ?? null,
        action: "issue.task_operation.reassigned", entityType: "issue", entityId: issueId,
        details: {
          receiptId: decision.receiptId, requestId: input.requestId,
          assigneeAgentId: input.assigneeAgentId, _previous: { assigneeAgentId: issue.assigneeAgentId },
          status: decision.issueStatus, statusVersion: decision.statusVersion, wakeRequired,
        },
      });
      return { decision, replayed: false };
    });
  }

  return {
    async reassignUnstarted(companyId: string, issueId: string, actor: TaskOperationActor,
      input: ReassignUnstartedIssue): Promise<IssueTaskReassignResult> {
      const payloadDigest = digestReassign(issueId, input);
      const outcome = await decideReassign(companyId, issueId, actor, input, payloadDigest).catch(async (error: unknown) => {
        // A concurrent identical request committed first: answer from its receipt.
        if (!isUniqueViolation(error, RECEIPT_REQUEST_INDEX)) throw error;
        const row = await findReceipt(db, companyId, issueId, actor, input.requestId);
        if (!row) throw error;
        return { decision: replayOf(row, payloadDigest), replayed: true };
      });
      const wake = await admitWake(outcome.decision, actor);
      return { receipt: outcome.decision, replayed: outcome.replayed, wake };
    },
  };
}
