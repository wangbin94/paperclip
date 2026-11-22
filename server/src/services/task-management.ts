import { createHash, randomUUID } from "node:crypto";
import { z } from "zod";
import { and, asc, desc, eq, inArray, or, sql } from "drizzle-orm";
import { agentWakeupRequests, agents, chatConversations, companies, executionWorkspaces,
  heartbeatRuns, issueThreadInteractions, issueTreeHolds, issueTreeHoldMembers, issues,
  projects, type Db } from "@paperclipai/db";
import { conflict, forbidden, notFound } from "../errors.js";
import { authorizationService, type AuthorizationActor } from "./authorization.js";
import { issueService, executeIssuePostCommitActions, type IssuePostCommitAction } from "./issues.js";
import { persistActivity, publishActivity, type ActivityPublication } from "./activity-log.js";
import { getExecutionBlocker, hasPendingRunCleanup } from "./execution-blocker.js";
import { isNativeRunnerOwnershipHeld } from "./native-runtime/native-runner-ownership.js";
import { recordedLocalExecutionStopped } from "./native-local-process-stop.js";
import { remoteExecutionHasStopped } from "./remote-execution-termination.js";
import { canRetryStoppedRun } from "./cancelled-native-startup.js";
import { issueTreeControlService } from "./issue-tree-control.js";
import { DELIVERY_QUEUES, notifyDeliveryWork } from "./delivery-work-notifications.js";
import type { heartbeatService } from "./heartbeat.js";

export const taskManagementSchema = z.object({
  requestId: z.string().uuid(), action: z.enum(["reassign", "resume"]),
  expectedStatusVersion: z.number().int().nonnegative().safe(),
  expectedAssigneeAgentId: z.string().uuid(), observedRunId: z.string().uuid().nullable(),
  targetAgentId: z.string().uuid().optional(),
}).strict().superRefine((value, ctx) => {
  if (value.action === "reassign" ? !value.targetAgentId || value.targetAgentId === value.expectedAssigneeAgentId : value.targetAgentId !== undefined)
    ctx.addIssue({ code: "custom", message: "Reassign needs a different target; resume preserves the assignee" });
});
type ManagementInput = z.infer<typeof taskManagementSchema>;
type Issue = typeof issues.$inferSelect;
const TERMINAL_RUNS = ["succeeded", "failed", "timed_out", "cancelled", "interrupted"];
const PENDING = "task_management_pending";
const REASON = "task_management";
const intentSchema = z.object({
  schema: z.literal("paperclip.task-management.v1"), fingerprint: z.string(),
  actorId: z.string().min(1), actorSource: z.enum(["local_implicit", "session", "board_key", "cloud_tenant"]),
  action: z.enum(["reassign", "resume"]), status: z.string(), statusVersion: z.number().int().safe(),
  wakeupRequestId: z.string().uuid().nullable(), assigneeAgentId: z.string().uuid(),
});
function intent(row: typeof agentWakeupRequests.$inferSelect) {
  return intentSchema.parse(row.payload?.taskManagement);
}
function result(row: typeof agentWakeupRequests.$inferSelect, replayed: boolean) {
  const saved = intent(row);
  return { contract: "paperclip.task-management.v1", companyId: row.companyId, issueId: row.payload?.issueId, requestId: row.id, action: saved.action,
    applied: true, replayed, status: saved.status, statusVersion: saved.statusVersion,
    assigneeAgentId: saved.assigneeAgentId, wakeupRequestId: saved.wakeupRequestId,
    dispatchStatus: row.status, runId: row.runId, runStarted: null };
}
async function authorize(db: Db, issue: Issue, actor: AuthorizationActor, target: string) {
  if (actor.type !== "board" || !actor.userId || !["local_implicit", "session", "board_key", "cloud_tenant"].includes(actor.source ?? ""))
    throw forbidden("Task management requires an authenticated Board operator");
  for (const action of ["issue:mutate", "tasks:assign"] as const) {
    const decision = await authorizationService(db).decide({ actor, action,
      resource: { type: "issue", companyId: issue.companyId, issueId: issue.id,
        projectId: issue.projectId, parentIssueId: issue.parentId,
        assigneeAgentId: target, assigneeUserId: null },
      scope: { issueId: issue.id, projectId: issue.projectId, parentIssueId: issue.parentId,
        assigneeAgentId: target, assigneeUserId: null } });
    if (!decision.allowed) throw forbidden(decision.explanation);
  }
}
async function guard(db: Db, issue: Issue, ownRequestId?: string) {
  if (!issue.assigneeAgentId || issue.assigneeUserId || issue.hiddenAt || issue.conversationAgentId ||
      issue.executionPolicy != null || issue.executionState != null || ![null, "anyone"].includes(issue.reviewPolicy) ||
      issue.executionRunId || issue.checkoutRunId || issue.executionLockedAt || issue.workspacePendingSelection ||
      issue.monitorNextCheckAt || issue.status === "in_review")
    throw conflict("Task has execution, Review, conversation or monitor ownership; management refused");
  const [company] = await db.select().from(companies).where(eq(companies.id, issue.companyId));
  if (company?.status !== "active") throw conflict("Company is not active");
  if (issue.projectId) {
    const [project] = await db.select().from(projects).where(and(eq(projects.id, issue.projectId), eq(projects.companyId, issue.companyId)));
    if (!project || project.archivedAt || project.pausedAt) throw conflict("Project is unavailable or paused");
  }
  const [chat] = await db.select({ id: chatConversations.id }).from(chatConversations)
    .where(and(eq(chatConversations.companyId, issue.companyId), eq(chatConversations.issueId, issue.id))).limit(1);
  const [interaction] = await db.select({ id: issueThreadInteractions.id }).from(issueThreadInteractions)
    .where(and(eq(issueThreadInteractions.companyId, issue.companyId), eq(issueThreadInteractions.issueId, issue.id),
      inArray(issueThreadInteractions.status, ["pending", "open", "accepted"]))).limit(1);
  const [hold] = await db.select({ id: issueTreeHolds.id }).from(issueTreeHolds)
    .leftJoin(issueTreeHoldMembers, eq(issueTreeHoldMembers.holdId, issueTreeHolds.id))
    .where(and(eq(issueTreeHolds.companyId, issue.companyId), eq(issueTreeHolds.status, "active"),
      or(eq(issueTreeHolds.rootIssueId, issue.id), eq(issueTreeHoldMembers.issueId, issue.id)))).limit(1);
  if (chat || interaction || hold || await issueTreeControlService(db).getActivePauseHoldGate(issue.companyId, issue.id))
    throw conflict("Task has a connected conversation, pending interaction or active tree hold");
  if (await getExecutionBlocker(db, issue.companyId, issue.id)) throw conflict("Task requires provider execution/workspace recovery");
  if (issue.executionWorkspaceId) {
    const [workspace] = await db.select().from(executionWorkspaces).where(and(
      eq(executionWorkspaces.id, issue.executionWorkspaceId), eq(executionWorkspaces.companyId, issue.companyId)));
    if (!workspace || workspace.status !== "active") throw conflict("Attached workspace is not active; restore it through provider recovery first");
  }
  const runs = await db.select().from(heartbeatRuns).where(and(eq(heartbeatRuns.companyId, issue.companyId),
    or(eq(heartbeatRuns.issueId, issue.id), eq(heartbeatRuns.nativeIssueId, issue.id),
      sql`${heartbeatRuns.contextSnapshot}->>'issueId' = ${issue.id}`))).orderBy(desc(heartbeatRuns.createdAt), desc(heartbeatRuns.id));
  for (const run of runs) {
    if (!run.finishedAt || (run.controllerLeaseExpiresAt && run.controllerLeaseExpiresAt > new Date()) ||
        isNativeRunnerOwnershipHeld(run)) throw conflict("Prior Run has not released execution ownership");
    if ((run.processPid || run.processGroupId || run.processStartedAt) &&
        !await remoteExecutionHasStopped(db, issue.companyId, run.id) && !await recordedLocalExecutionStopped(db, run))
      throw conflict("Prior provider process has not verifiably stopped");
    if (["cancelled", "interrupted"].includes(run.status) && !await canRetryStoppedRun(db, run))
      throw conflict("Prior cancellation needs provider recovery before management");
    if (!TERMINAL_RUNS.includes(run.status) || run.scheduledRetryAt || run.scheduledRetryReason ||
        await hasPendingRunCleanup(db, issue.companyId, run.id))
      throw conflict("Task has an active, retrying, unknown or uncleaned Run");
  }
  const wakes = await db.select({ id: agentWakeupRequests.id }).from(agentWakeupRequests).where(and(
    eq(agentWakeupRequests.companyId, issue.companyId),
    sql`coalesce(${agentWakeupRequests.payload}->>'issueId', ${agentWakeupRequests.payload}->>'taskId',
      ${agentWakeupRequests.payload}->'_paperclipWakeContext'->>'issueId') = ${issue.id}`,
    inArray(agentWakeupRequests.status, ["queued", "claimed", "deferred_issue_execution", PENDING])));
  if (wakes.some(row => row.id !== ownRequestId)) throw conflict("Task has outstanding wake authority");
  return runs[0]?.id ?? null;
}

/** The issue service owns mutation semantics. This transaction adds observed-state
 * CAS and one immutable receipt; it performs no network/process work. */
export async function requestTaskManagement(input: { db: Db; companyId: string; issueId: string;
  actor: AuthorizationActor; request: ManagementInput }) {
  const request = taskManagementSchema.parse(input.request);
  const fingerprint = createHash("sha256").update(JSON.stringify([input.companyId, input.issueId,
    request.action, request.expectedStatusVersion, request.expectedAssigneeAgentId, request.observedRunId, request.targetAgentId ?? null])).digest("hex");
  const publications: ActivityPublication[] = [], actions: IssuePostCommitAction[] = [];
  const output = await input.db.transaction(async transaction => {
    const tx = transaction as unknown as Db;
    // The primary key binds request IDs globally, including concurrent cross-task calls.
    await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`task-management:${request.requestId}`}, 0))`);
    const [issue] = await tx.select().from(issues).where(and(eq(issues.id, input.issueId), eq(issues.companyId, input.companyId))).for("update");
    if (!issue) throw notFound("Task not found");
    const target = request.targetAgentId ?? request.expectedAssigneeAgentId;
    await authorize(tx, issue, input.actor, target);
    const [prior] = await tx.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, request.requestId));
    if (prior) {
      if (prior.companyId !== input.companyId || prior.reason !== REASON || prior.payload?.issueId !== issue.id ||
          prior.requestedByActorId !== input.actor.userId || intent(prior).fingerprint !== fingerprint)
        throw conflict("Management request ID belongs to different input, task or operator");
      return result(prior, true);
    }
    if (issue.statusVersion !== request.expectedStatusVersion || issue.assigneeAgentId !== request.expectedAssigneeAgentId || issue.assigneeUserId)
      throw conflict("Task version or assignee changed; read the task again");
    if (!(request.action === "reassign" ? ["backlog", "todo", "blocked"] : ["backlog", "blocked", "cancelled", "done"]).includes(issue.status))
      throw conflict("Task status does not allow this management operation");
    const latest = await guard(tx, issue);
    if (latest !== request.observedRunId) throw conflict("Latest Run changed; read the task again");
    const [agent] = await tx.select().from(agents).where(and(eq(agents.id, target), eq(agents.companyId, issue.companyId))).for("update");
    if (!agent || agent.status !== "idle") throw conflict("Target Agent is not idle in this Company");
    const updated = await issueService(input.db).update(issue.id, { companyGuard: issue.companyId,
      actorUserId: input.actor.userId, workspaceSelectionActor: input.actor,
      ...(request.action === "reassign" ? { assigneeAgentId: target } : { status: "todo" }) }, tx, publications, actions);
    if (!updated || updated.assigneeAgentId !== target || updated.assigneeUserId || updated.executionPolicy != null ||
        updated.status !== (request.action === "resume" ? "todo" : issue.status))
      throw conflict("Provider did not preserve the requested management contract");
    const wakeupRequestId = updated.status === "todo" ? randomUUID() : null;
    const saved = intentSchema.parse({ schema: "paperclip.task-management.v1", fingerprint,
      actorId: input.actor.userId, actorSource: input.actor.source, action: request.action,
      status: updated.status, statusVersion: updated.statusVersion, assigneeAgentId: target, wakeupRequestId });
    if (wakeupRequestId) await notifyDeliveryWork(tx, DELIVERY_QUEUES.taskManagement, issue.id);
    const [row] = await tx.insert(agentWakeupRequests).values({ id: request.requestId, companyId: issue.companyId,
      agentId: target, source: "on_demand", triggerDetail: "manual", reason: REASON,
      requestedByActorType: "user", requestedByActorId: input.actor.userId,
      idempotencyKey: `task-management:${request.requestId}`, status: wakeupRequestId ? PENDING : "completed",
      payload: { issueId: issue.id, taskManagement: saved }, finishedAt: wakeupRequestId ? null : new Date() }).returning();
    const { publication } = await persistActivity(tx, { companyId: issue.companyId, actorType: "user", actorId: input.actor.userId!,
      action: "issue.management_requested", entityType: "issue", entityId: issue.id,
      details: { requestId: row!.id, action: request.action, previousStatus: issue.status, previousAssigneeAgentId: issue.assigneeAgentId,
        status: updated.status, assigneeAgentId: target, statusVersion: updated.statusVersion } });
    publications.push(publication);
    return result(row!, false);
  });
  publications.forEach(publishActivity);
  await executeIssuePostCommitActions(input.db, actions);
  return output;
}

/** Called both before and inside normal wake admission. No new scheduler or
 * native-status identity is manufactured. */
export async function authorizeTaskManagementWake(db: Db, requestId: string, issueId: string, agentId: string) {
  const [row] = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, requestId));
  if (!row || row.status !== PENDING || row.reason !== REASON || row.payload?.issueId !== issueId || row.agentId !== agentId)
    throw conflict("Management intent is no longer pending");
  const saved = intent(row);
  const [issue] = await db.select().from(issues).where(and(eq(issues.id, issueId), eq(issues.companyId, row.companyId)));
  if (!issue || issue.status !== "todo" || issue.statusVersion !== saved.statusVersion || issue.assigneeAgentId !== agentId || issue.assigneeUserId)
    throw conflict("Management intent was superseded by newer task state");
  await authorize(db, issue, { type: "board", userId: saved.actorId, source: saved.actorSource }, agentId);
  await guard(db, issue, requestId);
  return { id: saved.wakeupRequestId!, requestedAt: row.requestedAt, actorId: saved.actorId, statusVersion: saved.statusVersion };
}

function verifyWakeReceipt(row: typeof agentWakeupRequests.$inferSelect, receipt: typeof agentWakeupRequests.$inferSelect) {
  const saved = intent(row);
  if (receipt.companyId !== row.companyId || receipt.agentId !== row.agentId ||
      receipt.payload?.issueId !== row.payload?.issueId || receipt.payload?.taskManagementRequestId !== row.id ||
      receipt.requestedByActorType !== "user" || receipt.requestedByActorId !== saved.actorId ||
      receipt.idempotencyKey !== `task-management-wake:${row.id}`)
    throw conflict("Management wake receipt identity changed; operator inspection required");
}
async function settleDelivery(db: Db, row: typeof agentWakeupRequests.$inferSelect, failure?: unknown) {
  await db.transaction(async transaction => {
    const tx = transaction as unknown as Db;
    // Admission holds this same issue lock. Either reconcile its child, or revoke
    // pending authority before another admission can pass its final guard.
    await tx.select({ id: issues.id }).from(issues).where(and(eq(issues.id, String(row.payload?.issueId)),
      eq(issues.companyId, row.companyId))).for("update");
    const [receipt] = await tx.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, intent(row).wakeupRequestId!));
    let terminalError = failure;
    if (receipt) {
      try {
        verifyWakeReceipt(row, receipt);
        await tx.update(agentWakeupRequests).set({ status: "completed", runId: receipt.runId,
          error: receipt.error, finishedAt: new Date(), updatedAt: new Date() })
          .where(and(eq(agentWakeupRequests.id, row.id), eq(agentWakeupRequests.status, PENDING)));
        return;
      } catch (error) { terminalError = error; }
    }
    if (terminalError) {
      const status = (terminalError as { status?: number }).status;
      await tx.update(agentWakeupRequests).set({ ...(status === 403 || status === 404 || status === 409
        ? { status: "cancelled", finishedAt: new Date() } : {}),
        error: String(terminalError).slice(0, 1000), updatedAt: new Date() })
        .where(and(eq(agentWakeupRequests.id, row.id), eq(agentWakeupRequests.status, PENDING)));
    }
  });
}

export function taskManagementDelivery(db: Db, heartbeat: Pick<ReturnType<typeof heartbeatService>, "wakeup">) {
  return {
    async sweepPending(signal?: AbortSignal) {
      const rows = await db.select().from(agentWakeupRequests).where(and(eq(agentWakeupRequests.reason, REASON),
        eq(agentWakeupRequests.status, PENDING))).orderBy(asc(agentWakeupRequests.requestedAt)).limit(25);
      for (const row of rows) {
        if (signal?.aborted) break;
        try {
          const saved = intent(row), issueId = String(row.payload?.issueId);
          let [receipt] = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, saved.wakeupRequestId!));
          if (!receipt) {
            await authorizeTaskManagementWake(db, row.id, issueId, row.agentId);
            await heartbeat.wakeup(row.agentId, { source: "on_demand", triggerDetail: "manual", reason: REASON,
              requestedByActorType: "user", requestedByActorId: saved.actorId,
              idempotencyKey: `task-management-wake:${row.id}`, durableTaskManagementRequestId: row.id,
              issueStateGuard: { assigneeAgentId: row.agentId, statuses: ["todo"], statusVersion: saved.statusVersion },
              payload: { issueId, taskManagementRequestId: row.id },
              contextSnapshot: { issueId, taskKey: issueId, source: REASON, responsibleUserId: saved.actorId, forceFreshSession: true } });
            [receipt] = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, saved.wakeupRequestId!));
          }
          await settleDelivery(db, row);
        } catch (error) {
          await settleDelivery(db, row, error);
        }
      }
    },
    async hasPending() { return (await db.select({ id: agentWakeupRequests.id }).from(agentWakeupRequests)
      .where(and(eq(agentWakeupRequests.reason, REASON), eq(agentWakeupRequests.status, PENDING))).limit(1)).length > 0; },
  };
}
