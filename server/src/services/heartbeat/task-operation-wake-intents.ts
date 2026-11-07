import { and, asc, eq, inArray, isNull, like, sql } from "drizzle-orm";
import { z } from "zod";
import { agents, agentWakeupRequests, companies, heartbeatRuns, issues, type Db } from "@paperclipai/db";
import { conflict, HttpError, unprocessable } from "../../errors.js";
import { DELIVERY_QUEUES, notifyDeliveryWork } from "../delivery-work-notifications.js";
import type { WakeupOptions } from "./run-preparation.js";

/**
 * Task-operation wake intents.
 *
 * A task operation (reassignment, resuming a blocked or cancelled task) commits
 * its issue change and the wake it owes in one caller transaction. The intent
 * is an `agent_wakeup_requests` row that is not runnable by itself: the
 * heartbeat dispatcher below re-reads the task, checks that the exact committed
 * state (status, statusVersion, assignee, company) still holds and that the
 * agent, company and task tree may run, then admits the wake through the normal
 * `enqueueWakeup` path under the original requesting actor.
 *
 * Exactly-once rules:
 * - The intent key `task-operation:<operationId>` is unique per company across
 *   every status, so a replayed or retried operation can never create a second
 *   intent, including after the first one was skipped or failed.
 * - Each intent owns one downstream admission keyed
 *   `task-operation-dispatch:<intentId>`, also unique across every status. A
 *   crash after that row is written is recovered by linking it, and concurrent
 *   dispatchers cannot both insert one.
 *
 * statusVersion semantics: `expectedStatusVersion` is the issue's
 * `status_version` after the caller's transition. Insertion requires the row
 * read inside the caller transaction to equal it exactly; dispatch requires it
 * to be unchanged, and the downstream admission repeats the comparison under
 * the issue lock (`issueStateGuard.statusVersion`). Any later status change
 * bumps `status_version`, so the intent terminates `skipped` instead of waking.
 */

export const TASK_OPERATION_WAKE_SOURCE = "task_operation";
export const TASK_OPERATION_INTENT_KEY_PREFIX = "task-operation:";
export const TASK_OPERATION_DISPATCH_KEY_PREFIX = "task-operation-dispatch:";
export const TASK_OPERATION_KINDS = ["reassign", "resume_blocked", "resume_cancelled"] as const;
export type TaskOperationKind = (typeof TASK_OPERATION_KINDS)[number];
/** Only states in which the assignee is expected to start work may be targeted. */
export const TASK_OPERATION_WAKE_STATUSES = ["todo", "in_progress"] as const;
// Allowlist, not a denylist: an unknown or newly added agent status or an
// unfinished lifecycle cannot admit a task-operation wake.
const DISPATCHABLE_AGENT_STATUSES = new Set(["active", "idle", "running"]);
const ACTIVE_RUN_STATUSES = ["queued", "running", "scheduled_retry"];
const MAX_TRANSIENT_DISPATCH_FAILURES = 5;
const RETRY_DELAY_MS = 30_000;

export function taskOperationWakeReason(operation: TaskOperationKind) {
  return `task_operation_${operation}`;
}

export function taskOperationIntentKey(operationId: string) {
  return `${TASK_OPERATION_INTENT_KEY_PREFIX}${operationId}`;
}

export function taskOperationDispatchKey(intentId: string) {
  return `${TASK_OPERATION_DISPATCH_KEY_PREFIX}${intentId}`;
}

// Actor ids are attribution, never authority. Reject ids that would read as a
// system dispatcher or committer so a caller cannot borrow their provenance.
const actorIdSchema = z.string().min(1).max(200).regex(/^[A-Za-z0-9_.@:-]+$/)
  .refine(id => !/^(native-|task-operation|system$|heartbeat$)/.test(id), "reserved actor id");

const intentInputSchema = z.object({
  companyId: z.string().uuid(),
  issueId: z.string().uuid(),
  operation: z.enum(TASK_OPERATION_KINDS),
  operationId: z.string().uuid(),
  expectedAssigneeAgentId: z.string().uuid(),
  expectedStatus: z.enum(TASK_OPERATION_WAKE_STATUSES),
  expectedStatusVersion: z.number().int().min(0).max(2_147_483_647),
  requestedBy: z.discriminatedUnion("actorType", [
    z.object({ actorType: z.literal("user"), actorId: actorIdSchema }).strict(),
    z.object({ actorType: z.literal("agent"), actorId: z.string().uuid() }).strict(),
  ]),
}).strict();

export type TaskOperationWakeIntentInput = z.input<typeof intentInputSchema>;

const persistedIntentSchema = z.object({
  version: z.literal(1),
  operation: z.enum(TASK_OPERATION_KINDS),
  operationId: z.string().uuid(),
  issueId: z.string().uuid(),
  expectedAssigneeAgentId: z.string().uuid(),
  expectedStatus: z.enum(TASK_OPERATION_WAKE_STATUSES),
  expectedStatusVersion: z.number().int().min(0),
  requestedByActorType: z.enum(["user", "agent"]),
  requestedByActorId: z.string().min(1).max(200),
}).strict();
type PersistedTaskOperationIntent = z.infer<typeof persistedIntentSchema>;
type WakeRow = typeof agentWakeupRequests.$inferSelect;

/**
 * Durably record the wake a task operation owes, inside the caller's
 * transaction. The caller must already have written the issue transition in
 * `tx`; any validation failure throws so the whole operation rolls back.
 * Replaying the same `operationId` returns the existing intent in any state
 * and never creates another wake.
 */
export async function insertTaskOperationWakeIntent(tx: Db, rawInput: TaskOperationWakeIntentInput) {
  const parsed = intentInputSchema.safeParse(rawInput);
  if (!parsed.success) throw unprocessable("Invalid task-operation wake intent", parsed.error.flatten());
  const input = parsed.data;
  const intent: PersistedTaskOperationIntent = {
    version: 1,
    operation: input.operation,
    operationId: input.operationId,
    issueId: input.issueId,
    expectedAssigneeAgentId: input.expectedAssigneeAgentId,
    expectedStatus: input.expectedStatus,
    expectedStatusVersion: input.expectedStatusVersion,
    requestedByActorType: input.requestedBy.actorType,
    requestedByActorId: input.requestedBy.actorId,
  };
  const idempotencyKey = taskOperationIntentKey(input.operationId);
  // A replay after a lost response must reconcile with the intent already
  // committed, whatever the task looks like now. It never re-admits or
  // re-validates the operation; it only proves the identity is unchanged.
  const replay = async () => {
    const [existing] = await tx.select().from(agentWakeupRequests).where(and(
      eq(agentWakeupRequests.companyId, input.companyId), eq(agentWakeupRequests.idempotencyKey, idempotencyKey)));
    if (!existing) return null;
    const existingIntent = readTaskOperationIntent(existing);
    if (!existingIntent || JSON.stringify(existingIntent) !== JSON.stringify(intent)) {
      throw conflict("Task-operation id was reused with a different wake intent");
    }
    return { created: false as const, intent: existing };
  };
  const replayed = await replay();
  if (replayed) return replayed;
  const [issue] = await tx.select({ status: issues.status, statusVersion: issues.statusVersion,
    assigneeAgentId: issues.assigneeAgentId, assigneeUserId: issues.assigneeUserId })
    .from(issues).where(and(eq(issues.id, input.issueId), eq(issues.companyId, input.companyId)))
    .for("update");
  if (!issue) throw conflict("Task-operation wake target task does not exist in this company");
  if (issue.status !== input.expectedStatus || issue.statusVersion !== input.expectedStatusVersion ||
      issue.assigneeAgentId !== input.expectedAssigneeAgentId || issue.assigneeUserId !== null) {
    throw conflict("Task-operation wake does not match the committed task state", {
      expectedStatus: input.expectedStatus, actualStatus: issue.status,
      expectedStatusVersion: input.expectedStatusVersion, actualStatusVersion: issue.statusVersion,
    });
  }
  const [assignee] = await tx.select({ id: agents.id }).from(agents)
    .where(and(eq(agents.id, input.expectedAssigneeAgentId), eq(agents.companyId, input.companyId)));
  if (!assignee) throw conflict("Task-operation wake assignee is not an agent in this company");
  if (input.requestedBy.actorType === "agent") {
    const [actor] = await tx.select({ id: agents.id }).from(agents)
      .where(and(eq(agents.id, input.requestedBy.actorId), eq(agents.companyId, input.companyId)));
    if (!actor) throw conflict("Task-operation requesting agent is not in this company");
  }

  // ON CONFLICT keeps the caller transaction usable when the key already exists.
  const [created] = await tx.insert(agentWakeupRequests).values({
    companyId: input.companyId,
    agentId: input.expectedAssigneeAgentId,
    source: "assignment",
    triggerDetail: "system",
    reason: taskOperationWakeReason(input.operation),
    payload: { issueId: input.issueId, taskOperation: intent },
    status: "queued",
    requestedByActorType: input.requestedBy.actorType,
    requestedByActorId: input.requestedBy.actorId,
    idempotencyKey,
  }).onConflictDoNothing().returning();
  if (created) {
    await notifyDeliveryWork(tx, DELIVERY_QUEUES.taskOperationWakeIntents,
      JSON.stringify({ companyId: input.companyId, requestId: created.id }));
    return { created: true as const, intent: created };
  }
  // A concurrent identical insert committed first.
  const raced = await replay();
  if (!raced) throw conflict("Task-operation wake intent key is held by an unreadable row");
  return raced;
}

/** Parse the bounded intent written by `insertTaskOperationWakeIntent`; anything else is not an intent. */
export function readTaskOperationIntent(row: Pick<WakeRow, "idempotencyKey" | "payload" | "reason" | "source"
  | "requestedByActorType" | "requestedByActorId" | "agentId">): PersistedTaskOperationIntent | null {
  const parsed = persistedIntentSchema.safeParse(row.payload?.taskOperation);
  if (!parsed.success) return null;
  const intent = parsed.data;
  if (row.idempotencyKey !== taskOperationIntentKey(intent.operationId) || row.source !== "assignment" ||
      row.reason !== taskOperationWakeReason(intent.operation) || row.payload?.issueId !== intent.issueId ||
      row.agentId !== intent.expectedAssigneeAgentId || row.requestedByActorType !== intent.requestedByActorType ||
      row.requestedByActorId !== intent.requestedByActorId) return null;
  return intent;
}

/**
 * Admission-side authority check. Reserved task-operation keys, the
 * `task_operation` context source and the intent marker are accepted only for
 * the dispatcher's own admission of a claimed intent; generic and public wake
 * callers cannot mint them.
 */
export async function assertTaskOperationWakeAuthority(db: Db, input: {
  agentId: string;
  companyId: string;
  issueId: string | null;
  opts: Pick<WakeupOptions, "taskOperationIntentId" | "idempotencyKey" | "requestedByActorType" | "requestedByActorId">;
  contextSnapshot: Record<string, unknown>;
}) {
  const { opts, contextSnapshot } = input;
  const reservedKey = opts.idempotencyKey?.startsWith("task-operation") === true;
  const claimsSource = contextSnapshot.source === TASK_OPERATION_WAKE_SOURCE ||
    Object.hasOwn(contextSnapshot, "taskOperationIntentId");
  if (!opts.taskOperationIntentId) {
    if (reservedKey || claimsSource) throw conflict("Task-operation wake origin is reserved for its dispatcher");
    return;
  }
  const intentId = opts.taskOperationIntentId;
  if (opts.idempotencyKey !== taskOperationDispatchKey(intentId) ||
      contextSnapshot.source !== TASK_OPERATION_WAKE_SOURCE || contextSnapshot.taskOperationIntentId !== intentId) {
    throw conflict("Task-operation wake does not match its dispatch authority");
  }
  const [intentRow] = await db.select().from(agentWakeupRequests).where(and(
    eq(agentWakeupRequests.id, intentId), eq(agentWakeupRequests.companyId, input.companyId),
    eq(agentWakeupRequests.status, "claimed"), isNull(agentWakeupRequests.runId)));
  const intent = intentRow && readTaskOperationIntent(intentRow);
  if (!intent || intentRow.agentId !== input.agentId || intent.issueId !== input.issueId ||
      opts.requestedByActorType !== intent.requestedByActorType || opts.requestedByActorId !== intent.requestedByActorId) {
    throw conflict("Task-operation wake does not match its claimed intent");
  }
}

type DispatchInput = {
  companyId?: string;
  requestId?: string;
  limit?: number;
  staleClaimMs?: number;
  onDeferredDeadline?: (at: number) => void;
  onError?: (error: unknown) => void;
};

export type TaskOperationWakeDispatcherDependencies = {
  enqueueWakeup: (agentId: string, opts: WakeupOptions) => Promise<{ id: string } | null>;
  getAgentInvokability: (agent: typeof agents.$inferSelect) => Promise<{ invokable: boolean; message?: string }>;
  getActivePauseHoldGate: (companyId: string, issueId: string) => Promise<unknown>;
};

/**
 * Dispatch persisted task-operation intents through normal wake admission.
 * Recovery after restart is the same scan: queued intents, stale claims, and
 * claims whose downstream admission was written before the intent was linked.
 */
export function createTaskOperationWakeDispatcher(db: Db, deps: TaskOperationWakeDispatcherDependencies) {
  const pending = ["queued", "claimed"];

  /**
   * Settle an intent only from the exact state this dispatcher observed or
   * claimed. A dispatcher that lost its stale claim to a takeover cannot
   * overwrite the newer owner's outcome.
   */
  async function finishIntent(intent: Pick<WakeRow, "id" | "companyId" | "status" | "claimedAt">,
    patch: { status: string; runId?: string | null; error?: string | null }) {
    const now = new Date();
    const terminal = patch.status !== "queued";
    const updated = await db.update(agentWakeupRequests).set({
      status: patch.status,
      runId: patch.runId ?? null,
      error: patch.error?.slice(0, 1_000) ?? null,
      claimedAt: terminal ? undefined : null,
      finishedAt: terminal ? now : null,
      updatedAt: now,
    }).where(and(eq(agentWakeupRequests.id, intent.id), eq(agentWakeupRequests.companyId, intent.companyId),
      eq(agentWakeupRequests.status, intent.status), isNull(agentWakeupRequests.runId), sameClaim(intent.claimedAt)))
      .returning({ id: agentWakeupRequests.id });
    return updated.length > 0;
  }

  // claimed_at is written from JS dates, so millisecond equality is exact.
  function sameClaim(claimedAt: Date | null) {
    return claimedAt
      ? sql`date_trunc('milliseconds', ${agentWakeupRequests.claimedAt}) = ${claimedAt.toISOString()}::timestamptz`
      : isNull(agentWakeupRequests.claimedAt);
  }

  function findDownstream(intent: Pick<WakeRow, "id" | "companyId">) {
    return db.select().from(agentWakeupRequests).where(and(
      eq(agentWakeupRequests.companyId, intent.companyId),
      eq(agentWakeupRequests.idempotencyKey, taskOperationDispatchKey(intent.id)),
    )).then(rows => rows[0] ?? null);
  }

  /**
   * Link the intent to its single downstream admission. Only a downstream
   * row bound to a heartbeat run counts as delivered.
   */
  async function settleFromDownstream(intent: WakeRow, downstream: WakeRow): Promise<"dispatched" | "skipped" | "pending"> {
    if (downstream.runId) {
      await finishIntent(intent, { status: "coalesced", runId: downstream.runId });
      return "dispatched";
    }
    if (downstream.status === "deferred_issue_execution") {
      // Promotion from behind another run re-checks the assignee and terminal
      // status but not statusVersion. Withdraw it rather than let a stale
      // task-operation wake run later; lose the race only to a promotion that
      // already took ownership.
      const withdrawn = await db.update(agentWakeupRequests).set({
        status: "skipped", finishedAt: new Date(), updatedAt: new Date(),
        error: "Task-operation wake was not admitted immediately and was withdrawn",
      }).where(and(eq(agentWakeupRequests.id, downstream.id), eq(agentWakeupRequests.status, "deferred_issue_execution"),
        isNull(agentWakeupRequests.runId))).returning({ id: agentWakeupRequests.id });
      if (withdrawn.length === 0) {
        const current = await findDownstream(intent);
        if (current && current.status !== "deferred_issue_execution") return settleFromDownstream(intent, current);
      }
      await finishIntent(intent, { status: "skipped",
        error: "Task execution was acquired by another run during admission; the task-operation wake was withdrawn" });
      return "skipped";
    }
    if (["skipped", "failed", "cancelled"].includes(downstream.status)) {
      await finishIntent(intent, { status: "skipped",
        error: `Task-operation wake admission ${downstream.status}: ${downstream.reason ?? "unknown"}${downstream.error ? ` (${downstream.error})` : ""}` });
      return "skipped";
    }
    if (["queued", "claimed"].includes(downstream.status)) {
      // Admission has not bound a run yet. Keep the intent pending; its one
      // downstream key already prevents a second admission.
      await finishIntent(intent, { status: "queued", error: "Waiting for task-operation wake admission to bind a run" });
      return "pending";
    }
    await finishIntent(intent, { status: "failed",
      error: `Task-operation wake admission ended ${downstream.status} without a run` });
    return "skipped";
  }

  /** Current eligibility, read fresh; null means the intent may be admitted now. */
  async function ineligibility(intentRow: WakeRow, intent: PersistedTaskOperationIntent)
    : Promise<{ kind: "skip" | "wait"; reason: string } | null> {
    const [issue] = await db.select({ status: issues.status, statusVersion: issues.statusVersion,
      assigneeAgentId: issues.assigneeAgentId, assigneeUserId: issues.assigneeUserId, executionRunId: issues.executionRunId })
      .from(issues).where(and(eq(issues.id, intent.issueId), eq(issues.companyId, intentRow.companyId)));
    if (!issue) return { kind: "skip", reason: "Task-operation wake target no longer exists" };
    if (issue.statusVersion !== intent.expectedStatusVersion)
      return { kind: "skip", reason: `Task status changed after the operation (statusVersion ${intent.expectedStatusVersion} -> ${issue.statusVersion})` };
    if (issue.status !== intent.expectedStatus)
      return { kind: "skip", reason: `Task status is ${issue.status}, expected ${intent.expectedStatus}` };
    if (issue.assigneeAgentId !== intent.expectedAssigneeAgentId || issue.assigneeUserId !== null)
      return { kind: "skip", reason: "Task has a different assignee than the operation committed" };
    const [company] = await db.select({ status: companies.status }).from(companies).where(eq(companies.id, intentRow.companyId));
    if (company?.status !== "active") return { kind: "skip", reason: `Company is ${company?.status ?? "missing"}` };
    const [agent] = await db.select().from(agents)
      .where(and(eq(agents.id, intentRow.agentId), eq(agents.companyId, intentRow.companyId)));
    if (!agent) return { kind: "skip", reason: "Assignee agent no longer exists" };
    if (!DISPATCHABLE_AGENT_STATUSES.has(agent.status)) return { kind: "skip", reason: `Assignee agent is ${agent.status}` };
    if (agent.lifecycleState !== "ready" || agent.lifecycleHolds.length > 0)
      return { kind: "skip", reason: `Assignee agent lifecycle is ${agent.lifecycleState}${agent.lifecycleHolds.length ? " with holds" : ""}` };
    const invokability = await deps.getAgentInvokability(agent);
    if (!invokability.invokable) return { kind: "skip", reason: invokability.message ?? "Assignee agent cannot be invoked" };
    if (await deps.getActivePauseHoldGate(intentRow.companyId, intent.issueId))
      return { kind: "skip", reason: "Task tree is paused by an active hold" };
    if (issue.executionRunId) {
      const [owner] = await db.select({ id: heartbeatRuns.id }).from(heartbeatRuns).where(and(
        eq(heartbeatRuns.id, issue.executionRunId), eq(heartbeatRuns.companyId, intentRow.companyId),
        inArray(heartbeatRuns.status, ACTIVE_RUN_STATUSES)));
      // Wait for the current execution instead of deferring behind it, so the
      // exact statusVersion check still governs the moment of admission.
      if (owner) return { kind: "wait", reason: "Waiting for the task's current execution to finish" };
    }
    return null;
  }

  async function dispatch(input: DispatchInput = {}) {
    const now = Date.now();
    const staleClaimMs = Math.max(1_000, input.staleClaimMs ?? 60_000);
    const candidates = await db.select().from(agentWakeupRequests).where(and(
      input.companyId ? eq(agentWakeupRequests.companyId, input.companyId) : undefined,
      input.requestId ? eq(agentWakeupRequests.id, input.requestId) : undefined,
      like(agentWakeupRequests.idempotencyKey, `${TASK_OPERATION_INTENT_KEY_PREFIX}%`),
      inArray(agentWakeupRequests.status, pending),
      isNull(agentWakeupRequests.runId),
    )).orderBy(asc(agentWakeupRequests.requestedAt)).limit(Math.max(1, Math.min(input.limit ?? 100, 500)));

    let dispatched = 0, skipped = 0, deferred = 0, recovered = 0;
    const defer = (at: number) => { input.onDeferredDeadline?.(at); deferred += 1; };
    for (const candidate of candidates) {
      const intent = readTaskOperationIntent(candidate);
      if (!intent) {
        await finishIntent(candidate, { status: "failed", error: "Malformed task-operation wake intent" });
        skipped += 1;
        continue;
      }
      const existing = await findDownstream(candidate);
      if (existing) {
        if (await settleFromDownstream(candidate, existing) === "pending") defer(Date.now() + RETRY_DELAY_MS);
        else recovered += 1;
        continue;
      }
      if (candidate.status === "claimed" && candidate.claimedAt && now - candidate.claimedAt.getTime() < staleClaimMs) {
        defer(candidate.claimedAt.getTime() + staleClaimMs);
        continue;
      }
      if (candidate.status === "queued" && candidate.error && now - candidate.updatedAt.getTime() < RETRY_DELAY_MS) {
        defer(candidate.updatedAt.getTime() + RETRY_DELAY_MS);
        continue;
      }
      // Compare the exact prior claim so two takeovers of a stale claim cannot both win.
      const claimedAt = new Date(now);
      const [claimed] = await db.update(agentWakeupRequests).set({ status: "claimed", claimedAt, updatedAt: claimedAt })
        .where(and(eq(agentWakeupRequests.id, candidate.id), isNull(agentWakeupRequests.runId),
          eq(agentWakeupRequests.status, candidate.status), sameClaim(candidate.claimedAt)))
        .returning();
      if (!claimed) continue;

      const blocked = await ineligibility(claimed, intent);
      if (blocked?.kind === "wait") {
        await finishIntent(claimed, { status: "queued", error: blocked.reason });
        defer(Date.now() + RETRY_DELAY_MS);
        continue;
      }
      if (blocked) {
        await finishIntent(claimed, { status: "skipped", error: blocked.reason });
        skipped += 1;
        continue;
      }

      const reason = taskOperationWakeReason(intent.operation);
      let thrown: unknown = null;
      let run: { id: string } | null = null;
      try {
        run = await deps.enqueueWakeup(claimed.agentId, {
          taskOperationIntentId: claimed.id,
          allowRunCoalescing: false,
          source: "assignment",
          triggerDetail: "system",
          reason,
          payload: { issueId: intent.issueId, taskOperation: intent },
          idempotencyKey: taskOperationDispatchKey(claimed.id),
          requestedByActorType: intent.requestedByActorType,
          requestedByActorId: intent.requestedByActorId,
          issueStateGuard: { statuses: [intent.expectedStatus], assigneeAgentId: intent.expectedAssigneeAgentId,
            statusVersion: intent.expectedStatusVersion },
          contextSnapshot: {
            issueId: intent.issueId,
            taskId: intent.issueId,
            wakeReason: reason,
            source: TASK_OPERATION_WAKE_SOURCE,
            taskOperationIntentId: claimed.id,
            taskOperation: intent.operation,
            taskOperationExpectedStatusVersion: intent.expectedStatusVersion,
          },
        });
      } catch (error) {
        thrown = error;
      }
      const downstream = await findDownstream(claimed);
      if (downstream) {
        const settled = await settleFromDownstream(claimed, downstream);
        if (settled === "dispatched") dispatched += 1;
        else if (settled === "skipped") skipped += 1;
        else defer(Date.now() + RETRY_DELAY_MS);
        continue;
      }
      if (run) {
        // A run returned without this intent's downstream receipt means
        // admission coalesced into other work; record it but do not count it
        // as this intent's own durable admission.
        await finishIntent(claimed, { status: "failed",
          error: `Wake admission returned run ${run.id} without the task-operation dispatch receipt` });
        skipped += 1;
        continue;
      }
      if (!thrown) {
        await finishIntent(claimed, { status: "skipped", error: "Wake admission declined the task-operation wake" });
        skipped += 1;
        continue;
      }
      const message = thrown instanceof Error ? thrown.message : String(thrown);
      // A deterministic refusal will refuse again; fail closed instead of looping.
      if (thrown instanceof HttpError && thrown.status >= 400 && thrown.status < 500) {
        await finishIntent(claimed, { status: "skipped", error: `Wake admission refused: ${message}` });
        skipped += 1;
        continue;
      }
      input.onError?.(thrown);
      const failures = Number(claimed.payload?.taskOperationDispatchFailures ?? 0) + 1;
      if (failures >= MAX_TRANSIENT_DISPATCH_FAILURES) {
        await finishIntent(claimed, { status: "failed", error: `Task-operation wake failed ${failures} times: ${message}` });
        skipped += 1;
        continue;
      }
      await db.update(agentWakeupRequests).set({
        payload: { ...claimed.payload, taskOperationDispatchFailures: failures },
      }).where(and(eq(agentWakeupRequests.id, claimed.id), eq(agentWakeupRequests.status, "claimed"),
        isNull(agentWakeupRequests.runId), sameClaim(claimed.claimedAt)));
      await finishIntent(claimed, { status: "queued", error: message });
      defer(Date.now() + RETRY_DELAY_MS);
    }
    return { scanned: candidates.length, dispatched, skipped, deferred, recovered };
  }

  return { dispatch };
}
