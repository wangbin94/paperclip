import { z } from "zod";

/**
 * Compare-and-set reassignment of an existing, unstarted (backlog/todo) task.
 * `requestId` is the caller's stable idempotency id: a replay with the same
 * payload returns the stored receipt; a different payload is a 409 conflict.
 */
export const reassignUnstartedIssueSchema = z.object({
  requestId: z.string().trim().min(8).max(200),
  assigneeAgentId: z.string().uuid(),
  expectedStatusVersion: z.number().int().nonnegative(),
  expectedAssigneeAgentId: z.string().uuid().nullable(),
}).strict();

export type ReassignUnstartedIssue = z.infer<typeof reassignUnstartedIssueSchema>;

export const ISSUE_TASK_OPERATION_WAKE_STATES = ["not_required", "admitted", "not_admitted"] as const;
export type IssueTaskOperationWakeState = (typeof ISSUE_TASK_OPERATION_WAKE_STATES)[number];

export interface IssueTaskReassignDecision {
  operation: "reassign_unstarted";
  receiptId: string;
  companyId: string;
  issueId: string;
  requestId: string;
  payloadDigest: string;
  fromAssigneeAgentId: string | null;
  toAssigneeAgentId: string;
  issueStatus: "backlog" | "todo";
  statusVersion: number;
  wakeRequired: boolean;
  wakeIdempotencyKey: string | null;
  decidedAt: string;
}

export interface IssueTaskReassignResult {
  receipt: IssueTaskReassignDecision;
  replayed: boolean;
  wake: {
    state: IssueTaskOperationWakeState;
    wakeupRequestId: string | null;
    status: string | null;
    /** Admission failed after the decision committed; replay the same request. */
    error?: string;
  };
}
