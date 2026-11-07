import { z } from "zod";

/**
 * Compare-and-set reassignment of an existing, unstarted backlog task.
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

/**
 * Decision receipt for a backlog reassignment. Only backlog tasks are
 * supported: a todo reassignment would owe the new assignee a start wake that
 * cannot yet be admitted in the same transaction, so it is rejected with
 * `todo_reassign_unsupported` instead of committing without a durable wake.
 */
export interface IssueTaskReassignDecision {
  operation: "reassign_unstarted";
  receiptId: string;
  companyId: string;
  issueId: string;
  requestId: string;
  payloadDigest: string;
  fromAssigneeAgentId: string | null;
  toAssigneeAgentId: string;
  issueStatus: "backlog";
  statusVersion: number;
  startWake: "not_required";
  decidedAt: string;
}

export interface IssueTaskReassignResult {
  receipt: IssueTaskReassignDecision;
  replayed: boolean;
}
