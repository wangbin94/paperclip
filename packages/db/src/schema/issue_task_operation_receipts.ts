import { pgTable, uuid, text, timestamp, jsonb, index, uniqueIndex } from "drizzle-orm/pg-core";
import { companies } from "./companies.js";
import { issues } from "./issues.js";

/**
 * Durable decision receipt for a compare-and-set task operation (for example
 * reassigning an unstarted task). One receipt per issue, actor, and caller
 * request id; a replay with the same payload digest returns the stored decision
 * unchanged, and a different payload under the same request id is a conflict.
 */
export const issueTaskOperationReceipts = pgTable(
  "issue_task_operation_receipts",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    companyId: uuid("company_id").notNull().references(() => companies.id),
    issueId: uuid("issue_id").notNull().references(() => issues.id, { onDelete: "cascade" }),
    operation: text("operation").notNull(),
    actorType: text("actor_type").notNull(),
    actorId: text("actor_id").notNull(),
    requestId: text("request_id").notNull(),
    payloadDigest: text("payload_digest").notNull(),
    decision: jsonb("decision").$type<Record<string, unknown>>().notNull(),
    wakeIdempotencyKey: text("wake_idempotency_key"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => ({
    requestUq: uniqueIndex("issue_task_operation_receipts_request_uq").on(
      table.companyId,
      table.issueId,
      table.actorType,
      table.actorId,
      table.requestId,
    ),
    companyIssueIdx: index("issue_task_operation_receipts_company_issue_idx").on(table.companyId, table.issueId),
  }),
);
