import { index, pgTable, text, timestamp, unique, uuid } from "drizzle-orm/pg-core";
import { companies } from "./companies.js";
import { issues } from "./issues.js";

/**
 * Operator-only notes attached to a task. Deliberately separate from
 * `issue_comments`: nothing in the wake, review, board-response or comment
 * recovery paths reads this table, so inserting a note cannot be mistaken for
 * user input. `(issue_id, author_user_id, request_id)` is the idempotency
 * receipt; `body_sha256` makes a replay with different content detectable.
 */
export const issueOperatorNotes = pgTable(
  "issue_operator_notes",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    companyId: uuid("company_id").notNull().references(() => companies.id),
    issueId: uuid("issue_id").notNull().references(() => issues.id, { onDelete: "cascade" }),
    authorUserId: text("author_user_id").notNull(),
    requestId: text("request_id").notNull(),
    body: text("body").notNull(),
    bodySha256: text("body_sha256").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => ({
    requestUq: unique("issue_operator_notes_request_uq").on(table.issueId, table.authorUserId, table.requestId),
    companyIssueCreatedIdx: index("issue_operator_notes_company_issue_created_idx").on(
      table.companyId,
      table.issueId,
      table.createdAt,
    ),
  }),
);
