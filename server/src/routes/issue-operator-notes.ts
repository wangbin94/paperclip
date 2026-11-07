import { createHash } from "node:crypto";
import { Router, type Request } from "express";
import { z } from "zod";
import { and, desc, eq } from "drizzle-orm";
import { issueOperatorNotes, issues, type Db } from "@paperclipai/db";
import { validate } from "../middleware/validate.js";
import { conflict, forbidden, notFound } from "../errors.js";
import { assertCompanyAccess, getActorInfo, hasCompanyAccess } from "./authz.js";
import { authorizationService } from "../services/authorization.js";
import { logActivity, publishActivity, type ActivityPublication } from "../services/activity-log.js";

export const OPERATOR_NOTE_MAX_BODY_CHARS = 8_000;
const OPERATOR_NOTE_LIST_LIMIT = 100;

const createOperatorNoteSchema = z.object({
  requestId: z.string().trim().min(8).max(128).regex(/^[A-Za-z0-9._:-]+$/),
  body: z.string().refine((value) => value.trim().length > 0, "Note body is required")
    .refine((value) => value.length <= OPERATOR_NOTE_MAX_BODY_CHARS, "Note body is too long"),
}).strict();

function serialize(row: typeof issueOperatorNotes.$inferSelect) {
  return {
    id: row.id, companyId: row.companyId, issueId: row.issueId, requestId: row.requestId,
    authorUserId: row.authorUserId, body: row.body, createdAt: row.createdAt,
  };
}

/**
 * Operator notes: an authenticated board user's private annotation on a task.
 * The write touches only `issue_operator_notes` and the activity log. It never
 * updates the issue, enqueues a wake, or writes `issue_comments`, so it cannot
 * move status/assignee/workspace, reach an agent, or count as a review response.
 * It is not a human approval and carries no execution authority.
 */
export function issueOperatorNoteRoutes(db: Db) {
  const router = Router();

  async function loadIssue(req: Request, issueId: string, action: "issue:comment" | "issue:read") {
    const [issue] = await db.select({
      id: issues.id, companyId: issues.companyId, status: issues.status,
      assigneeAgentId: issues.assigneeAgentId, assigneeUserId: issues.assigneeUserId,
    }).from(issues).where(eq(issues.id, issueId));
    if (!issue || !hasCompanyAccess(req, issue.companyId)) throw notFound("Issue not found");
    assertCompanyAccess(req, issue.companyId);
    if (req.actor.type !== "board" || !req.actor.userId) throw forbidden("Board user access required");
    const decision = await authorizationService(db).decide({
      actor: { type: "board", userId: req.actor.userId, source: getActorInfo(req).actorSource as never },
      action,
      resource: { type: "issue", companyId: issue.companyId, issueId: issue.id, status: issue.status,
        assigneeAgentId: issue.assigneeAgentId, assigneeUserId: issue.assigneeUserId },
    });
    if (!decision.allowed) throw forbidden("Not permitted to access this task");
    return { issue, userId: req.actor.userId };
  }

  router.post("/issues/:id/operator-notes", validate(createOperatorNoteSchema), async (req, res) => {
    const { issue, userId } = await loadIssue(req, req.params.id as string, "issue:comment");
    const { requestId, body } = req.body as z.infer<typeof createOperatorNoteSchema>;
    const bodySha256 = createHash("sha256").update(body).digest("hex");
    const actor = getActorInfo(req);
    const publications: ActivityPublication[] = [];
    const result = await db.transaction(async (tx) => {
      const [inserted] = await tx.insert(issueOperatorNotes).values({
        companyId: issue.companyId, issueId: issue.id, authorUserId: userId, requestId, body, bodySha256,
      }).onConflictDoNothing().returning();
      if (inserted) {
        await logActivity(tx as unknown as Db, {
          companyId: issue.companyId, actorType: "user", actorId: actor.actorId,
          action: "issue.operator_note_added", entityType: "issue", entityId: issue.id,
          details: { noteId: inserted.id, requestId, bodySha256 },
        }, publications);
        return { note: inserted, replayed: false };
      }
      const [existing] = await tx.select().from(issueOperatorNotes).where(and(
        eq(issueOperatorNotes.issueId, issue.id), eq(issueOperatorNotes.authorUserId, userId),
        eq(issueOperatorNotes.requestId, requestId),
      ));
      if (!existing) throw conflict("Operator note request could not be recorded; retry with the same requestId");
      if (existing.bodySha256 !== bodySha256) throw conflict("requestId was already used for different content");
      return { note: existing, replayed: true };
    });
    for (const publication of publications) publishActivity(publication);
    res.status(result.replayed ? 200 : 201).json({ ...serialize(result.note), replayed: result.replayed });
  });

  router.get("/issues/:id/operator-notes", async (req, res) => {
    const { issue } = await loadIssue(req, req.params.id as string, "issue:read");
    const rows = await db.select().from(issueOperatorNotes).where(and(
      eq(issueOperatorNotes.companyId, issue.companyId), eq(issueOperatorNotes.issueId, issue.id),
    )).orderBy(desc(issueOperatorNotes.createdAt), desc(issueOperatorNotes.id)).limit(OPERATOR_NOTE_LIST_LIMIT);
    res.json(rows.map(serialize));
  });

  return router;
}
