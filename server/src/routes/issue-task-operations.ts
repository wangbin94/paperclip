import { Router } from "express";
import { and, eq } from "drizzle-orm";
import { issues as issueRows, type Db } from "@paperclipai/db";
import { isUuidLike, reassignUnstartedIssueSchema } from "@paperclipai/shared";
import { forbidden } from "../errors.js";
import { validate } from "../middleware/validate.js";
import { accessService, heartbeatService } from "../services/index.js";
import { authorizationDeniedDetails } from "../services/authorization.js";
import { issueTaskOperationService } from "../services/issue-task-operations.js";
import type { PluginWorkerManager } from "../services/plugin-worker-manager.js";
import { assertCompanyAccess, getActorInfo } from "./authz.js";

/**
 * Typed compare-and-set operations on existing tasks. These are the only
 * supported way for an automation provider to change task ownership without a
 * generic PATCH: each call is guarded by the caller's read versions and answered
 * from a durable receipt on replay.
 */
export function issueTaskOperationRoutes(
  db: Db,
  options: { pluginWorkerManager?: PluginWorkerManager } = {},
) {
  const router = Router();
  const access = accessService(db);
  const heartbeat = heartbeatService(db, { pluginWorkerManager: options.pluginWorkerManager });
  const operations = issueTaskOperationService(db, { wakeup: heartbeat.wakeup });

  router.post(
    "/companies/:companyId/issues/:issueId/task-operations/reassign",
    validate(reassignUnstartedIssueSchema),
    async (req, res) => {
      const companyId = req.params.companyId as string;
      const issueId = req.params.issueId as string;
      assertCompanyAccess(req, companyId);
      if (!isUuidLike(issueId)) {
        res.status(404).json({ error: "Issue not found" });
        return;
      }
      const actor = getActorInfo(req);
      if (actor.actorType === "agent" && !actor.agentId) throw forbidden("Agent identity is required");
      const [issue] = await db.select({ id: issueRows.id, projectId: issueRows.projectId, parentId: issueRows.parentId,
        assigneeAgentId: issueRows.assigneeAgentId, assigneeUserId: issueRows.assigneeUserId })
        .from(issueRows).where(and(eq(issueRows.companyId, companyId), eq(issueRows.id, issueId))).limit(1);
      if (!issue) {
        res.status(404).json({ error: "Issue not found" });
        return;
      }
      // Same authority as an assignment change through the issue API, checked
      // for both the current task scope and the requested assignee.
      for (const assigneeAgentId of [issue.assigneeAgentId, req.body.assigneeAgentId as string]) {
        const decision = await access.decide({
          actor: req.actor,
          action: "tasks:assign",
          resource: {
            type: "issue", companyId, issueId: issue.id, projectId: issue.projectId,
            parentIssueId: issue.parentId, assigneeAgentId, assigneeUserId: issue.assigneeUserId,
          },
          scope: { issueId: issue.id, projectId: issue.projectId, parentIssueId: issue.parentId,
            assigneeAgentId, assigneeUserId: issue.assigneeUserId },
        });
        if (!decision.allowed) throw forbidden(decision.explanation, authorizationDeniedDetails(decision));
      }
      const result = await operations.reassignUnstarted(companyId, issue.id, {
        actorType: actor.actorType,
        actorId: actor.actorId,
        agentId: actor.agentId,
        runId: actor.runId,
        agentApiKeyId: actor.agentApiKeyId,
      }, req.body);
      res.json(result);
    },
  );

  return router;
}
