import { randomUUID } from "node:crypto";
import request from "supertest";
import { eq } from "drizzle-orm";
import { afterEach, expect, it } from "vitest";
import {
  agents, agentWakeupRequests, heartbeatRuns, issueComments, issueOperatorNotes, issues, activityLog,
} from "@paperclipai/db";
import { issueOperatorNoteRoutes, OPERATOR_NOTE_MAX_BODY_CHARS } from "../routes/issue-operator-notes.js";
import {
  describeEmbeddedPostgres, resetCompanyIssueFixtures, routeApp, seedCompanyWithBoardAccess, useEmbeddedPostgres,
} from "./helpers/route-test-harness.js";

describeEmbeddedPostgres("issue operator notes routes", () => {
  const ctx = useEmbeddedPostgres("paperclip-operator-notes", {
    resetEach: async (db) => {
      await db.delete(issueOperatorNotes);
      await db.delete(activityLog);
      await db.delete(agentWakeupRequests);
      await db.delete(issueComments);
      await db.delete(issues);
      await db.delete(agents);
      await resetCompanyIssueFixtures(db);
    },
  });

  async function seedIssue(status: string, withAgent = true) {
    const seeded = await seedCompanyWithBoardAccess(ctx.db, "Notes");
    const agentId = randomUUID();
    if (withAgent) {
      await ctx.db.insert(agents).values({ id: agentId, companyId: seeded.companyId, name: "Worker", role: "engineer",
        status: "idle", adapterType: "process", adapterConfig: {}, runtimeConfig: {} } as never);
    }
    const [issue] = await ctx.db.insert(issues).values({
      companyId: seeded.companyId, title: "Task", status, assigneeAgentId: withAgent ? agentId : null,
      checkoutRunId: null, executionState: { status: "idle" },
    } as never).returning();
    return { ...seeded, issue: issue!, agentId };
  }
  const app = (s: Awaited<ReturnType<typeof seedIssue>>) => routeApp(ctx.db, s.actor, issueOperatorNoteRoutes as never);
  const snapshot = async (issueId: string) => {
    const [row] = await ctx.db.select().from(issues).where(eq(issues.id, issueId));
    const wakes = await ctx.db.select().from(agentWakeupRequests);
    const runs = await ctx.db.select().from(heartbeatRuns);
    const comments = await ctx.db.select().from(issueComments);
    return { row, wakes: wakes.length, runs: runs.length, comments: comments.length };
  };

  afterEach(() => undefined);

  for (const status of ["in_progress", "done", "blocked", "todo", "in_review"]) {
    it(`records a note on an agent-assigned ${status} task without any side effect`, async () => {
      const s = await seedIssue(status);
      const before = await snapshot(s.issue.id);
      const res = await request(app(s)).post(`/api/issues/${s.issue.id}/operator-notes`)
        .send({ requestId: "req-00000001", body: "operator says hi" });
      expect(res.status).toBe(201);
      expect(res.body).toMatchObject({ body: "operator says hi", replayed: false, authorUserId: s.userId });
      const after = await snapshot(s.issue.id);
      expect(after).toEqual(before); // row (status, version, assignee, workspace, updatedAt), wakes, runs, comments
      const list = await request(app(s)).get(`/api/issues/${s.issue.id}/operator-notes`);
      expect(list.status).toBe(200);
      expect(list.body.map((n: { body: string }) => n.body)).toEqual(["operator says hi"]);
    });
  }

  it("replays the exact payload without duplicating and rejects reuse with different content", async () => {
    const s = await seedIssue("in_progress");
    const send = (body: string, requestId = "req-00000002") =>
      request(app(s)).post(`/api/issues/${s.issue.id}/operator-notes`).send({ requestId, body });
    const first = await send("same");
    const replay = await send("same");
    expect(replay.status).toBe(200);
    expect(replay.body).toMatchObject({ id: first.body.id, replayed: true });
    expect((await send("different")).status).toBe(409);
    const concurrent = await Promise.all([send("race", "req-00000003"), send("race", "req-00000003")]);
    expect(concurrent.map((r) => r.status).sort()).toEqual([200, 201]);
    expect(await ctx.db.select().from(issueOperatorNotes)).toHaveLength(2);
    const audits = await ctx.db.select().from(activityLog).where(eq(activityLog.action, "issue.operator_note_added"));
    expect(audits).toHaveLength(2);
  });

  it("keeps the newest 100 operator notes visible when the history grows", async () => {
    const s = await seedIssue("done");
    const rows = Array.from({ length: 102 }, (_, index) => ({
      companyId: s.companyId, issueId: s.issue.id, authorUserId: s.userId,
      requestId: `history-${index}`, body: `note-${index}`, bodySha256: "test-fixture",
      createdAt: new Date(Date.UTC(2026, 0, 1, 0, 0, index)),
    }));
    await ctx.db.insert(issueOperatorNotes).values(rows);
    const list = await request(app(s)).get(`/api/issues/${s.issue.id}/operator-notes`);
    expect(list.status).toBe(200);
    expect(list.body).toHaveLength(100);
    expect(list.body[0].body).toBe("note-101");
    expect(list.body[99].body).toBe("note-2");
  });

  it("validates input bounds and rejects unknown fields", async () => {
    const s = await seedIssue("todo");
    const post = (b: unknown) => request(app(s)).post(`/api/issues/${s.issue.id}/operator-notes`).send(b as object);
    expect((await post({ requestId: "short", body: "x" })).status).toBe(400);
    expect((await post({ requestId: "req-00000004", body: "   " })).status).toBe(400);
    expect((await post({ requestId: "req-00000004", body: "x".repeat(OPERATOR_NOTE_MAX_BODY_CHARS + 1) })).status).toBe(400);
    expect((await post({ requestId: "req-00000004", body: "x", status: "done" })).status).toBe(400);
  });

  it("denies cross-company and agent actors and hides unknown issues", async () => {
    const a = await seedIssue("todo");
    const b = await seedIssue("todo");
    const cross = await request(routeApp(ctx.db, b.actor, issueOperatorNoteRoutes as never))
      .post(`/api/issues/${a.issue.id}/operator-notes`).send({ requestId: "req-00000005", body: "x" });
    expect(cross.status).toBe(404);
    const crossList = await request(routeApp(ctx.db, b.actor, issueOperatorNoteRoutes as never))
      .get(`/api/issues/${a.issue.id}/operator-notes`);
    expect(crossList.status).toBe(404);
    const agentActor = { type: "agent", agentId: a.agentId, companyId: a.companyId, source: "agent_key" } as never;
    const asAgent = await request(routeApp(ctx.db, agentActor, issueOperatorNoteRoutes as never))
      .post(`/api/issues/${a.issue.id}/operator-notes`).send({ requestId: "req-00000006", body: "x" });
    expect(asAgent.status).toBe(403);
    expect((await request(app(a)).post(`/api/issues/${randomUUID()}/operator-notes`)
      .send({ requestId: "req-00000007", body: "x" })).status).toBe(404);
    expect(await ctx.db.select().from(issueOperatorNotes)).toHaveLength(0);
  });
});
