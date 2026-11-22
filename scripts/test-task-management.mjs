// Run against built release artifacts, with real isolated PostgreSQL and HTTP.
// PAPERCLIP_MANAGEMENT_TEST_ROOT points at a staged server package (never production data).
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import path from "node:path";
const root = process.env.PAPERCLIP_MANAGEMENT_TEST_ROOT;
if (!root || !path.isAbsolute(root)) throw new Error("Set an isolated absolute PAPERCLIP_MANAGEMENT_TEST_ROOT");
const require = createRequire(path.join(root, "package.json"));
const load = relative => import(pathToFileURL(path.join(root, "dist", relative)).href);
const { createDb, startEmbeddedPostgresTestDatabase, companies, agents, issues, heartbeatRuns,
  agentWakeupRequests, issueTreeHolds, issueTreeHoldMembers, executionWorkspaces } = await import(pathToFileURL(path.join(root, "node_modules/@paperclipai/db/dist/index.js")).href);
const { eq } = await import(pathToFileURL(path.join(root, "node_modules/drizzle-orm/index.js")).href);
const { requestTaskManagement: manage, taskManagementDelivery, authorizeTaskManagementWake } = await load("services/task-management.js");
const { heartbeatService } = await load("services/heartbeat.js");
const express = require("express");
let postgres, db, server, port;
const actor = { type: "board", userId: "local-board", source: "local_implicit", isInstanceAdmin: true };
before(async () => {
  postgres = await startEmbeddedPostgresTestDatabase("ao315-management-"); db = createDb(postgres.connectionString);
  const { issueRoutes } = await load("routes/issues.js");
  const app = express(); app.use(express.json());
  app.use((req, _res, next) => { req.actor = req.headers["x-test-agent"] ? { type: "agent", agentId: randomUUID(), companyId: randomUUID(), source: "agent_key" } : actor; next(); });
  // The deterministic process fixture records task disposition. A bare /true
  // would correctly trigger Paperclip's missing-disposition recovery Runs.
  app.post("/fixture/finish/:runId", async (req, res) => {
    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, req.params.runId));
    const issueId = run?.issueId ?? run?.contextSnapshot?.issueId;
    if (!issueId) return res.sendStatus(404);
    await db.update(issues).set({ status: "done", completedAt: new Date() }).where(eq(issues.id, issueId));
    res.sendStatus(200);
  });
  app.use("/api", issueRoutes(db, {}));
  app.use((err, _req, res, _next) => res.status(err.name === "ZodError" ? 400 : err.status ?? 500).json({ error: err.message }));
  server = app.listen(0, "127.0.0.1"); await new Promise(resolve => server.once("listening", resolve)); port = server.address().port;
}, { timeout: 45000 });
after(async () => { await new Promise(resolve => server?.close(resolve)); await db?.$client.end(); await postgres?.cleanup(); });
async function seed(status = "backlog") {
  const companyId = randomUUID(), issueId = randomUUID(), original = randomUUID(), target = randomUUID();
  await db.insert(companies).values({ id: companyId, name: "Management fixture", issuePrefix: `M${companyId.slice(0, 6)}` });
  await db.insert(agents).values([original, target].map(id => ({ id, companyId, name: id, adapterType: "process", status: "idle",
    adapterConfig: { command: process.execPath, args: ["--input-type=module", "-e",
      `const r = await fetch('http://127.0.0.1:${port}/fixture/finish/' + process.env.PAPERCLIP_RUN_ID, {method:'POST'}); if (!r.ok) process.exit(1);`] }, runtimeConfig: { heartbeat: { wakeOnDemand: true } } })));
  await db.insert(issues).values({ id: issueId, companyId, title: "Disposable existing task", status, assigneeAgentId: original });
  const request = { action: "reassign", requestId: randomUUID(), expectedStatusVersion: 0,
    expectedAssigneeAgentId: original, targetAgentId: target, observedRunId: null };
  return { db, companyId, issueId, actor, request, original, target };
}
const row = async f => (await db.select().from(issues).where(eq(issues.id, f.issueId)))[0];
const wakes = f => db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.companyId, f.companyId));
const resume = f => ({ ...f, request: { action: "resume", requestId: randomUUID(), expectedStatusVersion: 0,
  expectedAssigneeAgentId: f.original, observedRunId: null } });
test("real HTTP Board route reassigns the same task; identical replay has one immutable receipt", async () => {
  const f = await seed();
  const call = () => fetch(`http://127.0.0.1:${port}/api/issues/${f.issueId}/manage`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(f.request) });
  const one = await call(); assert.equal(one.status, 202, await one.clone().text()); const a = await one.json();
  const two = await call(); assert.equal(two.status, 202, await two.clone().text()); const b = await two.json();
  assert.equal(a.applied, true); assert.equal(a.assigneeAgentId, f.target); assert.equal(a.statusVersion, 1);
  assert.equal(b.requestId, a.requestId); assert.equal(b.replayed, true); assert.equal((await wakes(f)).length, 1);
  assert.equal((await row(f)).status, "backlog"); assert.equal((await row(f)).id, f.issueId);
});
test("same key changed input and agent actors are refused through HTTP", async () => {
  const f = await seed(); await manage(f);
  for (const [body, extra, expected] of [[{ ...f.request, observedRunId: randomUUID() }, {}, 409],
    [f.request, { "x-test-agent": "1" }, 403], [{ ...f.request, arbitraryPatch: true }, {}, 400]]) {
    const response = await fetch(`http://127.0.0.1:${port}/api/issues/${f.issueId}/manage`, {
      method: "POST", headers: { "content-type": "application/json", ...extra }, body: JSON.stringify(body) });
    assert.equal(response.status, expected, await response.text());
  }
});
test("concurrent different keys against one observed version have exactly one winner", async () => {
  const f = await seed(); const outcomes = await Promise.allSettled([manage(f), manage({ ...f, request: { ...f.request, requestId: randomUUID() } })]);
  assert.equal(outcomes.filter(x => x.status === "fulfilled").length, 1); assert.equal((await wakes(f)).length, 1);
});
test("concurrent same request replays; cross-task key collision rolls back", async () => {
  const f = await seed(), other = await seed();
  const outputs = await Promise.all([manage(f), manage(f)]); assert.equal(outputs[0].requestId, outputs[1].requestId);
  await assert.rejects(manage({ ...other, request: { ...other.request, requestId: f.request.requestId } }), /different input/);
  assert.equal((await row(other)).assigneeAgentId, other.original); assert.equal((await wakes(other)).length, 0);
});
test("stale version, assignee, latest Run, foreign and paused target have no writes", async () => {
  for (const field of ["version", "assignee", "run", "foreign", "paused"]) {
    const f = await seed();
    if (field === "version") f.request.expectedStatusVersion = 9;
    if (field === "assignee") f.request.expectedAssigneeAgentId = randomUUID();
    if (field === "run") f.request.observedRunId = randomUUID();
    if (field === "foreign") f.request.targetAgentId = (await seed()).target;
    if (field === "paused") await db.update(agents).set({ status: "paused" }).where(eq(agents.id, f.target));
    await assert.rejects(manage(f)); assert.equal((await wakes(f)).length, 0); assert.equal((await row(f)).assigneeAgentId, f.original);
  }
});
test("active/unknown/retrying Runs and linked checkout are refused", async () => {
  for (const status of ["running", "queued", "scheduled_retry", "unknown"]) {
    const f = await seed(), id = randomUUID(); f.request.observedRunId = id;
    await db.insert(heartbeatRuns).values({ id, companyId: f.companyId, agentId: f.original, issueId: f.issueId, status, contextSnapshot: { issueId: f.issueId } });
    await assert.rejects(manage(f)); assert.equal((await wakes(f)).length, 0);
  }
});
test("native Review policy and active tree pause/cancel holds cannot be bypassed", async () => {
  for (const kind of ["policy", "pause", "cancel"]) {
    const f = resume(await seed("cancelled"));
    if (kind === "policy") await db.update(issues).set({ executionPolicy: { mode: "normal", stages: [] } }).where(eq(issues.id, f.issueId));
    else { const id = randomUUID(); await db.insert(issueTreeHolds).values({ id, companyId: f.companyId, rootIssueId: f.issueId, mode: kind }); }
    await assert.rejects(manage(f)); assert.equal((await row(f)).status, "cancelled"); assert.equal((await wakes(f)).length, 0);
  }
});
test("explicit blocked/cancelled/done reopen preserves assignee and clears terminal dates", async () => {
  for (const status of ["blocked", "cancelled", "done"]) {
    const f = resume(await seed(status)), out = await manage(f);
    assert.equal(out.status, "todo"); assert.equal(out.assigneeAgentId, f.original); assert.ok(out.wakeupRequestId);
    assert.equal((await row(f)).completedAt, null); assert.equal((await row(f)).cancelledAt, null);
    assert.equal((await manage(f)).requestId, out.requestId);
  }
});
test("stale pending wake after ownership A→B→A cannot dispatch", async () => {
  const f = resume(await seed("cancelled")); await manage(f);
  await db.update(issues).set({ assigneeAgentId: f.target }).where(eq(issues.id, f.issueId));
  await db.update(issues).set({ assigneeAgentId: f.original }).where(eq(issues.id, f.issueId));
  await assert.rejects(authorizeTaskManagementWake(db, f.request.requestId, f.issueId, f.original), /superseded/);
  let called = false; await taskManagementDelivery(db, { wakeup: async (_id, opts) => { if (opts.payload.issueId === f.issueId) called = true; } }).sweepPending();
  assert.equal(called, false); assert.equal((await wakes(f))[0].status, "cancelled");
});
test("real heartbeat admission creates one process Run across concurrent delivery and reconstruction", async () => {
  // Retire only this test database's earlier undelivered fixture requests.
  await db.update(agentWakeupRequests).set({ status: "cancelled" }).where(eq(agentWakeupRequests.status, "task_management_pending"));
  const f = resume(await seed("cancelled")); const accepted = await manage(f);
  const heartbeat = heartbeatService(db); const deliver = () => taskManagementDelivery(db, heartbeat).sweepPending();
  await Promise.all([deliver(), deliver()]);
  // Fresh service instance models receipt/ack-loss reconstruction.
  await deliver();
  const runs = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.issueId, f.issueId));
  assert.equal(runs.length, 1, JSON.stringify(await wakes(f))); assert.equal(runs[0].agentId, f.original);
  assert.equal(runs[0].wakeupRequestId, accepted.wakeupRequestId);
  assert.equal((await manage(f)).replayed, true);
  for (let i = 0; i < 60; i++) {
    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, runs[0].id));
    if (["succeeded", "failed", "cancelled", "timed_out"].includes(run.status)) {
      assert.equal(run.status, "succeeded", JSON.stringify(run)); await heartbeat.drainActiveRunExecutions(); assert.equal((await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.issueId, f.issueId))).length, 1); return;
    }
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  assert.fail("Actual process Run did not terminate");
}, { timeout: 25000 });

test("a colliding task request cannot masquerade as another task's wake receipt", async () => {
  const f = resume(await seed("cancelled")), accepted = await manage(f), other = await seed();
  await manage({ ...other, request: { ...other.request, requestId: accepted.wakeupRequestId } });
  await taskManagementDelivery(db, { wakeup: async () => { assert.fail("must not dispatch a colliding receipt"); } }).sweepPending();
  const saved = (await wakes(f)).find(x => x.id === f.request.requestId);
  assert.equal(saved.status, "cancelled"); assert.equal(saved.runId, null); assert.match(saved.error, /identity changed/);
});

test("terminal Run history requires finished receipt and no live controller/process", async () => {
  for (const kind of ["settled", "unfinished", "lease", "process", "unknown_host"]) {
    const f = await seed(), id = randomUUID(); f.request.observedRunId = id;
    await db.insert(heartbeatRuns).values({ id, companyId: f.companyId, agentId: f.original,
      issueId: f.issueId, status: "succeeded", finishedAt: kind === "unfinished" ? null : new Date(),
      ...(kind === "lease" ? { controllerLeaseExpiresAt: new Date(Date.now() + 60000) } : {}),
      ...(kind === "process" || kind === "unknown_host" ? { processPid: kind === "process" ? process.pid : 2147483000, processGroupId: kind === "process" ? process.pid : 2147483000, processStartedAt: new Date() } : {}) });
    if (kind === "settled") assert.equal((await manage(f)).applied, true);
    else { await assert.rejects(manage(f)); assert.equal((await wakes(f)).length, 0); }
  }
});

test("concurrent late admission conflict reconciles the already admitted Run", async () => {
  const f = resume(await seed("cancelled")); const accepted = await manage(f);
  const heartbeat = heartbeatService(db);
  let releaseA, aAdmitted;
  const admitted = new Promise(resolve => { aAdmitted = resolve; });
  const release = new Promise(resolve => { releaseA = resolve; });
  const a = taskManagementDelivery(db, { wakeup: async (id, opts) => {
    const output = await heartbeat.wakeup(id, opts); aAdmitted(); await release; return output;
  } }).sweepPending();
  await admitted;
  await taskManagementDelivery(db, heartbeat).sweepPending();
  releaseA(); await a; await heartbeat.drainActiveRunExecutions();
  const receipt = (await wakes(f)).find(x => x.id === f.request.requestId);
  assert.equal(receipt.status, "completed"); assert.ok(receipt.runId);
  assert.equal((await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.issueId, f.issueId))).length, 1);
  assert.equal((await manage(f)).wakeupRequestId, accepted.wakeupRequestId);
});
