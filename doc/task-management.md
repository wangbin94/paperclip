# Bounded Board task management

This local release extension adds `paperclip.task-management.v1` to
`2026.1106.0`. It is not part of the official release. Routine native Review
does not require it.

`GET /api/task-management/capabilities` reports the contract. Board-authenticated
`POST /api/issues/:id/manage` accepts only `requestId`, `action`,
`expectedStatusVersion`, `expectedAssigneeAgentId`, `observedRunId` and optional
`targetAgentId`. The request ID is a UUID. `observedRunId` is null only when the
task has no Runs. Both mutation and assignment permissions are checked.

`reassign` changes an idle Agent assignment on `backlog`, `todo` or `blocked`.
It preserves the status. A `todo` assignment requests execution. `resume`
preserves the Agent and deliberately reopens `backlog`, `blocked`, `cancelled`
or `done` as `todo`. Active execution, pending Review/Approval, conversations,
tree holds, unsafe workspaces, outstanding wakes and unresolved execution repair
are refused. Management never cancels the old executor or approves a review.

The existing issue service applies the change under the issue lock. The same
transaction stores an immutable request fingerprint and wake intent in
`agent_wakeup_requests`. There is no migration. Identical requests return the
original receipt before checking stale versions. A different request against an
old snapshot returns a conflict with no mutation. A request ID collision is a
conflict, including across Companies.

The existing delivery coordinator reconstructs pending work at startup and
receives a transaction-aware work signal. It retries only while accepted work
remains. Normal heartbeat admission checks original-user authority and the
accepted task version again. One stable wake UUID prevents duplicate admission.
Acknowledgement and cancellation reconcile the exact child receipt under the
same issue lock. Unknown process-host identity requires recovery; local PID
absence is accepted only with the provider's recorded local identity evidence.

The response separates applied task state from dispatch. `applied=true` means
the mutation committed. `dispatchStatus=completed` means queue acknowledgement
or an assignment that needs no wake. `runStarted=null` makes no execution claim.
Read the actual task and Run after dispatch.

## Verification and rollout

Build a separate artifact with `scripts/task-management-overlay.py build`.
Supply `--installed`, a fresh `--artifact` and the existing `--esbuild` executable.
The manifest binds the source SHA and before/after hashes of six files.
Run `scripts/test-task-management.mjs` with an isolated staged server root in
`PAPERCLIP_MANAGEMENT_TEST_ROOT`. It uses real PostgreSQL, HTTP and a deterministic
process fixture. It never uses production data or a paid model.

The independent review found two receipt defects: unrelated wake UUID adoption
and concurrent admission followed by false cancellation. Both have regression
tests. Earlier bare `/usr/bin/true` fixtures triggered normal missing-disposition
recovery; the final fixture explicitly completes its task and verifies one Run
after full execution drain. These earlier failures are not acceptance passes.

Before installation, verify the exact production release, no active Runs,
current database backup and source/artifact hashes. Stop the supervised service,
then run the script's `install` operation with a new `--backup` directory. Start
the same service and verify health, capability and a disposable task. The script
does not control services. If startup or acceptance fails, stop it, use the
script's `rollback` operation with the same artifact and backup, and start the
original service. Rollback checks current and backup hashes before each change.

No database schema changes occur. Keep the existing Claude cleanup patch and all
other package files. A future official upgrade can remove this extension;
clients must negotiate capability and fail closed. This is a bounded local
extension, not a reason to carry the four earlier speculative fork PRs forward.
