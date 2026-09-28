# 036 Stage 24 - Durable conditional Agent branching

## Status

Completed on 2026-09-28 after user acceptance ("可以，commit and push").
Implementation and post-codec verification are complete. Independent implementation and
codec-correction reviews returned PASS. The user authorized the concrete Stage 24
API, new descriptors/identity and feature edge by replying "开始" on 2026-09-28.
The [Stage 24 specification](../../specs/024-durable-agent-branching.md) is authoritative.

- [x] Initial baseline and proposed scope recorded.
- [x] Consumer ergonomics and existing mechanisms inspected.
- [x] Concrete specification and design independently reviewed.
- [x] Material public API and persistence changes authorized.
- [x] Implementation slices and direct tests complete.
- [x] Required verification passed.
- [x] Independent implementation review accepted and written back.
- [x] Completion evidence recorded.

## Goal

Allow a validated typed result from Agent A to select Agent B, Agent C or direct
completion from a construction-time target set. Once the selection is saved,
a fresh process resumes the selected path without rerunning A or the selector
from that boundary. Unselected Agents execute no model or Tool calls. Malformed multi-node
frontiers may still allow a valid selected node to execute before another node
fails; no whole-frontier corruption preflight is promised (see specification).

## Non-goals

Feedback loops, parallel branches, dynamic targets, arbitrary graph builders,
agent-as-tool, shared memory, automatic retry, new providers, distributed leases,
release/publication and product RAG/UI/policy. Do not generalize AgentSequence
or change its existing formats merely to accommodate this capability.

## Context and baseline

- [Roadmap](../../roadmap.md): order and deferred capabilities.
- [Architecture](../../../ARCHITECTURE.md), [Core Runtime](../../design/core-runtime.md),
  [Durability](../../design/durable-execution.md), [Model and Tools](../../design/model-and-tools.md).
- [Stage 23 specification](../../specs/023-durable-agent-sequence.md),
  [Plan 035](../completed/035-durable-agent-sequence.md), [Quality](../../quality.md).
- Inspect `crates/group-agent-prebuilt/src/sequence/`, shared `agent_nodes.rs`,
  sequence public tests and the offline example before proposing reuse.
- HEAD: `46c29a003c9d36a49c530fa86462fa6a546fdac8`, branch `master`.
- Worktree and diff were clean before these planning documents.
- AGENTS.md SHA-256: `90f6c30ce5e65baa914748e2f4cbc4d21681e6e81780d0cdb02b98e6b70affec`.
- Cargo.lock SHA-256: `1e0b7dfe28b76628143c59db4cbe96503dd91d2f0afe9648297e7a5695bc9a0a`.

Refresh this baseline at implementation start; preserve these planning changes
and any unrelated user work. Historical Stage 23 gates are not Stage 24 evidence.

## Roles and permissions

The user owns scope, material-change authorization and final acceptance. The
orchestrator prepares concrete choices and review boundaries. The implementer
updates this Plan and evidence within authorized scope. Independent reviewers
remain read-only and emit standalone findings; a write-authorized role records
the accepted disposition. The user explicitly authorized the concrete public API, new independent
persistence formats/identity and feature edge on 2026-09-28.
Git commit/push and publication are not part of this request.

## Invariants

Reuse Core execution with immutable node input and Runtime-applied updates.
Keep routing synchronous and read-only after commit; do not add a second
scheduler, global run lock, full State Clone or task per Agent. Preserve typed
errors, concrete sources, safe default formatting and Rust 1.88 compatibility.
Preserve existing Agent/AgentSequence APIs, snapshots, descriptors and identities.
Resume stays latest-only; Replay is read-only for lineage but may repeat external
effects; Fork is the writable historical operation. CAS is not execution locking.

## Reviewed design

Candidate topology: A -> saved selection -> B / C / completion. Each selected
Agent has isolated configuration and conversation. A pure bounded application
selector consumes validated output; the model does not name arbitrary nodes.

Prefer computing the selection in a node and committing a typed selection
Update. Core routing then reads that selection. Define and test the durable
boundary before invoking the selected downstream Agent. A selector may repeat
if its result was not saved; never label a provisional event as durable.

The linked specification resolves:

1. Public construction, branch/result types and typed handoff for different
   B/C outputs, including direct completion and A's MaxRounds outcome.
2. Selection/message validation, panic/error classification and event attribution.
3. Snapshot representation, codec/graph identity, semantic revision rules and
   restored selection/frontier/budget guards before each node
   performs external work, with the mixed-frontier limitation documented in the specification.
4. Approval checkpoint pinning and branch attribution, controls and transient
   stream sink restoration; reuse without multiplying public wrapper APIs.

The approved implementation adds independent formats; no old format migration.

## Ordered slices

### 0. Consumer evidence and concrete design

Depends on: refreshed baseline. Likely scope: external consumer fixture,
Stage 24 specification and this Plan (small documentation/design slice).

- [x] Exercise existing construction, streaming and approval recovery from an
      independent consumer; record friction and justified internal reuse.
- [x] Resolve the four design questions above with an API table, state/identity
      model and failure matrix; split later slices further if needed.
- [x] Obtain independent read-only design review.
- [x] Obtain material-change authorization for the reviewed proposal.

Verification: compile/run the offline consumer with existing features; check
proposed behavior against public tests and architecture. This is the first
review checkpoint, before product implementation.

### 1. Fixed branch selection

Depends on: slice 0. Likely scope: new Prebuilt branch construction/state/node
modules and focused public tests; split by contract if more than five files.

- [x] Implement A-to-B, A-to-C and direct completion with typed validated handoff.
- [x] Prove unselected branches make zero calls; reject invalid selections or
      messages before downstream effects; classify selector failures.
- [x] Verify A exhaustion and typed results without changing AgentSequence.

Verification: focused offline public tests, strict Clippy and self-review.

### 2. Durable selection and recovery

Depends on: slice 1. Likely scope: branch codec/durable modules and focused tests.

- [x] Add the approved snapshot/identity surface and enforce saved selection.
- [x] Test save failure and corrupt selection/budget and single-node frontier
      before effects; characterize forged mixed-frontier effects separately;
      verify Resume/Replay/Fork mutations and compatibility rejection boundaries.
- [x] Test process termination before and after selection persistence with fresh
      SQLite recovery and execution counters/journal.

Verification: public Store/process tests using markers and real kill/reap,
including no false durable terminal events. Review this boundary before slice 3.

### 3. Approval and streaming controls

Depends on: slice 2. Likely scope: branch approval/streaming modules and tests.

- [x] Bind approval to the selected branch and saved checkpoint; test approve,
      reject, stale/wrong decisions and competing attempts' documented limits.
- [x] Restore invocation-local sinks; distinguish provisional and durable events.
- [x] Test cancellation, deadline and stream drop with no detached execution.

Verification: offline public lifecycle tests and stable/MSRV focused checks.

### 4. Integration and acceptance

Depends on: slices 1-3. Likely scope: offline example, specification, design/quality
updates and this Plan; split documentation closure from example work if needed.

- [x] Deliver an independent-consumer example and confirm feature isolation.
- [x] Run full gates; review performance costs and report measurement limits.
- [x] Obtain independent read-only implementation review, fix required findings,
      and record accepted technical evidence.
- [x] Obtain user final acceptance, then move the completed Plan.

## Acceptance criteria and verification

The checklist above is the task list. Completion requires all three outcomes,
zero unselected calls, truthful saved-boundary recovery, compatibility, failure
classification/source reachability and controlled lifecycle behavior. Documentation
must explain unsaved-effect repetition and application coordination duties.

Planned commands (not yet Stage 24 results):

```bash
GROUP_VERIFY_OFFLINE=1 ./scripts/verify all
cargo test --locked --offline --workspace --all-features
git diff --check
```

Add exact focused tests, consumer commands and example commands once the public
surface exists. No live provider quota is needed. Bench compilation is not timing
evidence; any measured claims require a separately recorded fixture/environment.

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-09-26 | Save proposed roadmap now; defer implementation | User plans to return next session |
| 2026-09-26 | Branching before bounded feedback and fixed parallel join | Expand one execution semantic at a time |
| 2026-09-26 | Start with consumer evidence and concrete design | Avoid premature generic APIs and duplicate public surfaces |
| 2026-09-28 | Continue slice 0 before material product changes | User resumed development; concrete API/format approval remains required |
| 2026-09-28 | Propose branch feature implying sequence | Reuse unchanged public AgentStage types without relocation |
| 2026-09-28 | Save a closed B/C/Complete selection in one update | Persist intent before downstream work; avoid arbitrary node routing |
| 2026-09-28 | Document mixed corrupt frontier limits and test them | Per-node checks do not provide whole-frontier preflight; preserve Core API scope |

## Review findings

Independent read-only mechanism inspection confirmed reuse of existing private
ModelNode/ToolNode operations and identified per-node semantic frontier guards,
lifetime round caps, zero-node outcome checks, Fork write limits and the shared
AgentStage feature dependency as design requirements. Initial concrete design review returned REQUIRES FIX: per-node guards cannot
prevent a valid selected node from doing work in a forged multi-node frontier
before another node fails. The specification now explicitly limits the guarantee
to restore and per-node checks, retains zero unselected calls, and requires a
mixed-frontier adversarial test. A stronger whole-checkpoint preflight would
require separately authorized Core changes and is excluded. Final independent
read-only re-review returned PASS for slice 0; the orchestrator accepts the
correction and records that design conclusion. The reviewer independently ran
the stable consumer, checked the diff and protected hashes, and assessed
architecture, error/panic behavior, compatibility and performance boundaries.
MSRV, standalone Clippy and unified fast were run by the implementer, not
repeated by the reviewer. This was the slice 0 conclusion; subsequent user
implementation authorization and implementation review are recorded below.

## Completion evidence and next-session handoff

The authorized capability is implemented. Final technical review is PASS;
post-codec full verification passed and final evidence is recorded below. The user
accepted Stage 24 and authorized commit/push on 2026-09-28. This Plan is archived
in completed plans. Later roadmap stages and release publication remain separately scoped.

The following planning checks are historical slice 0 evidence, not implementation
verification; current results are recorded in the final evidence section below.

Planning-only checks on 2026-09-26: `GROUP_VERIFY_OFFLINE=1 ./scripts/verify fast`
passed (diff check, formatting, workspace all-target/all-feature check). Local
Markdown target existence and whitespace checks passed for all four changed
planning/index files. Full tests, MSRV, runtime measurements and independent
implementation review were not run because this change contains only proposed
planning documentation; no Stage 24 behavior is verified. AGENTS.md and Cargo.lock
hashes remain unchanged. No commit or push was requested for these documents.


## Resumed slice 0 evidence (2026-09-28)

Baseline: HEAD remains `46c29a003c9d36a49c530fa86462fa6a546fdac8`.
Existing dirty paths were modified `docs/exec-plans/active/README.md` and
`docs/index.md`, and untracked Plan 036 and `docs/roadmap.md`; all belong to the
saved roadmap and are preserved/continued. AGENTS.md and Cargo.lock hashes match
the original baseline above. Compilers: stable Rust 1.98.1 and installed 1.88.0.
No production files, public APIs, workspace manifests or lockfile were changed.

The [standalone consumer](../evidence/036-consumer/Cargo.toml) is outside the
workspace, with its own ignored generated lockfile and target directory. Its
path dependencies use public Group APIs, no repository test support. Run from
repository root:

```bash
CARGO_TARGET_DIR="$PWD/target" cargo run --offline --manifest-path docs/exec-plans/evidence/036-consumer/Cargo.toml
CARGO_TARGET_DIR="$PWD/target" cargo +1.88.0 run --locked --offline --manifest-path docs/exec-plans/evidence/036-consumer/Cargo.toml
cargo fmt --manifest-path docs/exec-plans/evidence/036-consumer/Cargo.toml --check
CARGO_TARGET_DIR="$PWD/target" cargo clippy --locked --offline --manifest-path docs/exec-plans/evidence/036-consumer/Cargo.toml -- -D warnings
GROUP_VERIFY_OFFLINE=1 ./scripts/verify fast
git diff --check
```

Stable consumer PASS: independent construction and typed handoff, saved B
approval with zero Tool calls, fresh-object streaming Resume with A=0/mapper=0/
B=1/Tool=1, B-attributed events and exactly one final typed Completed. This is
InMemoryCheckpointer recovery, not fresh-process/SQLite coverage. Rust 1.88 consumer also PASS with the same counters. Standalone rustfmt and
strict Clippy PASS. `GROUP_VERIFY_OFFLINE=1 ./scripts/verify fast` PASS (workspace
rustfmt, all-target/all-feature check and diff check). Local Markdown link-target
and whitespace checks PASS for all five design/index documents. Consumer dependencies
resolve separately from the unchanged workspace lock; no registry publication
or network/provider access is required.

The new specification resolves construction/result types, selection failures,
phase/frontier/budget validation, descriptors/identity, approval pinning and
transient sinks. Proposed `agent-branch -> agent-sequence` preserves AgentStage
signatures and avoids a premature shared public workflow abstraction. Direct
Complete returns A's output; A MaxRounds has no selection. Selector panic
conversion is local to the new capability and does not alter Sequence behavior.

At the slice 0 checkpoint, full implementation gates, process-kill tests, runtime
measurements, live providers and hosted CI were deferred because no Stage 24
production behavior existed yet. Benchmark compilation/timing claims are not made. No commit or push.


Final slice 0 scope: continued four pre-existing roadmap/Plan/index paths; added
Stage 24 specification and the three-file standalone consumer (manifest, source,
local ignore rules). Working tree remains uncommitted: two tracked Markdown
paths modified, Plan/roadmap/spec/evidence untracked. Protected hashes remain
unchanged. Slice 0 technical preparation is complete; its authorization checkbox
and all product implementation slices remain open.

## Implementation authorization and slices

2026-09-28: user authorized the reviewed proposal. No commit/push authority.
Slice 1 is split into admission/state/selection with public invoke tests, then
codec/durable reports and recovery tests; stream/control wrappers follow.
Existing Sequence source/format/identity must remain unchanged.


## Historical implementation progress (2026-09-28)

- Public fixed-choice test was red before AgentBranch existed, then passed for
  B/C/Complete with zero unselected model/Tool calls.
- New branch modules reuse shared ModelNode/ToolNode and AgentStage. Existing
  Sequence source remains unchanged; thin reports/entrypoints preserve its
  conventions without creating a generic workflow abstraction.
- B/C/Complete recovery, selector error/panic, distinct schemas, stage exhaustion,
  approval approve/reject/negative decisions, identity drift, save failures,
  codec/corruption/frontier, streaming and lifecycle tests pass locally.
- Sixteen process kill/reap scenarios cover B/C before selection, saved selection,
  approval approve/reject, saved Tool results, completion and identity mismatch,
  plus direct completion before/after selection. Journal counters prove selected
  paths; recovered output and ToolMessage identity/content are checked.
- A red regression showed old initial Sequence JSON was accepted by optional
  field defaults. New BranchSnapshot rejects unknown fields; the regression
  is green and old Sequence code/goldens are unchanged.
- Independent early review found only terminology residue, now corrected.
  Independent durable review passed 24 tests and requested precise failure
  classification/source assertions. These assertions were added for approval,
  stale checkpoint and cancellation/timeout; targeted checks pass.
- Strict Prebuilt all-target/all-feature Clippy and complete Prebuilt all-feature
  tests passed. Full stable/MSRV workspace verification and independent consumer
  matrix are running; no completion claim until results are recorded.


## Final review disposition and remaining limits

Independent read-only implementation review returned PASS after correcting
terminology and strengthening precise approval/control error assertions. The
reviewer independently ran the then-current 24 Branch tests and a 16-test changed
subset, inspected the full gate logs, compatibility hashes and documentation,
and assessed allocation/validation costs, panic behavior and lineage limits.

Subsequent self-review reproduced malformed JSON content leaking through direct
Branch codec error Debug. A safe BranchError wrapper now shields both JSON and
restore error sources without removing their concrete source chains or changing
format bytes. The regression was red then green. Independent delta review
returned PASS after running codec and expanded B/C/Complete corruption tests.
The suggested interrupt fixture was changed to a content-bearing invalid request
string; stable/MSRV codec checks verify the final fixture. The orchestrator
accepts these technical reviews; user final product acceptance remains separate.

An analogous older Sequence codec source-formatting risk was found by source
inspection only, recorded in docs/quality.md as follow-up debt. Existing Sequence
code and formats were preserved; this is not a claim of universal codec redaction.

Performance review: three immutable stage configurations, at most two mutable
transcripts, no full State Clone or per-stage task/global lock. Selected paths add
one selector node/save; repeated output validation and transcript admission incur
bounded-output and transcript-length costs. Existing unbounded event channels
remain. No runtime performance measurement, live providers, hosted CI, publication,
power-loss test or exactly-once certification was performed.

Feature validation: the reproducible `python3 docs/exec-plans/evidence/036-check-features.py`
passed eight stable/MSRV positive builds (default, structured-output, sequence,
branch) and six expected-failure checks proving AgentBranch remains absent without
its feature. An initial fixture used invalid impl-Trait method turbofish; corrected
to a public type probe, then the complete matrix passed. No product fix was needed.

Standalone Branch consumers both passed using the public example outside the
workspace:

```bash
CARGO_TARGET_DIR="$PWD/target/036-consumers" cargo run --offline --manifest-path docs/exec-plans/evidence/036-branch-consumer/Cargo.toml
CARGO_TARGET_DIR="$PWD/target/036-consumers" cargo +1.88.0 run --locked --offline --manifest-path docs/exec-plans/evidence/036-branch-consumer/Cargo.toml
```

The independent consumer and feature checks preceded the final codec diagnostic
wrapper; the wrapper changes no API/format/feature resolution. Final public codec
regression and full stable/MSRV gates cover that correction.


## Final verification and worktree evidence (2026-09-28)

| Executed command/check | Result |
| --- | --- |
| `GROUP_VERIFY_OFFLINE=1 ./scripts/verify all` after codec correction | PASS: format/diff, strict workspace Clippy, default tests/doctests, benchmark compilation, all-target/all-feature check and Rust 1.88 all-feature tests |
| `cargo test --locked --offline --workspace --all-features` after codec correction | PASS: stable 742 tests/doctests, zero failed/ignored |
| Rust 1.88 all-feature test phase of verify all | PASS: 742 tests/doctests, zero failed/ignored |
| Final codec regression on stable and Rust 1.88 | PASS: three tests each, including directly content-bearing malformed interrupt JSON |
| `cargo run --locked --offline -p group-agent-prebuilt --features agent-branch --example agent_branch` | PASS: public construction, typed result and saved approval streaming recovery |
| Independent Branch consumer on stable and Rust 1.88 | PASS: restored A=0, selector=0, selected model=1, Tool=1 |
| `python3 docs/exec-plans/evidence/036-check-features.py` | PASS: eight positive checks and six expected import failures |
| Independent implementation and codec delta review | PASS after required corrections; technical conclusion accepted |
| Local Markdown targets, whitespace and final `git diff --check` | PASS |

Full logs were captured locally in `/tmp/group-036-verify-all.log`,
`/tmp/group-036-workspace-all-features.log`, `/tmp/group-036-codec-final.log`,
`/tmp/group-036-consumer.log` and `/tmp/group-036-features.log`. Logs are temporary;
commands and durable test/consumer sources above are the reproducible evidence.
The final fixture-only codec assertion was also run directly on both compilers.

Final scope is 41 files including prior slice 0 work: 9 modified tracked files
and 32 untracked additions. Production changes are limited to Prebuilt's feature
and export declarations plus ten new `src/branch/` modules. Added one example,
one test-support module and eleven Branch integration-test files. Documentation
scope is README, ARCHITECTURE, model/durable design, quality, roadmap/spec/Plan,
active/index navigation and six evidence fixture/script files. Existing Agent,
Sequence, Core, Model, Tool, adapters, migrations, AGENTS.md and Cargo.lock are
unchanged; protected hashes match the recorded baseline. HEAD is unchanged.
No Git commit, push or publication was performed.

User final acceptance was granted on 2026-09-28, with the documented mixed-frontier,
unsaved-effect repetition, panic-hook and existing Sequence diagnostic limits.


## Acceptance and Git publication authorization

On 2026-09-28 the user accepted the reported capability and explicitly requested
"commit and push". Plan 036 is Completed and moved to this directory. Stage 25/26,
release tags and package publication are not included. Closure changes only
acceptance status/navigation; production code is unchanged from the reviewed,
fully verified implementation. Final closure checks cover Markdown links,
whitespace, protected hashes and the unified fast gate.

Closure checks passed: `GROUP_VERIFY_OFFLINE=1 ./scripts/verify fast`, local
Markdown target validation and `git diff --check`. AGENTS.md and Cargo.lock
hashes are unchanged. Remote master matched the implementation baseline before
commit creation; publication uses ordinary fast-forward Git push.

Implementation and public tests were committed as `2e0b660`
(`feat: add durable conditional agent branching`). The documentation/acceptance
closure is a separate commit in the same authorized push.
