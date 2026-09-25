# 035 Stage 23 - Durable two-Agent sequence

## Status

Completed on 2026-09-26. The user explicitly approved the public API, independent
snapshot/interrupt descriptors, graph identity and optional feature/dependency
edge. Independent implementation review: PASS. The user subsequently authorized
Git commit and push on 2026-09-26.

- [x] Baseline and scope recorded.
- [x] Specification and ordered implementation tasks written.
- [x] Independent design review accepted.
- [x] User authorizes the concrete implementation surface.
- [x] Implementation slices and direct tests complete.
- [x] Full verification and isolated feature consumers pass.
- [x] Independent implementation review accepted.
- [x] Completion evidence recorded; Plan moved to completed.

## Goal and context

Implement [Stage 23](../../specs/023-durable-agent-sequence.md): fixed A-to-B
handoff with isolated conversations/tools, typed results, parent-owned streaming
and durable approval. The decisive scenario is process termination at B's saved
approval interrupt followed by a fresh reconstruction and Resume with no A rerun.

Baseline:

- HEAD `8aece5d51d2d32140e8f1c863aba5826fd93844b`; branch master.
- `git status --short` and diff: clean before design work.
- AGENTS.md SHA-256 `90f6c30ce5e65baa914748e2f4cbc4d21681e6e81780d0cdb02b98e6b70affec`.
- Cargo.lock SHA-256 `71df47bc1da75343624c98403d42b28ab98fe097528efc71dbba9a0591581624`.
- No overlapping active Plan; Stage 22 / Plan 034 committed and pushed.

Read with [Architecture](../../../ARCHITECTURE.md),
[Model/Tool design](../../design/model-and-tools.md),
[durability](../../design/durable-execution.md),
[quality](../../quality.md), and
[Plan 034](../completed/034-structured-output.md).
Current code and executable tests outrank this proposed design.

## Roles and authority

The user owns product acceptance and material-change authorization. The
orchestrator owns technical decisions and review disposition. Implementation
updates this Plan and verification evidence. Independent reviewers remain
strictly read-only and return standalone reports; they do not edit the Plan.

Current write scope includes the approved implementation, feature/manifests, new
independent sequence formats, tests, examples and authoritative documentation.
The user separately requested Git commit and push after implementation acceptance.
Package publication remains outside scope.

## Invariants and non-goals

Preserve Core independence, immutable node input, Runtime-only updates,
deterministic routing, no full State Clone and no tasks per stage/Tool call.
Preserve ToolRuntime policy and observer composition. No global run lock,
automatic retry, arbitrary supervisor, parallel workers, child store orchestration,
agent-as-tool, shared memory, UI, product prompt policy or new provider.

Existing standalone Agent APIs, node paths, graph IDs, snapshot/codec bytes and
approval behavior remain compatible. New descriptors are separate formats, not
silent extensions of AgentSnapshotCodec. No Core/SQLite/Tool/Model/Genai production
change is expected. Rust 1.88 remains MSRV; package versions stay locked.

## Design and dependency order

One private Prebuilt graph contains A model/tools, an explicit handoff Node and
B model/tools. Stage wrappers project immutable stage state and tag updates.
The parent owns checkpoint lineage, run control and terminal reporting; stage IDs
attribute events and decisions. This resolves child approval without coordinating
two independently committed graphs. The spec defines exact identity and failure
boundaries; there is no claim of exactly-once external effects.

Dependency order: shared helper proof -> first two-stage path -> handoff failures
and budgets -> durable codecs/identity -> approval -> stream/control -> recovery
matrix -> examples/docs -> full verification -> independent implementation review.
Implementation remains sequential in shared Prebuilt files. Read-only review may
run alongside isolated verification; no parallel implementation ownership.

## Ordered tasks

Each task is independently verifiable and touches roughly five files or fewer.
Cross-cutting final docs are split by responsibility. Test filenames may be split
further without changing their required public-boundary acceptance.

| Task | Likely files | Acceptance / focused verification |
| --- | --- | --- |
| A1 | agent.rs; private agent_nodes.rs; existing agent_tests.rs | Extract shared private model/tool helpers; all existing Prebuilt tests pass; old IDs/codec bytes unchanged; read-only review before composition |
| A2 | Cargo.toml; lib.rs; sequence/{mod,state,handoff}.rs | Feature-gated stage/mapper construction and owned phase/update types; unsupported metadata, duplicate IDs, invalid revision and step overflow fail admission; isolated feature checks |
| A3 | sequence/{mod,nodes,state}.rs; tests/agent_sequence.rs | Invoke A -> mapper -> B through one Core graph; isolated transcripts/registries; typed result; no second private Agent invocation; focused test |
| A4 | sequence/{error,handoff,mod}.rs; tests/agent_sequence.rs | Mapper errors and malformed returned transcript stop before B; exact stage MaxRounds/step bounds; sources reachable and formatting safe |
| Checkpoint A | No new source group | Existing standalone and sequence tests pass; shared-helper independent review accepted |
| B1 | sequence/{codec,identity,state}.rs; tests/agent_sequence_codec.rs | New snapshot/interrupt descriptors and canonical graph digest; golden bytes/IDs, phase validation and feature compatibility tested |
| B2 | sequence/{durable,mod,error}.rs; tests/agent_sequence_durable.rs | Checkpointed start, Resume/Replay/Fork metadata; start policy admission and Resume/Fork policy normalization; restored completed output validated before downstream work; mismatches do no work/create no branch |
| B3 | sequence/{approval,nodes,codec}.rs; tests/agent_sequence_approval.rs | Approval/rejection at either stage; stage-bound decision plus pinned saved checkpoint; missing/wrong/stale input causes no Tool effects |
| Checkpoint B | No new source group | Save-failure, identity and malformed-snapshot matrices pass at public Store/Agent boundaries |
| C1 | sequence/{event,mod,nodes}.rs; tests/agent_sequence_stream.rs | Stage-tagged events, one parent terminal after persistence, no secret Debug, separate invocation sinks, complete/stream parity |
| C2 | sequence/{event,durable,state}.rs; tests/agent_sequence_control.rs | Invoke/resume sinks survive restore; observers compose; cancellation/timeouts/drop release active future; no false Completed or post-error events |
| C3 | test_support/sequence_agent.rs; tests/agent_sequence_process.rs | Marker-driven real child termination and SQLite reopen cover saved A/handoff/B approval/Tool/completed boundaries with synced execution journal |
| Checkpoint C | No new source group | Full sequence feature suite passes; saved versus unsaved external effects clearly distinguished |
| D1 | examples/agent_sequence.rs; Cargo.toml; sequence/mod.rs | Runnable offline two-stage approval recovery and typed Rust extraction example; feature-gated example/public docs |
| D2 | ARCHITECTURE.md; design/model-and-tools.md; design/durable-execution.md; quality.md | Document only implemented composition, new independent formats, source/lifecycle limits and performance evidence |
| D3 | README.md; docs/index.md; this Plan; active/completed Plan indexes | Final verified evidence, accepted review and correct navigation; only archive after implementation acceptance |

Task A2 admission tests may live alongside the private module until A3 adds the
public integration harness; behavior acceptance must ultimately run through the
public constructor/entrypoints. Every slice leaves existing default builds valid.

## Acceptance criteria

- [x] Both stage configurations remain isolated and A's validated result is the
      only automatically supplied handoff input.
- [x] First-stage MaxRounds/mapper failure cannot start B.
- [x] Parent control/events preserve Core and ToolRuntime lifecycle contracts.
- [x] Saved B approval restores in a fresh process without A or mapper calls.
- [x] Changed contract/order/revision/config fails before calls/branch creation.
- [x] Malformed saved A fails before mapper or B side effects.
- [x] Resume/Replay/Fork and output-conversion write boundaries are explicit.
- [x] Existing standalone formats, identities and API behavior remain unchanged.
- [x] Feature-off/on stable/MSRV consumers pass with no unintended dependency cost.
- [x] Full local gates and independent implementation review accepted; skipped
      external checks reported without production-readiness claims.

## Verification

Design phase only:

```bash
GROUP_VERIFY_OFFLINE=1 ./scripts/verify fast
git diff --check
sha256sum AGENTS.md Cargo.lock
```

Also validate local Markdown targets, changed-file scope and independent design
feasibility. A design PASS is not an implementation test or authorization.

Implementation phase additionally:

```bash
cargo test --locked --offline -p group-agent-prebuilt --features agent-sequence
cargo test --locked --offline --workspace --all-features
cargo run --locked --offline -p group-agent-prebuilt --features agent-sequence --example agent_sequence
GROUP_VERIFY_OFFLINE=1 ./scripts/verify all
```

Standalone Prebuilt consumers: feature off, structured-output alone, and
agent-sequence on stable and 1.88. Full gate includes benchmark compilation, not
runtime measurement. Review bounded identity encoding, per-round transcript
cloning, composite snapshot size, no unbounded stream buffers and no repeated
whole-history copying at stage projection. Record performance limits honestly.

## Decision log

| Date | Decision | Reason / residual risk |
| --- | --- | --- |
| 2026-09-26 | One end-to-end capability with exactly two stages | User approved minimal fixed-order composition, not a general agent scheduler |
| 2026-09-26 | One private graph and checkpoint lineage | Avoid lost updates and child-approval translation across two stores |
| 2026-09-26 | Fresh AgentStage construction ingredients | Existing ToolCallingAgent does not expose reusable graph internals |
| 2026-09-26 | Synchronous pure mapper plus saved mapped messages | Replayable boundary without serializing executable closures; purity is application responsibility |
| 2026-09-26 | Explicit application revision in graph identity | Mapper/prompts/Tool semantics cannot be inferred reliably from runtime objects |
| 2026-09-26 | Distinct new snapshot/interrupt descriptors | Preserve all existing Agent durable formats without migrations |
| 2026-09-26 | Start rejects FinalOnly; Resume/Fork force EverySuperstep | Saved A/handoff are required; reuse Core configs despite no resume policy getter |
| 2026-09-26 | Fix two stale structured-output exclusions as documentation only | Stage 22 is implemented; this does not assert Stage 23 already exists |

## Review findings

Independent read-only reviewer `review_035_design` returned **PASS** after
clarifications on 2026-09-26. The orchestrator accepts this design disposition.
At design review, implementation authorization was still pending; the user
subsequently approved the specified implementation surface.

Review confirmed immutable stage projection and tagged updates, private helper
reuse, unchanged standalone approval handling, separate descriptors and feature
boundaries. Clarifications recorded during review:

- Core ResumeConfig exposes target() but not checkpoint policy. Keep Core APIs
  unchanged: inspect the target for approval pinning and explicitly normalize
  Resume/Fork policy to EverySuperstep; test that documented override.
- Core Fork has no restore initializer. Stage/handoff guards validate restored
  results before external work; guard failure can follow branch creation.
- Sequence approval wrapper validates its own stage-bound resume value before
  shared execute/reject helpers; standalone nodes retain their original type.
- Replay is read-only for lineage, not external effects. Unfinished Replay can
  rerun Tools; completed Replay must execute no nodes.

No remaining design findings. Reviewer ran `git diff --check`; no implementation
behavior tests were claimed. Design PASS is not implementation acceptance.

## Implementation checkpoints

- A1 shared helpers: extracted code mechanically equivalent after removing crate
  visibility qualifiers; independent `review_035_helpers` PASS. Existing Prebuilt
  all-feature tests passed (`/tmp/group-035-a1.log`). Old paths/IDs unchanged.
- Initial public sequence test reproduced missing API at compile time
  (`/tmp/group-035-a-red.log`); typed isolated A-to-B path subsequently passed.
- Core composition/durable/stream, failure injection and corruption matrices now
  pass through public entrypoints. Fresh-process kill/reap with SQLite reopen
  passes seven scenarios; synced model/Tool/mapper journals prove saved-boundary
  recovery (`/tmp/group-035-process.log`).
- Initial implementation review required pre-effect active-frontier/budget checks
  and completed output stage attribution. Corrections passed public corruption
  tests; reviewer accepted correction slice. Follow-up preserves original saved
  stage attribution and avoids duplicate final output validation.
- Controls cover cancellation, Node timeout and drop while either model has sent
  Finished but raw EOF remains pending. No false terminal or unsaved checkpoint.
- Final gates, feature consumers, documentation and independent implementation
  review are complete; all ordered tasks A1-D3 are covered.
- Final read-only `review_035_impl` disposition: **PASS**. All required findings
  closed. Its last three verification requests (capability admission, invalid
  handoff transcripts, concrete mapper source) passed on stable and Rust 1.88.
  The orchestrator accepts this implementation disposition.

## Completion evidence

Executed on Rust 1.98.1 and Rust 1.88.0:

- `GROUP_VERIFY_OFFLINE=1 ./scripts/verify all`: PASS; includes strict Clippy,
  default tests/doctests, benchmark compilation, all-target/all-feature checks,
  and full MSRV all-feature tests (712 passed at that checkpoint).
  `/tmp/group-035-verify-all.log`.
- Final `cargo test --locked --offline --workspace --all-features`: PASS,
  714 tests/doctests, zero failures/ignored. `/tmp/group-035-final-stable.log`.
- Final strict workspace all-target/all-feature Clippy: PASS.
  `/tmp/group-035-final-clippy.log`.
- Final public constructor/handoff/source boundary tests: stable and Rust 1.88,
  4 passed each. `/tmp/group-035-final-boundaries.log` and
  `/tmp/group-035-final-boundaries-msrv.log`.
- Final sequence codec golden encoding/descriptor isolation test: stable and
  Rust 1.88, 1 passed each. `/tmp/group-035-codec.log` and
  `/tmp/group-035-codec-msrv.log`. These two final focused commands cover the
  tests added after the 712-test MSRV full gate.
- Isolated Prebuilt consumers using default, structured-output and agent-sequence
  on stable/1.88: all six checks PASS. `/tmp/group-035-consumers.log`.
  The default normal/build dependency tree has no sha2 edge.
- `cargo run --locked --offline -p group-agent-prebuilt --features agent-sequence
  --example agent_sequence`: PASS. `/tmp/group-035-example.log`.
- Process test: two harness tests containing seven real kill/reap/SQLite-reopen
  scenarios PASS. A saved first result, handoff, B approval (approve/reject), B
  Tool results, completed sequence and revision mismatch are covered. Synced
  journals distinguish mapper rerun before its save from no rerun after it.
- Public failure tests verify A-final/handoff/approval save failure, preserved
  previous head, concrete source reachability and no false terminal success.
  Corruption tests exercise both Replay and Fork, budgets and phase/frontier
  guards, with zero model/Tool calls and honest branch-write assertions.
- Cancellation, Node timeout and stream drop pass for either stage after raw
  Finished but before EOF; no false Completed or unexpected checkpoint.
- Identity golden vector and change matrix cover revision, stage IDs/order,
  limits, approval flags, contract name/schema. Rejected Fork creates no branch.
  Resume/Fork policy normalization and Replay's external-effect boundary pass.
- Formatting, `git diff --check`, local Markdown targets and protected artifacts
  pass. No browser UI was changed; Markdown anchor navigation was not browser-tested.

Protected AGENTS.md SHA-256 remains
`90f6c30ce5e65baa914748e2f4cbc4d21681e6e81780d0cdb02b98e6b70affec`.
Cargo.lock SHA-256 is
`1e0b7dfe28b76628143c59db4cbe96503dd91d2f0afe9648297e7a5695bc9a0a`.
The lockfile adds only the existing sha2 dependency edge to Prebuilt; every
package version/source/checksum matches baseline.

Implementation grouping follows the same task boundaries with small private
modules: stage/approval construction in sequence/mod.rs, identity in nodes.rs,
codec in codec.rs and outcomes/events/durable/stream entrypoints separately.
There is no need to create a file for each small type. Shared helper extraction
is mechanically equivalent apart from crate-private visibility.

Performance review: no State Clone requirement, global lock, per-Agent task or
new execution scheduler. Model turns clone only their stage transcript. Saved
completed outputs are bounded and revalidated before downstream work; repeated
validation at different resumed nodes is a documented safety cost. Final
conversion validates each output once. Stream delivery retains the existing
single-owned-future/cooperative token-yield pattern and synchronous observer
contract. Benchmark compilation passed; no runtime timing or speedup claim.

Final scope: Prebuilt shared helpers and optional sequence implementation,
manifest/lock edge, public tests, test support, offline example; README,
architecture/design/quality documents, spec and Plan indexes. Core, Tool, SQLite,
Model and Genai production code are unchanged. Existing Agent snapshot/interrupt
formats and identities are unchanged; sequence formats are independent.
At implementation acceptance, HEAD was
`8aece5d51d2d32140e8f1c863aba5826fd93844b` and Stage 23 changes were
uncommitted. No provider call occurred. The subsequent user request authorizes
committing and pushing this verified scope.

Skipped external checks: hosted CI, live providers, package publication and
runtime performance measurements. Local execution/recovery tests do not establish
provider capability, factual output accuracy or exactly-once effects. Mapper
purity/revision maintenance and coordination of concurrent approval attempts are
application responsibilities. Replay may repeat effects; Fork guards/conversion
may fail after branch creation. No required local gate remains skipped.
