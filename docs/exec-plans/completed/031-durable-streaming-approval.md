# 031 Durable streaming and resumable Tool approval

## Status

Completed on 2026-09-23. The User authorized the feature and the named stable
Core API addition. Implementation, full stable/MSRV gates, and independent
technical review are complete; final product acceptance remains with the User.

- [x] Baseline recorded.
- [x] Design independently reviewed; two bounded clarifications incorporated.
- [x] Stable Core API addition authorized.
- [x] Slices implemented.
- [x] Verification passed.
- [x] Independent implementation review completed.
- [x] Review disposition recorded; final product acceptance remains with User.
- [x] Completion evidence recorded.

## Goal

Allow an application to consume model deltas and Tool lifecycle events while
checkpointing a Prebuilt Agent, receive a durable approval suspension, then
approve or reject the saved batch and continue streaming from the latest head.
Both asynchronous streams and synchronous sinks must support this lifecycle.

## Non-goals

No durable event log, token replay, network transport, delivery acknowledgement,
automatic retry, exactly-once guarantee, or remote rollback. Streaming Replay
and Fork are deferred; existing non-streaming methods remain available.
No provider, MCP, dependency, MSRV, checkpoint format, codec identity, graph
version, or lineage changes. No release, Git commit, or push.

## Context and baseline

- HEAD: `6fb153b0d51dda759d6929ff1e70f8402e396250`.
- Initial worktree: clean; no active execution plans.
- AGENTS.md SHA-256:
  `90f6c30ce5e65baa914748e2f4cbc4d21681e6e81780d0cdb02b98e6b70affec`.
- Cargo.lock SHA-256:
  `1cf17e56d6f0860e7a4b5e4d487d0ff1381f8b3dc8ed7709b4bbb35901d369f9`.
- Development compiler: Rust 1.98.1; Rust 1.88.0 is also installed.
- `./scripts/verify fast`: passed before changes on 2026-09-23.
  Local log: `/tmp/group-031-baseline-fast.log` (ephemeral evidence).

Sources of truth:

- [Architecture](../../../ARCHITECTURE.md)
- [Durable Execution](../../design/durable-execution.md)
- [Model and Tools](../../design/model-and-tools.md)
- [Error and Control](../../design/error-cancellation-observability.md)
- [ADR-002](../../adr/002-immutable-state-updates.md)
- [ADR-004](../../adr/004-storage-neutral-checkpoints.md)
- [ADR-010](../../adr/010-stable-base-experimental-adapters.md)
- [Quality](../../quality.md)
- [Plan 030](030-prebuilt-streaming.md)

Current `AgentState::restore` reconstructs durable fields with no event sink.
Core `CompiledGraph::resume` restores State internally; Prebuilt cannot attach
an invocation-specific sink before resumed Nodes run through the existing API.
Plan 030 streaming entrypoints are non-durable, and `ApprovalRequired` is emitted
before Core attempts to save the interrupt. It is not persistence confirmation.

## Roles and permissions

The User selected this feature and owns material-change authorization and
final product acceptance. The primary agent prepares this design and will
implement authorized slices, maintain evidence, and assess review findings.
An independent read-only reviewer evaluates the design and final change under
the repository review workflow. Reviewers do not edit files.

Authorization covers the additive stable Core method below and the experimental
Prebuilt feature. It does not authorize unrelated stable APIs or persistence
changes.

## Invariants

- Core remains independent of Prebuilt, Model, Tool, and concrete sinks.
- Nodes still read immutable State; Runtime alone applies typed Updates.
- Compiled graphs remain immutable and reusable across concurrent invocations.
- No shared mutable Agent State, global run lock, detached task, or per-Tool task.
- A sink belongs to one invocation and never enters a snapshot or codec.
- Resume remains latest-only and preserves validation, counters, CAS, and errors.
- Approval decisions remain one-attempt values on the interrupted Tool node.
- Existing observer composition, cancellation, and source-chain behavior remain.
- Event Debug formatting excludes payloads; explicit typed access stays available.
- Tests use offline fixtures and local stores; all crates preserve Rust 1.88.

## Proposed design

### Explicit restore initialization in Core

Add one method to `CompiledGraph<S>` where `S: CheckpointState`:

```rust
pub async fn resume_with_state_initializer<F>(
    &self,
    resume_config: ResumeConfig<S::Snapshot>,
    initialize: F,
) -> Result<ExecutionOutcome<S>, GraphRunError>
where
    F: FnOnce(&mut S) -> Result<(), SnapshotError> + Send;
```

Existing `resume` delegates to this implementation with a no-op initializer.
After checkpoint/head/frontier/resume-value validation and successful
`S::restore`, Runtime checks control, invokes the initializer exactly once,
checks control again, then emits resume events and executes the saved frontier.
Initialization also occurs for a valid completed checkpoint with no Nodes left.
An initializer error follows the existing `RestoreFailed` classification and
retains its concrete `SnapshotError` source. No Node executes or checkpoint is
written on this failure. Pre-cancelled, invalid, missing, or failed-restore
requests do not invoke the initializer.

The callback is synchronous and intended only to attach transient resources.
Its API documentation requires preserving restored durable fields and avoiding
blocking work and external side effects. It is not an Update or a historical
editing API. Like existing synchronous restore work, it cannot be preempted
mid-call; panic behavior follows existing application restore callbacks.

This explicit seam is preferred over private task-local context: task-local
inheritance would require isolation wrappers around all restored-state APIs,
including nested non-streaming invocations. Recompiling the graph per invocation
would abandon the existing compile-once contract. A Checkpointer wrapper would
mix transient execution context with storage responsibilities.

### Prebuilt entrypoints

Add these experimental `ToolCallingAgent` methods:

| Method | Inputs beyond `&self` | Result |
| --- | --- | --- |
| `stream_with_checkpoint` | messages, checkpoint config | `AgentEventStream` |
| `stream_with_checkpoint_control` | messages, event config, run control, checkpoint config | `AgentEventStream` |
| `invoke_with_checkpoint_stream_sink` | messages, checkpoint config, sink | async `Result<AgentRunOutcome, AgentError>` |
| `invoke_with_checkpoint_stream_sink_control` | messages, event config, run control, checkpoint config, sink | async `Result<AgentRunOutcome, AgentError>` |
| `resume_stream` | resume config | `AgentEventStream` |
| `resume_with_stream_sink` | resume config, sink | async `Result<AgentRunOutcome, AgentError>` |

Resume configuration already carries Core controls and events, so it needs no
additional control variant. The Agent's existing default additional step budget
rule remains in force. Prebuilt uses the Core initializer solely to install the
new sink on restored private State. Existing checkpoints can switch between
streaming and non-streaming invocation modes with the same approval setting.

Keep durable streaming implementation in a focused Prebuilt module instead of
adding another large block to `agent.rs`. Share private orchestration where it
preserves the existing public contracts and avoids duplicate completion events.

### Terminal event contract

Add `AgentStreamEvent::Interrupted(AgentInterrupted)` and the value traits needed
to preserve `AgentStreamEvent: Clone + PartialEq`. Preserve all current variants.
The new variant contains the saved thread/checkpoint identity and optional
approval request through the existing payload-safe wrapper.

- Model deltas pass collector validation before delivery.
- `ApprovalRequired` remains a provisional pre-save notification.
- Only after Core successfully saves the interrupt does the durable API emit
  `Interrupted`; that event is the resumable approval handoff.
- A normal durable result emits `Completed` only after required saves succeed.
- Each successful durable stream has exactly one terminal outcome event and
  then ends. A failed stream yields exactly one typed error after already
  buffered events, without a successful terminal event.
- Sinks receive the same events; typed failures remain in the returned Result.
- Resume emits Model/Tool events only for newly executed work. Earlier deltas
  are not replayed. Resuming a completed checkpoint emits exactly one terminal
  `Completed` event with no Model/Tool calls and no checkpoint write.
  A pending batch is dispatched only after an explicit Approve value;
  Reject creates ToolMessages without Tool execution events.
- Dropping a stream drops its owned invocation. Delivered deltas do not prove
  State commit; only stored checkpoints define recoverable boundaries.

Internally, the stream driver may own a `Result<(), AgentError>` future because
terminal outcomes are already delivered as events. This avoids making durable
interruption look like an ordinary completed AgentOutcome.

## Implementation slices

### Slice 1: Explicit restore initialization

- [x] Obtain authorization for the proposed Core API.
- [x] Add direct Core public-boundary tests before implementation.
- [x] Implement the method and preserve existing `resume` behavior.
- [x] Test successful and completed restore, invalid/missing checkpoints,
  restore/initializer failure, cancellation, and concrete error sources.
- [x] Run focused Core tests and `./scripts/verify fast`; self-review.

Expected scope: Core runtime, focused integration tests, durable design docs.

### Slice 2: Durable streaming invoke and approval suspension

- [x] Add direct tests of stream and sink entrypoints before implementation.
- [x] Add terminal interruption event and checkpointed streaming methods.
- [x] Test normal completion, MaxRounds, persisted approval, rejected protocol
  events, save failure, observer composition, and stream fusion.
- [x] Verify a failed interrupt save emits no durable handoff or Tool execution.
- [x] Run focused Prebuilt tests and fast verification; self-review.

Expected scope: focused durable-streaming module, stream/outcome types, module
wiring and direct integration tests; minimal necessary visibility adjustments.

### Slice 3: Streaming resume and isolation

- [x] Add approve/reject resume methods using the explicit Core initializer.
- [x] Test restart with a reopened SQLite store and a newly constructed Agent.
- [x] Test ordinary-to-streaming and streaming-to-ordinary resume compatibility.
- [x] Test two interleaved runs on one Agent: no sink, decision, or event leakage.
- [x] Test nested resumes with distinct sinks: inner events never reach the
  outer sink, including a nested ordinary non-streaming invocation.
- [x] Test completed checkpoint resume through both new interfaces: one terminal
  completion, no Model/Tool execution, and no checkpoint write.
- [x] Test missing/wrong decision and stale targets: typed failure and no dispatch.
- [x] Test pending Model/Tool cancellation, run/node timeout, and immediate drop
  through both interfaces with markers, channels, or paused time.
- [x] Verify committed transcript/usage continuity and no completed Tool replay.
- [x] Run focused tests and fast verification; self-review.

Expected scope: durable-streaming module and focused success/control/SQLite tests.

### Slice 4: Documentation, example, and acceptance review

- [x] Add an offline example showing streaming, durable suspension, and resume.
- [x] Synchronize README, crate docs, architecture, design, and quality ledger.
- [x] Run `./scripts/verify all` and the new offline example.
- [x] Review allocation, Clone, channel, polling, and callback scope; benchmark
  compilation is not a runtime performance measurement.
- [x] Independent read-only implementation review; correct findings and rerun
  affected gates before recording disposition.

## Acceptance criteria

- [x] Both interfaces complete a streamed Model -> approval suspension ->
  approve/reject Resume -> streamed answer flow through public APIs.
- [x] Durable handoff occurs only after a successful interrupt save.
- [x] Existing durable/streaming methods, stored format, and observer contracts
  remain compatible; no cross-invocation sink sharing occurs.
- [x] Store/protocol/control failures preserve typed sources and local lifecycle;
  no automatic retry or false success event occurs.
- [x] SQLite reopen and restored transcript tests prove the persistence boundary.
- [x] Documentation and example explain provisional events, durable outcomes,
  local cancellation, and lack of durable event replay.
- [x] Full stable/MSRV verification and independent review are recorded.

## Verification

Verification commands (actual outcomes recorded below):

```text
cargo test --locked -p group-agent-core --test resume
cargo test --locked -p group-agent-prebuilt
./scripts/verify fast
./scripts/verify all
cargo run --locked --offline -p group-agent-prebuilt --example durable_streaming
git diff --check
```

Add exact focused-test commands as test targets are created. Verify protected
hashes and final worktree scope before handoff. No provider quota or hosted CI
is needed for this local implementation.

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-09-23 | Prepare an explicit Core initializer for authorization | Current restore API cannot attach invocation-local resources; avoid hidden context and preserve graph reuse. |
| 2026-09-23 | Distinguish provisional approval from durable interruption | A pre-save event cannot safely identify a resumable checkpoint. |
| 2026-09-23 | Limit this Plan to checkpointed invoke and Resume | Covers the selected approval lifecycle; streaming Replay/Fork are separate follow-ups. |
| 2026-09-23 | Proceed with explicit Core initializer | User authorized the named method and implementation. |

## Review findings

Independent read-only design reviewer: `design_review`.
Conclusion: **PASS WITH MINOR FIXES**, limited to this proposed design.

Both bounded findings are incorporated in this Plan:

1. Add nested-resume isolation acceptance alongside interleaved invocation tests.
2. Clarify completed-checkpoint streaming Resume: emit a terminal Completed
   event without Model/Tool calls or checkpoint writes, and test both interfaces.

The reviewer confirmed the explicit fallible initializer, validation/control
ordering, existing RestoreFailed source mapping, and completed-checkpoint
initialization contract. The User subsequently authorized the Core API addition.

Final independent implementation review by `design_review`: **PASS**, no Major
or Minor findings and no required corrections. The reviewer inspected all 23
changed/new files at review time, independently ran the 17 Core resume tests
and 20 focused Prebuilt tests, ran the offline example and diff checks, and
verified protected hashes. It confirmed persistence-before-terminal-event,
per-invocation restored sinks, approval/rejection behavior, typed failure and
observer contracts, cancellation/fusion, SQLite reopen, cross-mode compatibility,
and absence of hidden tasks or graph recompilation. The primary agent accepts
this technical review disposition and records it here; final product acceptance
remains with the User. The reviewer did not rerun the entire full/MSRV matrix;
its conclusion explicitly leaves that completion gate to the primary agent.

## Implementation and verification evidence

- Core tests were written first; the initial compile failed on the missing new
  public method. After implementation, `cargo test --locked -p group-agent-core
  --test resume` passed all 17 tests, including four new initializer tests.
- Prebuilt durable-stream tests initially failed compilation on the missing
  public methods and interruption event. After implementation, focused tests
  passed for invoke/resume, approval, MaxRounds, and completed checkpoints.
- `cargo test --locked -p group-agent-prebuilt` passed before the final observer
  regression was added. The later focused `durable_streaming_failures` target
  passed all three tests including resumed observer start rejection.
- Expanded existing stream control tests cover plain, checkpointed, and resumed
  execution; each tests both interfaces and pending Model/Tool work under
  cancellation, run timeout, node timeout, and Future drop. All four tests pass.
- Six existing protocol/observer regression tests now also exercise both
  checkpointed streaming interfaces; all pass.
- Isolation tests (three) and SQLite reopen test (four combinations of interface
  and approval decision) pass. A failed final model stream test proves completed
  Tool work is not repeated on Resume.
- `./scripts/verify fast` passed after adapting the private stream future's
  unit-test output type and correcting the new checkpoint accessor assertion.
- `cargo run --locked --offline -p group-agent-prebuilt --example durable_streaming`
  passed, showing provisional approval, persisted interruption, Tool lifecycle,
  new model deltas, and completion.
- The first full gate found a synchronous test MutexGuard spanning an await;
  the test now clones its recorded transcript before asynchronous inspection.
  The next full run reached an existing Genai loopback HTTP test but sandbox
  socket binding failed with PermissionDenied. An approved out-of-sandbox rerun
  of the same `./scripts/verify all` passed: strict Clippy, workspace tests and
  doctests, benchmark compilation, all targets/features, and Rust 1.88 workspace
  checks/tests. Both the stable workspace test phase and the Rust 1.88 phase
  passed 641 tests including doctests, with zero failures and zero ignored tests.
  The final successful log contains no warnings.

Independent Slice 1 review by `design_review`: **PASS**, no required findings.
The reviewer independently ran all 17 Core resume tests and assessed allocation,
callback placement, typed errors, cancellation, and compatibility. An optional
additional cancellation-inside-restore fixture was suggested; the existing
post-restore control check is unchanged and visibly precedes initialization.

Performance self-review: the Core path adds no task, lock, or allocation. Each
Prebuilt asynchronous stream retains its existing owned-future/channel model;
resumed State receives one Arc sink attachment. No graph recompilation, snapshot
sink retention, or transcript clone per token was introduced. Terminal completion
still owns/clones the transcript for event delivery, and synchronous sink APIs
also retain the returned outcome. No runtime performance measurement is claimed.

Local logs are ephemeral: `/tmp/group-031-core-red.log`,
`/tmp/group-031-core-green.log`, `/tmp/group-031-stream-red.log`,
`/tmp/group-031-expanded-tests.log`, `/tmp/group-031-isolation-tests.log`,
`/tmp/group-031-prebuilt.log`, `/tmp/group-031-failures.log`,
`/tmp/group-031-example.log`, and `/tmp/group-031-verify-all.log`.

## Completion evidence

- Acceptance, all four slices, full verification, and independent review are
  complete. The primary agent accepted the PASS technical disposition and
  archived this Plan after the full gate succeeded.
- Final HEAD: `6fb153b0d51dda759d6929ff1e70f8402e396250` (unchanged).
- Final worktree: 16 modified tracked files and 8 new files, all in this Plan's
  scope. No commit, push, hosted CI, live-provider request, or runtime performance
  benchmark was performed. Benchmark compilation passed; no speedup claim.
- AGENTS.md and Cargo.lock retain the baseline hashes. Manifest dependencies,
  checkpoint format, codec identity, and graph versions are unchanged.
- Final documentation-only closure passed `git diff --check` and local Markdown
  link checks. Full verification preceded closure; no code changed afterward.
- Limits: event delivery is process-local and not durable, token history is not
  replayed, synchronous callbacks cannot be preempted, remote effects cannot be
  rolled back by local cancellation, and streaming Replay/Fork remain deferred.

Final changed-file scope:

- Production: Core `src/runtime.rs`; Prebuilt `src/agent.rs`, `src/durable_stream.rs`,
  `src/lib.rs`, `src/outcome.rs`, `src/state.rs`, and `src/stream.rs`.
- Tests and fixture: Core `tests/resume.rs`; Prebuilt
  `test_support/streaming_agent.rs`, `tests/durable_streaming.rs`,
  `tests/durable_streaming_failures.rs`, `tests/durable_streaming_isolation.rs`,
  `tests/durable_streaming_sqlite.rs`, `tests/streaming_control.rs`, and
  `tests/streaming_regressions.rs`.
- Example: Prebuilt `examples/durable_streaming.rs`.
- Documentation: root README and ARCHITECTURE, durable-execution and
  model-and-tools design docs, quality ledger, active/completed Plan indexes,
  and this completed Plan.
