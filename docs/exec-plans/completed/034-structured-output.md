# 034 Stage 22 - Structured output and typed results

## Status

Completed on 2026-09-26 after the user authorized the API, optional feature/
dependency edges and contract-bound graph identities. Independent implementation
review: PASS. The user separately authorized commit and push after acceptance;
package publication is not part of this Stage.

- [x] Baseline and protected artifacts recorded.
- [x] Proposed specification and ordered tasks written.
- [x] Independent design review accepted (design only).
- [x] User authorizes the concrete implementation surface.
- [x] Implementation slices and direct tests complete.
- [x] Full stable/MSRV and feature matrix passed.
- [x] Independent implementation review accepted and written back.
- [x] Completion evidence recorded; Plan moved to completed.

## Goal and context

Deliver [Stage 22](../../specs/022-structured-output.md): an opt-in, bounded,
provider-neutral structured final result that survives durable approval and
process recovery without schema drift. The specification records the implemented contract; executable behavior remains
authoritative.

Baseline 2026-09-26:

- HEAD: `1b70e2356df4e4c90c8fa7ed69552f052926af6d`.
- `git status --short`: clean; `git diff`: empty before authoring.
- AGENTS.md SHA-256: `90f6c30ce5e65baa914748e2f4cbc4d21681e6e81780d0cdb02b98e6b70affec`.
- Cargo.lock SHA-256: `1b1cf4b431f50bf933585ac3d598a74c750852a8b8c420de61a42057a15321c1`.
- No overlapping active plan. Plan 033 is completed and committed.

Read with [Architecture](../../../ARCHITECTURE.md),
[Model/Tool design](../../design/model-and-tools.md),
[Durability](../../design/durable-execution.md),
[Genai guide](../../adapters/genai.md), [ADR-005](../../adr/005-validated-model-facade.md),
[ADR-010](../../adr/010-stable-base-experimental-adapters.md),
[quality](../../quality.md), and completed Plans 031-033.

## Non-goals and invariants

No Core/Tool/SQLite production changes, snapshot/codec/migration changes, old
GraphVersion changes, full JSON Schema engine implementation, schema inference,
new providers, hidden repair/retry/fallback, UI, RAG, Multi-Agent or publication.
Do not change AgentConfig's Copy semantics to carry a heap-owned schema.

Approval was for a new Stage, not a Stage 21 corrective stage. Technical
acceptance and independent implementation review are complete.
No plan-approval claim is a capability or production-readiness claim.

## Roles and permissions

- User owns product acceptance and authorization of the concrete API, feature
  and dependency changes, plus new structured graph identities.
- Orchestrator owns technical direction, bounded slices and review disposition.
- Implementer writes authorized source/tests/docs, maintains this plan and runs
  checks. Current authority covers the approved implementation, tests and documentation.
- Independent reviewer is strictly read-only, checks code-grounded feasibility
  and returns a standalone report. Design PASS is not implementation PASS.

## Implemented design

One immutable, compiled Model output contract gates requests and complete/stream
results. Genai supports one explicit native Chat path with exact schema mapping
and visible refusal handling. Prebuilt carries the contract in its constructor,
validates final responses before commit, derives transient validated results,
and binds structured graph versions to a canonical contract digest. Existing
plain paths, snapshot bytes and APIs remain unchanged.

The [specification](../../specs/022-structured-output.md) fixes the public API
proposal, schema bounds, feature topology, failure precedence, stream terminal
boundary and restore behavior. Resolve changes there before implementation.

## Ordered implementation slices

Each row is a small checkpoint with acceptance and a focused check. Source file
names listed as new were proposed during design; actual coverage is listed below. Wire-up files may be revisited across slices;
no slice should absorb unrelated cleanup. Each slice must leave the workspace
compilable and pass its public tests before the next begins. Cross-cutting gates
and documentation updates are separate tasks rather than hidden implementation.

| Task | Delta / likely file group (at most about five) | Acceptance and verification |
| --- | --- | --- |
| A1 | Root manifest; Model manifest, lib.rs, structured_output/mod.rs, structured_output/schema.rs | Add optional feature using locked packages; immutable v1 profile compiles; feature-off Model check and schema unit tests pass |
| A2 | Model structured_output/{mod,validate,error}.rs; tests/structured_output.rs | Reject malformed/oversized schemas, duplicate JSON keys and output violations; typed extraction and payload-safe errors work; focused Model public tests |
| A3 | Model request.rs, metadata.rs, model.rs, error.rs, tests/structured_output.rs | Add request/capability API; fake adapter is never called on invalid requests; invalid complete results cannot pass facade; focused Model tests |
| A4 | Model structured_output/stream.rs, model.rs, structured_output/mod.rs, tests/structured_output_stream.rs | Hold terminal until EOF, one-error-then-EOF, bounded accumulation, cancellation/drop after raw Finished; public stream tests and Model all-features suite |
| B1 | Genai manifest, config.rs, adapter.rs, openai_chat/request.rs, tests/structured_output.rs | Gate target/capability and map exact schema with shared request builder; preserve default behavior, request parity and no dispatch on rejection |
| B2 | Genai openai_chat/{mod,complete,decode}.rs, error.rs, tests/structured_output.rs | Native bounded completion plus refusal/error parsing; SSE refusal handling; local HTTP success/failure/limit matrix passes |
| B3 | Genai tests/structured_output_control.rs and existing tests/openai_chat.rs or support modules as needed | Complete/stream cancellation, transport parity, no retries, supported Tool rounds and incompatible defaults covered; full Genai feature suite |
| C1 | Prebuilt manifest, lib.rs, agent.rs, error.rs, structured_output.rs | Add additive constructor and compiled-node contract propagation without removing Copy; existing constructors unchanged; feature-off build |
| C2 | Prebuilt agent.rs, outcome.rs, structured_output.rs, tests/structured_output.rs | Expose validated result, typed decoding, MaxRounds and ordinary Tool loop; invalid final never commits; focused Prebuilt tests |
| C3 | Prebuilt durable_stream.rs, outcome.rs, structured_output.rs, tests/structured_output_durable.rs | Same identity restores across invoke/resume/replay/fork and sinks; completed snapshots revalidate before Agent terminal; mismatches prevent execution/lineage effects |
| C4 | Prebuilt tests/structured_output_process.rs and test_support/structured_output_process.rs | Real process kill/recovery under same contract; changed schema/name/plain mode fails before model/tool effects; reuse Plan 033 lifecycle discipline |
| D1 | Model/Genai/Prebuilt examples and public doctests (split if more than five files) | Offline examples demonstrate plain, stream and durable typed extraction; wrong Rust type does not rerun Agent |
| D2 | Architecture; Model/Tool design; durable design; Genai guide; quality | Update only implemented contracts, dependency edges, identity and limitations |
| D3 | README; docs index; this Plan; Plan indexes | Synchronize entrypoints and status after full verification and independent implementation review |

Dependency order: A1 -> A2 -> A3 -> A4 -> B1 -> B2 -> B3 -> C1 -> C2 -> C3
-> C4 -> D1 -> D2 -> final gates/review -> D3. Do not parallelize edits to shared
facade/outcome files. Independent review remains a separate read-only role.

### Slice checkpoints

- [x] A: public Model contracts verified, default feature cost unchanged.
- [x] B: exact HTTP/SSE behavior and controls verified locally.
- [x] C: full Agent loop, approval and durable identity verified publicly.
- [x] D: runnable offline examples, authoritative docs and final review complete.

## Acceptance criteria

- [x] Specification acceptance matrix is covered by direct public tests.
- [x] No rejected request reaches transport; no invalid live final reaches State commit.
- [x] Provisional stream data cannot masquerade as a validated terminal result.
- [x] Complete and streaming produce equivalent classifications and values.
- [x] Optional features build alone and together on supported compilers.
- [x] Same-contract fresh-process recovery succeeds; changed contract fails closed.
- [x] Old plain and approval snapshots/codecs/graph identities remain unchanged.
- [x] Graph failures retain immediate GraphRunError; output conversion/extraction
      failures have distinct typed sources and honest lifecycle documentation.
- [x] No unnecessary default-feature dependency cost or quadratic prefix parsing.
- [x] Required implementation review is accepted; no unexplained skipped gates.

## Verification

Design-only work: validate Markdown links, `git diff --check`, protected hashes,
changed-file scope, and `./scripts/verify fast`. That is not behavior validation.

Implementation uses focused commands from the spec plus:

```bash
cargo test --locked -p group-agent-model --all-features
cargo test --locked -p group-agent-genai --all-features
cargo test --locked -p group-agent-prebuilt --all-features
cargo test --locked --workspace --all-features
cargo +1.88.0 test --locked --workspace --all-features
cargo tree --locked -p group-agent-model --no-default-features
cargo tree --locked -p group-agent-model --features structured-output -e features
./scripts/verify all
git diff --check
```

Use standalone manifest consumers for feature-off checks if workspace feature
unification obscures the result. Add no-default-feature checks for Model, Genai
and Prebuilt on both stable and 1.88. The existing full gate does not run stable
all-feature tests, so the separate stable all-feature test command is required.
Benchmark compilation is required; timed performance claims require separate
measurements. Review bounded memory/CPU and compile-once behavior structurally.

## Risks and decision log

| Date | Decision | Rationale / residual risk |
| --- | --- | --- |
| 2026-09-26 | Keep API and durable identity implementation pending approval | User approved drafting a concrete design; AGENTS requires explicit public-change authority |
| 2026-09-26 | One small closed-schema profile, no remote refs | Bound validation and avoid implying universal provider compatibility |
| 2026-09-26 | Optional features and locked dependency reuse | Preserve lightweight default Model consumers; audit feature tree before enabling |
| 2026-09-26 | Add new constructor rather than grow AgentConfig | Preserve Copy and existing construction semantics |
| 2026-09-26 | Native complete path only for structured requests | Avoid upstream schema rewriting and refusal loss; adds private adapter coverage obligations |
| 2026-09-26 | Contract digest in new structured graph IDs | Prevent cross-configuration recovery without changing snapshots; cannot authenticate a malicious store |
| 2026-09-26 | Hold structured Finished until raw EOF | Invalid trailing events cannot follow a successful terminal; pending EOF remains caller-controlled |
| 2026-09-26 | Post-run typed extraction | Rust deserialization mismatch must not retry the graph or repeat tools |

## Review findings

Independent design review `review_034_design`: PASS after clarifying the 1 MiB
text bound for intermediate ToolCalls commentary on both complete and stream.
Independent Model slice review `review_034_model`: PASS WITH MINOR FIXES;
follow-up tests now cover pending EOF/drop, transport cleanup, canonical golden
identity, facade commentary parity, inclusive schema bounds and typed sources.

Independent implementation review `review_034_impl` initially requested changes:

- Major: malformed completed snapshots with no final Assistant message skipped
  output validation. A public Resume test reproduced false Completed; conversion
  now returns Incomplete. Resume/Replay/Fork/stream/sink tests verify failure,
  zero calls, and the already-created Fork branch boundary.
- Minor: native complete discarded collector error sources. It now preserves
  ModelError and StreamProtocolError; a public over-limit tool-call case proves it.
- Minor: native complete transport errors lacked provider/model context.
  Connection-failure tests now verify complete/stream parity and reqwest sources.
- Follow-up coverage: successful native complete/SSE multi-tool rounds now assert
  identities, arguments, commentary, usage, finish and single dispatch.

Correction and final implementation review: **PASS**, no remaining findings.
The orchestrator accepts this technical disposition. This is local capability
acceptance, not a live-provider or production-readiness claim.

## Completion evidence

Executed on Rust 1.98.1 and Rust 1.88.0:

- `GROUP_VERIFY_OFFLINE=1 ./scripts/verify all`: PASS. Includes strict Clippy,
  default workspace tests/doctests, benchmark compilation, all-target/all-feature
  checks, and MSRV full all-feature tests (693 passed, 0 failed/ignored at that
  checkpoint). Log: `/tmp/group-034-verify-all.log`.
- Final `cargo test --locked --offline --workspace --all-features`: PASS,
  694 tests/doctests, 0 failed/ignored. `/tmp/group-034-final-stable.log`.
- Final strict workspace all-target/all-feature Clippy: PASS.
  `/tmp/group-034-final-clippy.log`.
- Final Rust 1.88 Genai/Prebuilt structured output and process tests: PASS,
  17 tests including the added successful multi-tool case and tightened journal
  I/O assertion. `/tmp/group-034-final-msrv-focused.log`.
- Standalone Model/Genai/Prebuilt consumers, feature off and on, on stable and
  Rust 1.88: all 12 checks PASS. `/tmp/group-034-consumers.log`. Model normal/build
  trees contain no Tokio/reqwest; feature-off Model has no jsonschema/sha2/serde.
- `cargo run --locked --offline -p group-agent-prebuilt --features structured-output
  --example structured_output`: PASS for ordinary, stream and fresh-Agent approval
  Resume typed results; local Rust extraction failure causes no rerun.
- Focused public matrices cover real killed child processes with SQLite reopen:
  approve/reject, saved Tool results, completed outputs, name/schema/plain mismatch;
  persisted Tool execution journals prove no repeated execution in these cases.
- Cancellation/Node timeout after raw Finished but pending EOF drops the raw stream,
  emits no Completed and commits no checkpoint. HTTP complete/stream request drop
  closes real loopback connections.
- `git diff --check`, formatting, local document-link checks and protected artifact
  audit: PASS. AGENTS.md SHA-256 remains
  `90f6c30ce5e65baa914748e2f4cbc4d21681e6e81780d0cdb02b98e6b70affec`.
  Cargo.lock SHA-256 is
  `71df47bc1da75343624c98403d42b28ab98fe097528efc71dbba9a0591581624`;
  only two Model dependency edges changed; all package versions/sources/checksums
  match baseline.

Implementation file grouping differs slightly from initial proposals: Model
schema/control tests and Prebuilt control/process tests are separate integration
files. One runnable Prebuilt example demonstrates all three modes; Genai wire
configuration and Model contracts are documented alongside direct public tests.
No redundant provider example or live request was added.

Performance review confirms compile-once shared schema validators, bounded
buffers and no repeated prefix JSON parsing. Benchmark compilation passed;
runtime benchmarks were not measured, and no speedup is claimed.

Final scope: Model/Genai/Prebuilt implementation, manifests, focused tests and
an offline example; root lock/dependency declaration; README, architecture,
authoritative design/adapter/quality documents, Stage 22 spec and Plan indexes.
Core/Tool/SQLite production files and all snapshot/codec formats are unchanged.
At implementation acceptance, HEAD was
`1b70e2356df4e4c90c8fa7ed69552f052926af6d` and the worktree contained only
this Stage's changes. The subsequent user request authorized committing and
pushing these changes as Model, Genai, Prebuilt and documentation slices.

Skipped: hosted CI, package publication, live provider calls and runtime timing.
Offline tests establish local mapping and lifecycle behavior, not provider schema
support or factual accuracy. Completed conversion may fail after Core writes;
the digest is configuration identity, not Store authentication. Existing
at-least-once side-effect uncertainty remains. No remaining local gate is skipped.
