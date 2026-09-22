# 032 Reliable provider tool-call streaming

## Status

Completed on 2026-09-23. Independent read-only review returned PASS and the
implementer accepted it. Product acceptance remains with the User.

- [x] Baseline recorded.
- [x] Design reviewed and additive API authorized.
- [x] Slices implemented.
- [x] Verification passed.
- [x] Independent review completed.
- [x] Review accepted and written back by a write-authorized role.
- [x] Completion evidence recorded.

## Goal

Make one explicitly supported OpenAI Chat Completions protocol path work through
streamed text and tool arguments, persisted approval interruption, SQLite reopen
with a new Agent, approval or rejection, and a final streamed model response.
Prove the path with local HTTP/SSE fixtures; live provider access is a separate
opt-in validation, not part of the offline gate.

## Non-goals

Responses streaming, general OpenAI-compatible endpoint certification, other
providers, PostgreSQL, dynamic fan-out, partial super-step recovery, product
authorization, RAG, UI, server deployment, release, commit, or push.
No Core API, checkpoint format, codec identity, lineage, or MSRV changes.

## Context and baseline

- Starting HEAD: `1724ebcba0370da8899583453d6d874a260e1f1e`.
- Starting worktree: clean (`git status --short` produced no entries).
- `AGENTS.md` SHA-256:
  `90f6c30ce5e65baa914748e2f4cbc4d21681e6e81780d0cdb02b98e6b70affec`.
- Starting `Cargo.lock` SHA-256:
  `1cf17e56d6f0860e7a4b5e4d487d0ff1381f8b3dc8ed7709b4bbb35901d369f9`.
- [Architecture](../../../ARCHITECTURE.md),
  [Genai adapter](../../adapters/genai.md),
  [Model and tools](../../design/model-and-tools.md),
  [error and control design](../../design/error-cancellation-observability.md),
  [ADR-005](../../adr/005-validated-model-facade.md),
  [ADR-012](../../adr/012-unified-msrv.md), and
  [quality ledger](../../quality.md) remain authoritative.
- [Plan 031](../completed/031-durable-streaming-approval.md) supplies durable
  streaming and approval. Its provider-neutral tests do not prove the actual
  provider protocol path.
- Existing `tests/stream_compatibility.rs` reproduces genai 0.6.5 dropping the
  second tool delta in one SSE frame. The current upstream main OpenAI streamer
  still uses first-event selection; no upgrade has been shown to fix the gap.
- Upstream `Client::config`, `Client::web_client`, request conversion, and SSE
  transport types are crate-private. An injected opaque Client cannot expose
  its original transport for replacement parsing.

## Roles and permissions

The User authorized this stage's implementation. The implementer owns this
Plan, code, tests, verification, and review corrections. Independent reviewers
are strictly read-only. The User retains product acceptance.

The User explicitly authorized the additive adapter API below on 2026-09-23.
Previous commit/push authority
applied to Plan 031, not this new change.

## Invariants

- Core and Prebuilt remain provider-neutral. ToolRuntime owns tool validation,
  execution policy, side effects, and ToolMessage identity.
- Keep genai pinned to 0.6.5 and Rust 1.88. Direct reqwest/bytes dependencies use
  already locked versions/features; no beta upgrade or vendored provider SDK.
- Existing disabled and text-only policies retain their behavior.
- No hidden retry, detached stream task, automatic credential discovery, raw
  payload logging, or exactly-once claim.
- A malformed or incomplete stream cannot produce a successful terminal event,
  approval checkpoint containing partial arguments, or tool execution.
- Preserve concrete error sources behind payload-safe default formatting.
- Resume after a saved tool result does not repeat that completed tool; crash
  windows around external effects retain the documented at-least-once risk.

## Accepted design

### Authorized additive public API

- `GenaiStreamingPolicy::OpenAiChat`: explicitly enables Group's strict native
  OpenAI Chat streaming implementation, including tool calls. Requires existing
  `new_with_stable_target` with matching `AdapterKind::OpenAI` bindings.
- Add typed configuration errors for a missing stable transport binding,
  unsupported client options on this restricted path, endpoint resolution, and
  HTTP client build failure. Add payload-safe protocol/limit errors as needed within the existing
  non-exhaustive error enums.
- Add `GenaiStreamingLimits::with_max_sse_event_bytes(usize)` and
  `with_max_tool_argument_bytes(usize)`. Bound an individual SSE event and the
  aggregate retained tool argument bytes, in addition to existing tool count
  and reasoning limits. Defaults and exact accounting are documented and tested.

### Transport and mapping

Only the new policy constructs a shared reqwest client from the supplied
`ClientConfig` WebConfig. Clone that client into genai for ordinary completions
and retain it for strict streaming. Preserve the exact stable target, explicit
authentication and configured headers. Reject unsupported configuration before
dispatch rather than silently losing settings. Opaque injected Clients cannot
enable this mode.

Implement private request serialization and an incremental byte-oriented SSE
decoder in the Genai adapter. Reuse current Group request validation and common
response/usage mapping where compatible. Preserve every tool delta by provider
index, including multiple calls in one event and interleaved fragments. Validate
identifiers, immutable tool identity, complete JSON, finish reason, and `[DONE]`.
Preserve text and tool content sharing an event. Decode UTF-8 after complete SSE
framing, handle CR/LF framing, and fail permanently on transport/protocol errors.
Do not emulate parallel request controls unsupported by existing configuration.

## Implementation slices

### Slice 1: Protocol audit and regression probes

- [x] Run existing upstream-loss and fail-closed public-boundary tests.
- [x] Add a failing public multi-call/interleaved-fragment acceptance test.
- [x] Review upstream alternatives, transport ownership, configuration, and API.
- [x] Record the selected design and explicit additive API authorization.

### Slice 2: Strict provider stream

- [x] Add authorized opt-in policy, shared transport, request mapping, and decoder.
- [x] Test multi-call frames, interleaved fragments, UTF-8/framing splits, request
  semantics, headers, usage tail, terminal ordering, malformed events, early EOF,
  limits, errors/source reachability, and payload-safe formatting.
- [x] Test stream drop, cancellation, and timeout through public boundaries.
- [x] Verify existing policy rejection, non-streaming behavior, and Rust 1.88.
- [x] Self-review resource bounds, copies, polling, and failure lifecycle.

### Slice 3: Durable approval integration and closure

- [x] Exercise local HTTP provider -> stream -> approval -> SQLite reopen -> new
  Agent -> approve/reject -> final stream through public APIs.
- [x] Verify invalid/unfinished provider output cannot dispatch tools, and saved
  tool results survive later model failure without repeating tool execution.
- [x] Document the opt-in configuration, supported matrix, example, and limits.
- [x] Run the unified full stable/MSRV gate and an independent read-only review.
- [x] Apply required corrections, update the quality ledger, and archive the Plan.

## Acceptance criteria

- [x] One declared provider protocol supports the complete durable streaming
  approval flow using the actual HTTP adapter, without live credentials.
- [x] Every tool delta is retained; partial/invalid output never runs a tool.
- [x] Error, cancellation, timeout, drop, and restart behavior are directly tested.
- [x] Existing public behavior outside the opt-in path remains compatible.
- [x] Required gates pass and documentation accurately separates local protocol
  evidence from unexecuted live-provider, hosted-CI, and performance checks.

## Verification

```text
cargo test -p group-agent-genai --test stream_compatibility
cargo test -p group-agent-genai
./scripts/verify all
git diff --check
```

Add focused regression and integration commands as tests are introduced. Loopback
fixtures may require sandbox escalation. No provider credentials or quota are
needed. Benchmark compilation is a compatibility gate, not timing evidence.

## Decision log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-09-23 | Start with explicit OpenAI Chat protocol support | Existing text-stream and stable-target foundations minimize scope. |
| 2026-09-23 | Keep genai 0.6.5; implement private strict streaming | Main still has event loss; internal request/transport APIs cannot be reused publicly. |
| 2026-09-23 | Reuse the stable-target constructor and share one HTTP client | Preserve application transport configuration without an opaque-client fallback or another constructor. |
| 2026-09-23 | Keep new mode opt-in and require additive API authorization | Existing TextOnly policies must remain text-only; AGENTS.md requires explicit public API authorization. |
| 2026-09-23 | User authorized the proposed policy, limits, and typed errors | Explicit response to the concrete API question; no further API escalation is needed within that contract. |
| 2026-09-23 | Disable HTTP retries and redirects on the shared strict-mode client | Prevent protocol-NACK replay and preserve the exact destination. Other modes retain their existing clients. |
| 2026-09-23 | Reject reasoning/signature continuation on the strict path | This slice certifies native Chat text/function calls, not other protocols or reasoning extensions. |
| 2026-09-23 | Preserve pinned request precedence and namespaced model mapping | Preliminary review found stop fallback and literal namespace differences; paired public HTTP tests now cover both modes. |
| 2026-09-23 | Promote locked bytes 1.12.1 to a direct dependency | Retain each reqwest body chunk without a full buffer copy; no version or feature upgrade. |
| 2026-09-23 | Preserve URL join/query rules and document unauthenticated mode differences | Paired HTTP tests verify endpoint parity; native streams allow None while unchanged Genai completions reject it before dispatch. |
| 2026-09-23 | Add Prebuilt, Tool, SQLite, and SQLx development dependencies only | Exercise actual public composition and explicitly close/reopen SQLite without changing production dependency direction. |

## Review findings

Independent read-only design review (`plan032_design_review`) confirmed the
private strict implementation is feasible. Required design boundaries are
shared WebConfig transport, explicit option/auth admission, no retries/redirects,
bounded SSE/tool accumulation, and direct argument fragments. Preliminary implementation review
requested fixes for stop precedence and namespaced model serialization, and
flagged token-limit/header consistency. These were corrected: stop/name/token
mapping follows pinned Genai, reasoning suffixes and ChatOptions extra headers
are explicitly rejected, and paired request fixtures pass. Final review also
checked the endpoint-resolution correction and authentication distinction. A
paired fixture initially used an SSE content type for its JSON response; it was
corrected before final validation.

Final independent read-only review returned **PASS**, with no remaining required
findings. The reviewer independently passed all 19 final protocol/integration
tests, 14 earlier compatibility/non-stream tests, and `git diff --check`, and
confirmed protected instructions and locked package versions were unchanged.
The implementer accepted and recorded the review; product acceptance remains
with the User.

## Completion evidence

Final commands and evidence:

- `cargo test -p group-agent-genai --test stream_compatibility`: sandbox denied
  loopback binding; approved unsandboxed rerun passed all 11 tests.
- `cargo test -p group-agent-genai --test openai_chat`: first corrected test
  compiled and failed with `UnsupportedCapability(Streaming)` before transport
  implementation; the same multi-call/interleaved-fragment test now passes.
- `cargo test --locked -p group-agent-genai`: all crate tests and its doctest
  passed after implementation (15 new tests at that point).
- `cargo test --locked -p group-agent-genai --test openai_chat`: all 19 final
  tests passed, including protocol case matrices, SQLite close/reopen, and controls.
- `cargo clippy -p group-agent-genai --all-targets --all-features -- -D warnings`:
  passed after collapsing conditionals and eliminating the HTTP chunk copy.
- `cargo run --locked --offline -p group-agent-genai --example openai_chat_model`:
  passed; creates and drops an unpolled stream without HTTP dispatch.
- `./scripts/verify all`: passed after the final endpoint/auth corrections;
  log `/tmp/group-032-verify-all.log`. Stable Rust 1.98.1 and MSRV Rust 1.88.0
  each passed 660 workspace tests including doctests, with zero failures or
  ignored tests. Formatting, strict workspace Clippy, all-target/all-feature
  checks, and benchmark compilation passed.
- `git diff --check`: passed after documentation synchronization.

Validation boundaries: no live provider calls, credential use, hosted CI,
process-kill recovery, package publication, or measured performance run.
SQLite recovery evidence is explicit pool close/reopen with a new Agent in the
same test process. Native OpenAI Chat remains opt-in and experimental; it does
not certify every compatible endpoint. Saved tool results prevent replay of
that saved work, but external-effect crash windows are not exactly-once.

Final changed-file scope: root dependency manifests and entrypoint/architecture
notes; Genai adapter/configuration/errors/native streaming implementation;
Genai offline example, public HTTP/durable tests, and local HTTP support;
adapter guide, documentation index, quality ledger, and Plan indexes/archive.
The worktree retains these uncommitted changes. HEAD remains the recorded
baseline; no commit or push was performed.

Self-review performance evidence: SSE scans each byte once; tool arguments append
to one buffer and parse once at completion. Cardinality and normalized frame
limits bound a per-frame pending queue. No detached task, accumulated text,
index-sized vector, or repeated cumulative argument copy is introduced. Network
chunks are retained with `Bytes` ownership. This is structural evidence, not a
timed throughput or allocation measurement.

Current `Cargo.lock` SHA-256:
`1b1cf4b431f50bf933585ac3d598a74c750852a8b8c420de61a42057a15321c1`.
Only this crate's dependency edges changed; package versions/checksums did not.
`AGENTS.md` remains byte-identical to baseline. No Core/Prebuilt/Tool production
source, checkpoint format, codec, or lineage changed.

Protocol sources checked on 2026-09-23:

- https://developers.openai.com/api/reference/resources/chat/subresources/completions/streaming-events
- https://developers.openai.com/api/docs/guides/function-calling
- https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream
- https://github.com/jeremychone/rust-genai/blob/main/src/adapter/adapters/openai/streamer.rs
- Local locked genai 0.6.5 and reqwest 0.13.4 source for API visibility,
  WebConfig application, request serialization, and retry defaults.
