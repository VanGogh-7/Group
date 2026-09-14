# 026 Review corrections

## Status

Completed on 2026-09-15. Authorized by the User after review of the proposed fixes.

## Goal and scope

Correct strict Clippy, terminal stream errors, unsupported MCP idempotency-key
requirements, and decoded checkpoint retention. Publication remains cancelled.
No commit, publication, dependency upgrade, codec, record format, database
migration, or durable lineage change is in scope.

## Baseline and references

HEAD: `7ca99206effa7101b5265ec957c0d6bf02c17631`.
Existing changes: `.harness/{CURRENT_STAGE.md,PROJECT_STATE.md,state.json}`,
`README.md`, `docs/exec-plans/active/README.md`, `docs/quality.md`,
`docs/runbooks/release.md`, and untracked `docs/release/`. Preserve these.
Protected baseline hashes are recorded in `/tmp/group-fix-baseline.json`.
Contracts: `ARCHITECTURE.md`, `docs/design/durable-execution.md`,
`docs/design/model-and-tools.md`, `docs/adapters/mcp.md`, and ADR-008.

## Design and invariants

- Remove the redundant test import without suppressing warnings.
- Validate terminal Usage before queuing stream events; errors discard pending
  events and all later polls terminate.
- Reject required-idempotency-key behavior overrides in the public MCP
  configuration builder with an additive typed configuration error. Do not
  invent a remote key protocol or change ordinary argument forwarding.
- Cache decoded checkpoints weakly, preserving identity while callers retain
  them; clean expired entries including encoded records. Store content
  idempotency and CAS remain authoritative. No full-state clone or async work
  under the cache lock.
- Preserve foundation Rust 1.85 and adapter Rust 1.88 compatibility.

## Slices and acceptance

- [x] Strict Clippy import correction verified.
- [x] Public stream regression fails before correction and passes afterward.
- [x] Public MCP configuration rejects required keys; ordinary overrides work.
- [x] SQLite-backed public checkpoint lifetime regression proves reclamation;
      live pointer identity, concurrent lookup, and content conflicts remain tested.
- [x] Expired cache record retention and cleanup complexity checked.
- [x] Full and layered MSRV gates pass.
- [x] Authoritative documentation synchronized.
- [x] Independent read-only review accepted and recorded.

## Verification

Use `CARGO_BUILD_JOBS=2 CC=/usr/bin/gcc`. Run focused red/green tests,
`./scripts/verify full`, `./scripts/verify msrv`, and `git diff --check`.
Provider tests use only local fixtures. No live quota or publication operations.

## Decision log

- 2026-09-15: User approved the four proposed fixes. Use a repository correction
  plan rather than a new product Stage. Keep the cancelled legacy release
  Harness blocked; this plan is the source of truth for the current work.

- 2026-09-15: Cache cleanup uses an insertion/replacement budget based on surviving
  handles, with a minimum of 64 operations, and shrinks buckets after expiry.
  Snapshot reclamation is immediate when
  all checkpoint/snapshot handles are dropped; encoded-record reclamation is
  amortized on subsequent insertions. Live cache entries retain full-content
  conflict checks; evicted history is verified by the authoritative Store.

## Review findings

Independent reviewer `/root/fix_review` initially identified a P2 cleanup gap:
entry-count thresholds did not reclaim expired history when only existing IDs
were reloaded. A 200-entry history regression reproduced the failure. The
insertion/replacement budget correction passed that regression. A two-thread
Codec Barrier test now forces contested cold-cache insertion through public
`get`. The reviewer re-read the final changes and returned PASS with no
remaining required corrections. The primary agent accepts this code-review
disposition. Full and layered MSRV verification subsequently passed.

## Completion evidence

- Strict workspace Clippy passes after removing the redundant MCP import and
  unnecessary borrows in the new test closure.
- Stream regression failed before correction; all 8 stream mapping tests pass.
- MCP required-key regression failed before correction.
- SQLite lifetime regression failed before correction; all 9 restart tests pass.
- All 13 resume tests pass after fixing test instrumentation to observe the
  actual decoded snapshot while retaining its checkpoint handle.
- Workspace tests (including doctests): 546 passed, 0 failed, 0 ignored.
- `CARGO_BUILD_JOBS=2 CC=/usr/bin/gcc ./scripts/verify full`: PASS on
  Rust 1.98.1, including strict Clippy, workspace tests, explicit doctests,
  benchmark compilation, and all-target/all-feature checking.
- `CARGO_BUILD_JOBS=2 CC=/usr/bin/gcc ./scripts/verify msrv`: PASS for
  Rust 1.85 foundation checks/tests and Rust 1.88 Genai/MCP checks/tests/doctests.
  The two minimal toolchains were installed locally for this verification.
- Benchmark linking emitted the host linker's non-fatal deprecated optimization
  setting warning. Actual benchmarks were not run; no timing or throughput
  improvement is claimed. Cache performance evidence is structural plus
  deterministic reclamation and concurrency tests.
- `git diff --check`: PASS. No tests were disabled or ignored.
- Logs: `/tmp/group-fix-{clippy,stream-red,stream-green,mcp-red,cache-red,
  cache-reload-red,cache-unit-green,resume-green,full,msrv}.log`.
- HEAD remains `7ca99206effa7101b5265ec957c0d6bf02c17631`; no commit or
  release operation. Cargo.lock, manifests, AGENTS.md, ARCHITECTURE.md, and
  .harness/config.toml are unchanged. Legacy Harness documentation and its
  protected hash snapshot are synchronized at closure without unblocking release.
- Final correction scope: nine source/test files across Core, SQLite, Genai,
  and MCP; MCP and Durable design docs; quality ledger; this plan and indexes;
  legacy Harness status documentation. Pre-existing release cancellation work
  remains uncommitted and preserved.
- Residual cache limitation: callers may retain arbitrarily many live handles;
  expired encoded records are cleaned on subsequent insertion/replacement
  budgets, not immediately during idle periods. No byte-size cap is promised.
