# Current Stage

- Stage: T1
- Title: Prebuilt public-boundary offline smoke coverage
- Current Slice: T1.2
- Approved: 2026-08-03T10:56:41+00:00

## Plan and acceptance criteria

# Stage T1 Task Brief: Prebuilt public-boundary offline smoke coverage

Status: Approved by the User on 2026-08-03.

## Goal

Turn the documented offline `group-agent-prebuilt` paths into executable external integration tests that exercise only the crate's public API. This is a test-only Harness trial and must not change Group production behavior.

## Slice T1.1: model-only public-boundary smoke test

Allowed scope:

- Add an integration test under `crates/group-agent-prebuilt/tests/`.
- Reuse or minimally organize `crates/group-agent-prebuilt/test_support/offline_agent.rs` only if needed.
- Do not modify any `src/` production file.

Acceptance:

1. The integration test constructs `ToolCallingAgent` from an external crate boundary.
2. It uses the offline scripted model and an empty `ToolRuntime`; no network or provider access.
3. It verifies `AgentStopReason::FinalAnswer`, exactly one committed model round, an existing final message, and the exact deterministic final text.
4. The focused integration test passes.
5. Harness fast and slice gates pass.
6. The independent read-only Reviewer passes with no unresolved finding.

## Slice T1.2: tool-round public-boundary smoke test

Allowed scope:

- Add a second integration test under `crates/group-agent-prebuilt/tests/`.
- Minimally strengthen `crates/group-agent-prebuilt/test_support/offline_agent.rs` offline validation.
- Adjust the existing offline example only if strictly necessary to avoid duplicated or drifting test logic.
- Do not modify any `src/` production file.

Acceptance:

1. The external integration test runs `Model -> ToolCall -> ToolRuntime -> ToolMessage -> Model -> FinalAnswer` offline.
2. The scripted model validates the original ToolCall identity, expected successful ToolMessage, and deterministic `offline-label` Tool output.
3. It verifies `FinalAnswer`, exactly two committed model rounds, and the exact deterministic final text.
4. The focused integration test and the documented offline example pass.
5. Harness fast and slice gates pass.
6. Harness stage gates pass on the final worktree.
7. A final independent read-only Reviewer runs after stage gates, and its evidence is valid for the final worktree.

## Hard exclusions

- No Group business production-code changes, including `crates/group-agent-prebuilt/src/**`.
- No public API changes.
- No changes to other crates.
- No dependency additions or upgrades.
- No persistence, migration, Codec, CAS, lineage, concurrency, cancellation, timeout, or scheduling changes.
- No changes to `AGENTS.md`, `ARCHITECTURE.md`, or `.harness/config.toml`.
- No live provider, remote MCP, or external test-service access.
- No unrelated refactors or formatting.
- No commit, push, tag, publish, or release.

## Required Harness order

- T1.1: Implementer -> fast gates -> slice gates -> read-only Reviewer -> approve next Slice.
- T1.2: Implementer -> fast gates -> slice gates -> stage gates -> final read-only Reviewer -> `approve-slice --complete-stage`.

The workflow proceeds automatically unless it reaches `BLOCKED`, `HUMAN_CHECKPOINT`, exhausts the three-attempt cap, or requires expanding this approved scope.
