# Agent Workflow Roadmap

Status: Proposed direction recorded on 2026-09-26 at the user's request.
Stage 24 was authorized, implemented and locally verified on 2026-09-28, with
independent review PASS. Plan 036 is completed following user acceptance. Stages 25 and 26 remain proposed future work.

## Starting point

[Stage 23](specs/023-durable-agent-sequence.md) provides experimental fixed
A-to-B composition with typed handoff, scoped streaming and durable approval.
Core already supports conditional routing, bounded graph execution and
concurrent super-steps. Future composition should reuse those mechanisms.
See [Architecture](../ARCHITECTURE.md) and [Quality](quality.md) for current
contracts and verification limits.

## Ordered capability candidates

| Order | Candidate | User outcome | Decisive acceptance |
| --- | --- | --- | --- |
| 1 | Stage 24: Durable conditional branching | A validated result selects B, C or completion from a fixed target set | After the selection is saved, process recovery preserves it; unselected Agents make zero model/Tool calls |
| 2 | Stage 25: Bounded feedback loop | Writer and Reviewer exchange typed results until acceptance or an explicit limit | Recovery retains iteration and feedback; exhaustion is distinguishable from success |
| 3 | Stage 26: Fixed parallel execution and join | Independent Agents work concurrently and produce a deterministic merged result | Failure, cancellation, approval, checkpoint barriers and recovery have explicit tested semantics |

Only Stage 24 has a [completed execution plan](exec-plans/completed/036-durable-agent-branching.md).
Stages 25 and 26 need their own specifications and plans after evaluating the
preceding stage. Their numbers and exact scope remain provisional.

## Preparation and continuous engineering

Before choosing Stage 24's API, inspect an independent consumer of the existing
Prebuilt API: construction, typed results, approval Resume and streaming. Record
repeated configuration, error attribution and duplicated public surfaces. Use
that evidence to justify reuse; do not start by designing an arbitrary-topology
workflow API. This preparation is repository engineering, not a product Stage.

Track checkpoint size and save/restore latency with reproducible fixtures and
recorded environments as composition grows. Compile-only benchmarks do not
establish runtime performance. Schedule actual measurement explicitly; do not
block the initial design on an unbounded benchmark project.

Before multi-process deployment, define application serialization or external
coordination for competing recovery attempts. Lineage CAS protects checkpoint
commits, not external side effects. This roadmap does not promise exactly-once
execution or introduce a lease protocol.

## Deferred scope

Dynamic supervisors, arbitrary Agent counts/topologies, shared memory and hidden
retry are outside these initial slices. RAG, UI, product authorization and prompt
policy remain application-owned. Provider expansion and release preparation are
separate decisions, not implied by this roadmap.

## Next-session entrypoint

[Plan 036](exec-plans/completed/036-durable-agent-branching.md) records the accepted
Stage 24 implementation and verification. Refresh the worktree baseline before
new work. The next capability candidate is Stage 25 bounded feedback; it still
requires its own concrete specification, independent design review and material
change authorization. The existing Sequence codec diagnostic debt is recorded
in [Quality](quality.md) for a separately scoped correction.
