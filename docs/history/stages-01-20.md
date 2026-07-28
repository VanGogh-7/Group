# Group Stages 01-20

This document preserves the major capability and correction sequence through
the Stage 20 architecture review. It is historical context, not the source of
truth for current behavior. Use [ARCHITECTURE.md](../../ARCHITECTURE.md) and
the design documents for current contracts.

## Stages 01-05: deterministic graph execution

### Stage 01: typed linear graph

The first runtime established the central ownership rule: an invocation owns
State, a Node reads `&State` and returns a typed Update, and Runtime alone
applies it. Compilation separated graph declaration errors from execution
errors.

Early review rejected a Clone requirement on State and reinforced isolated
concurrent invocations.

### Stage 02: conditional routing

Synchronous read-only routers added state-dependent transitions after Update
commit. Build, compile, Node, State, route, and run failures became structured
errors.

### Stage 03: run identity and lifecycle events

Every event gained a RunId so shared sinks could distinguish interleaved runs.
Event delivery remained synchronous and payload-free.

### Stage 04: cancellation and timeout

Tokio-native cancellation, run timeout, and per-Node timeout established
absolute deadlines, deterministic tie ordering, and Future-drop ownership.
Cancellation was kept out of State.

### Stage 05: parallel super-steps

Frontiers introduced concurrent Node polling over one immutable State
snapshot. A barrier, stable source ordering, and atomic `apply_batch`
separated completion order from merge order.

Review rejected per-Node spawn and partial frontier execution. The correction
made the default reducer reject multiple Updates before mutation and required
custom reducers to validate a complete batch.

## Stages 06-10: durable and nested execution

### Stage 06: checkpoint foundation

Checkpointing became opt-in through a separate `CheckpointState` capability.
Snapshots were saved only at successful execution boundaries.

### Stage 07: latest-only Resume

Resume restored State, frontier, and counters from a compatible versioned
checkpoint. The additional step budget remained per invocation while emitted
positions stayed cumulative.

### Stage 08: interrupt and typed resume values

Singleton Nodes could suspend without applying an Update. Interrupted
checkpoints retained the same Node frontier, and resume values remained typed,
process-local, and scoped to one re-execution attempt.

### Stage 09: shared-state subgraphs

Owned compiled children added nested execution without changing State type.
Structured GraphPath and NodePath values replaced string parsing. Child Nodes
shared the parent RunId, control, events, checkpoint lineage, and counters.

### Stage 10: architecture review

The first ten-stage review found that the initial checkpoint port did not
fully define a durable storage-neutral contract.

### Stage 10.1: durable checkpoint contract

The correction separated:

- `CheckpointRecord` from typed State;
- Codec from Store;
- content idempotency from expected-parent CAS;
- format version from graph version;
- snapshot work from store locks.

UUID identity, fixed-width counters, typed record validation, interrupted
payload encoding, and shared Arc query values became durable boundaries.

## Stages 11-15: transitions, adapters, Replay, and branches

### Stage 11: unified transition kernel

Conditional fan-out joined fixed, conditional, static fan-out, and structural
subgraph transitions behind one post-commit kernel. Runtime enforced nonempty,
duplicate-free, whitelisted results and stable successor order.

### Stage 12: SQLite durable adapter

An independent SQLx SQLite Store demonstrated file-restart recovery, embedded
migrations, exact UUID and `u64` storage, short write transactions,
idempotency, and lineage CAS without adding SQLx to Core.

### Stage 13: Tokio observability adapter

A bounded Tokio broadcast adapter converted synchronous EventSink delivery
into a process-local stream. Subscriber lag became explicit; retention and
delivery remained independent.

### Stage 14: read-only Replay

Replay loaded an exact historical checkpoint and reused execution with writes
disabled. It neither selected latest nor advanced lineage. A new interrupt
during Replay became an explicit unsupported failure.

### Stage 15: explicit Fork

Fork became the only operation that creates a writable historical branch.
Branch metadata remained an additive Store capability rather than changing
`CheckpointRecord`.

### Stage 15.1: branch Resume and ownership

Branch heads gained explicit Thread ownership, independent CAS, durable
restart, and branch-aware Resume events.

### Stage 15.2: membership and read consistency

The correction prevented same-thread forged heads and discontinuous lineage.
SQLite reads validated source, ownership, membership, head, ordering, and the
complete parent chain in one read transaction.

## Stages 16-17: provider-neutral Model and Genai

### Stage 16: provider-neutral Model

The Model crate introduced strongly typed messages, Tool data, requests,
responses, capabilities, errors, Extensions, usage, and streaming without
depending on Core or a provider SDK.

### Stage 16.1: validated facade and redaction

`ChatModel` centralized request and capability validation. Partial usage,
continuation Extensions, provider sources, and payload-safe default formatting
were strengthened.

### Stage 16.2: non-bypassable validation and atomic collector

Review showed that facade convention alone could be bypassed if raw adapters
accepted unchecked requests. Raw ports changed to the privately constructed
`ValidatedChatRequest`. Stream, usage, and extension updates validate before
commit, and the collector remains permanently failed after its first error.

### Stage 17: Genai adapter

The first provider adapter mapped Model data to exactly `genai` 0.6.5 with an
injected Client, explicit continuation metadata, typed errors, offline local
HTTP coverage, and online stream normalization.

The initial idea that a caller-declared protocol profile could establish trust
was rejected. Compatibility became tied to actual adapter identity, returned
stream identity, or an exact stable ServiceTarget.

### Stage 17.1: streaming correctness

Tool argument deltas became append-only and terminal JSON values were checked
against the accumulated value. Known lossy Tool and Responses streaming paths
failed closed. Response IDs became redacted by default.

### Stage 17.2: trusted protocol binding

Dynamic ordinary text remained usable, while ToolCall generation and
Responses signature recovery required an immutable target. A real two-turn
loopback fixture replaced manual signature injection as continuation evidence.

### Stage 17.3: parser admission and deterministic signatures

The configured limit was accurately defined as a post-capture parser admission
limit, not a network or peak-memory limit. Checked accounting and stable
per-call signature ordering replaced ambiguous behavior.

The final correction made unexpected thought-signature chunks on audited text
streams terminal Protocol errors and documented the boundary between
Group-owned redaction and explicit upstream source traversal.

## Stages 18-19: Tool Runtime and MCP backend

### Stage 18: Tool Runtime

An independent Rust 1.85 crate added object-safe Tools, immutable
registration, precompiled JSON Schema validation, explicit side-effect
behavior, caller-runtime timeout, bounded spawn-free batches, and observers.

Review found three important defects in the initial semantics:

- concrete schema sources were unavailable;
- fail-fast could call unobserved started work `Cancelled`;
- terminal observer failure could replace the true Tool outcome.

### Stage 18.1: execution-fact corrections

Schema errors retained concrete sources with safe default formatting.
Fail-fast changed to stop scheduling and drain started Futures. Terminal
observer failure became a secondary diagnostic. Message helpers preserved the
original ToolCallId.

### Stage 19: MCP Tool backend

A client-only MCP adapter exposed remote Tools through the existing Tool
Runtime. It added reusable stdio sessions, discovery, naming, schema mapping,
result mapping, conservative behavior, and fail-closed content.

Review found unbounded cursor traversal and possible direct-child survival
during runtime teardown.

### Stage 19.1: bounded discovery and direct-child lifecycle

Discovery moved into the adapter with cycle detection, page and Tool limits,
checked accounting, and all-or-nothing immutable snapshot publication. Stdio
gained explicit close/kill/wait plus a runtime-independent Drop fallback.

Review then found incomplete rmcp JoinError mapping and cleanup ownership tied
too closely to the first shutdown waiter.

### Stage 19.2: Session-owned completion

The Session retained one cleanup supervisor and shared result. Cancelling a
waiter no longer cancelled cleanup. Outer and rmcp QuitReason JoinErrors were
preserved, and zero grace kept one exit check before immediate kill/wait.

Review then found that worker panic could publish an error before the
independent child path completed and that Drop documentation overstated reap
guarantees.

### Stage 19.3: shutdown path convergence

Service close and child cleanup became independent tasks that always converge.
Worker panic no longer skipped direct-child cleanup. Result storage and CLOSED
publication preceded waiter notification. Drop was documented as best-effort,
including standard-thread creation failure and no process-tree guarantee.

## Stage 20: full repository architecture review

The review inspected dependencies, public APIs, execution and durable
correctness, provider and Tool boundaries, MCP lifecycle, performance,
security, tests, MSRV, documentation, and release readiness.

Result: **PASS WITH MINOR FIXES**.

The reviewed base contracts were suitable for compatibility-first evolution:
Core, Durable, Model, and Tool. Genai and MCP adapter configuration surfaces
remained experimental.

The Minor findings were release and documentation engineering debt, upstream
dependency logging guidance, and SQLite restart-benchmark teardown noise.
They were not Runtime architecture defects and did not require a blocking
Stage 20.1 before planning Stage 21. They did block claiming that public
v0.1.0 was ready.

## After Stage 20

H-001 introduces a repository Harness so future complex work is driven by
tracked Execution Plans, stable documentation, unified verification, and
independent review. H-001 is repository engineering, not a product Stage.

No Stage 21 implementation is described here.
