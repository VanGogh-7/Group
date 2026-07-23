# Group Repository Instructions

## Project identity

Group is a strongly typed, asynchronous state-graph runtime for Rust agents. The
name borrows a metaphor from the hierarchy of algebraic structures: Group is the
execution foundation for higher-level agent frameworks. The project does not
implement or claim to implement a mathematical group.

## Current capability

The current core supports immutable compiled state graphs with:

- asynchronous trait-based nodes;
- sequential fixed edges;
- synchronous conditional edges with declared target whitelists;
- explicit conditional loops guarded by `max_steps`;
- local success-only run reports with optional event retention;
- synchronous, thread-safe, runtime-neutral event sinks;
- per-run identifiers and typed failure lifecycle events;
- structured build, compile, node, state, route, and run errors;
- reusable and concurrently shareable compiled graphs.

## Mandatory state and execution boundaries

- `GraphState` is `Send + Sync + 'static`; it must not require `Clone`.
- A state defines one strongly typed `Update`.
- A `Node` receives only `&S` and `&NodeContext`.
- A `Node` returns `S::Update`; it never receives shared writable state.
- Only the Runtime calls `GraphState::apply`.
- State updates must be applied before successor routing.
- Routers are synchronous, read-only functions. Async work belongs in a node,
  which writes the routing decision into state through an update.
- Do not introduce `Arc<RwLock<State>>`, global run locks, or per-node
  `tokio::spawn`.

## Graph responsibilities

- `StateGraph` is the mutable declaration builder. It owns node registrations,
  fixed-edge declarations, conditional routers, and target whitelists.
- The compiler validates all identifiers, edge-shape constraints, possible
  reachability, and possible END reachability. It performs expensive resolution
  work once.
- `CompiledGraph` is immutable and reusable. It stores pre-resolved internal
  indices and must not expose `petgraph` or other internal cursor types.
- Runtime invocation owns its input state, events, visited nodes, and step count.
  Separate invocations must remain isolated, including concurrent invocations.
- Every lifecycle event carries a `RunId`. A shared sink must be able to
  distinguish concurrent invocations without receiving state or update values.

## Observability boundaries

- `EventSink` is synchronous, `Send + Sync`, and independent of Tokio.
- The Runtime delivers each event to the configured sink when the event occurs.
- Successful-report retention and sink delivery are independent. Default
  invocation behavior retains all events for compatibility.
- Sink callbacks run inline and must remain lightweight and non-blocking.
- Failed invocations return `GraphRunError`, not `RunReport`. Before returning,
  the Runtime emits all reached lifecycle events followed by `RunFailed`.
- `RunFailed` contains a typed stable failure classification and execution
  context. It does not copy state, updates, or stringify the error source chain.
- There is no built-in channel or stream adapter in this stage.

## Conditional edge semantics

- `START` has exactly one fixed successor and cannot use conditional routing.
- `END` has no outgoing edge.
- Each executable node has either one fixed successor or one conditional router,
  never both.
- A conditional router declares a non-empty, duplicate-free target whitelist.
- Every allowed target must resolve during compilation.
- Runtime calls a conditional router only after the source update is applied.
- A router result outside the whitelist is a structured run error.
- Conditional routes may revisit nodes. START and END do not count as steps.
- `max_steps = N` permits at most N real node executions.

## Performance principles

- Aggregate fixed edges, conditional routers, source counts, and successor
  presence once during compilation and reuse the result across validation and
  transition compilation. Compilation should remain approximately O(V + E).
- Use internal indices on the Runtime hot path.
- Do not clone complete state values per step.
- Do not use a global Mutex or RwLock for execution.
- Do not spawn sequential work without a concurrency reason.
- One `Arc<dyn Node<S>>` dispatch and the `async-trait` boxed future are accepted
  costs at this stage.
- Performance conclusions require repeatable benchmarks. Do not make comparative
  performance claims from architecture alone.

## Out of scope

Do not add built-in Tokio channels or streams, cancellation, timeouts, LLM
providers, tool calling, MCP, RAG, token streaming, parallel super-steps,
reducers, checkpoints, resume, interrupts, subgraphs, Tower middleware, SQLx,
Axum, distributed workers, macro DSLs, or a visual interface unless a later
stage explicitly authorizes them. Do not create placeholder crates for future
capabilities.

## Rust and code standards

- Use Rust 2024 edition and preserve the declared MSRV unless a stage explicitly
  changes it.
- Unsafe code is forbidden.
- Keep code comments and public API documentation in English.
- Use typed public errors, preserve source chains, and do not introduce
  `anyhow::Error` into framework APIs.
- Avoid lints that make ordinary development needlessly difficult.
- Keep public `NodeId` values readable and cheap to clone; never expose internal
  graph indices.

## Required validation

Run these commands after implementation changes:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo run -p group-agent-core --example linear
cargo run -p group-agent-core --example conditional
cargo bench --workspace --no-run
cargo check --workspace --all-targets --all-features
```

If the declared MSRV toolchain is already installed, also run:

```bash
cargo +1.85 check --workspace
```

Run benchmarks for measurements with:

```bash
cargo bench --workspace
```

## Codex implementation and review workflow

Development Codex must read this file, the current README, source, and tests
before changing behavior. It should preserve stage scope, implement the smallest
coherent change, add semantic tests, run all required validation, and report
public API changes and unverified checks.

Review Codex should default to read-only inspection unless explicitly asked to
fix issues. It must separate confirmed passes from unexecuted checks, verify
state ownership and event ordering, inspect error source chains and compiled hot
paths, and ground findings in current repository files rather than planned
features.

After every stage, update both `AGENTS.md` and `README.md` so repository guidance,
examples, supported features, and exclusions match the implementation.
