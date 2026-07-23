# Group

Group is a strongly typed, asynchronous state-graph runtime for Rust agents. Its
name borrows the metaphor of a hierarchy of algebraic structures: Group is
intended to be the execution foundation beneath higher-level agent frameworks.
It does not claim to implement a mathematical group.

## Current stage

Stage 3 adds a synchronous, runtime-neutral observability boundary to the
sequential fixed-edge and conditional-edge runtime:

```text
START
  |
router
  |
  +-- answer ----------------> END
  |
  +-- revise --> router ------+
```

Nodes inspect state immutably and return strongly typed updates. The Runtime
applies each update through `GraphState::apply` before it chooses the fixed or
conditional successor. Conditional routers therefore read the updated state.

The current core includes:

- asynchronous trait-based nodes;
- fixed edges and conditional target whitelists;
- explicit loops protected by a per-run `max_steps`;
- immutable, reusable, concurrently shareable compiled graphs;
- immediate lifecycle delivery through a thread-safe `EventSink`;
- independent full or disabled event retention for successful run reports;
- typed `RunFailed` events and per-invocation `RunId` values;
- ordered successful run reports and extensible lifecycle events;
- source-preserving structured errors;
- topology, edge-shape, whitelist, and reachability validation.

## Event observation

`EventSink` is a small synchronous trait that is independent of Tokio and does
not require a channel per invocation. `EventConfig` controls delivery and report
retention independently:

```rust
use std::sync::Arc;

use group_agent_core::{
    EventConfig, EventRetention, EventSink, GraphEvent, RunConfig,
};

let sink: Arc<dyn EventSink> = Arc::new(|event: &GraphEvent| {
    // Keep this callback short and non-blocking.
    eprintln!("run {}: {event:?}", event.run_id());
});

let report = compiled
    .invoke_with_events(
        initial_state,
        RunConfig::default(),
        EventConfig::new(EventRetention::None).with_sink(sink),
    )
    .await?;
assert!(report.events().is_empty());
```

The supported combinations are:

- `EventConfig::default()`: retain all events, with no sink. This preserves the
  earlier `invoke` and `invoke_with_config` behavior.
- `EventRetention::None` plus a sink: deliver events immediately without
  retaining them in a successful `RunReport`.
- `EventRetention::All` plus a sink: deliver and retain the same ordered event
  sequence.

Every event carries a lightweight `RunId`, so concurrent invocations sharing one
sink remain distinguishable. Events never contain the complete state or an
update. A sink runs inline on the execution path, must be thread-safe, and
should not perform blocking or otherwise expensive work. Group intentionally
does not provide a Tokio channel or stream adapter at this stage.

`RunReport` remains a success-only result. On a node, state-apply, router,
undeclared-target, or step-limit failure, the Runtime first delivers all events
already reached and then a final `GraphEvent::RunFailed` to the sink before
returning `GraphRunError`. The `RunFailed` payload contains a stable typed
`RunFailure` classification and execution context; the original source chain
stays on `GraphRunError` and is not stringified into the event. A failed run
does not return a partial `RunReport`.

## Conditional routing

A conditional router is synchronous, read-only, and fallible:

```rust
graph.add_conditional_edges(
    "router",
    ["answer", "revise"],
    |state: &DraftState| {
        if state.ready {
            Ok(NodeId::from("answer"))
        } else {
            Ok(NodeId::from("revise"))
        }
    },
)?;
```

Each executable node has either one fixed edge or one conditional router. A
router may only return a declared target. Async model, database, or tool work
belongs in a node; the node should write its result into state, and the router
should only inspect that updated state.

See
[`examples/conditional.rs`](crates/group-agent-core/examples/conditional.rs) for
an executable loop that revises state before routing back to the router.

## Runtime structure and performance policy

Public graph construction uses readable `NodeId` values backed by `Arc<str>`.
Compilation aggregates fixed successors, source counts, conditional routers,
and successor presence once, then reuses that data for shape validation,
outgoing-edge completeness, and transition compilation. Together with topology
construction and reachability traversal, compilation targets approximately
O(V + E). It resolves transitions and conditional target whitelists to internal
graph indices. Runtime execution uses those indices directly, so a fixed-edge
step does not allocate strings or perform a `NodeId` hash lookup. Internal
`petgraph` types remain private.

Each invocation owns its state, events, visited-node list, and step counter. The
Runtime does not clone complete states, take a global execution lock, spawn each
sequential node, create mandatory channels, or repeat full graph validation.
`GraphState` does not require `Clone`; `RunReport<S>` is cloneable only when
`S: Clone`.

Criterion benchmarks provide regression baselines only; no comparative
performance claim is made.

```bash
cargo bench --workspace
```

The baseline covers compilation of 100-node and 1,000-node fixed graphs,
execution of 10-node and 100-node fixed graphs, a 1,000-step conditional loop,
and repeated invocation of one compiled graph.

## Workspace

```text
.
├── AGENTS.md
├── Cargo.toml
├── README.md
├── rust-toolchain.toml
├── rustfmt.toml
└── crates
    └── group-agent-core
        ├── Cargo.toml
        ├── benches
        │   └── runtime.rs
        ├── examples
        │   ├── conditional.rs
        │   └── linear.rs
        ├── src
        │   ├── context.rs
        │   ├── edge.rs
        │   ├── error.rs
        │   ├── event.rs
        │   ├── graph.rs
        │   ├── lib.rs
        │   ├── node.rs
        │   ├── runtime.rs
        │   └── state.rs
        └── tests
            ├── compile_validation.rs
            ├── conditional_routing.rs
            ├── linear_execution.rs
            ├── observability.rs
            └── review_regressions.rs
```

## Run

```bash
cargo test --workspace
cargo run -p group-agent-core --example linear
cargo run -p group-agent-core --example conditional
cargo bench --workspace --no-run
```

## Current exclusions

This stage does not support built-in Tokio channels or streams, cancellation,
timeouts, parallel execution or super-steps, reducers, checkpoints, resume,
interrupts, LLM or tool APIs, MCP, RAG, token streaming, subgraphs, Tower
middleware, SQLx, Axum, HTTP services, distributed workers, macro DSLs, or
visualization. The event sink is only an adapter boundary for possible later
integration; those excluded capabilities are not designed or implemented here.
