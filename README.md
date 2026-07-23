# Group

Group is a strongly typed, asynchronous state-graph runtime for Rust agents. Its
name borrows the metaphor of a hierarchy of algebraic structures: Group is
intended to be the execution foundation beneath higher-level agent frameworks.
It does not claim to implement a mathematical group.

## Current stage

The first-stage runtime supports a deliberately small vertical slice:

```text
START -> asynchronous node -> END
```

It includes:

- strongly typed state and state updates;
- asynchronous trait-based nodes;
- fixed directed edges;
- build-time topology validation;
- immutable, reusable compiled graphs;
- sequential execution with a step limit;
- ordered run reports and basic lifecycle events;
- structured build, compile, node, state, and run errors.

Nodes only receive an immutable state reference. A node returns an update, and
the runtime applies that update through `GraphState::apply`.

This stage intentionally does not include conditional edges, parallel nodes,
reducers, LLM or tool APIs, MCP, checkpoints, interrupts, subgraphs, middleware,
HTTP clients, databases, macro DSLs, or visualization.

## Workspace

```text
.
├── Cargo.toml
├── rust-toolchain.toml
├── rustfmt.toml
└── crates
    └── group-agent-core
        ├── Cargo.toml
        ├── examples
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
            └── linear_execution.rs
```

## Run

```bash
cargo test --workspace
cargo run -p group-agent-core --example linear
```

## Planned extensions

Likely next steps are conditional edges, explicit loop routing, a streaming event
API, parallel super-step execution, and checkpoint support.

