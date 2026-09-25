# Group Architecture

This document is the source of truth for Group's current repository
architecture. It describes present contracts, not the order in which they were
implemented. Code and executable tests take precedence if this document drifts.

## Optional two-Agent sequence

Prebuilt's `agent-sequence` feature enables experimental `AgentSequence`: two
independently configured AgentStage values, one pure synchronous handoff mapper,
and one private Core graph. Stage nodes borrow disjoint AgentState values and
return tagged updates; Runtime alone commits them. One parent thread/lineage owns
all checkpoints and controls. Stage IDs attribute events and approvals, not
independent child runs. There is no nested Agent invocation or extra scheduler.

The feature implies `structured-output` and activates existing optional sha2 in
Prebuilt. Existing single-Agent node paths, graph IDs and codec bytes remain
unchanged. Sequence snapshots/approval descriptors are new independent version-one
formats. Application revision plus ordered stage configuration/output identities
bind recovery. See [Stage 23](docs/specs/023-durable-agent-sequence.md).


## Optional structured output

Model, Genai and Prebuilt expose an opt-in `structured-output` feature. Model
owns immutable compiled contracts and response validation; it adds optional
`jsonschema` (without default features) and `sha2` dependencies, and enables
Model serde. It gains no runtime or transport dependency. Genai and Prebuilt
forward the feature to Model. Existing default features remain unchanged.

Genai maps contracts through an explicitly configured native OpenAI Chat path.
Prebuilt attaches the same contract to every model request and derives a new
graph identity from its canonical digest. Core, Tool, SQLite, snapshots and
codec identities are unchanged. See the [Stage 22 contract](docs/specs/022-structured-output.md).


## Purpose

Group is a strongly typed, asynchronous state-graph runtime for Rust agents. It
provides deterministic graph execution, durable execution ports, provider-
neutral model types, local Tool execution, an experimental prebuilt
Tool-calling Agent, and adapters for SQLite, observability, Genai, and MCP.

Group is a foundation, not a complete Agent product. Its prebuilt loop is
experimental and does not add RAG, embeddings, PDF/OCR ingestion, product
memory extraction, user interfaces, or product authorization and prompt
policy.

## Workspace dependency direction

```mermaid
flowchart TB
    App[Application]
    Prebuilt[group-agent-prebuilt]
    Core[group-agent-core]
    SQLite[group-agent-checkpoint-sqlite]
    Obs[group-agent-observability-tokio]
    Model[group-agent-model]
    Tool[group-agent-tool]
    Genai[group-agent-genai]
    MCP[group-agent-mcp]
    GenaiSDK[genai 0.6.5]
    Rmcp[rmcp 2.2.0]

    App --> Prebuilt
    Prebuilt --> Core
    Prebuilt --> Model
    Prebuilt --> Tool
    App --> Core
    App --> SQLite
    App --> Obs
    App --> Model
    App --> Tool
    App --> Genai
    App --> MCP
    SQLite --> Core
    Obs --> Core
    Tool --> Model
    Genai --> Model
    Genai --> GenaiSDK
    MCP --> Model
    MCP --> Tool
    MCP --> Rmcp
```

Normal dependencies are one-way:

- Core does not depend on Model, Tool, Provider SDKs, MCP, SQLx, or adapters.
- Model does not depend on Core, Tool, Provider SDKs, MCP, or a Tokio runtime.
- Tool depends on Model. Core is only a development integration dependency.
- Genai depends on Model and the fixed `genai` adapter dependency.
- MCP depends on Model, Tool, and the fixed `rmcp` client dependency.
- Prebuilt depends on Core, Model, and Tool, and remains provider-neutral.
- SQLite and Tokio observability are external adapters over Core ports.
- The application is the composition root.

Provider, transport, persistence, and UI concerns must not leak downward into
Core or Model public APIs.

## Core Runtime

`group-agent-core` owns graph declaration, compilation, and invocation.

- `GraphState` is `Send + Sync + 'static` and defines one typed `Update`.
- `GraphState` has no `Clone` or Serde requirement.
- A `Node` receives `&State` and `&NodeContext`, then returns an Update.
- Only Runtime calls `apply` or `apply_batch`.
- Compilation validates identifiers, transition shape, target whitelists,
  possible reachability, END reachability, and nested paths.
- `CompiledGraph` is immutable, pre-resolved, reusable, and concurrently
  shareable.
- Conditional routing is synchronous and read-only. Async work belongs in a
  Node and reaches State through an Update.
- Loops are explicit conditional transitions bounded by `max_steps`.
- Shared-state subgraphs use structured `GraphPath` and `NodePath` values.

Parallel super-steps poll Nodes concurrently over one immutable State snapshot.
Runtime waits at a barrier, restores updates to stable compiled-node order,
calls `apply_batch` once, and routes only after a successful commit. It does
not spawn one task per Node.

See [Core Runtime Design](docs/design/core-runtime.md).

## Durable Execution

Durability is opt-in and separate from `GraphState`:

- `CheckpointState` owns snapshot and restore behavior.
- `CheckpointRecord` is the storage-neutral durable domain record.
- `CheckpointCodec` converts typed snapshots and interrupt payloads to bytes.
- `CheckpointStore` persists records and lineage metadata.
- `Checkpointer` adapts the record store to typed Runtime operations.

Content idempotency and lineage compare-and-swap are distinct. An operation ID
replay is checked before expected-parent CAS. State snapshot and codec work run
outside storage locks.

Execution modes have separate contracts:

- Resume continues only the selected latest head.
- Replay reads an exact historical checkpoint and disables all writes.
- Fork is the only operation that creates a writable historical branch.
- Branches enforce thread ownership, membership, independent head CAS, and a
  complete source-to-head parent lineage.
- Interrupt checkpoints retain the interrupted Node and require an explicit,
  single-attempt typed resume value.

The SQLite adapter uses short transactions, exact UUID bytes, sortable
big-endian `u64` values, and embedded migrations.

See [Durable Execution Design](docs/design/durable-execution.md).

## Model and Provider boundary

`group-agent-model` defines provider-neutral messages, Tool data, requests,
responses, capabilities, usage, extensions, errors, and stream events.

Applications call the `ChatModel` facade. The facade validates a `ChatRequest`
and capabilities before constructing the non-bypassable
`ValidatedChatRequest` accepted by raw adapters. The stream collector validates
events atomically, preserves stable ToolCall ordering, merges partial usage,
requires a logical finish, and remains permanently failed after the first
error.

Provider-specific request mapping, response decoding, continuation metadata,
protocol trust, and provider errors remain in adapters. The current Genai
adapter is fixed to `genai` 0.6.5 and deliberately fails closed for unsupported
or untrustworthy streaming paths. Its opt-in `OpenAiChat` policy uses private
native request/SSE mapping because upstream loses multi-tool deltas. The stable
constructor shares one configured HTTP client with Genai; retries and redirects
are disabled on that opt-in client. Only this adapter depends directly on
reqwest/bytes. Core, Model, and Prebuilt remain protocol-neutral.

See [Model and Tools Design](docs/design/model-and-tools.md) and
[Genai Adapter](docs/adapters/genai.md).

## Tool Runtime

`group-agent-tool` is the single execution layer for local and remote-backed
Tools:

- immutable registration and one-time JSON Schema compilation;
- argument validation before Tool execution;
- explicit `ReadOnly`, `IdempotentWrite`, and `NonIdempotentWrite` behavior;
- caller-runtime per-call timeout and Future-drop ownership;
- bounded spawn-free batches with stable output order;
- collect-all or stop-scheduling-and-drain fail-fast;
- panic-safe, payload-free observers;
- ToolCall ID-safe ToolMessage helpers.

The Runtime does not provide automatic retry, exactly-once execution, durable
idempotency storage, rollback, or sandboxing.

See [Model and Tools Design](docs/design/model-and-tools.md).

## MCP Adapter

`group-agent-mcp` exposes remote MCP Tools through the existing Tool trait. It
is not a second Registry, timeout, batch, or side-effect runtime.

The current production transport is child-process stdio. Discovery is adapter-
owned and bounded by cursor-cycle, page, and Tool limits. A complete validated
immutable Registry snapshot is published only after all pages succeed. Remote
Tools default to conservative non-idempotent behavior unless the application
supplies an exact validated override.

Explicit Session shutdown owns one shared completion, stops new calls, joins
service close and direct-child cleanup, publishes CLOSED, and then wakes
waiters. Drop is only a best-effort direct-child fallback and does not promise
graceful protocol close, wait/reap under every OS failure, or process-tree
cleanup.

MCP HTTP, OAuth, credential storage, Resources, Prompts, Sampling, Roots,
server hosting, automatic refresh, and retry are not implemented.

See [MCP Adapter](docs/adapters/mcp.md).

## SQLite and Observability adapters

`group-agent-checkpoint-sqlite` implements the durable store without making
Core depend on SQLx. It provides file-restart recovery, transactional
idempotency and CAS, branch metadata, and defensive lineage validation.
Applications still supply the Codec.

`group-agent-observability-tokio` converts synchronous `EventSink` delivery to
a bounded Tokio broadcast channel. It is process-local and lossy. Subscriber
lag is explicit, retention is independent from sink delivery, and no
subscriber failure can fail graph execution.

## Cross-layer control and error ownership

| Layer | Cancellation and timeout ownership | Error boundary |
| --- | --- | --- |
| Core | Run and Node cancellation; run and per-node absolute deadlines; in-flight Node Future drop | Typed build, compile, Node, State, route, checkpoint, and control failures |
| Model / Genai | Caller-owned model Future or stream drop | Validation, capability, protocol, decode, transport, and provider source |
| Tool | Per-call timeout; single and batch Future drop; drain already-started calls during fail-fast | Business `ToolResult` versus typed runtime failure |
| MCP | Local call ownership; independent explicit Session shutdown | Protocol, transport, session, discovery, content, and shutdown failures |
| Prebuilt | Forwarded Core run/Node control; current Model or Tool Future ownership | `AgentError` with immediate `GraphRunError` source and optional current batch report |

No layer performs hidden retry. Dropping a Future releases local ownership but
does not roll back external side effects or prove a remote operation stopped.

Group-owned default Debug, Display, lifecycle events, and Tool observer events
exclude State, updates, prompt text, arguments, output, raw protocol bodies,
environment values, panic payloads, and source messages. Concrete sources are
retained for deliberate diagnostics; applications that traverse or log full
source chains must filter sensitive data. Production logging must also filter
upstream `genai` and `rmcp` targets.

See [Error, Cancellation, and Observability Design](docs/design/error-cancellation-observability.md).

## MSRV policy

All eight crates and the complete workspace require Rust 1.88 or newer.
Every crate inherits `rust-version` from `[workspace.package]`.

The common floor matches the already-required Genai and MCP syntax level and
removes the separate foundation compatibility policy. Rust 1.85 through 1.87
is no longer supported, including for foundation-only consumers. Runtime,
provider, and storage dependency boundaries remain unchanged.

CI runs whole-workspace checks and tests at Rust 1.88 and full quality gates
on stable. Raising the floor requires an explicit compatibility decision;
updating the development toolchain alone does not raise it. See
[ADR-012](docs/adr/012-unified-msrv.md).

## Stability boundary

The following base contracts have completed the current architecture review
and should evolve compatibly:

- Core State, Node, compiled graph, control, event, and error-source semantics;
- durable Record, Codec, Store, CAS, Resume, Replay, Fork, and branch lineage;
- Model messages, Tool data, requests, responses, validated facade, collector,
  and extensions;
- Tool trait, behavior, Registry, Runtime, report, observer, and message helpers.

`Stable` means compatibility-first additive evolution, not `never changes`.

Genai provider configuration, extension keys, stable-target policy, MCP
transport constructors, MCP discovery configuration, future HTTP/OAuth
surfaces, and the Prebuilt public API remain experimental. Prebuilt's private
State, Update, Nodes, router, topology, and `CompiledGraph` are not public
extension points or permanent compatibility promises. Upstream SDK evolution
may require adapter-level migration without changing the stable base layers.

## Experimental prebuilt Agent and application boundary

`group-agent-prebuilt` composes the existing components into this
technical loop:

```mermaid
flowchart LR
    User[User Message]
    ChatA[ChatModel]
    Call[Assistant ToolCall]
    Runtime[ToolRuntime]
    Backend[Local Tool or MCP Tool]
    Result[ToolMessage]
    ChatB[ChatModel]
    Final[Final Assistant Answer]

    User --> ChatA --> Call --> Runtime --> Backend --> Result --> ChatB --> Final
```

The experimental `ToolCallingAgent` owns a private constructor-compiled Core
graph, one canonical per-invocation transcript, maximum committed model rounds,
ToolCall dispatch through `ToolRuntime`, and ordinary `FinalAnswer` or
`MaxRounds` outcomes. It forwards Core `RunControl` and `EventConfig`, reuses
Core `EventSink`, continues after business Tool errors, stops on Tool
infrastructure errors, and exposes the complete current failing batch report
when one exists. There is no hidden retry.

Streaming execution is experimental and opt-in: `ToolCallingAgent::stream`
returns an asynchronous `AgentEventStream`, and
`ToolCallingAgent::invoke_with_stream_sink` dispatches to an `AgentEventSink`.
Streaming invocations emit `AgentStreamEvent` items (model token deltas, tool
call fragments, tool lifecycle events, approval requests, and completion
outcome). Dropping the stream or invocation drops the underlying execution
future and drops locally owned provider and Tool futures without creating
detached background tasks; remote side-effect cancellation is not guaranteed.
Provider deltas are validated before publication. Agent Tool events compose with
existing Tool observers through `with_additional_event_sink`, retaining start
rejection and secondary terminal diagnostics. `stream_with_checkpoint` and
`invoke_with_checkpoint_stream_sink` opt into checkpointed streaming;
`resume_stream` and `resume_with_stream_sink` continue from the latest head with
a fresh invocation-local sink. Core's generic `resume_with_state_initializer`
attaches transient resources after validated restore without a dependency on
Prebuilt or sinks. Snapshots and codec identities remain unchanged.

`ApprovalRequired` is provisional. Only `Interrupted`, emitted after a successful
interrupt save, confirms a resumable suspension and supplies its checkpoint
identity. A successful stream ends with exactly one `Completed` or `Interrupted`
event. Errors produce no successful terminal event. Resume streams only newly
executed work; events are not persisted or replayed. Plain `stream` remains
non-durable and fails closed when approval needs an interrupt checkpoint.

Durability is opt-in. The Agent checkpoints each committed super-step through
Core's `CheckpointConfig<AgentSnapshot>` and `Checkpointer` ports, and
`AgentSnapshotCodec` encodes the opaque public snapshot as canonical JSON
(descriptor identity `group-agent-prebuilt-agent-state` version 1). Resume is
latest-head-only, Replay is exact and read-only, and Fork is the only writable
historical branch. The private graph carries the durable `GraphVersion`
identity `group-agent-prebuilt/tool-calling-agent/1`, which must change with
any node or topology change.

Opt-in Tool approval (`AgentConfig::with_tool_approval`) registers the `tools`
node as an InterruptibleNode: a durable invocation suspends before any Tool
side effect with an `AgentApprovalRequest` payload (descriptor identity
`group-agent-prebuilt-tool-approval` version 1, sharing the snapshot codec's
`json` encoding), and Resume consumes a single-attempt
`AgentApprovalDecision`. Approval graphs carry the distinct `GraphVersion`
identity `group-agent-prebuilt/tool-calling-agent/approval/1`, and the
non-durable invoke paths of an approval-enabled Agent fail closed. Agent
`resume`, `replay`, and `fork` inherit the Agent's stored step budget when the
caller leaves the Core default unset; the Core configs expose read-only
`run_config()` accessors for that decision.

Applications create provider adapters, own MCP sessions and Tool registration,
select persistence adapters, and supply product prompts and policy. Local and
MCP-backed Tools enter the Agent through the same ToolRuntime boundary.
Provider construction, MCP lifecycle ownership, retry/fallback, Tool
rollback, exactly-once, Memory, RAG, PDF/OCR, dynamic Multi-Agent scheduling,
and middleware are not provided. Repository selection, citation rendering,
product permissions, UI, and prompt policy remain application-owned.

## Further reading

- [Documentation Index](docs/index.md)
- [Quality and Release Status](docs/quality.md)
- [Architecture Decision Records](docs/adr/README.md)
- [Development Runbook](docs/runbooks/development.md)
