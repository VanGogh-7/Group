# Error, Cancellation, and Observability Design

Group uses ownership and typed errors instead of hidden workers, retries, or
string-only diagnostics. Each layer controls only its own in-flight work.

## Control ownership

| Layer | Owns | Does not own |
| --- | --- | --- |
| Core | external run cancellation; run and Node deadlines; Node Future polling and drop | Provider retry; Tool retry; external rollback |
| Model / Genai | returned completion Future or stream ownership | Graph deadlines; hidden runtime; automatic fallback |
| Tool | per-call timeout; single and batch Future drop; fail-fast drain | Graph run timeout; durable deduplication; exactly-once |
| MCP | local request ownership; explicit Session shutdown; direct stdio child lifecycle | remote rollback; process-tree cleanup; automatic retry |

No layer automatically retries. Cancellation and timeout release local
ownership but cannot preempt synchronous callbacks or reverse external effects.

## Core deadline ordering

Runtime uses absolute deadlines and a single selector for synchronous checks
and asynchronous waiting. When conditions coincide:

1. external cancellation wins;
2. run timeout wins;
3. Node timeout wins;
4. the Node result is observed last.

Cancellation and run timeout also precede `max_steps` at Node boundaries.
Parallel failure drops siblings and discards the uncommitted batch.

## Business outcomes and runtime failures

Do not collapse these categories:

- a model protocol failure is not a validation failure;
- `ToolResult { is_error: true }` is a Tool business result;
- `ToolRuntimeError` is an execution infrastructure failure;
- MCP `isError = true` maps to a business Tool result;
- MCP protocol, transport, discovery, content, and session errors remain typed
  runtime failures;
- SQLite busy is a storage failure, not a lineage CAS conflict;
- Replay side effects are user-code effects, not durable writes.

Typed classifications must remain stable enough for applications to make
policy decisions without parsing Display strings.

## Source chains

Framework errors retain concrete lower-level sources where useful:

- Node and State errors;
- codec and Store errors;
- `jsonschema` compilation and validation errors;
- provider and transport errors;
- rmcp and process errors;
- observer errors.

Sources are not prematurely stringified. Application code may use
`Error::source()` and downcasting for diagnostics.

## Default redaction

Group-owned default Debug, Display, events, and observer records do not include:

- State or Update values;
- message, prompt, argument, output, or schema bodies;
- raw HTTP or protocol payloads;
- executable arguments or environment values;
- credentials or secret prefixes;
- panic payloads;
- concrete source messages.

Explicit source-chain traversal can expose upstream details. Applications must
filter before logging a complete chain. The same rule applies to upstream
dependency targets: production configurations should not enable unfiltered
`genai=trace` or `rmcp=debug`.

## Core EventSink

`EventSink` is synchronous, infallible, `Send + Sync`, and called inline when
an event occurs. It receives lifecycle metadata, not State or Update values.
Callbacks must be lightweight and non-blocking.

Event retention in a successful report is independent from sink delivery.
Failed invocations return `GraphRunError` after emitting the reached lifecycle
events and exactly one `RunFailed`.

A sink panic propagates directly; Runtime does not disguise it as a graph
error.

## Tokio broadcast adapter

`group-agent-observability-tokio` adapts EventSink to a bounded Tokio broadcast
channel:

- send is synchronous and non-blocking;
- each subscriber has an independent cursor;
- slow subscribers receive explicit lag counts;
- absent or closed subscribers do not fail graph execution;
- multiple RunIds may interleave while per-run emission order remains stable.

This is process-local lossy observation, not durable delivery, history replay,
backpressure, disk queue, or network transport.

## Tool observer

`ToolEventSink` is synchronous and fallible. It reports call identity,
behavior, and lifecycle classification without arguments or outputs.

Runtime catches callback panic:

- a start-event failure prevents the Tool call;
- a terminal-event failure remains secondary to the true Tool outcome.

This prevents observability failure from falsely claiming that a
non-idempotent action did or did not run.

## MCP Session shutdown

Explicit shutdown is lifecycle control, separate from dropping an individual
call Future. The first shutdown atomically enters Closing and creates one
Session-owned supervisor. All waiters share the stored completion.

Service close and direct-child cleanup run as independent paths. The
supervisor waits for both, stores the combined result, publishes CLOSED, and
then wakes waiters. Cancelling one waiter does not cancel cleanup.

Drop is a runtime-independent best-effort fallback: it attempts direct-child
kill and attempts to start a standard thread for wait/reap. It is not graceful
shutdown and cannot guarantee wait/reap when thread creation fails.

## Direct evidence

- `crates/group-agent-core/src/context.rs`
- `crates/group-agent-core/src/runtime.rs`
- `crates/group-agent-core/src/event.rs`
- `crates/group-agent-observability-tokio/src/lib.rs`
- `crates/group-agent-model/src/error.rs`
- `crates/group-agent-tool/src/error.rs`
- `crates/group-agent-tool/src/event.rs`
- `crates/group-agent-mcp/src/error.rs`
- `crates/group-agent-mcp/src/session.rs`

Related documents:

- [Core Runtime Design](core-runtime.md)
- [MCP Adapter](../adapters/mcp.md)
- [Development Runbook](../runbooks/development.md)
