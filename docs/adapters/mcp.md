# MCP Adapter

> Status: experimental client adapter over the compatibility-first Tool and
> Model APIs. The current production transport is child-process stdio.

`group-agent-mcp` discovers remote MCP Tools and exposes them as immutable
`group-agent-tool` registrations. It is a Tool backend, not a second execution
runtime.

## Dependency boundary

The adapter normally depends on:

- `group-agent-model` for Tool definitions and results;
- `group-agent-tool` for Tool, Registry, validation, timeout, batch,
  side-effect, and ToolMessage behavior;
- exactly `rmcp` 2.2.0 with client and async read/write transport features.

Core is only a development integration dependency. Core, Model, and Tool do
not depend back on MCP, and rmcp types do not enter their public APIs.

The MCP crate declares Rust 1.88 because the fixed rmcp release requires syntax
not accepted by the Rust 1.85 foundation toolchain.

## Session and stdio transport

`McpClientSession` initializes one rmcp client service and reuses its peer for
discovery and Tool calls. It does not reconnect for each Tool or keep a global
Session.

The production constructor starts a direct child process with an executable,
separate arguments, and explicit environment configuration. It does not build
a shell command string. An async read/write constructor exists for embedding
and offline tests; it is not a claim of production HTTP support.

Initialization records server capabilities. Discovery fails if the server
does not advertise Tools.

## Bounded discovery

The adapter owns the complete `tools/list` traversal:

1. request the first page without a cursor;
2. track every returned cursor;
3. reject repeated cursors and arbitrary cycles;
4. enforce nonzero page and Tool limits with checked arithmetic;
5. accumulate definitions in private temporary state;
6. validate names, mappings, behaviors, and schemas;
7. publish one immutable `McpToolSet` only after complete success.

Protocol, transport, cycle, limit, naming, schema, or behavior failure publishes
no partial Registry. A failed refresh leaves an existing snapshot unchanged.
A successful refresh returns a new immutable snapshot rather than mutating one
already used by a ToolRuntime.

## Naming

`McpToolMapping` freezes:

- the local Tool name;
- the server identifier;
- the exact remote Tool name.

A single-server application may preserve a legal remote name. Multi-server
applications can apply a stable namespace or prefix. Invalid names and
collisions fail discovery rather than using last-write-wins.

Remote dispatch always uses the recorded remote name. Routing metadata is not
injected into Tool arguments.

## Schema and behavior

MCP definitions are converted into Model Tool definitions, then registered
through `ToolRegistryBuilder`. JSON Schema compilation and call-time argument
validation therefore remain in ToolRuntime and occur before a remote request.

Every remote Tool defaults to conservative `NonIdempotentWrite` behavior and
serial execution. Network completion, server annotations, timeout, and local
Future drop cannot prove that a remote side effect did not occur.

Applications may provide an exact server/Tool behavior override. Overrides are
validated and frozen during discovery. The adapter never infers safe retry,
exactly-once execution, rollback, or remote cancellation from an annotation.

## Call and result mapping

`McpRemoteTool::execute` sends validated arguments and the exact remote name
through the reusable Session.

Supported result mapping:

- text blocks preserve wire order;
- structured content is serialized once and appended as one JSON text part;
- MCP `isError = true` becomes a business `ToolResult` with `is_error = true`;
- ToolRuntime message helpers associate the result with the original
  `ToolCallId`.

Unsupported image, audio, binary or unknown blocks, embedded resources, and
resource links fail closed. The adapter does not silently discard content,
download resources, or invent placeholder text.

## Error classification and redaction

- MCP or JSON-RPC error responses are Protocol failures.
- I/O, send, and unexpected connection closure are Transport failures.
- New work after explicit close is `SessionClosed`.
- Unsupported blocks are content-mapping failures.
- Discovery and shutdown retain their own typed errors.

Concrete rmcp, JSON, I/O, and process sources remain reachable through
`Error::source()`. Default Debug and Display show safe categories and
identifiers, not command arguments, environment values, Tool arguments,
results, protocol payloads, panic payloads, or source messages.

Applications that log complete source chains or enable upstream rmcp targets
must apply sensitive-data filtering. Keep the upstream `rmcp` tracing target
disabled unless the exact deployed source and every sink have been audited;
see the authoritative
[Production tracing policy](../design/error-cancellation-observability.md#production-tracing-policy)
for a copyable least-privilege filter and audit checklist.

## Explicit shutdown

The first `shutdown()` atomically enters Closing and stops new discovery and
Tool calls. The Session owns one cleanup supervisor and shared completion;
concurrent and repeated callers wait for the same stored result.

The supervisor always joins two independent paths:

- rmcp service close;
- direct-child cleanup.

A service error, inner or outer JoinError, or worker panic does not skip child
cleanup. Child cleanup failure does not skip service outcome collection. If
both fail, service failure is primary.

The final order is:

1. complete direct-child cleanup;
2. combine and store the result;
3. publish CLOSED;
4. wake completion waiters.

Cancelling one waiter does not cancel Session-owned cleanup. Zero grace still
performs one non-blocking exit check and then immediately kills and waits for a
live direct child. The contract covers the direct child only, not an arbitrary
process tree.

## Drop fallback

Drop is not explicit shutdown. It cannot perform graceful protocol close or
asynchronously report a result.

If Drop still owns a direct child, it synchronously attempts kill and then
attempts to start a standard thread for wait/reap. Thread creation can fail;
in that case kill has been attempted but wait/reap is not guaranteed and a
zombie may remain temporarily.

Applications that require a reliable final result and confirmed direct-child
cleanup must await explicit `shutdown()`.

## Reused ToolRuntime behavior

The adapter does not duplicate:

- argument validation;
- per-call timeout;
- batch concurrency limits;
- side-effect serialization;
- collect-all or fail-fast drain;
- stable result ordering;
- ToolCall ID pairing.

Dropping or timing out a ToolRuntime call releases local ownership. It does not
prove the server stopped or that no remote effect occurred.

## Unsupported capabilities

The current adapter does not implement:

- HTTP or OAuth transports;
- credential storage;
- MCP server hosting;
- Resources, Prompts, Sampling, or Roots;
- automatic `tools/list_changed` refresh;
- full multimodal or resource content;
- retry, remote rollback, or exactly-once semantics;
- an Agent loop.

## Direct evidence

- `crates/group-agent-mcp/src/session.rs`
- `crates/group-agent-mcp/src/discovery.rs`
- `crates/group-agent-mcp/src/mapping.rs`
- `crates/group-agent-mcp/src/tool.rs`
- `crates/group-agent-mcp/src/error.rs`
- `crates/group-agent-mcp/tests/mcp_adapter.rs`
- `crates/group-agent-mcp/tests/group_integration.rs`

Related documents:

- [Architecture](../../ARCHITECTURE.md)
- [Model and Tools Design](../design/model-and-tools.md)
- [Error, Cancellation, and Observability](../design/error-cancellation-observability.md)
- [ADR-007](../adr/007-mcp-tool-backend.md)
- [ADR-009](../adr/009-mcp-session-shutdown.md)
