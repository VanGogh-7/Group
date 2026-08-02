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
| Prebuilt | forwarding Core run/Node control; current Model or Tool Future ownership | remote cancellation proof; rollback; retry; durability |

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

`AgentError` keeps `GraphRunError` as its immediate source. Deliberate traversal
can therefore reach the concrete Model or Tool chain. Its default formatting
does not traverse that chain. For a Tool infrastructure failure,
`tool_batch_report()` borrows the complete current batch report when one was
produced; it does not clone results or expose internal committed State.

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
filter before logging a complete chain. Group cannot redact a source chain
after an application or formatter has rendered it.

## Production tracing policy

Treat Group-owned formatting, explicit source traversal, and upstream tracing
as three separate data paths:

1. Group-owned default `Debug`, `Display`, `GraphEvent`, and Tool observer
   formatting contains lifecycle classifications and identifiers but excludes
   payload and concrete source messages as described above.
2. Calling `Error::source()`, downcasting a source, or formatting a complete
   source chain is an explicit application diagnostic operation. A concrete
   provider, transport, JSON, I/O, or process source may contain sensitive
   data. Group preserves those sources for diagnosis; it cannot sanitize an
   already-formatted chain.
3. Events emitted directly by dependencies bypass Group formatting. In the
   pinned source, `genai` 0.6.5 can trace complete response bodies. The enabled
   `rmcp` 2.2.0 async-read/write codec debug-logs the raw incoming line when
   parsing fails, and its client service Debug-formats an unexpected complete
   protocol-message value at `warn`. Group cannot redact those upstream
   events, and applications must not assume that upstream error or message
   formatting is payload-safe across paths or upgrades.

For a production application using `tracing-subscriber` with its `env-filter`
feature, this is a conservative copyable starting point:

```rust
use tracing_subscriber::EnvFilter;

fn init_group_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_new(concat!(
        "off,",
        "group_agent_core=debug,",
        "group_agent_model=info,",
        "group_agent_tool=info,",
        "group_agent_checkpoint_sqlite=info,",
        "group_agent_observability_tokio=info,",
        "group_agent_prebuilt=info,",
        "group_agent_genai=info,",
        "group_agent_mcp=info,",
        "genai=off,",
        "rmcp=off"
    ))?;

    tracing_subscriber::fmt().with_env_filter(filter).try_init()?;
    Ok(())
}
```

`tracing` target names use Rust module spelling, so Group package dashes become
underscores. `EnvFilter` target directives use raw target-string prefix
matching, without a Rust module-boundary check. For example,
`group_agent_core=debug` covers `group_agent_core::runtime`, while `genai=off`
covers `genai`, `genai::webc`, and conservatively also a collateral target such
as `genai_extra`. The leading `off` denies every unlisted target; applications
should add their own audited targets explicitly. The final `genai=off` and
`rmcp=off` directives make the upstream boundary visible and fail closed even
while the Group adapter targets remain enabled. Group currently emits its Core
lifecycle diagnostics at `debug`; enabling an upstream target is not required
to obtain them.

Before raising either upstream target from `off`, audit the exact deployed
versions, enabled features, transports, provider/server behavior, subscriber
formatters, exporters, retention, and access controls. Exercise both success
and malformed/error paths with synthetic secrets and verify every sink. Filter
or transform events before they leave the process, and keep the opt-in scoped
to the audited environment. Re-audit after any dependency or feature change.

At minimum, treat these as sensitive payload classes:

- prompts, messages, reasoning, and model or MCP content;
- Tool definitions, schemas, arguments, results, and outputs;
- raw HTTP bodies, SSE data, MCP/JSON-RPC frames, and error bodies;
- request/response headers, URLs or endpoints carrying parameters, and
  response, request, session, or event identifiers;
- executable arguments, environment names and values, and local paths; and
- API keys, bearer tokens, cookies, credentials, secret prefixes, and any
  derived authentication material.

This is a least-privilege baseline, not a universal redaction guarantee. The
application remains responsible for every additional target, source-chain
formatter, sink, and exporter.

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

## Prebuilt invocation effects and events

The experimental Agent forwards `RunControl` and `EventConfig` unchanged to
Core. Cancellation, run timeout, and Model/Tool node timeout therefore retain
Core classifications and precedence, and observers receive one Core graph
lifecycle rather than a duplicate Agent event protocol. Default lifecycle
events contain node metadata, not transcript or payload content.

An error may follow earlier successfully committed Tool rounds. Those Tools
may already have produced external side effects, while `AgentError` does not
return the internal committed transcript. Dropping the top-level Future drops
locally owned graph, Model, and Tool Futures but does not prove a remote
operation stopped. Prebuilt provides no rollback, exactly-once, durability,
automatic retry, or safe-blind-retry guarantee.

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
- `crates/group-agent-prebuilt/src/error.rs`
- `crates/group-agent-prebuilt/src/agent_tests.rs`

Related documents:

- [Core Runtime Design](core-runtime.md)
- [Genai Adapter](../adapters/genai.md)
- [MCP Adapter](../adapters/mcp.md)
- [Development Runbook](../runbooks/development.md)
