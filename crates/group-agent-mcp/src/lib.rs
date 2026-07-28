//! MCP client tools for the independent Group Tool Runtime.
//!
//! This crate connects one reusable MCP client session to
//! `group-agent-tool`. Adapter-owned discovery checks the server's tools
//! capability, follows every `tools/list` page with cursor-cycle, page-count,
//! and tool-count guards, and publishes nothing until the traversal succeeds.
//! The complete result becomes an immutable registry snapshot while JSON Schema
//! compilation remains owned by `ToolRegistry`.
//!
//! MCP is only a Tool backend. `ToolRuntime` remains responsible for call
//! identity, cached schema validation, timeout, side-effect policy, batches,
//! fail-fast draining, and Tool-message pairing. The adapter adds no retry,
//! exactly-once claim, rollback, sandbox, credential store, HTTP, OAuth,
//! Resources, Prompts, Sampling, Roots, or Agent loop.
//!
//! The supported production transport is child-process stdio configured as an
//! executable plus separate arguments. Arbitrary async read/write transports
//! are also accepted for embedding and offline tests. Sessions are initialized
//! once, cheaply cloned, explicitly shut down, and never reconnected per call.
//! Explicit stdio shutdown closes rmcp, then waits for the direct child, kills
//! it after a bounded grace period when necessary, and waits again to reap it.
//! Zero grace performs one non-blocking exit check before immediate kill/wait.
//! Concurrent and repeated shutdown callers wait for one Session-owned
//! completion; cancelling any caller Future does not cancel that cleanup.
//! Service close and direct-child cleanup use independent tasks that the
//! supervisor always awaits, so `QuitReason::JoinError`, an outer rmcp task
//! JoinError, or a worker panic becomes source-preserving `ShutdownFailed`
//! without skipping child cleanup. When both paths fail, the service failure is
//! primary. The final result is stored, `CLOSED` is published, and only then
//! are waiters woken. rmcp 2.2.0 logs but does not return `transport.close()`
//! errors, so those errors are outside Group's observable guarantee.
//!
//! Stdio children also have a runtime-independent Drop fallback: it requests a
//! direct-child kill synchronously, then tries to hand wait/reap to a standard
//! thread. Thread creation can fail under OS or resource exhaustion; in that
//! case the kill has still been attempted, but this process cannot guarantee
//! wait/reap and a zombie may remain until later OS or parent-process cleanup.
//! Drop neither performs graceful close nor blocks indefinitely, and no
//! process-tree cleanup is claimed. Explicit `shutdown` is the reliable,
//! recommended lifecycle path. Dropping an individual call future releases
//! that request's ownership but does not promise remote rollback or immediate
//! side-effect termination.
//!
//! rmcp MCP/JSON-RPC error responses are protocol failures; transport closure
//! and I/O failures remain transport failures. Duplicate behavior overrides
//! are invalid even when their values match. Default `Debug` and `Display`
//! output is payload-safe, including `McpToolSet` summary Debug. Explicit
//! `Error::source()` traversal retains rmcp, JSON, I/O, and process details, so
//! callers own filtering of complete source chains.
//!
//! A stdio server is configured without shell parsing:
//!
//! ```
//! use group_agent_mcp::{McpServerConfig, McpServerId};
//!
//! let config = McpServerConfig::new(
//!     McpServerId::new("local-server")?,
//!     "local-mcp-server",
//! )?
//! .with_arg("--stdio");
//! assert_eq!(config.server_id().as_str(), "local-server");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod config;
mod discovery;
mod error;
mod mapping;
mod session;
mod tool;

pub use config::{
    DEFAULT_MAX_DISCOVERED_TOOLS, DEFAULT_MAX_DISCOVERY_PAGES, DEFAULT_STDIO_SHUTDOWN_GRACE,
    McpDiscoveryConfig, McpServerConfig, McpServerId, McpToolNamePolicy, McpToolNamePolicyKind,
    McpToolPrefix,
};
pub use discovery::{McpToolMapping, McpToolSet};
pub use error::{McpAdapterError, McpAdapterErrorKind, McpConfigError};
pub use mapping::map_call_tool_result;
pub use session::McpClientSession;
