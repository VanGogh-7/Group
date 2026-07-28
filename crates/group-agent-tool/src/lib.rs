//! Provider-neutral local tool execution for Group applications.
//!
//! `group-agent-model` defines [`ToolCall`], [`ToolDefinition`], and
//! [`ToolResult`] as data. This independent crate adds an object-safe [`Tool`]
//! port, immutable [`ToolRegistry`], precompiled JSON Schema validation, a
//! timeout-aware [`ToolRuntime`], deterministic bounded batches, and optional
//! payload-free lifecycle events.
//!
//! Tool implementations receive neither Group `NodeContext` nor a cancellation
//! token. The runtime creates no Tokio runtime, task, or channel. Dropping a
//! single or batch execution future drops every still-pending tool future.
//! Explicit fail-fast batches instead stop scheduling new calls and drain every
//! already-started call to its observable outcome.
//! Schema failures use redacted default formatting while retaining concrete
//! `jsonschema::ValidationError` sources. Explicit source traversal may expose
//! upstream arguments or schema details, so callers own full-chain log
//! filtering.
//!
//! [`ToolEventSink`] runs before and after execution. A failed or panicking
//! `ExecutionStarted` callback prevents execution and returns `ObserverFailed`.
//! Terminal callback failure cannot replace the primary Tool outcome and is
//! available through [`ToolExecutionReport`]. Panic payloads are discarded
//! without formatting. [`ToolRuntime::execute_message`] and
//! [`ToolBatchReport::into_tool_messages`] pair results with their original
//! [`ToolCallId`].
//! Per-call timeouts use the caller's Tokio runtime. No retry, exactly-once
//! guarantee, durable idempotency store, MCP, sandbox, credential store, or
//! prebuilt agent is implemented.
//!
//! ```
//! use async_trait::async_trait;
//! use group_agent_model::{ToolCall, ToolCallId, ToolDefinition, ToolName};
//! use group_agent_tool::{
//!     Tool, ToolBehavior, ToolError, ToolInput, ToolOutput, ToolRegistry,
//!     ToolRuntime,
//! };
//! use serde_json::json;
//!
//! struct Echo {
//!     definition: ToolDefinition,
//! }
//!
//! #[async_trait]
//! impl Tool for Echo {
//!     fn name(&self) -> &ToolName {
//!         self.definition.name()
//!     }
//!
//!     fn definition(&self) -> &ToolDefinition {
//!         &self.definition
//!     }
//!
//!     fn behavior(&self) -> ToolBehavior {
//!         ToolBehavior::read_only()
//!     }
//!
//!     async fn execute(&self, _input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
//!         Ok(ToolOutput::success_text("ok"))
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let definition = ToolDefinition::new(
//!     ToolName::new("echo")?,
//!     "Echo validated input",
//!     json!({"type": "object"}),
//! );
//! let mut builder = ToolRegistry::builder();
//! builder.register(Echo { definition })?;
//! let runtime = ToolRuntime::new(builder.build());
//! let call = ToolCall::new(
//!     ToolCallId::new("call-1")?,
//!     ToolName::new("echo")?,
//!     json!({}),
//! );
//! let message = runtime.execute_message(&call).await?;
//! assert_eq!(message.as_tool().expect("tool message").tool_call_id(), call.id());
//! # Ok(())
//! # }
//! ```

mod error;
mod event;
mod registry;
mod runtime;
mod tool;

pub use error::{
    IdempotencyKeyError, SchemaViolation, ToolBatchError, ToolBehaviorError, ToolCallContext,
    ToolDefinitionError, ToolError, ToolErrorKind, ToolObserverError, ToolObserverFailure,
    ToolObserverFailureKind, ToolRegistryError, ToolRuntimeError, ToolRuntimeErrorKind,
};
pub use event::{ToolEvent, ToolEventSink};
pub use group_agent_model::{ToolCall, ToolCallId, ToolDefinition, ToolName, ToolResult};
pub use registry::{ToolRegistry, ToolRegistryBuilder};
pub use runtime::{
    ToolBatchConfig, ToolBatchFailurePolicy, ToolBatchItem, ToolBatchReport, ToolExecutionOptions,
    ToolExecutionReport, ToolRuntime,
};
pub use tool::{IdempotencyKey, Tool, ToolBehavior, ToolInput, ToolOutput, ToolSideEffect};
