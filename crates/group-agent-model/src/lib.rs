//! Provider-neutral chat model types and asynchronous invocation boundaries.
//!
//! This crate deliberately does not depend on `group-agent-core`. Applications
//! construct a validated [`ChatModel`] facade around an `Arc`-backed
//! [`ChatModelAdapter`] and call it from any asynchronous context, including a
//! Group node. The facade always validates requests and capabilities before
//! creating a [`ValidatedChatRequest`], whose private construction prevents
//! ordinary application code from bypassing validation through the raw port.
//! Independent adapter crates can still implement [`ChatModelAdapter`] and
//! inspect or consume that wrapper.
//!
//! [`ChatStreamCollector`] applies each event atomically. Its first active-state
//! error permanently poisons manual collection, and [`collect_chat_stream`]
//! stops polling on the first item or protocol error. [`TokenUsage`] and
//! [`Extensions`] validate cumulative merges before updating in place, without
//! cloning previously accumulated extension values.
//!
//! Implementations must not hide model work in detached background tasks.
//! Dropping the future returned by [`ChatModel::complete`] or
//! [`ChatModel::stream`] is the cancellation boundary. Timeouts, retries, tool
//! execution, and provider authentication belong to callers or adapter crates.
//! Content-bearing types use redacted `Debug` implementations.
//!
//! ```
//! use group_agent_model::{ChatModel, ChatRequest, Message};
//!
//! async fn ask(model: ChatModel) -> Result<String, Box<dyn std::error::Error>> {
//!     let request = ChatRequest::new(vec![Message::user("Hello")]);
//!     let response = model.complete(request).await?;
//!     Ok(response.message().text_content())
//! }
//! ```

mod content;
mod error;
mod extensions;
mod message;
mod metadata;
mod model;
mod request;
mod response;
mod stream;
mod tool;

pub use content::ContentPart;
pub use error::{ModelError, ModelErrorKind, Retryability};
pub use extensions::{ExtensionError, ExtensionMergeError, Extensions};
pub use message::{AssistantMessage, Message, Role, SystemMessage, ToolMessage, UserMessage};
pub use metadata::{
    IdentifierError, MetadataValidationError, ModelCapabilities, ModelCapability, ModelId,
    ModelMetadata, ProviderId,
};
pub use model::{ChatEventStream, ChatModel, ChatModelAdapter, ValidatedChatRequest};
pub use request::{ChatRequest, GenerationConfig, RequestValidationError};
pub use response::{ChatResponse, FinishReason, ResponseId, TokenUsage, TokenUsageError};
pub use stream::{
    ChatStreamCollector, ChatStreamEvent, StreamProtocolError, ToolCallDelta, collect_chat_stream,
};
pub use tool::{ToolCall, ToolCallId, ToolChoice, ToolDefinition, ToolName, ToolResult};
