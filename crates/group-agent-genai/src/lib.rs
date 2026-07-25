//! A provider adapter between `group-agent-model` and `genai` 0.6.5.
//!
//! [`GenaiChatModelAdapter`] accepts an application-configured
//! [`genai::Client`]. This crate never reads `.env`, chooses credentials,
//! executes tools, retries requests, or keeps hidden conversation state.
//! Request extensions owned by this adapter are explicit and use the
//! `group.genai.*` namespace.
//!
//! The adapter maps provider-neutral messages, tools, generation controls,
//! responses, partial usage, and streaming events. Binary and custom response
//! content are rejected. Reasoning is never mixed into assistant answer text.
//! Streaming is fail-closed by default. Enabling it requires a Client bound to
//! [`genai::adapter::AdapterKind::OpenAI`], and the exact resolved stream
//! identity is checked before its lazy HTTP stream is polled. With genai 0.6.5,
//! OpenAI Chat text-only streaming is supported, while OpenAI Chat tool
//! streaming and every OpenAI Responses streaming request are rejected before
//! HTTP dispatch. Non-streaming tool generation is allowed only through
//! [`GenaiChatModelAdapter::new_with_stable_target`], which binds verification
//! and dispatch to one exact [`genai::ServiceTarget`]. For OpenAI Responses,
//! genai first captures the complete raw value; Group then applies a
//! post-capture parser admission limit before correlating encrypted reasoning
//! with normalized tool calls. That limit is not an HTTP-body or peak-memory
//! bound. Its counting serializer retains no serialized bytes, and the raw
//! value from a successful response is taken and released immediately after
//! mapping. It is not exposed through Group responses, Extensions, adapter
//! mapping errors, or their default formatting. A genai parsing failure still
//! remains available as an explicit [`std::error::Error::source`]; applications
//! that log complete source chains must filter potentially sensitive upstream
//! data.
//! Within one function call, identical signatures are retained once in first
//! occurrence order, distinct signatures preserve provider order, and no
//! deduplication crosses function-call boundaries. Empty signatures and
//! checked count or byte-limit failures are rejected.
//! A thought-signature chunk on the audited OpenAI Chat text-only stream is a
//! terminal protocol error; its content is neither retained nor exposed.
//! Dropping a completion future or returned event stream drops the underlying
//! genai future or stream; no forwarding task or channel is created.
//!
//! ```
//! use genai::{Client, adapter::AdapterKind};
//! use group_agent_genai::{
//!     GenaiAdapterConfig, GenaiChatModelAdapter, GenaiModelConfig,
//!     GenaiStreamingPolicy,
//! };
//! use group_agent_model::{
//!     ChatModel, ModelCapabilities, ModelId, ProviderId,
//! };
//!
//! # fn build() -> Result<ChatModel, Box<dyn std::error::Error>> {
//! let client = Client::builder()
//!     .with_adapter_kind(AdapterKind::OpenAI)
//!     .build();
//! let model_config = GenaiModelConfig::new(
//!     "gpt-4o-mini",
//!     ProviderId::new("openai")?,
//!     ModelId::new("gpt-4o-mini")?,
//!     ModelCapabilities::new()
//!         .with_streaming(true)
//!         .with_tool_calling(true)
//!         .with_usage_reporting(true),
//! )?;
//! let adapter = GenaiChatModelAdapter::new(
//!     client,
//!     GenaiAdapterConfig::new(model_config)
//!         .with_streaming_policy(GenaiStreamingPolicy::AuditedTextOnly),
//! )?;
//! let model = ChatModel::from_adapter(adapter)?;
//! # Ok(model)
//! # }
//! ```

mod adapter;
mod config;
mod error;
pub mod extensions;
mod request;
mod response;
mod stream;
mod usage;

pub use adapter::GenaiChatModelAdapter;
pub use config::{
    GenaiAdapterConfig, GenaiContentPolicy, GenaiModelConfig, GenaiStreamingLimits,
    GenaiStreamingPolicy,
};
pub use error::{GenaiAdapterConfigError, GenaiMappingError};
pub use request::{MappedChatRequest, map_chat_request};
pub use response::map_chat_response;
pub use usage::map_genai_usage;
