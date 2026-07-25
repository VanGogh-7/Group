use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures_core::Stream;

use crate::{
    ChatRequest, ChatResponse, ChatStreamEvent, Extensions, GenerationConfig, Message,
    MetadataValidationError, ModelCapability, ModelError, ModelMetadata, ToolChoice,
    ToolDefinition,
};

/// A boxed, provider-neutral stream whose items may fail during consumption.
pub type ChatEventStream =
    Pin<Box<dyn Stream<Item = Result<ChatStreamEvent, ModelError>> + Send + 'static>>;

/// The raw implementation boundary for a provider adapter.
///
/// Adapter methods receive [`ValidatedChatRequest`], which only the public
/// [`ChatModel`] facade can construct after provider-neutral request and
/// capability validation. An independent adapter crate can inspect the
/// request through its accessors or consume it with
/// [`ValidatedChatRequest::into_inner`].
///
/// The validation order is fixed: request invariants, common capabilities,
/// streaming capability for [`ChatModel::stream`], then one raw dispatch.
/// Adapters should translate the already-validated request and must not spawn
/// detached work that survives cancellation of the returned future.
///
/// ```
/// use async_trait::async_trait;
/// use group_agent_model::{
///     AssistantMessage, ChatModelAdapter, ChatResponse, FinishReason,
///     ModelCapabilities, ModelError, ModelId, ModelMetadata, ProviderId,
///     ValidatedChatRequest,
/// };
///
/// struct Adapter {
///     metadata: ModelMetadata,
/// }
///
/// #[async_trait]
/// impl ChatModelAdapter for Adapter {
///     fn metadata(&self) -> &ModelMetadata {
///         &self.metadata
///     }
///
///     async fn complete_raw(
///         &self,
///         request: ValidatedChatRequest,
///     ) -> Result<ChatResponse, ModelError> {
///         let _provider_request = request.into_inner();
///         Ok(ChatResponse::new(
///             AssistantMessage::text("offline"),
///             FinishReason::Stop,
///         ))
///     }
/// }
///
/// let _adapter = Adapter {
///     metadata: ModelMetadata::new(
///         ProviderId::new("example").unwrap(),
///         ModelId::new("offline").unwrap(),
///         ModelCapabilities::new(),
///     ),
/// };
/// ```
#[async_trait]
pub trait ChatModelAdapter: Send + Sync {
    /// Returns stable metadata captured when the facade is constructed.
    fn metadata(&self) -> &ModelMetadata;

    /// Performs one non-streaming provider call.
    async fn complete_raw(&self, request: ValidatedChatRequest)
    -> Result<ChatResponse, ModelError>;

    /// Starts one streaming provider call.
    ///
    /// Adapters that do not support streaming may keep this default. A facade
    /// whose metadata declares no streaming support rejects the request before
    /// this method can be entered.
    async fn stream_raw(
        &self,
        _request: ValidatedChatRequest,
    ) -> Result<ChatEventStream, ModelError> {
        Err(ModelError::unsupported(
            ModelCapability::Streaming,
            self.metadata(),
        ))
    }
}

/// A request admitted through the complete public validation sequence.
///
/// Values are created only by [`ChatModel::complete`] or [`ChatModel::stream`]
/// after request and required capability validation. The type is public so an
/// adapter in an independent crate can receive it, but its field and
/// constructor are crate-private. It has no unchecked public constructor or
/// conversion from [`ChatRequest`].
///
/// Accessors are read-only. [`Self::into_inner`] consumes the validated wrapper
/// so an adapter can move the original request into provider mapping without
/// cloning it.
///
/// Ordinary application code cannot pass a raw request to an adapter:
///
/// ```compile_fail
/// use group_agent_model::{ChatModelAdapter, ChatRequest, Message};
///
/// async fn bypass(adapter: &dyn ChatModelAdapter) {
///     let request = ChatRequest::new(vec![Message::user("hello")]);
///     adapter.complete_raw(request).await.unwrap();
/// }
/// ```
///
/// It also cannot construct the validated wrapper:
///
/// ```compile_fail
/// use group_agent_model::{ChatRequest, Message, ValidatedChatRequest};
///
/// let request = ChatRequest::new(vec![Message::user("hello")]);
/// let validated = ValidatedChatRequest::new(request);
/// ```
pub struct ValidatedChatRequest {
    request: ChatRequest,
}

impl ValidatedChatRequest {
    pub(crate) const fn from_validated(request: ChatRequest) -> Self {
        Self { request }
    }

    /// Borrows the complete provider-neutral request.
    #[must_use]
    pub const fn request(&self) -> &ChatRequest {
        &self.request
    }

    /// Returns ordered conversation messages.
    #[must_use]
    pub fn messages(&self) -> &[Message] {
        self.request.messages()
    }

    /// Returns declared tools.
    #[must_use]
    pub fn tools(&self) -> &[ToolDefinition] {
        self.request.tools()
    }

    /// Returns tool selection.
    #[must_use]
    pub const fn tool_choice(&self) -> &ToolChoice {
        self.request.tool_choice()
    }

    /// Returns common generation controls.
    #[must_use]
    pub const fn generation(&self) -> &GenerationConfig {
        self.request.generation()
    }

    /// Returns provider-specific request extensions.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        self.request.extensions()
    }

    /// Consumes the wrapper and returns the original request without cloning.
    #[must_use]
    pub fn into_inner(self) -> ChatRequest {
        self.request
    }
}

impl fmt::Debug for ValidatedChatRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ValidatedChatRequest")
            .field(&self.request)
            .finish()
    }
}

/// A validated, cheaply cloneable dynamic chat-model handle.
///
/// Construction validates and snapshots metadata. Every invocation follows the
/// fixed order `request.validate()`, common capability validation, the
/// streaming capability check when applicable, private validated-wrapper
/// construction, then exactly one dynamic dispatch into the raw adapter.
/// Adapter implementations cannot override these public methods.
#[derive(Clone)]
pub struct ChatModel {
    adapter: Arc<dyn ChatModelAdapter>,
    metadata: Arc<ModelMetadata>,
}

impl ChatModel {
    /// Wraps an adapter after validating and snapshotting its metadata.
    pub fn new(adapter: Arc<dyn ChatModelAdapter>) -> Result<Self, MetadataValidationError> {
        let metadata = adapter.metadata().clone();
        metadata.validate()?;
        Ok(Self {
            adapter,
            metadata: Arc::new(metadata),
        })
    }

    /// Wraps an owned concrete adapter.
    pub fn from_adapter<A>(adapter: A) -> Result<Self, MetadataValidationError>
    where
        A: ChatModelAdapter + 'static,
    {
        Self::new(Arc::new(adapter))
    }

    /// Returns the validated immutable metadata snapshot.
    #[must_use]
    pub fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    /// Validates and performs one complete response call.
    ///
    /// Request validation precedes common capability validation. A failure at
    /// either boundary prevents raw adapter dispatch.
    pub async fn complete(&self, request: ChatRequest) -> Result<ChatResponse, ModelError> {
        self.validate_common(&request)?;
        self.adapter
            .complete_raw(ValidatedChatRequest::from_validated(request))
            .await
    }

    /// Validates and starts one incremental response.
    ///
    /// Initialization errors are returned by this future. Later failures are
    /// returned as stream items. Request validation precedes common capability
    /// validation, which precedes the streaming capability check. Any failure
    /// prevents raw adapter dispatch.
    pub async fn stream(&self, request: ChatRequest) -> Result<ChatEventStream, ModelError> {
        self.validate_common(&request)?;
        self.require_capability(ModelCapability::Streaming)?;
        self.adapter
            .stream_raw(ValidatedChatRequest::from_validated(request))
            .await
    }

    fn validate_common(&self, request: &ChatRequest) -> Result<(), ModelError> {
        request.validate().map_err(ModelError::invalid_request)?;

        let parallel = request.generation().parallel_tool_calls() == Some(true);
        let has_tool_history = request.messages().iter().any(|message| {
            message
                .as_assistant()
                .is_some_and(|assistant| !assistant.tool_calls().is_empty())
                || message.as_tool().is_some()
        });
        let uses_tools = !request.tools().is_empty()
            || !matches!(request.tool_choice(), ToolChoice::Auto | ToolChoice::None)
            || has_tool_history
            || parallel;
        if uses_tools {
            self.require_capability(ModelCapability::ToolCalling)?;
        }
        if parallel {
            self.require_capability(ModelCapability::ParallelToolCalls)?;
        }
        Ok(())
    }

    fn require_capability(&self, capability: ModelCapability) -> Result<(), ModelError> {
        if self.metadata.capabilities().supports(capability) {
            Ok(())
        } else {
            Err(ModelError::unsupported(capability, &self.metadata))
        }
    }
}

impl fmt::Debug for ChatModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatModel")
            .field("metadata", &self.metadata)
            .finish_non_exhaustive()
    }
}
