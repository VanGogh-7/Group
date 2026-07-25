use std::fmt;

use group_agent_model::{ModelCapabilities, ModelId, ModelMetadata, ProviderId};

use crate::GenaiAdapterConfigError;

/// Requested genai model and conservative Group metadata.
#[derive(Clone)]
pub struct GenaiModelConfig {
    requested_model: String,
    metadata: ModelMetadata,
}

impl GenaiModelConfig {
    /// Creates model configuration and validates its static invariants.
    pub fn new(
        requested_model: impl Into<String>,
        provider: ProviderId,
        model: ModelId,
        capabilities: ModelCapabilities,
    ) -> Result<Self, GenaiAdapterConfigError> {
        let requested_model = requested_model.into();
        if requested_model.trim().is_empty() {
            return Err(GenaiAdapterConfigError::EmptyRequestedModel);
        }
        if capabilities.parallel_tool_calls() {
            return Err(GenaiAdapterConfigError::ParallelToolCallsUnsupported);
        }
        let metadata = ModelMetadata::new(provider, model, capabilities);
        metadata
            .validate()
            .map_err(GenaiAdapterConfigError::InvalidMetadata)?;
        Ok(Self {
            requested_model,
            metadata,
        })
    }

    /// Adds redacted adapter metadata.
    #[must_use]
    pub fn with_metadata_extensions(mut self, extensions: group_agent_model::Extensions) -> Self {
        self.metadata = self.metadata.with_extensions(extensions);
        self
    }

    /// Returns the requested genai model or namespaced identifier.
    #[must_use]
    pub fn requested_model(&self) -> &str {
        &self.requested_model
    }

    /// Returns Group model metadata.
    #[must_use]
    pub const fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }
}

impl fmt::Debug for GenaiModelConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GenaiModelConfig")
            .field("requested_model", &self.requested_model)
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// Policy for genai response content not represented by Group.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum GenaiContentPolicy {
    /// Reject binary, custom, and role-incompatible response parts.
    #[default]
    Reject,
}

/// Streaming behavior allowed by this adapter instance.
///
/// genai 0.6.5 is not safe for OpenAI Responses streaming or OpenAI Chat tool
/// streaming. Consequently this policy has no public tool-streaming mode.
/// Enabled variants require an injected Client bound to
/// `genai::adapter::AdapterKind::OpenAI`. The adapter also validates the exact
/// resolved stream target before polling, so a custom resolver cannot expand
/// this policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum GenaiStreamingPolicy {
    /// Reject every streaming request before calling the injected client.
    #[default]
    Disabled,
    /// Permit text-only streaming on a trusted OpenAI Chat binding.
    TextOnly,
    /// Permit the adapter-audited OpenAI Chat text-only path.
    AuditedTextOnly,
}

/// Bounds for online stream normalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenaiStreamingLimits {
    max_tool_calls: u32,
    max_reasoning_bytes: usize,
    max_thought_signature_bytes: usize,
    max_thought_signatures_per_tool_call: usize,
}

impl GenaiStreamingLimits {
    /// Creates default bounds.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_tool_calls: 1_024,
            max_reasoning_bytes: 16 * 1024 * 1024,
            max_thought_signature_bytes: 4 * 1024 * 1024,
            max_thought_signatures_per_tool_call: 1_024,
        }
    }

    /// Sets the maximum discovered tool-call count.
    #[must_use]
    pub const fn with_max_tool_calls(mut self, maximum: u32) -> Self {
        self.max_tool_calls = maximum;
        self
    }

    /// Sets the maximum retained reasoning byte count.
    #[must_use]
    pub const fn with_max_reasoning_bytes(mut self, maximum: usize) -> Self {
        self.max_reasoning_bytes = maximum;
        self
    }

    /// Sets the maximum retained thought-signature byte count.
    #[must_use]
    pub const fn with_max_thought_signature_bytes(mut self, maximum: usize) -> Self {
        self.max_thought_signature_bytes = maximum;
        self
    }

    /// Sets the maximum distinct signatures associated with one tool call.
    #[must_use]
    pub const fn with_max_thought_signatures_per_tool_call(mut self, maximum: usize) -> Self {
        self.max_thought_signatures_per_tool_call = maximum;
        self
    }

    pub(crate) const fn max_tool_calls(self) -> u32 {
        self.max_tool_calls
    }

    pub(crate) const fn max_reasoning_bytes(self) -> usize {
        self.max_reasoning_bytes
    }

    pub(crate) const fn max_thought_signature_bytes(self) -> usize {
        self.max_thought_signature_bytes
    }

    pub(crate) const fn max_thought_signatures_per_tool_call(self) -> usize {
        self.max_thought_signatures_per_tool_call
    }
}

impl Default for GenaiStreamingLimits {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable adapter policy.
#[derive(Clone)]
pub struct GenaiAdapterConfig {
    model: GenaiModelConfig,
    retain_reasoning_content: bool,
    retain_usage_details: bool,
    allow_response_id_continuation: bool,
    content_policy: GenaiContentPolicy,
    streaming_limits: GenaiStreamingLimits,
    streaming_policy: GenaiStreamingPolicy,
    responses_parser_admission_limit: usize,
}

impl GenaiAdapterConfig {
    /// Creates a privacy-preserving configuration.
    #[must_use]
    pub const fn new(model: GenaiModelConfig) -> Self {
        Self {
            model,
            retain_reasoning_content: false,
            retain_usage_details: true,
            allow_response_id_continuation: false,
            content_policy: GenaiContentPolicy::Reject,
            streaming_limits: GenaiStreamingLimits::new(),
            streaming_policy: GenaiStreamingPolicy::Disabled,
            responses_parser_admission_limit: 8 * 1024 * 1024,
        }
    }

    /// Enables or disables reasoning retention in redacted Extensions.
    #[must_use]
    pub const fn with_reasoning_content(mut self, retain: bool) -> Self {
        self.retain_reasoning_content = retain;
        self
    }

    /// Enables or disables prompt/completion usage-detail Extensions.
    #[must_use]
    pub const fn with_usage_details(mut self, retain: bool) -> Self {
        self.retain_usage_details = retain;
        self
    }

    /// Enables explicit previous-response-ID request continuation.
    #[must_use]
    pub const fn with_response_id_continuation(mut self, allow: bool) -> Self {
        self.allow_response_id_continuation = allow;
        self
    }

    /// Sets unsupported response content policy.
    #[must_use]
    pub const fn with_content_policy(mut self, policy: GenaiContentPolicy) -> Self {
        self.content_policy = policy;
        self
    }

    /// Sets online normalization bounds.
    #[must_use]
    pub const fn with_streaming_limits(mut self, limits: GenaiStreamingLimits) -> Self {
        self.streaming_limits = limits;
        self
    }

    /// Sets the fail-closed streaming policy.
    #[must_use]
    pub const fn with_streaming_policy(mut self, policy: GenaiStreamingPolicy) -> Self {
        self.streaming_policy = policy;
        self
    }

    /// Sets the post-capture parser admission limit for OpenAI Responses.
    ///
    /// genai has already read, parsed, and possibly cloned the complete
    /// Provider body before Group applies this limit. It does not bound network
    /// bytes, HTTP body size, or peak memory. It only prevents Group from
    /// continuing its restricted continuation parse when the captured JSON
    /// value's serialized representation exceeds `maximum`. Measurement stores
    /// no serialized bytes. A successfully captured value is taken and released
    /// after mapping and is not exposed through Group responses, Extensions,
    /// adapter mapping errors, or their default formatting. A genai parsing
    /// failure remains available through the explicit error source chain, which
    /// applications must filter before logging in full.
    #[must_use]
    pub const fn with_responses_parser_admission_limit(mut self, maximum: usize) -> Self {
        self.responses_parser_admission_limit = maximum;
        self
    }

    /// Returns model configuration.
    #[must_use]
    pub const fn model(&self) -> &GenaiModelConfig {
        &self.model
    }

    /// Returns whether reasoning content is retained.
    #[must_use]
    pub const fn retain_reasoning_content(&self) -> bool {
        self.retain_reasoning_content
    }

    /// Returns whether detailed usage objects are retained.
    #[must_use]
    pub const fn retain_usage_details(&self) -> bool {
        self.retain_usage_details
    }

    /// Returns whether previous-response-ID continuation is accepted.
    #[must_use]
    pub const fn allow_response_id_continuation(&self) -> bool {
        self.allow_response_id_continuation
    }

    /// Returns unsupported content policy.
    #[must_use]
    pub const fn content_policy(&self) -> GenaiContentPolicy {
        self.content_policy
    }

    /// Returns stream normalization bounds.
    #[must_use]
    pub const fn streaming_limits(&self) -> GenaiStreamingLimits {
        self.streaming_limits
    }

    /// Returns the streaming compatibility policy.
    #[must_use]
    pub const fn streaming_policy(&self) -> GenaiStreamingPolicy {
        self.streaming_policy
    }

    /// Returns the post-capture Responses parser admission limit.
    ///
    /// This is not a network, HTTP body, or peak-memory bound.
    #[must_use]
    pub const fn responses_parser_admission_limit(&self) -> usize {
        self.responses_parser_admission_limit
    }
}

impl fmt::Debug for GenaiAdapterConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GenaiAdapterConfig")
            .field("model", &self.model)
            .field("retain_reasoning_content", &self.retain_reasoning_content)
            .field("retain_usage_details", &self.retain_usage_details)
            .field(
                "allow_response_id_continuation",
                &self.allow_response_id_continuation,
            )
            .field("content_policy", &self.content_policy)
            .field("streaming_limits", &self.streaming_limits)
            .field("streaming_policy", &self.streaming_policy)
            .field(
                "responses_parser_admission_limit",
                &self.responses_parser_admission_limit,
            )
            .finish()
    }
}
