use std::fmt;
use std::sync::Arc;

use thiserror::Error;

use crate::Extensions;

/// An invalid stable public identifier.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum IdentifierError {
    /// A tool name was empty or whitespace-only.
    #[error("tool name must not be empty")]
    EmptyToolName,
    /// A tool-call identifier was empty or whitespace-only.
    #[error("tool call id must not be empty")]
    EmptyToolCallId,
    /// A provider identifier was empty or whitespace-only.
    #[error("provider id must not be empty")]
    EmptyProviderId,
    /// A model identifier was empty or whitespace-only.
    #[error("model id must not be empty")]
    EmptyModelId,
    /// A response identifier was empty or whitespace-only.
    #[error("response id must not be empty")]
    EmptyResponseId,
}

macro_rules! string_id {
    ($name:ident, $error:expr, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Arc<str>);

        impl $name {
            /// Creates a validated identifier.
            pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err($error);
                }
                Ok(Self(Arc::from(value)))
            }

            /// Returns the identifier text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl TryFrom<&str> for $name {
            type Error = IdentifierError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdentifierError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
    };
}

string_id!(
    ProviderId,
    IdentifierError::EmptyProviderId,
    "A stable provider identifier chosen by an adapter."
);
string_id!(
    ModelId,
    IdentifierError::EmptyModelId,
    "A stable model identifier, independent of provider SDK types."
);

/// One model capability used by common request validation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ModelCapability {
    /// Incremental response streaming.
    Streaming,
    /// Tool definitions and tool calls.
    ToolCalling,
    /// Multiple tool calls in one model turn.
    ParallelToolCalls,
    /// Token usage reporting.
    UsageReporting,
}

/// Provider-neutral capabilities declared by a model implementation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ModelCapabilities {
    streaming: bool,
    tool_calling: bool,
    parallel_tool_calls: bool,
    usage_reporting: bool,
}

impl ModelCapabilities {
    /// Creates a capability set with every capability disabled.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            streaming: false,
            tool_calling: false,
            parallel_tool_calls: false,
            usage_reporting: false,
        }
    }

    /// Enables or disables streaming.
    #[must_use]
    pub const fn with_streaming(mut self, supported: bool) -> Self {
        self.streaming = supported;
        self
    }

    /// Enables or disables tool calling.
    #[must_use]
    pub const fn with_tool_calling(mut self, supported: bool) -> Self {
        self.tool_calling = supported;
        self
    }

    /// Enables or disables parallel tool calls.
    #[must_use]
    pub const fn with_parallel_tool_calls(mut self, supported: bool) -> Self {
        self.parallel_tool_calls = supported;
        self
    }

    /// Enables or disables usage reporting.
    #[must_use]
    pub const fn with_usage_reporting(mut self, supported: bool) -> Self {
        self.usage_reporting = supported;
        self
    }

    /// Returns whether a specific capability is supported.
    #[must_use]
    pub const fn supports(self, capability: ModelCapability) -> bool {
        match capability {
            ModelCapability::Streaming => self.streaming,
            ModelCapability::ToolCalling => self.tool_calling,
            ModelCapability::ParallelToolCalls => self.parallel_tool_calls,
            ModelCapability::UsageReporting => self.usage_reporting,
        }
    }

    /// Returns whether incremental streaming is supported.
    #[must_use]
    pub const fn streaming(self) -> bool {
        self.streaming
    }

    /// Returns whether tool calling is supported.
    #[must_use]
    pub const fn tool_calling(self) -> bool {
        self.tool_calling
    }

    /// Returns whether parallel tool calls are supported.
    #[must_use]
    pub const fn parallel_tool_calls(self) -> bool {
        self.parallel_tool_calls
    }

    /// Returns whether usage reporting is supported.
    #[must_use]
    pub const fn usage_reporting(self) -> bool {
        self.usage_reporting
    }
}

/// Provider and model identity plus capabilities and adapter metadata.
#[derive(Clone, PartialEq)]
pub struct ModelMetadata {
    provider: ProviderId,
    model: ModelId,
    capabilities: ModelCapabilities,
    extensions: Extensions,
}

impl ModelMetadata {
    /// Creates model metadata.
    #[must_use]
    pub fn new(provider: ProviderId, model: ModelId, capabilities: ModelCapabilities) -> Self {
        Self {
            provider,
            model,
            capabilities,
            extensions: Extensions::new(),
        }
    }

    /// Adds provider-defined metadata without changing common fields.
    #[must_use]
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Validates relationships between declared capabilities.
    pub fn validate(&self) -> Result<(), MetadataValidationError> {
        if self.capabilities.parallel_tool_calls && !self.capabilities.tool_calling {
            return Err(MetadataValidationError::ParallelToolCallsRequireToolCalling);
        }
        Ok(())
    }

    /// Returns the provider identifier.
    #[must_use]
    pub const fn provider(&self) -> &ProviderId {
        &self.provider
    }

    /// Returns the model identifier.
    #[must_use]
    pub const fn model(&self) -> &ModelId {
        &self.model
    }

    /// Returns declared capabilities.
    #[must_use]
    pub const fn capabilities(&self) -> ModelCapabilities {
        self.capabilities
    }

    /// Returns provider-defined model metadata.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl fmt::Debug for ModelMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelMetadata")
            .field("provider", &self.provider)
            .field("model", &self.model)
            .field("capabilities", &self.capabilities)
            .field("extensions", &self.extensions)
            .finish()
    }
}

/// Invalid model identity or capability metadata.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum MetadataValidationError {
    /// Parallel tool calls cannot exist without ordinary tool calling.
    #[error("parallel tool calls require tool-calling support")]
    ParallelToolCallsRequireToolCalling,
}
