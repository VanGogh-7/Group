use std::collections::BTreeSet;
use std::fmt;

use thiserror::Error;

use crate::{Extensions, Message, ToolCallId, ToolChoice, ToolDefinition, ToolName};

/// Common provider-neutral generation controls.
#[derive(Clone, Default, PartialEq)]
pub struct GenerationConfig {
    temperature: Option<f64>,
    top_p: Option<f64>,
    max_output_tokens: Option<u32>,
    stop_sequences: Vec<String>,
    parallel_tool_calls: Option<bool>,
}

impl GenerationConfig {
    /// Creates an empty configuration that leaves every value provider-default.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            stop_sequences: Vec::new(),
            parallel_tool_calls: None,
        }
    }

    /// Sets sampling temperature.
    ///
    /// Provider-neutral validation accepts every finite non-negative value.
    /// Adapters may reject values their provider does not support.
    #[must_use]
    pub const fn with_temperature(mut self, temperature: f64) -> Self {
        self.temperature = Some(temperature);
        self
    }

    /// Sets nucleus-sampling probability in the inclusive range `[0, 1]`.
    ///
    /// Zero is retained because it is a meaningful deterministic boundary for
    /// some providers; adapters may impose narrower documented constraints.
    #[must_use]
    pub const fn with_top_p(mut self, top_p: f64) -> Self {
        self.top_p = Some(top_p);
        self
    }

    /// Sets the maximum number of output tokens.
    #[must_use]
    pub const fn with_max_output_tokens(mut self, max_output_tokens: u32) -> Self {
        self.max_output_tokens = Some(max_output_tokens);
        self
    }

    /// Sets ordered stop sequences.
    #[must_use]
    pub fn with_stop_sequences<I, T>(mut self, stop_sequences: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.stop_sequences = stop_sequences.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the provider-neutral parallel-tool-call preference.
    #[must_use]
    pub const fn with_parallel_tool_calls(mut self, parallel: bool) -> Self {
        self.parallel_tool_calls = Some(parallel);
        self
    }

    /// Returns sampling temperature.
    #[must_use]
    pub const fn temperature(&self) -> Option<f64> {
        self.temperature
    }

    /// Returns nucleus-sampling probability.
    #[must_use]
    pub const fn top_p(&self) -> Option<f64> {
        self.top_p
    }

    /// Returns the maximum output token count.
    #[must_use]
    pub const fn max_output_tokens(&self) -> Option<u32> {
        self.max_output_tokens
    }

    /// Returns ordered stop sequences.
    #[must_use]
    pub fn stop_sequences(&self) -> &[String] {
        &self.stop_sequences
    }

    /// Returns the parallel-tool-call preference.
    #[must_use]
    pub const fn parallel_tool_calls(&self) -> Option<bool> {
        self.parallel_tool_calls
    }

    fn validate(&self) -> Result<(), RequestValidationError> {
        if let Some(value) = self.temperature {
            if !value.is_finite() || value < 0.0 {
                return Err(RequestValidationError::InvalidTemperature { value });
            }
        }
        if let Some(value) = self.top_p {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(RequestValidationError::InvalidTopP { value });
            }
        }
        if self.max_output_tokens == Some(0) {
            return Err(RequestValidationError::ZeroMaxOutputTokens);
        }
        if let Some((index, _)) = self
            .stop_sequences
            .iter()
            .enumerate()
            .find(|(_, sequence)| sequence.is_empty())
        {
            return Err(RequestValidationError::EmptyStopSequence { index });
        }
        Ok(())
    }
}

impl fmt::Debug for GenerationConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GenerationConfig")
            .field("temperature", &self.temperature)
            .field("top_p", &self.top_p)
            .field("max_output_tokens", &self.max_output_tokens)
            .field(
                "stop_sequence_bytes",
                &self
                    .stop_sequences
                    .iter()
                    .map(String::len)
                    .collect::<Vec<_>>(),
            )
            .field("parallel_tool_calls", &self.parallel_tool_calls)
            .finish()
    }
}

/// A complete provider-neutral chat request.
#[derive(Clone, PartialEq)]
pub struct ChatRequest {
    messages: Vec<Message>,
    tools: Vec<ToolDefinition>,
    tool_choice: ToolChoice,
    generation: GenerationConfig,
    extensions: Extensions,
}

impl ChatRequest {
    /// Creates a request. Call [`Self::validate`] before provider invocation.
    #[must_use]
    pub fn new(messages: Vec<Message>) -> Self {
        Self {
            messages,
            tools: Vec::new(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            extensions: Extensions::new(),
        }
    }

    /// Sets declared tools.
    #[must_use]
    pub fn with_tools(mut self, tools: Vec<ToolDefinition>) -> Self {
        self.tools = tools;
        self
    }

    /// Sets tool selection.
    #[must_use]
    pub fn with_tool_choice(mut self, tool_choice: ToolChoice) -> Self {
        self.tool_choice = tool_choice;
        self
    }

    /// Sets common generation controls.
    #[must_use]
    pub fn with_generation(mut self, generation: GenerationConfig) -> Self {
        self.generation = generation;
        self
    }

    /// Sets provider-specific extension data.
    #[must_use]
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Returns ordered conversation messages.
    #[must_use]
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// Returns declared tools.
    #[must_use]
    pub fn tools(&self) -> &[ToolDefinition] {
        &self.tools
    }

    /// Returns tool selection.
    #[must_use]
    pub const fn tool_choice(&self) -> &ToolChoice {
        &self.tool_choice
    }

    /// Returns common generation controls.
    #[must_use]
    pub const fn generation(&self) -> &GenerationConfig {
        &self.generation
    }

    /// Returns provider-specific extension data.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Validates provider-neutral request invariants.
    pub fn validate(&self) -> Result<(), RequestValidationError> {
        if self.messages.is_empty() {
            return Err(RequestValidationError::EmptyMessages);
        }
        self.generation.validate()?;

        let mut tool_names = BTreeSet::new();
        for tool in &self.tools {
            if !tool_names.insert(tool.name().clone()) {
                return Err(RequestValidationError::DuplicateToolDefinition {
                    name: tool.name().clone(),
                });
            }
        }

        if let ToolChoice::Named(name) = &self.tool_choice {
            if !tool_names.contains(name) {
                return Err(RequestValidationError::UnknownNamedTool { name: name.clone() });
            }
        }
        if matches!(self.tool_choice, ToolChoice::Required) && self.tools.is_empty() {
            return Err(RequestValidationError::RequiredToolChoiceWithoutTools);
        }

        let mut known_calls = BTreeSet::<ToolCallId>::new();
        let mut completed_calls = BTreeSet::<ToolCallId>::new();
        for (message_index, message) in self.messages.iter().enumerate() {
            if let Some(assistant) = message.as_assistant() {
                for call in assistant.tool_calls() {
                    if !known_calls.insert(call.id().clone()) {
                        return Err(RequestValidationError::DuplicateToolCallId {
                            id: call.id().clone(),
                        });
                    }
                }
            }
            if let Some(tool) = message.as_tool() {
                if !known_calls.contains(tool.tool_call_id()) {
                    return Err(RequestValidationError::UnknownToolCallReference {
                        id: tool.tool_call_id().clone(),
                        message_index,
                    });
                }
                if !completed_calls.insert(tool.tool_call_id().clone()) {
                    return Err(RequestValidationError::DuplicateToolResult {
                        id: tool.tool_call_id().clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

impl fmt::Debug for ChatRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatRequest")
            .field("messages", &self.messages)
            .field("tools", &self.tools)
            .field("tool_choice", &self.tool_choice)
            .field("generation", &self.generation)
            .field("extensions", &self.extensions)
            .finish()
    }
}

/// A provider-neutral request validation failure.
#[derive(Clone, Debug, Error, PartialEq)]
#[non_exhaustive]
pub enum RequestValidationError {
    /// No message was supplied.
    #[error("chat request must contain at least one message")]
    EmptyMessages,
    /// Temperature was non-finite or negative.
    #[error("temperature {value:?} must be finite and non-negative")]
    InvalidTemperature { value: f64 },
    /// Top-p was non-finite or outside the common range.
    #[error("top_p {value:?} must be finite and in 0.0..=1.0")]
    InvalidTopP { value: f64 },
    /// A configured maximum token count was zero.
    #[error("max_output_tokens must be greater than zero")]
    ZeroMaxOutputTokens,
    /// A stop sequence was empty.
    #[error("stop sequence at index {index} must not be empty")]
    EmptyStopSequence { index: usize },
    /// Two declared tools used one name.
    #[error("tool `{name}` is declared more than once")]
    DuplicateToolDefinition { name: ToolName },
    /// A named choice did not reference a declared tool.
    #[error("named tool choice `{name}` is not declared")]
    UnknownNamedTool { name: ToolName },
    /// Required selection was requested without any declared tool.
    #[error("required tool choice needs at least one declared tool")]
    RequiredToolChoiceWithoutTools,
    /// Two assistant calls used one stable identifier.
    #[error("tool call id `{id}` appears more than once")]
    DuplicateToolCallId { id: ToolCallId },
    /// A tool result referenced no earlier assistant tool call.
    #[error(
        "tool result at message index {message_index} references unknown or later call id `{id}`"
    )]
    UnknownToolCallReference {
        id: ToolCallId,
        message_index: usize,
    },
    /// One call identifier received multiple results.
    #[error("tool call id `{id}` has more than one tool result")]
    DuplicateToolResult { id: ToolCallId },
}
