use std::fmt;
use std::sync::Arc;

use serde_json::Value;

use crate::{ContentPart, Extensions, IdentifierError};

macro_rules! tool_string_id {
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

tool_string_id!(
    ToolName,
    IdentifierError::EmptyToolName,
    "A validated, provider-neutral tool name."
);
tool_string_id!(
    ToolCallId,
    IdentifierError::EmptyToolCallId,
    "A stable identifier linking one tool call to its result."
);

/// A tool exposed to a model.
///
/// The input schema is provider-neutral JSON Schema data. This type describes
/// a tool but never executes it.
#[derive(Clone, PartialEq)]
pub struct ToolDefinition {
    name: ToolName,
    description: String,
    input_schema: Value,
}

impl ToolDefinition {
    /// Creates a tool definition.
    #[must_use]
    pub fn new(name: ToolName, description: impl Into<String>, input_schema: Value) -> Self {
        Self {
            name,
            description: description.into(),
            input_schema,
        }
    }

    /// Returns the stable tool name.
    #[must_use]
    pub const fn name(&self) -> &ToolName {
        &self.name
    }

    /// Returns the human-readable description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the provider-neutral input schema.
    #[must_use]
    pub const fn input_schema(&self) -> &Value {
        &self.input_schema
    }
}

impl fmt::Debug for ToolDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolDefinition")
            .field("name", &self.name)
            .field("description_bytes", &self.description.len())
            .field("schema_bytes", &self.input_schema.to_string().len())
            .finish()
    }
}

/// A complete tool call produced by a model.
#[derive(Clone, PartialEq)]
pub struct ToolCall {
    id: ToolCallId,
    name: ToolName,
    arguments: Value,
    extensions: Extensions,
}

impl ToolCall {
    /// Creates a complete tool call with structured JSON arguments.
    #[must_use]
    pub fn new(id: ToolCallId, name: ToolName, arguments: Value) -> Self {
        Self {
            id,
            name,
            arguments,
            extensions: Extensions::new(),
        }
    }

    /// Adds provider-neutral continuation metadata.
    #[must_use]
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Returns the stable call identifier.
    #[must_use]
    pub const fn id(&self) -> &ToolCallId {
        &self.id
    }

    /// Returns the called tool name.
    #[must_use]
    pub const fn name(&self) -> &ToolName {
        &self.name
    }

    /// Returns structured call arguments.
    #[must_use]
    pub const fn arguments(&self) -> &Value {
        &self.arguments
    }

    /// Returns provider-neutral continuation metadata.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl fmt::Debug for ToolCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolCall")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("arguments_bytes", &self.arguments.to_string().len())
            .field("extensions", &self.extensions)
            .finish()
    }
}

/// Content returned by application tool execution.
///
/// Tool execution itself is intentionally outside this crate.
#[derive(Clone, Eq, PartialEq)]
pub struct ToolResult {
    content: Vec<ContentPart>,
    is_error: bool,
}

impl fmt::Debug for ToolResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolResult")
            .field("content_parts", &self.content.len())
            .field(
                "text_bytes",
                &self
                    .content
                    .iter()
                    .map(ContentPart::text_len)
                    .sum::<usize>(),
            )
            .field("is_error", &self.is_error)
            .finish()
    }
}

impl ToolResult {
    /// Creates a text result.
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self::new(vec![ContentPart::text(text)], false)
    }

    /// Creates an application-error text result.
    #[must_use]
    pub fn error_text(text: impl Into<String>) -> Self {
        Self::new(vec![ContentPart::text(text)], true)
    }

    /// Creates a result from ordered content parts.
    ///
    /// Empty content and empty text parts are valid.
    #[must_use]
    pub const fn new(content: Vec<ContentPart>, is_error: bool) -> Self {
        Self { content, is_error }
    }

    /// Returns ordered result content.
    #[must_use]
    pub fn content(&self) -> &[ContentPart] {
        &self.content
    }

    /// Returns whether the application classified this as a business error.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        self.is_error
    }
}

/// How a model should select tools for one request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolChoice {
    /// Let the model decide whether to call a tool.
    #[default]
    Auto,
    /// Prevent tool calls.
    None,
    /// Require at least one tool call.
    Required,
    /// Require a specific declared tool.
    Named(ToolName),
}
