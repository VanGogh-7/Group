use std::fmt;

use crate::{ContentPart, Extensions, ToolCall, ToolCallId, ToolResult};

/// A provider-neutral message role.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum Role {
    /// Model behavior and context instructions.
    System,
    /// End-user input.
    User,
    /// Model output, including optional tool calls.
    Assistant,
    /// A result linked to a prior assistant tool call.
    Tool,
}

/// A system message.
#[derive(Clone, Eq, PartialEq)]
pub struct SystemMessage {
    content: Vec<ContentPart>,
}

impl fmt::Debug for SystemMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        content_debug("SystemMessage", &self.content, formatter)
    }
}

impl SystemMessage {
    /// Creates a system message from ordered content parts.
    #[must_use]
    pub const fn new(content: Vec<ContentPart>) -> Self {
        Self { content }
    }

    /// Returns ordered content.
    #[must_use]
    pub fn content(&self) -> &[ContentPart] {
        &self.content
    }
}

/// A user message.
#[derive(Clone, Eq, PartialEq)]
pub struct UserMessage {
    content: Vec<ContentPart>,
}

impl fmt::Debug for UserMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        content_debug("UserMessage", &self.content, formatter)
    }
}

impl UserMessage {
    /// Creates a user message from ordered content parts.
    #[must_use]
    pub const fn new(content: Vec<ContentPart>) -> Self {
        Self { content }
    }

    /// Returns ordered content.
    #[must_use]
    pub fn content(&self) -> &[ContentPart] {
        &self.content
    }
}

/// An assistant message containing text, tool calls, or both.
#[derive(Clone, PartialEq)]
pub struct AssistantMessage {
    content: Vec<ContentPart>,
    tool_calls: Vec<ToolCall>,
    extensions: Extensions,
}

impl AssistantMessage {
    /// Creates an assistant message.
    ///
    /// Empty content is valid, including for a tool-only assistant turn.
    #[must_use]
    pub const fn new(content: Vec<ContentPart>, tool_calls: Vec<ToolCall>) -> Self {
        Self {
            content,
            tool_calls,
            extensions: Extensions::new(),
        }
    }

    /// Creates a text-only assistant message.
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self::new(vec![ContentPart::text(text)], Vec::new())
    }

    /// Returns ordered assistant content.
    #[must_use]
    pub fn content(&self) -> &[ContentPart] {
        &self.content
    }

    /// Returns tool calls in model-produced order.
    #[must_use]
    pub fn tool_calls(&self) -> &[ToolCall] {
        &self.tool_calls
    }

    /// Adds provider-neutral assistant continuation metadata.
    #[must_use]
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Returns provider-neutral assistant continuation metadata.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Iterates only text parts, in content order.
    pub fn text_parts(&self) -> impl Iterator<Item = &str> {
        self.content.iter().filter_map(ContentPart::as_text)
    }

    /// Returns whether this message has at least one text part.
    #[must_use]
    pub fn has_text(&self) -> bool {
        self.text_parts().next().is_some()
    }

    /// Concatenates text parts without adding separators.
    ///
    /// Future non-text parts are ignored. A message with no text returns an
    /// empty string.
    #[must_use]
    pub fn text_content(&self) -> String {
        let capacity = self.text_parts().map(str::len).sum();
        let mut text = String::with_capacity(capacity);
        for part in self.text_parts() {
            text.push_str(part);
        }
        text
    }
}

impl fmt::Debug for AssistantMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssistantMessage")
            .field("content_parts", &self.content.len())
            .field(
                "text_bytes",
                &self
                    .content
                    .iter()
                    .map(ContentPart::text_len)
                    .sum::<usize>(),
            )
            .field("tool_calls", &self.tool_calls)
            .field("extensions", &self.extensions)
            .finish()
    }
}

/// A tool-result message linked to a prior assistant tool call.
#[derive(Clone, Eq, PartialEq)]
pub struct ToolMessage {
    tool_call_id: ToolCallId,
    result: ToolResult,
}

impl fmt::Debug for ToolMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolMessage")
            .field("tool_call_id", &self.tool_call_id)
            .field("result", &self.result)
            .finish()
    }
}

impl ToolMessage {
    /// Creates a tool-result message.
    #[must_use]
    pub const fn new(tool_call_id: ToolCallId, result: ToolResult) -> Self {
        Self {
            tool_call_id,
            result,
        }
    }

    /// Returns the referenced call identifier.
    #[must_use]
    pub const fn tool_call_id(&self) -> &ToolCallId {
        &self.tool_call_id
    }

    /// Returns the tool result.
    #[must_use]
    pub const fn result(&self) -> &ToolResult {
        &self.result
    }
}

/// A strongly typed provider-neutral chat message.
#[derive(Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    /// System instructions.
    System(SystemMessage),
    /// User input.
    User(UserMessage),
    /// Assistant text and tool calls.
    Assistant(AssistantMessage),
    /// Tool execution result.
    Tool(ToolMessage),
}

impl Message {
    /// Creates a single-part system text message.
    #[must_use]
    pub fn system(text: impl Into<String>) -> Self {
        Self::System(SystemMessage::new(vec![ContentPart::text(text)]))
    }

    /// Creates a single-part user text message.
    #[must_use]
    pub fn user(text: impl Into<String>) -> Self {
        Self::User(UserMessage::new(vec![ContentPart::text(text)]))
    }

    /// Creates a text-only assistant message.
    #[must_use]
    pub fn assistant(text: impl Into<String>) -> Self {
        Self::Assistant(AssistantMessage::text(text))
    }

    /// Creates a tool-result message.
    #[must_use]
    pub const fn tool(tool_call_id: ToolCallId, result: ToolResult) -> Self {
        Self::Tool(ToolMessage::new(tool_call_id, result))
    }

    /// Returns the message role.
    #[must_use]
    pub const fn role(&self) -> Role {
        match self {
            Self::System(_) => Role::System,
            Self::User(_) => Role::User,
            Self::Assistant(_) => Role::Assistant,
            Self::Tool(_) => Role::Tool,
        }
    }

    /// Returns ordered message content.
    #[must_use]
    pub fn content(&self) -> &[ContentPart] {
        match self {
            Self::System(message) => message.content(),
            Self::User(message) => message.content(),
            Self::Assistant(message) => message.content(),
            Self::Tool(message) => message.result().content(),
        }
    }

    /// Iterates only text content in part order.
    pub fn text_parts(&self) -> impl Iterator<Item = &str> {
        self.content().iter().filter_map(ContentPart::as_text)
    }

    /// Returns whether this message contains a text part.
    #[must_use]
    pub fn has_text(&self) -> bool {
        self.text_parts().next().is_some()
    }

    /// Concatenates text parts without adding separators.
    ///
    /// Future non-text parts are ignored. A message with no text returns an
    /// empty string.
    #[must_use]
    pub fn text_content(&self) -> String {
        let capacity = self.text_parts().map(str::len).sum();
        let mut text = String::with_capacity(capacity);
        for part in self.text_parts() {
            text.push_str(part);
        }
        text
    }

    /// Returns the assistant representation when this is an assistant message.
    #[must_use]
    pub const fn as_assistant(&self) -> Option<&AssistantMessage> {
        match self {
            Self::Assistant(message) => Some(message),
            _ => None,
        }
    }

    /// Returns the tool representation when this is a tool message.
    #[must_use]
    pub const fn as_tool(&self) -> Option<&ToolMessage> {
        match self {
            Self::Tool(message) => Some(message),
            _ => None,
        }
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::System(message) => formatter.debug_tuple("System").field(message).finish(),
            Self::User(message) => formatter.debug_tuple("User").field(message).finish(),
            Self::Assistant(message) => formatter.debug_tuple("Assistant").field(message).finish(),
            Self::Tool(message) => formatter.debug_tuple("Tool").field(message).finish(),
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tool_calls = self
            .as_assistant()
            .map_or(0, |message| message.tool_calls().len());
        write!(
            formatter,
            "{:?} message ({} content parts, {tool_calls} tool calls)",
            self.role(),
            self.content().len()
        )
    }
}

fn content_debug(
    name: &str,
    content: &[ContentPart],
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    formatter
        .debug_struct(name)
        .field("content_parts", &content.len())
        .field(
            "text_bytes",
            &content.iter().map(ContentPart::text_len).sum::<usize>(),
        )
        .finish()
}
