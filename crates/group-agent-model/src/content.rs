/// One provider-neutral part of message content.
///
/// Empty text is valid. Some model protocols use an empty assistant text
/// alongside tool calls, and this crate does not impose provider-specific
/// minimum content lengths.
use std::fmt;

#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ContentPart {
    /// UTF-8 text in message order.
    Text(String),
}

impl ContentPart {
    /// Creates a text content part.
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text(text.into())
    }

    /// Returns text for a text part.
    ///
    /// Future non-text variants return `None`.
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
        }
    }

    pub(crate) fn text_len(&self) -> usize {
        self.as_text().map_or(0, str::len)
    }
}

impl fmt::Debug for ContentPart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(text) => formatter
                .debug_struct("Text")
                .field("bytes", &text.len())
                .field("chars", &text.chars().count())
                .finish(),
        }
    }
}
