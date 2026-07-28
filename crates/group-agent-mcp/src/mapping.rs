use group_agent_model::ContentPart;
use group_agent_tool::ToolOutput;
use rmcp::model::{CallToolResult, ContentBlock};

use crate::{McpAdapterError, McpAdapterErrorKind, error::UnsupportedContentSource};

/// Maps one MCP call result into the existing Tool Runtime output domain.
///
/// Text blocks retain wire order. `structuredContent`, when present, is
/// serialized exactly once and appended as one JSON text part. Image, audio,
/// embedded-resource, resource-link, and future unknown variants fail closed.
pub fn map_call_tool_result(result: CallToolResult) -> Result<ToolOutput, McpAdapterError> {
    let mut content =
        Vec::with_capacity(result.content.len() + usize::from(result.structured_content.is_some()));
    for block in result.content {
        match block {
            ContentBlock::Text(text) => content.push(ContentPart::text(text.text)),
            ContentBlock::Image(_) => return unsupported("image"),
            ContentBlock::Audio(_) => return unsupported("audio"),
            ContentBlock::Resource(_) => return unsupported("embedded-resource"),
            ContentBlock::ResourceLink(_) => return unsupported("resource-link"),
            _ => return unsupported("unknown"),
        }
    }
    if let Some(structured_content) = result.structured_content {
        let text = serde_json::to_string(&structured_content).map_err(|source| {
            McpAdapterError::with_source(McpAdapterErrorKind::Protocol, source)
        })?;
        content.push(ContentPart::text(text));
    }
    Ok(ToolOutput::from_content(
        content,
        result.is_error.unwrap_or(false),
    ))
}

fn unsupported(kind: &'static str) -> Result<ToolOutput, McpAdapterError> {
    Err(McpAdapterError::with_source(
        McpAdapterErrorKind::UnsupportedContent,
        UnsupportedContentSource::new(kind),
    ))
}
