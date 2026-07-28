use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use group_agent_model::{ToolDefinition, ToolName};
use group_agent_tool::{Tool, ToolBehavior, ToolError, ToolErrorKind, ToolInput, ToolOutput};

use crate::{McpClientSession, McpServerId, map_call_tool_result};

pub(crate) struct McpRemoteTool {
    session: McpClientSession,
    server_id: McpServerId,
    remote_name: Arc<str>,
    definition: ToolDefinition,
    behavior: ToolBehavior,
}

impl McpRemoteTool {
    pub(crate) fn new(
        session: McpClientSession,
        remote_name: impl Into<Arc<str>>,
        definition: ToolDefinition,
        behavior: ToolBehavior,
    ) -> Self {
        let server_id = session.server_id().clone();
        Self {
            session,
            server_id,
            remote_name: remote_name.into(),
            definition,
            behavior,
        }
    }
}

impl fmt::Debug for McpRemoteTool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpRemoteTool")
            .field("server_id", &self.server_id)
            .field("remote_name", &self.remote_name)
            .field("local_name", &self.definition.name())
            .field("behavior", &self.behavior)
            .finish()
    }
}

#[async_trait]
impl Tool for McpRemoteTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        self.behavior
    }

    async fn execute(&self, input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        let result = self
            .session
            .call_tool(&self.remote_name, input.arguments())
            .await
            .map_err(|source| {
                ToolError::with_source(ToolErrorKind::Other, "MCP tool execution failed", source)
            })?;
        map_call_tool_result(result).map_err(|source| {
            ToolError::with_source(ToolErrorKind::Other, "MCP result mapping failed", source)
        })
    }
}
