use async_trait::async_trait;
use group_agent_core::{
    END, GraphState, Node, NodeContext, NodeError, START, StateError, StateGraph,
};
use group_agent_model::Message;
use group_agent_tool::{
    Tool, ToolBehavior, ToolCall, ToolCallId, ToolDefinition, ToolError, ToolInput, ToolName,
    ToolOutput, ToolRegistry, ToolRuntime,
};
use serde_json::json;

struct GraphStateWithTool {
    pending_call: ToolCall,
    tool_message: Option<Message>,
}

struct ToolUpdate {
    message: Message,
}

impl GraphState for GraphStateWithTool {
    type Update = ToolUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.tool_message = Some(update.message);
        Ok(())
    }
}

struct LocalLookup {
    definition: ToolDefinition,
    prefix: &'static str,
}

#[async_trait]
impl Tool for LocalLookup {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    async fn execute(&self, input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        let value = input
            .arguments()
            .get("value")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        Ok(ToolOutput::success_text(format!(
            "{}: {value}",
            self.prefix
        )))
    }
}

struct ToolNode {
    runtime: ToolRuntime,
}

#[async_trait]
impl Node<GraphStateWithTool> for ToolNode {
    async fn run(
        &self,
        state: &GraphStateWithTool,
        _context: &NodeContext,
    ) -> Result<ToolUpdate, NodeError> {
        let message = self
            .runtime
            .execute_message(&state.pending_call)
            .await
            .map_err(|source| NodeError::with_source("local tool execution failed", source))?;
        Ok(ToolUpdate { message })
    }
}

fn lookup(name: &str, prefix: &'static str) -> Result<LocalLookup, Box<dyn std::error::Error>> {
    Ok(LocalLookup {
        definition: ToolDefinition::new(
            ToolName::new(name)?,
            format!("Offline {name} lookup"),
            json!({
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
                "additionalProperties": false
            }),
        ),
        prefix,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = ToolRegistry::builder();
    registry
        .register(lookup("lookup_label", "label")?)?
        .register(lookup("lookup_category", "category")?)?;
    let runtime = ToolRuntime::new(registry.build());

    let mut graph = StateGraph::new();
    graph.add_node("tool", ToolNode { runtime })?;
    graph.add_edge(START, "tool").add_edge("tool", END);

    // Group owns cancellation and node timeouts. Dropping this node future
    // drops ToolRuntime and its in-flight Tool future without a detached task.
    let report = graph
        .compile()?
        .invoke(GraphStateWithTool {
            pending_call: ToolCall::new(
                ToolCallId::new("node-call-1")?,
                ToolName::new("lookup_category")?,
                json!({"value": "example"}),
            ),
            tool_message: None,
        })
        .await?;
    let message = report
        .final_state()
        .tool_message
        .as_ref()
        .expect("tool node stores a paired Tool message");
    let result = message.as_tool().expect("execute_message returns Tool");
    println!(
        "node result {}: error={}, parts={}",
        result.tool_call_id(),
        result.result().is_error(),
        result.result().content().len()
    );
    Ok(())
}
