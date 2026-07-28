use async_trait::async_trait;
use group_agent_tool::{
    Tool, ToolBatchConfig, ToolBehavior, ToolCall, ToolCallId, ToolDefinition, ToolError,
    ToolInput, ToolName, ToolOutput, ToolRegistry, ToolRuntime,
};
use serde_json::json;

struct LocalTool {
    definition: ToolDefinition,
    response: &'static str,
}

#[async_trait]
impl Tool for LocalTool {
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
        let requested = input
            .arguments()
            .get("value")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        Ok(ToolOutput::success_text(format!(
            "{}: {requested}",
            self.response
        )))
    }
}

fn local_tool(name: &str, response: &'static str) -> Result<LocalTool, Box<dyn std::error::Error>> {
    Ok(LocalTool {
        definition: ToolDefinition::new(
            ToolName::new(name)?,
            format!("Offline {name} example"),
            json!({
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
                "additionalProperties": false
            }),
        ),
        response,
    })
}

fn call(id: &str, name: &str, value: &str) -> Result<ToolCall, Box<dyn std::error::Error>> {
    Ok(ToolCall::new(
        ToolCallId::new(id)?,
        ToolName::new(name)?,
        json!({"value": value}),
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = ToolRegistry::builder();
    builder
        .register(local_tool("lookup_label", "label")?)?
        .register(local_tool("lookup_category", "category")?)?;
    let runtime = ToolRuntime::new(builder.build());

    let single = runtime
        .execute_message(&call("single-1", "lookup_label", "alpha")?)
        .await?;
    let single = single.as_tool().expect("execute_message returns Tool");
    println!(
        "single {}: error={}, parts={}",
        single.tool_call_id(),
        single.result().is_error(),
        single.result().content().len()
    );

    let batch = runtime
        .execute_batch(
            vec![
                call("batch-1", "lookup_label", "beta")?,
                call("batch-2", "lookup_category", "gamma")?,
            ],
            ToolBatchConfig::new(2),
        )
        .await?;
    for (index, message) in batch.into_tool_messages().into_iter().enumerate() {
        let message = message?;
        let result = message.as_tool().expect("batch helper returns Tool");
        println!(
            "batch[{index}] {}: error={}, parts={}",
            result.tool_call_id(),
            result.result().is_error(),
            result.result().content().len()
        );
    }
    Ok(())
}
