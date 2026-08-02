#![allow(dead_code)]

use async_trait::async_trait;
use group_agent_model::{
    AssistantMessage, ChatModel, ChatModelAdapter, ChatResponse, FinishReason, Message,
    ModelCapabilities, ModelError, ModelId, ModelMetadata, ProviderId, ToolCall, ToolCallId,
    ToolDefinition, ToolName, ValidatedChatRequest,
};
use group_agent_tool::{
    Tool, ToolBehavior, ToolError, ToolInput, ToolOutput, ToolRegistry, ToolRuntime,
};
use serde_json::json;

pub enum Script {
    ModelOnly,
    OneToolRound,
}

pub struct ScriptedModel {
    metadata: ModelMetadata,
    script: Script,
}

impl ScriptedModel {
    pub fn model_only() -> Result<ChatModel, Box<dyn std::error::Error>> {
        Self::build(Script::ModelOnly)
    }

    pub fn one_tool_round() -> Result<ChatModel, Box<dyn std::error::Error>> {
        Self::build(Script::OneToolRound)
    }

    fn build(script: Script) -> Result<ChatModel, Box<dyn std::error::Error>> {
        let capabilities = match script {
            Script::ModelOnly => ModelCapabilities::new(),
            Script::OneToolRound => ModelCapabilities::new().with_tool_calling(true),
        };
        Ok(ChatModel::from_adapter(Self {
            metadata: ModelMetadata::new(
                ProviderId::new("offline-script")?,
                ModelId::new("deterministic")?,
                capabilities,
            ),
            script,
        })?)
    }
}

#[async_trait]
impl ChatModelAdapter for ScriptedModel {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    async fn complete_raw(
        &self,
        request: ValidatedChatRequest,
    ) -> Result<ChatResponse, ModelError> {
        let has_tool_message = request
            .messages()
            .iter()
            .any(|message| matches!(message, Message::Tool(_)));
        let message = match self.script {
            Script::ModelOnly => AssistantMessage::text("Offline model-only answer."),
            Script::OneToolRound if has_tool_message => {
                let tool_message = request
                    .messages()
                    .iter()
                    .find_map(Message::as_tool)
                    .expect("the scripted second request contains a ToolMessage");
                assert_eq!(tool_message.tool_call_id().as_str(), "offline-call-1");
                AssistantMessage::text("Offline tool-assisted answer.")
            }
            Script::OneToolRound => AssistantMessage::new(
                Vec::new(),
                vec![ToolCall::new(
                    ToolCallId::new("offline-call-1").expect("static call id is valid"),
                    ToolName::new("lookup_label").expect("static tool name is valid"),
                    json!({"item": "sample"}),
                )],
            ),
        };
        let finish_reason = if message.tool_calls().is_empty() {
            FinishReason::Stop
        } else {
            FinishReason::ToolCalls
        };
        Ok(ChatResponse::new(message, finish_reason))
    }
}

pub struct LookupLabel {
    definition: ToolDefinition,
}

impl LookupLabel {
    fn new() -> Result<Self, group_agent_model::IdentifierError> {
        Ok(Self {
            definition: ToolDefinition::new(
                ToolName::new("lookup_label")?,
                "Looks up an offline label",
                json!({
                    "type": "object",
                    "properties": {"item": {"type": "string"}},
                    "required": ["item"],
                    "additionalProperties": false
                }),
            ),
        })
    }
}

#[async_trait]
impl Tool for LookupLabel {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    async fn execute(&self, _input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        Ok(ToolOutput::success_text("offline-label"))
    }
}

pub fn empty_runtime() -> ToolRuntime {
    ToolRuntime::new(ToolRegistry::empty())
}

pub fn local_runtime() -> Result<ToolRuntime, Box<dyn std::error::Error>> {
    let mut builder = ToolRegistry::builder();
    builder.register(LookupLabel::new()?)?;
    Ok(ToolRuntime::new(builder.build()))
}
