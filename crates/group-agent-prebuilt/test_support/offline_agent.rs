#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use group_agent_model::{
    AssistantMessage, ChatModel, ChatModelAdapter, ChatResponse, FinishReason, Message,
    ModelCapabilities, ModelError, ModelErrorKind, ModelId, ModelMetadata, ProviderId, ToolCall,
    ToolCallId, ToolDefinition, ToolName, ValidatedChatRequest,
};
use group_agent_tool::{
    Tool, ToolBehavior, ToolError, ToolInput, ToolOutput, ToolRegistry, ToolRuntime,
};
use serde_json::json;

#[derive(Clone, Copy)]
pub enum Script {
    ModelOnly,
    OneToolRound,
}

/// Every transcript received by a scripted facade built with
/// [`ScriptedModel::fail_once`], in call order.
#[derive(Clone)]
pub struct RecordedTranscripts {
    requests: Arc<Mutex<Vec<Vec<Message>>>>,
}

impl RecordedTranscripts {
    /// Returns every received transcript in call order.
    pub fn all(&self) -> Vec<Vec<Message>> {
        self.requests.lock().expect("requests lock").clone()
    }
}

pub struct ScriptedModel {
    metadata: ModelMetadata,
    script: Script,
    fail_once_on_call: Option<usize>,
    calls: AtomicUsize,
    requests: Arc<Mutex<Vec<Vec<Message>>>>,
}

impl ScriptedModel {
    pub fn model_only() -> Result<ChatModel, Box<dyn std::error::Error>> {
        Self::build(Script::ModelOnly)
    }

    pub fn one_tool_round() -> Result<ChatModel, Box<dyn std::error::Error>> {
        Self::build(Script::OneToolRound)
    }

    /// Builds a scripted facade that fails exactly once with an
    /// infrastructure-style error on the 1-based `fail_on_call` call and then
    /// follows `script`, alongside a recording of every received transcript.
    pub fn fail_once(
        script: Script,
        fail_on_call: usize,
    ) -> Result<(ChatModel, RecordedTranscripts), Box<dyn std::error::Error>> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let model = Self::facade(script, Some(fail_on_call), Arc::clone(&requests))?;
        Ok((model, RecordedTranscripts { requests }))
    }

    fn build(script: Script) -> Result<ChatModel, Box<dyn std::error::Error>> {
        Self::facade(script, None, Arc::new(Mutex::new(Vec::new())))
    }

    fn facade(
        script: Script,
        fail_once_on_call: Option<usize>,
        requests: Arc<Mutex<Vec<Vec<Message>>>>,
    ) -> Result<ChatModel, Box<dyn std::error::Error>> {
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
            fail_once_on_call,
            calls: AtomicUsize::new(0),
            requests,
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
        let call_number = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.requests
            .lock()
            .expect("requests lock")
            .push(request.messages().to_vec());
        if self.fail_once_on_call == Some(call_number) {
            return Err(ModelError::new(
                ModelErrorKind::Other,
                "scripted one-time infrastructure failure",
            ));
        }
        let has_tool_message = request
            .messages()
            .iter()
            .any(|message| matches!(message, Message::Tool(_)));
        let message = match self.script {
            Script::ModelOnly => AssistantMessage::text("Offline model-only answer."),
            Script::OneToolRound if has_tool_message => {
                let tool_call = request
                    .messages()
                    .iter()
                    .find_map(Message::as_assistant)
                    .and_then(|message| message.tool_calls().first())
                    .expect("the scripted second request contains the original ToolCall");
                assert_eq!(tool_call.id().as_str(), "offline-call-1");
                assert_eq!(tool_call.name().as_str(), "lookup_label");
                assert_eq!(tool_call.arguments(), &json!({"item": "sample"}));

                let tool_message = request
                    .messages()
                    .iter()
                    .find_map(Message::as_tool)
                    .expect("the scripted second request contains a ToolMessage");
                assert_eq!(tool_message.tool_call_id().as_str(), "offline-call-1");
                assert!(!tool_message.result().is_error());
                assert_eq!(tool_message.result().content().len(), 1);
                assert_eq!(
                    tool_message.result().content()[0].as_text(),
                    Some("offline-label")
                );
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
