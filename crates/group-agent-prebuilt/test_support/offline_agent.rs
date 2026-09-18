#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use group_agent_model::{
    AssistantMessage, ChatModel, ChatModelAdapter, ChatResponse, FinishReason, Message,
    ModelCapabilities, ModelError, ModelErrorKind, ModelId, ModelMetadata, ProviderId, ToolCall,
    ToolCallId, ToolDefinition, ToolName, ValidatedChatRequest,
};
use group_agent_tool::{
    Tool, ToolBehavior, ToolError, ToolErrorKind, ToolInput, ToolOutput, ToolRegistry, ToolRuntime,
};
use serde_json::json;

#[derive(Clone, Copy)]
pub enum Script {
    ModelOnly,
    OneToolRound,
    OneToolRoundAnyResult,
    ToolLoop,
}

/// Every transcript received by a scripted facade built with
/// [`ScriptedModel::fail_once`] or [`ScriptedModel::recording`], in call
/// order.
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

    /// Builds a scripted facade that requests the offline Tool once and then
    /// answers with a final message regardless of the ToolMessage result.
    pub fn one_tool_round_any_result() -> Result<ChatModel, Box<dyn std::error::Error>> {
        Self::build(Script::OneToolRoundAnyResult)
    }

    /// Builds a scripted facade that requests the offline Tool on every call,
    /// so the Agent loop runs until its configured round limit.
    pub fn tool_loop() -> Result<ChatModel, Box<dyn std::error::Error>> {
        Self::build(Script::ToolLoop)
    }

    /// Builds a scripted facade following `script`, alongside a recording of
    /// every received transcript.
    pub fn recording(
        script: Script,
    ) -> Result<(ChatModel, RecordedTranscripts), Box<dyn std::error::Error>> {
        Self::recorded(script, None)
    }

    /// Builds a scripted facade that fails exactly once with an
    /// infrastructure-style error on the 1-based `fail_on_call` call and then
    /// follows `script`, alongside a recording of every received transcript.
    pub fn fail_once(
        script: Script,
        fail_on_call: usize,
    ) -> Result<(ChatModel, RecordedTranscripts), Box<dyn std::error::Error>> {
        Self::recorded(script, Some(fail_on_call))
    }

    fn recorded(
        script: Script,
        fail_once_on_call: Option<usize>,
    ) -> Result<(ChatModel, RecordedTranscripts), Box<dyn std::error::Error>> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let model = Self::facade(script, fail_once_on_call, Arc::clone(&requests))?;
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
            Script::OneToolRound | Script::OneToolRoundAnyResult | Script::ToolLoop => {
                ModelCapabilities::new().with_tool_calling(true)
            }
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
            Script::OneToolRoundAnyResult if has_tool_message => {
                AssistantMessage::text("Offline final answer.")
            }
            Script::OneToolRoundAnyResult => AssistantMessage::new(
                Vec::new(),
                vec![ToolCall::new(
                    ToolCallId::new("offline-call-1").expect("static call id is valid"),
                    ToolName::new("lookup_label").expect("static tool name is valid"),
                    json!({"item": "sample"}),
                )],
            ),
            Script::ToolLoop => AssistantMessage::new(
                Vec::new(),
                vec![ToolCall::new(
                    ToolCallId::new(format!("offline-call-{call_number}"))
                        .expect("generated call id is valid"),
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

/// The offline label Tool with one scripted infrastructure failure before it
/// behaves like [`LookupLabel`].
pub struct FlakyLabel {
    inner: LookupLabel,
    fail_once: AtomicBool,
}

impl FlakyLabel {
    fn new() -> Result<Self, group_agent_model::IdentifierError> {
        Ok(Self {
            inner: LookupLabel::new()?,
            fail_once: AtomicBool::new(true),
        })
    }
}

#[async_trait]
impl Tool for FlakyLabel {
    fn name(&self) -> &ToolName {
        self.inner.name()
    }

    fn definition(&self) -> &ToolDefinition {
        self.inner.definition()
    }

    fn behavior(&self) -> ToolBehavior {
        self.inner.behavior()
    }

    async fn execute(&self, input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(ToolError::new(
                ToolErrorKind::Other,
                "scripted one-time tool infrastructure failure",
            ));
        }
        self.inner.execute(input).await
    }
}

/// Shared execution count of a [`CountingLabel`] Tool.
#[derive(Clone)]
pub struct ToolInvocations {
    count: Arc<AtomicUsize>,
}

impl ToolInvocations {
    /// Returns how many times the wrapped Tool has executed.
    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

/// The offline label Tool that counts every execution.
struct CountingLabel {
    inner: LookupLabel,
    count: Arc<AtomicUsize>,
}

impl CountingLabel {
    fn new(count: Arc<AtomicUsize>) -> Result<Self, group_agent_model::IdentifierError> {
        Ok(Self {
            inner: LookupLabel::new()?,
            count,
        })
    }
}

#[async_trait]
impl Tool for CountingLabel {
    fn name(&self) -> &ToolName {
        self.inner.name()
    }

    fn definition(&self) -> &ToolDefinition {
        self.inner.definition()
    }

    fn behavior(&self) -> ToolBehavior {
        self.inner.behavior()
    }

    async fn execute(&self, input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.execute(input).await
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

/// A [`local_runtime`] whose only Tool counts its executions.
pub fn counting_runtime() -> Result<(ToolRuntime, ToolInvocations), Box<dyn std::error::Error>> {
    let count = Arc::new(AtomicUsize::new(0));
    let mut builder = ToolRegistry::builder();
    builder.register(CountingLabel::new(Arc::clone(&count))?)?;
    Ok((ToolRuntime::new(builder.build()), ToolInvocations { count }))
}

/// A [`local_runtime`] whose only Tool fails once with an infrastructure
/// error and then succeeds.
pub fn flaky_local_runtime() -> Result<ToolRuntime, Box<dyn std::error::Error>> {
    let mut builder = ToolRegistry::builder();
    builder.register(FlakyLabel::new()?)?;
    Ok(ToolRuntime::new(builder.build()))
}
