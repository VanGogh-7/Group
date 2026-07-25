mod support;

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    END, EventConfig, GraphRunError, GraphState, Node, NodeContext, NodeError, RunConfig,
    RunControl, START, StateError, StateGraph,
};
use group_agent_model::{
    AssistantMessage, ChatModel, ChatModelAdapter, ChatRequest, ChatStreamCollector,
    ChatStreamEvent, Extensions, FinishReason, GenerationConfig, Message, MetadataValidationError,
    ModelCapabilities, ModelCapability, ModelError, ModelErrorKind, ToolCall, ToolCallDelta,
    ToolCallId, ToolChoice, ToolDefinition, ToolName,
};
use serde_json::json;
use support::{PendingControl, ScriptedModel, facade};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn validated_clone_handle_completes_and_captures_exact_request() {
    let scripted = ScriptedModel::fixed(ModelCapabilities::new(), "answer");
    let model = facade(scripted.clone());
    assert_eq!(Arc::strong_count(&scripted), 2);
    let cloned = model.clone();
    assert_eq!(Arc::strong_count(&scripted), 3);
    assert!(std::ptr::eq(model.metadata(), cloned.metadata()));
    assert_eq!(model.metadata(), cloned.metadata());
    let request = ChatRequest::new(vec![Message::system("rules"), Message::user("question")]);

    let original_response = model
        .complete(request.clone())
        .await
        .expect("mock completion");
    let cloned_response = cloned
        .complete(request.clone())
        .await
        .expect("mock completion through clone");

    assert_eq!(original_response.message().text_content(), "answer");
    assert_eq!(cloned_response.message().text_content(), "answer");
    assert_eq!(scripted.call_count(), 2);
    assert_eq!(scripted.captured(), [request.clone(), request]);
    drop(cloned);
    assert_eq!(Arc::strong_count(&scripted), 2);
}

#[tokio::test]
async fn unsupported_streaming_is_structured() {
    let scripted = ScriptedModel::fixed(ModelCapabilities::new(), "answer");
    let model = facade(scripted.clone());
    let error = match model
        .stream(ChatRequest::new(vec![Message::user("question")]))
        .await
    {
        Ok(_) => panic!("streaming should be unsupported"),
        Err(error) => error,
    };

    assert_eq!(
        error.kind(),
        &ModelErrorKind::UnsupportedCapability(ModelCapability::Streaming)
    );
    assert_eq!(scripted.call_count(), 0);
}

#[tokio::test]
async fn facade_rejects_empty_or_invalid_generation_before_raw_dispatch() {
    let scripted = ScriptedModel::fixed(ModelCapabilities::new(), "unused");
    let model = facade(scripted.clone());

    assert!(matches!(
        model.complete(ChatRequest::new(Vec::new())).await,
        Err(error) if matches!(error.kind(), ModelErrorKind::InvalidRequest)
    ));
    assert!(matches!(
        model
            .complete(
                ChatRequest::new(vec![Message::user("question")])
                    .with_generation(GenerationConfig::new().with_top_p(f64::NAN))
            )
            .await,
        Err(error) if matches!(error.kind(), ModelErrorKind::InvalidRequest)
    ));
    assert_eq!(scripted.call_count(), 0);
}

#[tokio::test]
async fn request_validation_precedes_all_capability_checks_and_raw_dispatch() {
    let scripted = ScriptedModel::fixed(ModelCapabilities::new(), "unused");
    let model = facade(scripted.clone());
    let request = ChatRequest::new(Vec::new())
        .with_tools(vec![ToolDefinition::new(
            ToolName::new("lookup").expect("valid tool name"),
            "lookup",
            json!({"type": "object"}),
        )])
        .with_tool_choice(ToolChoice::Required)
        .with_generation(
            GenerationConfig::new()
                .with_top_p(f64::NAN)
                .with_parallel_tool_calls(true),
        );

    let error = match model.stream(request).await {
        Ok(_) => panic!("invalid request must be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), &ModelErrorKind::InvalidRequest);
    assert!(
        error
            .source()
            .is_some_and(|source| source.is::<group_agent_model::RequestValidationError>())
    );
    assert_eq!(scripted.call_count(), 0);
}

#[tokio::test]
async fn common_capability_validation_precedes_streaming_capability() {
    let scripted = ScriptedModel::fixed(ModelCapabilities::new(), "unused");
    let model = facade(scripted.clone());
    let request =
        ChatRequest::new(vec![Message::user("question")]).with_tools(vec![ToolDefinition::new(
            ToolName::new("lookup").expect("valid tool name"),
            "lookup",
            json!({"type": "object"}),
        )]);

    let error = match model.stream(request).await {
        Ok(_) => panic!("tool capability must be checked before streaming"),
        Err(error) => error,
    };

    assert_eq!(
        error.kind(),
        &ModelErrorKind::UnsupportedCapability(ModelCapability::ToolCalling)
    );
    assert_eq!(scripted.call_count(), 0);
}

#[tokio::test]
async fn parallel_tool_calls_require_both_capabilities_before_raw_call() {
    let scripted = ScriptedModel::fixed(ModelCapabilities::new().with_tool_calling(true), "unused");
    let model = facade(scripted.clone());
    let request = ChatRequest::new(vec![Message::user("question")])
        .with_generation(GenerationConfig::new().with_parallel_tool_calls(true));

    let error = model
        .complete(request)
        .await
        .expect_err("parallel capability is absent");

    assert_eq!(
        error.kind(),
        &ModelErrorKind::UnsupportedCapability(ModelCapability::ParallelToolCalls)
    );
    assert_eq!(scripted.call_count(), 0);
}

#[test]
fn contradictory_metadata_is_rejected_at_facade_construction() {
    let scripted = ScriptedModel::fixed(
        ModelCapabilities::new().with_parallel_tool_calls(true),
        "unused",
    );
    let adapter: Arc<dyn ChatModelAdapter> = scripted;
    let error = ChatModel::new(adapter).expect_err("metadata is contradictory");

    assert_eq!(
        error,
        MetadataValidationError::ParallelToolCallsRequireToolCalling
    );
}

#[tokio::test]
async fn tool_request_is_rejected_before_unsupported_model_work() {
    let scripted = ScriptedModel::fixed(ModelCapabilities::new(), "unused");
    let tool = ToolDefinition::new(
        ToolName::new("lookup").expect("valid tool name"),
        "lookup",
        json!({"type": "object"}),
    );
    let model = facade(scripted.clone());
    let error = model
        .complete(
            ChatRequest::new(vec![Message::user("question")])
                .with_tools(vec![tool])
                .with_tool_choice(ToolChoice::Required),
        )
        .await
        .expect_err("tool capability is absent");

    assert_eq!(
        error.kind(),
        &ModelErrorKind::UnsupportedCapability(ModelCapability::ToolCalling)
    );
    assert_eq!(scripted.call_count(), 0);
}

#[tokio::test]
async fn tool_history_requires_capability_and_preserves_continuation_metadata() {
    let continuation = Extensions::new()
        .with("provider.continuation", json!({"opaque": 7}))
        .expect("valid extension");
    let call = ToolCall::new(
        ToolCallId::new("call-1").expect("valid call id"),
        ToolName::new("lookup").expect("valid tool name"),
        json!({"query": "rust"}),
    )
    .with_extensions(continuation.clone());
    let request = ChatRequest::new(vec![
        Message::user("question"),
        Message::Assistant(
            AssistantMessage::new(Vec::new(), vec![call]).with_extensions(continuation),
        ),
    ]);

    let unsupported = ScriptedModel::fixed(ModelCapabilities::new(), "unused");
    let error = facade(unsupported.clone())
        .complete(request.clone())
        .await
        .expect_err("tool history requires capability");
    assert_eq!(
        error.kind(),
        &ModelErrorKind::UnsupportedCapability(ModelCapability::ToolCalling)
    );
    assert_eq!(unsupported.call_count(), 0);

    let supported =
        ScriptedModel::fixed(ModelCapabilities::new().with_tool_calling(true), "answer");
    facade(supported.clone())
        .complete(request.clone())
        .await
        .expect("supported tool history");
    assert_eq!(supported.captured(), [request]);
}

#[tokio::test]
async fn streamed_tool_continuation_metadata_reaches_next_raw_adapter_request() {
    let continuation = Extensions::try_from_iter([
        ("provider.continuation.a", json!({"opaque": 1})),
        ("provider.continuation.z", json!({"opaque": 2})),
    ])
    .expect("valid continuation metadata");
    let mut collector = ChatStreamCollector::new();
    collector
        .push(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_id(ToolCallId::new("call-1").expect("valid call id"))
                .with_extensions(continuation.clone()),
        ))
        .expect("first fragment");
    collector
        .push(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_name(ToolName::new("lookup").expect("valid tool name"))
                .with_arguments_fragment("{\"query\":\"rust\"}"),
        ))
        .expect("second fragment");
    collector
        .push(ChatStreamEvent::Finished(FinishReason::ToolCalls))
        .expect("complete tool call");
    let streamed = collector.finish().expect("valid streamed response");

    let request = ChatRequest::new(vec![
        Message::user("question"),
        Message::Assistant(streamed.message().clone()),
    ]);
    let scripted = ScriptedModel::fixed(ModelCapabilities::new().with_tool_calling(true), "answer");
    facade(scripted.clone())
        .complete(request)
        .await
        .expect("continuation request passes facade");

    let captured = scripted.captured();
    let raw_request = &captured[0];
    assert!(raw_request.extensions().is_empty());
    let assistant = raw_request.messages()[1]
        .as_assistant()
        .expect("assistant continuation");
    assert!(assistant.extensions().is_empty());
    let tool_extensions = assistant.tool_calls()[0].extensions();
    assert_eq!(tool_extensions, &continuation);
    assert_eq!(
        tool_extensions.keys().collect::<Vec<_>>(),
        ["provider.continuation.a", "provider.continuation.z"]
    );
}

#[tokio::test]
async fn concurrent_complete_calls_remain_isolated() {
    let scripted = ScriptedModel::fixed(ModelCapabilities::new(), "answer");
    let model = facade(scripted.clone());
    let first = model.complete(ChatRequest::new(vec![Message::user("first")]));
    let second = model.complete(ChatRequest::new(vec![Message::user("second")]));
    let (first, second) = tokio::join!(first, second);

    assert_eq!(
        first.expect("first response").message().text_content(),
        "answer"
    );
    assert_eq!(
        second.expect("second response").message().text_content(),
        "answer"
    );
    let captured = scripted.captured();
    assert_eq!(captured.len(), 2);
    assert_ne!(captured[0], captured[1]);
}

#[tokio::test]
async fn supported_stream_initializes_and_returns_items() {
    let scripted = ScriptedModel::with_stream(
        ModelCapabilities::new().with_streaming(true),
        vec![
            Ok(ChatStreamEvent::TextDelta("ok".to_owned())),
            Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
        ],
    );
    let model = facade(scripted);
    let stream = model
        .stream(ChatRequest::new(vec![Message::user("question")]))
        .await
        .expect("stream starts");
    let response = group_agent_model::collect_chat_stream(stream)
        .await
        .expect("stream aggregates");

    assert_eq!(response.message().text_content(), "ok");
}

#[derive(Debug)]
struct ProviderRoot;

impl fmt::Display for ProviderRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("provider root")
    }
}

impl StdError for ProviderRoot {}

#[test]
fn model_error_source_chain_reaches_concrete_root() {
    let error = ModelError::with_source(
        ModelErrorKind::ProviderUnavailable,
        "provider unavailable",
        ProviderRoot,
    );

    assert!(
        error
            .source()
            .is_some_and(|source| source.is::<ProviderRoot>())
    );
    assert!(error.is_retryable());
}

#[derive(Debug)]
struct AgentState {
    prompt: String,
    answer: Option<String>,
}

struct AgentUpdate(String);

impl GraphState for AgentState {
    type Update = AgentUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.answer = Some(update.0);
        Ok(())
    }
}

struct ModelNode {
    model: ChatModel,
}

#[async_trait]
impl Node<AgentState> for ModelNode {
    async fn run(
        &self,
        state: &AgentState,
        _context: &NodeContext,
    ) -> Result<AgentUpdate, NodeError> {
        let response = self
            .model
            .complete(ChatRequest::new(vec![Message::user(&state.prompt)]))
            .await
            .map_err(|source| NodeError::with_source("model call failed", source))?;
        Ok(AgentUpdate(response.message().text_content()))
    }
}

fn model_graph(model: ChatModel) -> group_agent_core::CompiledGraph<AgentState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("model", ModelNode { model })
        .expect("node registers");
    graph.add_edge(START, "model").add_edge("model", END);
    graph.compile().expect("graph compiles")
}

fn initial_state() -> AgentState {
    AgentState {
        prompt: "question".to_owned(),
        answer: None,
    }
}

#[tokio::test]
async fn chat_model_runs_as_an_ordinary_group_node() {
    let model = facade(ScriptedModel::fixed(
        ModelCapabilities::new(),
        "graph answer",
    ));
    let report = model_graph(model)
        .invoke(initial_state())
        .await
        .expect("graph completes");

    assert_eq!(report.final_state().answer.as_deref(), Some("graph answer"));
}

#[tokio::test]
async fn graph_error_chain_preserves_model_and_provider_sources() {
    let model = facade(ScriptedModel::error(ModelError::with_source(
        ModelErrorKind::ProviderUnavailable,
        "provider unavailable",
        ProviderRoot,
    )));
    let error = model_graph(model)
        .invoke(initial_state())
        .await
        .expect_err("node fails");

    assert!(matches!(error, GraphRunError::NodeFailed { .. }));
    let node_error = error.source().expect("node error");
    let model_error = node_error.source().expect("model error");
    let provider = model_error.source().expect("provider source");
    assert!(provider.is::<ProviderRoot>());
}

#[tokio::test]
async fn group_cancellation_drops_pending_model_future() {
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let model = facade(ScriptedModel::pending(PendingControl {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
    }));
    let graph = model_graph(model);
    let token = CancellationToken::new();
    let run_token = token.clone();
    let task = tokio::spawn(async move {
        graph
            .invoke_with_control(
                initial_state(),
                RunConfig::default(),
                EventConfig::default(),
                RunControl::new().with_cancellation_token(run_token),
            )
            .await
    });

    started.notified().await;
    token.cancel();
    let error = task
        .await
        .expect("run task does not panic")
        .expect_err("run is cancelled");

    assert!(matches!(error, GraphRunError::Cancelled { .. }));
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn group_node_timeout_drops_pending_model_future_without_model_token() {
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let model = facade(ScriptedModel::pending(PendingControl {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
    }));
    let graph = model_graph(model);
    let task = tokio::spawn(async move {
        graph
            .invoke_with_control(
                initial_state(),
                RunConfig::default(),
                EventConfig::default(),
                RunControl::new().with_node_timeout(Duration::from_secs(1)),
            )
            .await
    });

    started.notified().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let error = task
        .await
        .expect("run task does not panic")
        .expect_err("node times out");

    assert!(matches!(error, GraphRunError::NodeTimedOut { .. }));
    assert!(dropped.load(Ordering::SeqCst));
}
