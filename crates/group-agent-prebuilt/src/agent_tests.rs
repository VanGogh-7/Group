use std::collections::VecDeque;
use std::error::Error as StdError;
use std::fmt;
use std::future::{Future, pending, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use group_agent_core::{
    EventConfig, EventRetention, EventSink, GraphBuildError, GraphEvent, GraphRunError, NodeError,
    NodeId, NodePath, RunControl, RunFailure,
};
use group_agent_model::{
    AssistantMessage, ChatModel, ChatModelAdapter, ChatRequest, ChatResponse, FinishReason,
    Message, ModelCapabilities, ModelError, ModelErrorKind, ModelId, ModelMetadata, ProviderId,
    RequestValidationError, TokenUsage, ToolCall, ToolCallId, ToolChoice, ToolDefinition, ToolName,
    ValidatedChatRequest,
};
use group_agent_tool::{
    Tool, ToolBatchError, ToolBehavior, ToolError, ToolErrorKind, ToolEvent, ToolInput,
    ToolObserverError, ToolOutput, ToolRegistry, ToolRuntime, ToolRuntimeError,
    ToolRuntimeErrorKind,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{AgentBuildError, AgentConfig, AgentError, AgentStopReason, ToolCallingAgent};

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

struct ScriptedAdapter {
    metadata: ModelMetadata,
    responses: Mutex<VecDeque<Result<ChatResponse, ModelError>>>,
    requests: Mutex<Vec<ChatRequest>>,
    calls: AtomicUsize,
}

impl ScriptedAdapter {
    fn new(
        capabilities: ModelCapabilities,
        responses: Vec<Result<ChatResponse, ModelError>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            metadata: ModelMetadata::new(
                ProviderId::new("offline").expect("valid provider"),
                ModelId::new("scripted").expect("valid model"),
                capabilities,
            ),
            responses: Mutex::new(responses.into()),
            requests: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
        })
    }

    fn facade(self: &Arc<Self>) -> ChatModel {
        let adapter: Arc<dyn ChatModelAdapter> = self.clone();
        ChatModel::new(adapter).expect("scripted metadata is valid")
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn requests(&self) -> Vec<ChatRequest> {
        self.requests.lock().expect("requests lock").clone()
    }
}

impl ChatModelAdapter for ScriptedAdapter {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    fn complete_raw<'life0, 'async_trait>(
        &'life0 self,
        request: ValidatedChatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, ModelError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests
                .lock()
                .expect("requests lock")
                .push(request.into_inner());
            self.responses
                .lock()
                .expect("responses lock")
                .pop_front()
                .expect("one scripted response per raw call")
        })
    }
}

struct ConcurrentAdapter {
    metadata: ModelMetadata,
    calls: AtomicUsize,
    pending_probe: Arc<PendingProbe>,
}

impl ConcurrentAdapter {
    fn new(pending_probe: Arc<PendingProbe>) -> Arc<Self> {
        Arc::new(Self {
            metadata: ModelMetadata::new(
                ProviderId::new("offline").expect("valid provider"),
                ModelId::new("concurrent").expect("valid model"),
                ModelCapabilities::new(),
            ),
            calls: AtomicUsize::new(0),
            pending_probe,
        })
    }

    fn facade(self: &Arc<Self>) -> ChatModel {
        let adapter: Arc<dyn ChatModelAdapter> = self.clone();
        ChatModel::new(adapter).expect("concurrent metadata is valid")
    }
}

impl ChatModelAdapter for ConcurrentAdapter {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    fn complete_raw<'life0, 'async_trait>(
        &'life0 self,
        request: ValidatedChatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, ModelError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let caller_message = request
                .messages()
                .first()
                .expect("one caller message")
                .text_content();
            if caller_message == "cancel this invocation" {
                let _guard = PendingDropGuard {
                    probe: Arc::clone(&self.pending_probe),
                };
                self.pending_probe.started.notify_one();
                return pending::<Result<ChatResponse, ModelError>>().await;
            }
            Ok(ChatResponse::new(
                AssistantMessage::text(format!("answer for {caller_message}")),
                FinishReason::Stop,
            ))
        })
    }
}

#[derive(Default)]
struct PendingProbe {
    started: Notify,
    dropped: AtomicUsize,
}

impl PendingProbe {
    async fn wait_started(&self) {
        self.started.notified().await;
    }

    fn dropped_count(&self) -> usize {
        self.dropped.load(Ordering::SeqCst)
    }
}

struct PendingDropGuard {
    probe: Arc<PendingProbe>,
}

impl Drop for PendingDropGuard {
    fn drop(&mut self) {
        self.probe.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingAdapter {
    metadata: ModelMetadata,
    responses: Mutex<VecDeque<ChatResponse>>,
    requests: Mutex<Vec<ChatRequest>>,
    calls: AtomicUsize,
    probe: Arc<PendingProbe>,
}

impl PendingAdapter {
    fn new(responses: Vec<ChatResponse>, probe: Arc<PendingProbe>) -> Arc<Self> {
        Arc::new(Self {
            metadata: ModelMetadata::new(
                ProviderId::new("offline").expect("valid provider"),
                ModelId::new("pending").expect("valid model"),
                ModelCapabilities::new().with_tool_calling(true),
            ),
            responses: Mutex::new(responses.into()),
            requests: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            probe,
        })
    }

    fn facade(self: &Arc<Self>) -> ChatModel {
        let adapter: Arc<dyn ChatModelAdapter> = self.clone();
        ChatModel::new(adapter).expect("pending metadata is valid")
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ChatModelAdapter for PendingAdapter {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    fn complete_raw<'life0, 'async_trait>(
        &'life0 self,
        request: ValidatedChatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, ModelError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests
                .lock()
                .expect("requests lock")
                .push(request.into_inner());
            if let Some(response) = self.responses.lock().expect("responses lock").pop_front() {
                return Ok(response);
            }

            let _guard = PendingDropGuard {
                probe: Arc::clone(&self.probe),
            };
            self.probe.started.notify_one();
            pending::<Result<ChatResponse, ModelError>>().await
        })
    }
}

struct PendingTool {
    definition: ToolDefinition,
    executions: Arc<AtomicUsize>,
    probe: Arc<PendingProbe>,
}

impl Tool for PendingTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _input: ToolInput<'life1>,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput, ToolError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.executions.fetch_add(1, Ordering::SeqCst);
            let _guard = PendingDropGuard {
                probe: Arc::clone(&self.probe),
            };
            self.probe.started.notify_one();
            pending::<Result<ToolOutput, ToolError>>().await
        })
    }
}

#[derive(Default)]
struct RecordingEventSink {
    events: Mutex<Vec<GraphEvent>>,
}

impl RecordingEventSink {
    fn snapshot(&self) -> Vec<GraphEvent> {
        self.events.lock().expect("event sink lock").clone()
    }
}

impl EventSink for RecordingEventSink {
    fn on_event(&self, event: &GraphEvent) {
        self.events
            .lock()
            .expect("event sink lock")
            .push(event.clone());
    }
}

fn event_config(sink: &Arc<RecordingEventSink>) -> EventConfig {
    let sink: Arc<dyn EventSink> = sink.clone();
    EventConfig::new(EventRetention::None).with_sink(sink)
}

fn graph_error(error: &AgentError) -> &GraphRunError {
    error
        .source()
        .and_then(|source| source.downcast_ref::<GraphRunError>())
        .expect("AgentError directly retains GraphRunError")
}

fn state_update_count(events: &[GraphEvent]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, GraphEvent::StateUpdated { .. }))
        .count()
}

fn event_count(events: &[GraphEvent], predicate: impl Fn(&GraphEvent) -> bool) -> usize {
    events.iter().filter(|event| predicate(event)).count()
}

fn node_started_metadata(events: &[GraphEvent]) -> Vec<(NodePath, usize)> {
    events
        .iter()
        .filter_map(|event| match event {
            GraphEvent::NodeStarted { node_id, step, .. } => Some((node_id.clone(), *step)),
            _ => None,
        })
        .collect()
}

fn node_completed_metadata(events: &[GraphEvent]) -> Vec<(NodePath, usize)> {
    events
        .iter()
        .filter_map(|event| match event {
            GraphEvent::NodeCompleted { node_id, step, .. } => Some((node_id.clone(), *step)),
            _ => None,
        })
        .collect()
}

fn assert_failed_lifecycle(events: &[GraphEvent], expected: &RunFailure) {
    assert_eq!(
        event_count(events, |event| matches!(
            event,
            GraphEvent::RunStarted { .. }
        )),
        1
    );
    assert_eq!(
        event_count(events, |event| matches!(
            event,
            GraphEvent::RunCompleted { .. }
        )),
        0
    );
    let failures = events
        .iter()
        .filter_map(|event| match event {
            GraphEvent::RunFailed { failure, .. } => Some(failure),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(failures, [expected]);
}

fn assert_events_redacted(events: &[GraphEvent], markers: &[&str]) {
    for event in events {
        let formatted = format!("{event:?}");
        for marker in markers {
            assert!(!formatted.contains(marker));
        }
    }
}

fn assert_error_formats_redacted(error: &AgentError, markers: &[&str]) {
    for formatted in [
        format!("{error}"),
        format!("{error:?}"),
        format!("{}", graph_error(error)),
        format!("{:?}", graph_error(error)),
    ] {
        for marker in markers {
            assert!(!formatted.contains(marker));
        }
    }
}

struct CountingTool {
    definition: ToolDefinition,
    executions: Arc<AtomicUsize>,
}

#[derive(Clone, Copy)]
enum TestToolOutcome {
    Success(&'static str),
    BusinessError(&'static str),
    InfrastructureError(&'static str),
}

struct TestTool {
    definition: ToolDefinition,
    executions: Arc<AtomicUsize>,
    outcome: TestToolOutcome,
    pending_polls: usize,
    completion_order: Option<Arc<Mutex<Vec<String>>>>,
}

impl TestTool {
    fn new(name: &str, executions: Arc<AtomicUsize>, outcome: TestToolOutcome) -> Self {
        Self {
            definition: definition(name),
            executions,
            outcome,
            pending_polls: 0,
            completion_order: None,
        }
    }

    fn with_completion_probe(
        mut self,
        pending_polls: usize,
        completion_order: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        self.pending_polls = pending_polls;
        self.completion_order = Some(completion_order);
        self
    }
}

impl Tool for TestTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _input: ToolInput<'life1>,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput, ToolError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let mut pending_polls = self.pending_polls;
        let completion_order = self.completion_order.clone();
        let tool_name = self.name().as_str().to_owned();
        let outcome = self.outcome;
        Box::pin(poll_fn(move |context| {
            if pending_polls > 0 {
                pending_polls -= 1;
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            if let Some(order) = &completion_order {
                order
                    .lock()
                    .expect("completion order lock")
                    .push(tool_name.clone());
            }
            Poll::Ready(match outcome {
                TestToolOutcome::Success(text) => Ok(ToolOutput::success_text(text)),
                TestToolOutcome::BusinessError(text) => Ok(ToolOutput::business_error_text(text)),
                TestToolOutcome::InfrastructureError(message) => Err(ToolError::with_source(
                    ToolErrorKind::Other,
                    message,
                    SecretToolRoot,
                )),
            })
        }))
    }
}

#[derive(Debug)]
struct SecretToolRoot;

impl fmt::Display for SecretToolRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SECRET_TOOL_ROOT_SOURCE")
    }
}

impl StdError for SecretToolRoot {}

impl Tool for CountingTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _input: ToolInput<'life1>,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput, ToolError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.executions.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutput::success_text("not expected in Slice 2"))
        })
    }
}

fn definition(name: &str) -> ToolDefinition {
    ToolDefinition::new(
        ToolName::new(name).expect("valid tool name"),
        format!("{name} test tool"),
        "{\"type\":\"object\"}"
            .parse()
            .expect("valid JSON Schema value"),
    )
}

fn nonempty_runtime(executions: Arc<AtomicUsize>) -> ToolRuntime {
    let mut builder = ToolRegistry::builder();
    builder
        .register(CountingTool {
            definition: definition("zeta"),
            executions: Arc::clone(&executions),
        })
        .expect("zeta registers")
        .register(CountingTool {
            definition: definition("alpha"),
            executions,
        })
        .expect("alpha registers");
    ToolRuntime::new(builder.build())
}

fn agent(model: ChatModel, tools: ToolRuntime) -> ToolCallingAgent {
    agent_with_rounds(model, tools, 1)
}

fn agent_with_rounds(model: ChatModel, tools: ToolRuntime, max_rounds: usize) -> ToolCallingAgent {
    ToolCallingAgent::new(
        model,
        tools,
        AgentConfig::new(max_rounds).expect("valid round config"),
    )
    .expect("fixed model graph builds")
}

fn call(id: &str, name: &str, arguments: &str) -> ToolCall {
    ToolCall::new(
        ToolCallId::new(id).expect("valid call id"),
        ToolName::new(name).expect("valid tool name"),
        arguments.parse().expect("valid arguments"),
    )
}

fn tool_response(calls: Vec<ToolCall>) -> ChatResponse {
    ChatResponse::new(
        AssistantMessage::new(Vec::new(), calls),
        FinishReason::ToolCalls,
    )
}

fn runtime_with_test_tools(tools: Vec<TestTool>) -> ToolRuntime {
    let mut builder = ToolRegistry::builder();
    for tool in tools {
        builder.register(tool).expect("test tool registers");
    }
    ToolRuntime::new(builder.build())
}

fn runtime_with_pending_tool(
    name: &str,
    executions: Arc<AtomicUsize>,
    probe: Arc<PendingProbe>,
) -> ToolRuntime {
    let mut builder = ToolRegistry::builder();
    builder
        .register(PendingTool {
            definition: definition(name),
            executions,
            probe,
        })
        .expect("pending Tool registers");
    ToolRuntime::new(builder.build())
}

fn tool_result_text(result: &group_agent_model::ToolResult) -> String {
    result
        .content()
        .iter()
        .filter_map(group_agent_model::ContentPart::as_text)
        .collect()
}

#[test]
fn empty_registry_builds_explicit_none_request_and_final_answer() {
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new(),
        vec![Ok(ChatResponse::new(
            AssistantMessage::text("answer"),
            FinishReason::Stop,
        ))],
    );
    let agent = agent(adapter.facade(), ToolRuntime::new(ToolRegistry::empty()));
    let user = Message::user("question");

    let outcome = block_on(agent.invoke(vec![user.clone()])).expect("model-only final answer");

    assert_eq!(adapter.call_count(), 1);
    let requests = adapter.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].messages(), std::slice::from_ref(&user));
    assert!(requests[0].tools().is_empty());
    assert_eq!(requests[0].tool_choice(), &ToolChoice::None);
    assert_eq!(outcome.messages().len(), 2);
    assert_eq!(outcome.messages()[0], user);
    assert_eq!(outcome.messages()[1].text_content(), "answer");
    assert_eq!(outcome.model_rounds(), 1);
    assert_eq!(outcome.usage_by_round(), [None]);
    assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
    assert_eq!(
        outcome
            .final_message()
            .expect("final assistant message")
            .text_content(),
        "answer"
    );
    assert!(std::ptr::eq(
        outcome.final_message().expect("derived final message"),
        outcome.messages()[1]
            .as_assistant()
            .expect("canonical assistant turn"),
    ));
}

#[test]
fn nonempty_registry_uses_lexical_definitions_and_auto_without_tool_execution() {
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = nonempty_runtime(Arc::clone(&executions));
    let expected_definitions = runtime
        .registry()
        .definitions()
        .cloned()
        .collect::<Vec<_>>();
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(ChatResponse::new(
            AssistantMessage::text("no tool needed"),
            FinishReason::ToolCalls,
        ))],
    );
    let agent = agent(adapter.facade(), runtime);

    let outcome = block_on(agent.invoke(vec![Message::user("question")]))
        .expect("actual ToolCalls, not finish reason, control completion");

    assert_eq!(adapter.call_count(), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    let requests = adapter.requests();
    assert_eq!(requests[0].tools(), expected_definitions);
    assert_eq!(
        requests[0]
            .tools()
            .iter()
            .map(|tool| tool.name().as_str())
            .collect::<Vec<_>>(),
        ["alpha", "zeta"]
    );
    assert_eq!(requests[0].tool_choice(), &ToolChoice::Auto);
    assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
}

#[test]
fn real_graph_compile_occurs_once_and_two_invocations_are_isolated() {
    let usage = TokenUsage::from_parts(Some(3), Some(5), Some(8)).expect("valid usage");
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "reuse",
        Arc::clone(&executions),
        TestToolOutcome::Success("first tool result"),
    )]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![
            Ok(tool_response(vec![call("reuse-call", "reuse", "{}")])),
            Ok(
                ChatResponse::new(AssistantMessage::text("first answer"), FinishReason::Stop)
                    .with_usage(usage.clone()),
            ),
            Ok(ChatResponse::new(
                AssistantMessage::text("second answer"),
                FinishReason::Stop,
            )),
        ],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 2);
    assert_eq!(agent.observed_graph_compiles(), 1);

    let first =
        block_on(agent.invoke(vec![Message::user("first question")])).expect("first invocation");
    let second =
        block_on(agent.invoke(vec![Message::user("second question")])).expect("second invocation");

    assert_eq!(agent.observed_graph_compiles(), 1);
    assert_eq!(adapter.call_count(), 3);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(first.model_rounds(), 2);
    assert_eq!(second.model_rounds(), 1);
    assert_eq!(first.usage_by_round(), [None, Some(usage)]);
    assert_eq!(second.usage_by_round(), [None]);
    assert_eq!(first.messages().len(), 4);
    assert_eq!(second.messages().len(), 2);
    assert_eq!(first.messages()[0].text_content(), "first question");
    assert_eq!(second.messages()[0].text_content(), "second question");
    assert_eq!(first.messages()[3].text_content(), "first answer");
    assert_eq!(second.messages()[1].text_content(), "second answer");
}

#[test]
fn invalid_initial_transcript_fails_before_raw_dispatch_with_full_chain() {
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new(),
        vec![Ok(ChatResponse::new(
            AssistantMessage::text("unused"),
            FinishReason::Stop,
        ))],
    );
    let agent = agent(adapter.facade(), ToolRuntime::new(ToolRegistry::empty()));

    let error = block_on(agent.invoke(Vec::new())).expect_err("empty transcript fails");

    assert_eq!(adapter.call_count(), 0);
    let graph = error
        .source()
        .and_then(|source| source.downcast_ref::<GraphRunError>())
        .expect("AgentError -> GraphRunError");
    assert!(matches!(graph, GraphRunError::NodeFailed { .. }));
    let node = graph
        .source()
        .and_then(|source| source.downcast_ref::<NodeError>())
        .expect("GraphRunError -> NodeError");
    let model = node
        .source()
        .and_then(|source| source.downcast_ref::<ModelError>())
        .expect("NodeError -> ModelError");
    assert!(
        model
            .source()
            .is_some_and(|source| source.is::<RequestValidationError>())
    );
}

#[derive(Debug)]
struct ProviderRoot;

impl fmt::Display for ProviderRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SECRET_PROVIDER_ROOT")
    }
}

impl StdError for ProviderRoot {}

#[test]
fn model_failure_has_no_retry_and_preserves_redacted_concrete_chain() {
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new(),
        vec![Err(ModelError::with_source(
            ModelErrorKind::ProviderUnavailable,
            "SECRET_MODEL_MESSAGE",
            ProviderRoot,
        ))],
    );
    let agent = agent(adapter.facade(), ToolRuntime::new(ToolRegistry::empty()));
    let sink = Arc::new(RecordingEventSink::default());

    let error = block_on(agent.invoke_with_control(
        vec![Message::user("SECRET_PROMPT")],
        event_config(&sink),
        RunControl::default(),
    ))
    .expect_err("model failure stops the graph");

    assert_eq!(adapter.call_count(), 1);
    assert_eq!(adapter.requests().len(), 1);
    let graph = error
        .source()
        .and_then(|source| source.downcast_ref::<GraphRunError>())
        .expect("AgentError -> GraphRunError");
    let node = graph.source().expect("GraphRunError -> NodeError");
    let model = node
        .source()
        .and_then(|source| source.downcast_ref::<ModelError>())
        .expect("NodeError -> ModelError");
    assert!(
        model
            .source()
            .is_some_and(|source| source.is::<ProviderRoot>())
    );
    let events = sink.snapshot();
    assert_eq!(
        node_started_metadata(&events),
        [(NodeId::from("model").into(), 1)]
    );
    assert!(node_completed_metadata(&events).is_empty());
    assert_failed_lifecycle(
        &events,
        &RunFailure::NodeFailed {
            node_id: NodeId::from("model").into(),
            step: 1,
        },
    );
    let markers = [
        "SECRET_PROMPT",
        "SECRET_MODEL_MESSAGE",
        "SECRET_PROVIDER_ROOT",
    ];
    assert_events_redacted(&events, &markers);
    assert_error_formats_redacted(&error, &markers);
}

#[test]
fn single_tool_call_pairs_original_id_and_stops_at_max_rounds() {
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "single",
        Arc::clone(&executions),
        TestToolOutcome::Success("single result"),
    )]);
    let tool_call = call("call-1", "single", r#"{"value":"SECRET_ARGUMENT"}"#);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(tool_response(vec![tool_call.clone()]))],
    );
    let agent = agent(adapter.facade(), runtime);

    let outcome = block_on(agent.invoke(vec![Message::user("question")]))
        .expect("one final-round Tool batch succeeds");

    assert_eq!(adapter.call_count(), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.model_rounds(), 1);
    assert_eq!(outcome.usage_by_round(), [None]);
    assert_eq!(outcome.stop_reason(), AgentStopReason::MaxRounds);
    assert!(outcome.final_message().is_none());
    assert_eq!(outcome.messages().len(), 3);
    assert_eq!(
        outcome.messages()[1].as_assistant().unwrap().tool_calls(),
        [tool_call]
    );
    let message = outcome.messages()[2].as_tool().expect("paired ToolMessage");
    assert_eq!(message.tool_call_id().as_str(), "call-1");
    assert_eq!(tool_result_text(message.result()), "single result");
}

#[test]
fn multiple_tool_calls_keep_input_order_when_completion_order_differs() {
    let slow_calls = Arc::new(AtomicUsize::new(0));
    let fast_calls = Arc::new(AtomicUsize::new(0));
    let completion_order = Arc::new(Mutex::new(Vec::new()));
    let runtime = runtime_with_test_tools(vec![
        TestTool::new(
            "slow",
            Arc::clone(&slow_calls),
            TestToolOutcome::Success("slow result"),
        )
        .with_completion_probe(3, Arc::clone(&completion_order)),
        TestTool::new(
            "fast",
            Arc::clone(&fast_calls),
            TestToolOutcome::Success("fast result"),
        )
        .with_completion_probe(0, Arc::clone(&completion_order)),
    ]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(tool_response(vec![
            call("slow-call", "slow", "{}"),
            call("fast-call", "fast", "{}"),
        ]))],
    );
    let agent = agent(adapter.facade(), runtime);

    let outcome = block_on(agent.invoke(vec![Message::user("question")]))
        .expect("ordered Tool batch succeeds");

    assert_eq!(
        completion_order.lock().unwrap().as_slice(),
        ["fast", "slow"]
    );
    assert_eq!(slow_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fast_calls.load(Ordering::SeqCst), 1);
    let ids = outcome.messages()[2..]
        .iter()
        .map(|message| message.as_tool().unwrap().tool_call_id().as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, ["slow-call", "fast-call"]);
    assert_eq!(outcome.stop_reason(), AgentStopReason::MaxRounds);
}

#[test]
fn business_error_becomes_real_tool_message_and_normal_outcome() {
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "business",
        Arc::clone(&executions),
        TestToolOutcome::BusinessError("business result"),
    )]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(tool_response(vec![call(
            "business-call",
            "business",
            "{}",
        )]))],
    );
    let agent = agent(adapter.facade(), runtime);

    let outcome = block_on(agent.invoke(vec![Message::user("question")]))
        .expect("business failure remains model-visible");

    assert_eq!(executions.load(Ordering::SeqCst), 1);
    let message = outcome.messages()[2].as_tool().expect("ToolMessage");
    assert_eq!(message.tool_call_id().as_str(), "business-call");
    assert!(message.result().is_error());
    assert_eq!(outcome.stop_reason(), AgentStopReason::MaxRounds);
}

#[test]
fn unknown_tool_returns_ordered_report_without_fake_message_or_retry() {
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new(),
        vec![Ok(tool_response(vec![call(
            "missing-call",
            "missing",
            r#"{"secret":"SECRET_ARGUMENT"}"#,
        )]))],
    );
    let agent = agent(adapter.facade(), ToolRuntime::new(ToolRegistry::empty()));
    let sink = Arc::new(RecordingEventSink::default());

    let error = block_on(agent.invoke_with_control(
        vec![Message::user("question")],
        event_config(&sink),
        RunControl::default(),
    ))
    .expect_err("unknown Tool is an infrastructure failure");

    assert_eq!(adapter.call_count(), 1);
    let events = sink.snapshot();
    assert_eq!(state_update_count(&events), 1);
    assert_eq!(
        node_started_metadata(&events),
        [
            (NodeId::from("model").into(), 1),
            (NodeId::from("tools").into(), 2),
        ]
    );
    assert_eq!(
        node_completed_metadata(&events),
        [(NodeId::from("model").into(), 1)]
    );
    assert_failed_lifecycle(
        &events,
        &RunFailure::NodeFailed {
            node_id: NodeId::from("tools").into(),
            step: 2,
        },
    );
    let report = error
        .tool_batch_report()
        .expect("ordered report is retained");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report.results()[0].as_ref().unwrap_err().kind(),
        ToolRuntimeErrorKind::ToolNotFound
    );
}

#[test]
fn mixed_batch_report_retains_all_facts_and_redacted_concrete_chain() {
    let success_calls = Arc::new(AtomicUsize::new(0));
    let business_calls = Arc::new(AtomicUsize::new(0));
    let failure_calls = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![
        TestTool::new(
            "success",
            Arc::clone(&success_calls),
            TestToolOutcome::Success("SECRET_SUCCESS_RESULT"),
        ),
        TestTool::new(
            "business",
            Arc::clone(&business_calls),
            TestToolOutcome::BusinessError("SECRET_BUSINESS_RESULT"),
        ),
        TestTool::new(
            "failure",
            Arc::clone(&failure_calls),
            TestToolOutcome::InfrastructureError("SECRET_TOOL_ERROR_MESSAGE"),
        ),
    ]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(tool_response(vec![
            call("success-call", "success", r#"{"value":"SECRET_A"}"#),
            call("business-call", "business", r#"{"value":"SECRET_B"}"#),
            call("failure-call", "failure", r#"{"value":"SECRET_C"}"#),
        ]))],
    );
    let agent = agent(adapter.facade(), runtime);
    let sink = Arc::new(RecordingEventSink::default());

    let error = block_on(agent.invoke_with_control(
        vec![Message::user("SECRET_PROMPT")],
        event_config(&sink),
        RunControl::default(),
    ))
    .expect_err("one infrastructure failure stops the Agent");

    assert_eq!(adapter.call_count(), 1);
    assert_eq!(success_calls.load(Ordering::SeqCst), 1);
    assert_eq!(business_calls.load(Ordering::SeqCst), 1);
    assert_eq!(failure_calls.load(Ordering::SeqCst), 1);
    let events = sink.snapshot();
    assert_eq!(state_update_count(&events), 1);
    assert_eq!(
        node_started_metadata(&events),
        [
            (NodeId::from("model").into(), 1),
            (NodeId::from("tools").into(), 2),
        ]
    );
    assert_eq!(
        node_completed_metadata(&events),
        [(NodeId::from("model").into(), 1)]
    );
    assert_failed_lifecycle(
        &events,
        &RunFailure::NodeFailed {
            node_id: NodeId::from("tools").into(),
            step: 2,
        },
    );
    let report = error.tool_batch_report().expect("complete report");
    assert_eq!(report.len(), 3);
    assert_eq!(
        tool_result_text(report.results()[0].as_ref().unwrap()),
        "SECRET_SUCCESS_RESULT"
    );
    assert!(report.results()[1].as_ref().unwrap().is_error());
    assert_eq!(
        tool_result_text(report.results()[1].as_ref().unwrap()),
        "SECRET_BUSINESS_RESULT"
    );
    assert_eq!(
        report.results()[2].as_ref().unwrap_err().kind(),
        ToolRuntimeErrorKind::ExecutionFailed
    );

    let graph = error
        .source()
        .unwrap()
        .downcast_ref::<GraphRunError>()
        .unwrap();
    let node = graph.source().unwrap().downcast_ref::<NodeError>().unwrap();
    let aggregate = node.source().expect("private aggregate source");
    let runtime = aggregate
        .source()
        .unwrap()
        .downcast_ref::<ToolRuntimeError>()
        .expect("first ordered runtime failure");
    let tool = runtime
        .source()
        .unwrap()
        .downcast_ref::<ToolError>()
        .unwrap();
    assert!(
        tool.source()
            .is_some_and(|source| source.is::<SecretToolRoot>())
    );
    for formatted in [
        format!("{error}"),
        format!("{error:?}"),
        format!("{node}"),
        format!("{node:?}"),
        format!("{aggregate}"),
        format!("{aggregate:?}"),
    ] {
        for secret in [
            "SECRET_PROMPT",
            "SECRET_A",
            "SECRET_B",
            "SECRET_C",
            "SECRET_SUCCESS_RESULT",
            "SECRET_BUSINESS_RESULT",
            "SECRET_TOOL_ERROR_MESSAGE",
            "SECRET_TOOL_ROOT_SOURCE",
        ] {
            assert!(!formatted.contains(secret));
        }
    }
}

#[test]
fn multiple_infrastructure_failures_keep_order_and_first_failure_as_source() {
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![
        TestTool::new(
            "first_failure",
            first_calls,
            TestToolOutcome::InfrastructureError("first"),
        ),
        TestTool::new(
            "second_failure",
            second_calls,
            TestToolOutcome::InfrastructureError("second"),
        ),
    ]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(tool_response(vec![
            call("first-call", "first_failure", "{}"),
            call("second-call", "second_failure", "{}"),
        ]))],
    );
    let agent = agent(adapter.facade(), runtime);

    let error = block_on(agent.invoke(vec![Message::user("question")])).unwrap_err();
    let report = error.tool_batch_report().expect("all failures retained");
    assert_eq!(report.results().len(), 2);
    assert_eq!(
        report.results()[0]
            .as_ref()
            .unwrap_err()
            .context()
            .batch_index(),
        Some(0)
    );
    assert_eq!(
        report.results()[1]
            .as_ref()
            .unwrap_err()
            .context()
            .batch_index(),
        Some(1)
    );
    let graph = error.source().unwrap();
    let node = graph.source().unwrap();
    let aggregate = node.source().unwrap();
    let first = aggregate
        .source()
        .unwrap()
        .downcast_ref::<ToolRuntimeError>()
        .expect("first failure is generic source");
    assert_eq!(first.context().call_id().as_str(), "first-call");
}

#[test]
fn duplicate_call_ids_preserve_tool_batch_error_without_report_or_execution() {
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "duplicate",
        Arc::clone(&executions),
        TestToolOutcome::Success("unused"),
    )]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(tool_response(vec![
            call("duplicate-call", "duplicate", "{}"),
            call("duplicate-call", "duplicate", "{}"),
        ]))],
    );
    let agent = agent(adapter.facade(), runtime);

    let error = block_on(agent.invoke(vec![Message::user("question")])).unwrap_err();

    assert_eq!(adapter.call_count(), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert!(error.tool_batch_report().is_none());
    let graph = error.source().unwrap();
    let node = graph.source().unwrap();
    assert!(
        node.source()
            .is_some_and(|source| source.is::<ToolBatchError>())
    );
}

#[test]
fn terminal_observer_failures_remain_secondary_to_success_and_business_results() {
    let observer_failures = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![
        TestTool::new(
            "observed_success",
            Arc::new(AtomicUsize::new(0)),
            TestToolOutcome::Success("success"),
        ),
        TestTool::new(
            "observed_business",
            Arc::new(AtomicUsize::new(0)),
            TestToolOutcome::BusinessError("business"),
        ),
    ])
    .with_event_sink({
        let observer_failures = Arc::clone(&observer_failures);
        Arc::new(move |event: &ToolEvent| {
            if matches!(event, ToolEvent::ExecutionCompleted { .. }) {
                observer_failures.fetch_add(1, Ordering::SeqCst);
                Err(ToolObserverError::new("SECRET_OBSERVER_FAILURE"))
            } else {
                Ok(())
            }
        })
    });
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(tool_response(vec![
            call("observed-success", "observed_success", "{}"),
            call("observed-business", "observed_business", "{}"),
        ]))],
    );
    let agent = agent(adapter.facade(), runtime);

    let outcome = block_on(agent.invoke(vec![Message::user("question")]))
        .expect("terminal diagnostics do not replace primary outcomes");

    assert_eq!(observer_failures.load(Ordering::SeqCst), 2);
    assert_eq!(outcome.messages().len(), 4);
    assert!(!outcome.messages()[2].as_tool().unwrap().result().is_error());
    assert!(outcome.messages()[3].as_tool().unwrap().result().is_error());
    assert_eq!(outcome.stop_reason(), AgentStopReason::MaxRounds);
}

#[test]
fn two_rounds_pass_canonical_tool_transcript_and_usage_to_final_answer() {
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "continue",
        Arc::clone(&executions),
        TestToolOutcome::Success("round one result"),
    )]);
    let first_usage = TokenUsage::from_parts(Some(2), Some(3), Some(5)).expect("valid usage");
    let first_call = call("round-one-call", "continue", "{}");
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![
            Ok(tool_response(vec![first_call.clone()]).with_usage(first_usage.clone())),
            Ok(ChatResponse::new(
                AssistantMessage::text("round two answer"),
                FinishReason::Stop,
            )),
        ],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 2);

    let user = Message::user("question");
    let outcome = block_on(agent.invoke(vec![user.clone()])).expect("two rounds complete");

    assert_eq!(adapter.call_count(), 2);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.model_rounds(), 2);
    assert_eq!(outcome.usage_by_round(), [Some(first_usage), None]);
    assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
    assert_eq!(
        outcome.final_message().unwrap().text_content(),
        "round two answer"
    );
    assert_eq!(outcome.messages().len(), 4);
    assert_eq!(outcome.messages()[0], user);
    assert_eq!(
        outcome.messages()[1].as_assistant().unwrap().tool_calls(),
        [first_call]
    );
    let tool_message = outcome.messages()[2]
        .as_tool()
        .expect("round one ToolMessage");
    assert_eq!(tool_message.tool_call_id().as_str(), "round-one-call");
    assert_eq!(tool_result_text(tool_message.result()), "round one result");
    assert_eq!(outcome.messages()[3].text_content(), "round two answer");

    let requests = adapter.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].messages(), std::slice::from_ref(&user));
    assert_eq!(requests[1].messages(), &outcome.messages()[..3]);
    assert_eq!(requests[0].tools(), requests[1].tools());
    assert_eq!(requests[0].tool_choice(), &ToolChoice::Auto);
    assert_eq!(requests[1].tool_choice(), &ToolChoice::Auto);
}

#[test]
fn three_rounds_keep_full_order_without_hidden_retry() {
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "repeat",
        Arc::clone(&executions),
        TestToolOutcome::Success("repeated result"),
    )]);
    let second_usage = TokenUsage::from_parts(Some(1), Some(1), Some(2)).expect("valid usage");
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![
            Ok(tool_response(vec![call("first-call", "repeat", "{}")])),
            Ok(tool_response(vec![call("second-call", "repeat", "{}")])
                .with_usage(second_usage.clone())),
            Ok(ChatResponse::new(
                AssistantMessage::text("third round answer"),
                FinishReason::Stop,
            )),
        ],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 3);

    let outcome =
        block_on(agent.invoke(vec![Message::user("question")])).expect("three rounds complete");

    assert_eq!(adapter.call_count(), 3);
    assert_eq!(executions.load(Ordering::SeqCst), 2);
    assert_eq!(outcome.model_rounds(), 3);
    assert_eq!(outcome.usage_by_round(), [None, Some(second_usage), None]);
    assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
    assert_eq!(outcome.messages().len(), 6);
    assert_eq!(
        outcome.messages()[2]
            .as_tool()
            .unwrap()
            .tool_call_id()
            .as_str(),
        "first-call"
    );
    assert_eq!(
        outcome.messages()[4]
            .as_tool()
            .unwrap()
            .tool_call_id()
            .as_str(),
        "second-call"
    );
    assert_eq!(outcome.messages()[5].text_content(), "third round answer");
    let requests = adapter.requests();
    assert_eq!(requests[1].messages(), &outcome.messages()[..3]);
    assert_eq!(requests[2].messages(), &outcome.messages()[..5]);
}

#[test]
fn longest_two_round_path_uses_four_node_steps_and_stops_at_max_rounds() {
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "limit",
        Arc::clone(&executions),
        TestToolOutcome::Success("limit result"),
    )]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![
            Ok(tool_response(vec![call("limit-one", "limit", "{}")])),
            Ok(tool_response(vec![call("limit-two", "limit", "{}")])),
        ],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 2);

    let outcome = block_on(agent.invoke(vec![Message::user("question")]))
        .expect("the exact 2 * max_rounds path fits the Core step budget");

    assert_eq!(adapter.call_count(), 2);
    assert_eq!(executions.load(Ordering::SeqCst), 2);
    assert_eq!(outcome.model_rounds(), 2);
    assert_eq!(outcome.usage_by_round(), [None, None]);
    assert_eq!(outcome.stop_reason(), AgentStopReason::MaxRounds);
    assert!(outcome.final_message().is_none());
    assert_eq!(outcome.messages().len(), 5);
    assert_eq!(
        outcome.messages()[2]
            .as_tool()
            .unwrap()
            .tool_call_id()
            .as_str(),
        "limit-one"
    );
    assert_eq!(
        outcome.messages()[4]
            .as_tool()
            .unwrap()
            .tool_call_id()
            .as_str(),
        "limit-two"
    );
}

#[test]
fn business_error_tool_message_continues_to_the_next_model_round() {
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "recoverable",
        Arc::clone(&executions),
        TestToolOutcome::BusinessError("recoverable business error"),
    )]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![
            Ok(tool_response(vec![call(
                "business-round",
                "recoverable",
                "{}",
            )])),
            Ok(ChatResponse::new(
                AssistantMessage::text("recovered answer"),
                FinishReason::Stop,
            )),
        ],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 2);

    let outcome = block_on(agent.invoke(vec![Message::user("question")]))
        .expect("business error remains model-visible");

    assert_eq!(adapter.call_count(), 2);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert!(outcome.messages()[2].as_tool().unwrap().result().is_error());
    assert!(
        adapter.requests()[1].messages()[2]
            .as_tool()
            .unwrap()
            .result()
            .is_error()
    );
    assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
}

#[test]
fn second_round_model_failure_follows_an_executed_tool_without_retry() {
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "before_failure",
        Arc::clone(&executions),
        TestToolOutcome::Success("external effect may exist"),
    )]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![
            Ok(tool_response(vec![call(
                "before-model-failure",
                "before_failure",
                "{}",
            )])),
            Err(ModelError::with_source(
                ModelErrorKind::ProviderUnavailable,
                "SECRET_LATER_MODEL_FAILURE",
                ProviderRoot,
            )),
        ],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 2);

    let error = block_on(agent.invoke(vec![Message::user("SECRET_QUESTION")]))
        .expect_err("second Model failure stops the invocation");

    assert_eq!(adapter.call_count(), 2);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert!(error.tool_batch_report().is_none());
    let graph = error
        .source()
        .unwrap()
        .downcast_ref::<GraphRunError>()
        .unwrap();
    let node = graph.source().unwrap().downcast_ref::<NodeError>().unwrap();
    assert!(
        node.source()
            .and_then(|source| source.downcast_ref::<ModelError>())
            .is_some()
    );
    for formatted in [format!("{error}"), format!("{error:?}")] {
        assert!(!formatted.contains("SECRET_QUESTION"));
        assert!(!formatted.contains("SECRET_LATER_MODEL_FAILURE"));
    }
}

#[test]
fn later_tool_infrastructure_failure_reports_only_current_batch_and_stops() {
    let early_executions = Arc::new(AtomicUsize::new(0));
    let failure_executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![
        TestTool::new(
            "early",
            Arc::clone(&early_executions),
            TestToolOutcome::Success("early result"),
        ),
        TestTool::new(
            "later_failure",
            Arc::clone(&failure_executions),
            TestToolOutcome::InfrastructureError("later failure"),
        ),
    ]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![
            Ok(tool_response(vec![call("early-call", "early", "{}")])),
            Ok(tool_response(vec![call(
                "current-failure-call",
                "later_failure",
                "{}",
            )])),
        ],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 3);

    let error = block_on(agent.invoke(vec![Message::user("question")]))
        .expect_err("later Tool infrastructure failure stops the invocation");

    assert_eq!(adapter.call_count(), 2);
    assert_eq!(early_executions.load(Ordering::SeqCst), 1);
    assert_eq!(failure_executions.load(Ordering::SeqCst), 1);
    let report = error
        .tool_batch_report()
        .expect("current failed batch report");
    assert_eq!(report.len(), 1);
    let current = report.results()[0].as_ref().unwrap_err();
    assert_eq!(current.context().call_id().as_str(), "current-failure-call");
    assert_eq!(current.context().batch_index(), Some(0));
}

#[tokio::test]
async fn invoke_with_control_completes_final_answer_and_multiround_loop() {
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "controlled",
        Arc::clone(&executions),
        TestToolOutcome::Success("controlled result"),
    )]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![
            Ok(tool_response(vec![call(
                "controlled-call",
                "controlled",
                "{}",
            )])),
            Ok(ChatResponse::new(
                AssistantMessage::text("controlled answer"),
                FinishReason::Stop,
            )),
        ],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 2);

    let outcome = agent
        .invoke_with_control(
            vec![Message::user("question")],
            EventConfig::default(),
            RunControl::default(),
        )
        .await
        .expect("controlled multi-round invocation completes");

    assert_eq!(adapter.call_count(), 2);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.model_rounds(), 2);
    assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
    assert_eq!(
        outcome.final_message().unwrap().text_content(),
        "controlled answer"
    );
}

#[tokio::test]
async fn concurrent_invocations_isolate_state_control_rounds_and_outcomes() {
    let probe = Arc::new(PendingProbe::default());
    let adapter = ConcurrentAdapter::new(Arc::clone(&probe));
    let agent = agent(adapter.facade(), ToolRuntime::new(ToolRegistry::empty()));
    let cancellation = CancellationToken::new();
    let cancel_control = RunControl::new().with_cancellation_token(cancellation.clone());

    let cancelled_invocation = agent.invoke_with_control(
        vec![Message::user("cancel this invocation")],
        EventConfig::default(),
        cancel_control,
    );
    let completed_invocation = agent.invoke(vec![Message::user("complete independently")]);
    let trigger = async {
        probe.wait_started().await;
        cancellation.cancel();
    };

    let (cancelled, completed, ()) =
        tokio::join!(cancelled_invocation, completed_invocation, trigger);
    let cancelled = cancelled.expect_err("only the controlled invocation is cancelled");
    assert!(matches!(
        graph_error(&cancelled),
        GraphRunError::Cancelled { .. }
    ));
    assert_eq!(probe.dropped_count(), 1);

    let completed = completed.expect("the other invocation completes independently");
    assert_eq!(completed.model_rounds(), 1);
    assert_eq!(completed.messages().len(), 2);
    assert_eq!(
        completed.final_message().unwrap().text_content(),
        "answer for complete independently"
    );
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn pending_model_cancellation_drops_future_without_retry() {
    let probe = Arc::new(PendingProbe::default());
    let adapter = PendingAdapter::new(Vec::new(), Arc::clone(&probe));
    let agent = agent(adapter.facade(), ToolRuntime::new(ToolRegistry::empty()));
    let token = CancellationToken::new();
    let sink = Arc::new(RecordingEventSink::default());
    let mut invocation = Box::pin(agent.invoke_with_control(
        vec![Message::user("SECRET_CANCELLED_MODEL_PROMPT")],
        event_config(&sink),
        RunControl::new().with_cancellation_token(token.clone()),
    ));

    tokio::select! {
        result = &mut invocation => panic!("model unexpectedly completed: {result:?}"),
        () = probe.wait_started() => {}
    }
    token.cancel();
    let error = invocation
        .await
        .expect_err("model cancellation fails invocation");

    assert!(matches!(
        graph_error(&error),
        GraphRunError::Cancelled { step: 1, .. }
    ));
    assert_eq!(adapter.call_count(), 1);
    assert_eq!(probe.dropped_count(), 1);
    let events = sink.snapshot();
    assert_eq!(state_update_count(&events), 0);
    assert_eq!(
        node_started_metadata(&events),
        [(NodeId::from("model").into(), 1)]
    );
    assert!(node_completed_metadata(&events).is_empty());
    assert_failed_lifecycle(
        &events,
        &RunFailure::Cancelled {
            node_id: Some(NodeId::from("model").into()),
            step: 1,
        },
    );
    assert!(error.tool_batch_report().is_none());
    let markers = ["SECRET_CANCELLED_MODEL_PROMPT"];
    assert_events_redacted(&events, &markers);
    assert_error_formats_redacted(&error, &markers);
}

#[tokio::test]
async fn pending_tool_cancellation_drops_future_without_retry() {
    let probe = Arc::new(PendingProbe::default());
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime =
        runtime_with_pending_tool("pending_tool", Arc::clone(&executions), Arc::clone(&probe));
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(tool_response(vec![call(
            "pending-tool-call",
            "pending_tool",
            r#"{"secret":"SECRET_PENDING_TOOL_ARGUMENT"}"#,
        )]))],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 2);
    let token = CancellationToken::new();
    let sink = Arc::new(RecordingEventSink::default());
    let mut invocation = Box::pin(agent.invoke_with_control(
        vec![Message::user("question")],
        event_config(&sink),
        RunControl::new().with_cancellation_token(token.clone()),
    ));

    tokio::select! {
        result = &mut invocation => panic!("Tool unexpectedly completed: {result:?}"),
        () = probe.wait_started() => {}
    }
    token.cancel();
    let error = invocation
        .await
        .expect_err("Tool cancellation fails invocation");

    assert!(matches!(
        graph_error(&error),
        GraphRunError::Cancelled { step: 2, .. }
    ));
    assert_eq!(adapter.call_count(), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped_count(), 1);
    let events = sink.snapshot();
    assert_eq!(state_update_count(&events), 1);
    assert_eq!(
        node_started_metadata(&events),
        [
            (NodeId::from("model").into(), 1),
            (NodeId::from("tools").into(), 2),
        ]
    );
    assert_eq!(
        node_completed_metadata(&events),
        [(NodeId::from("model").into(), 1)]
    );
    assert_failed_lifecycle(
        &events,
        &RunFailure::Cancelled {
            node_id: Some(NodeId::from("tools").into()),
            step: 2,
        },
    );
    assert!(error.tool_batch_report().is_none());
    let markers = ["SECRET_PENDING_TOOL_ARGUMENT"];
    assert_events_redacted(&events, &markers);
    assert_error_formats_redacted(&error, &markers);
}

#[tokio::test]
async fn dropping_top_level_pending_invocations_drops_model_and_tool_futures() {
    let model_probe = Arc::new(PendingProbe::default());
    let model_adapter = PendingAdapter::new(Vec::new(), Arc::clone(&model_probe));
    let model_agent = agent(
        model_adapter.facade(),
        ToolRuntime::new(ToolRegistry::empty()),
    );
    let mut model_invocation = Box::pin(model_agent.invoke_with_control(
        vec![Message::user("question")],
        EventConfig::default(),
        RunControl::default(),
    ));
    tokio::select! {
        result = &mut model_invocation => panic!("model unexpectedly completed: {result:?}"),
        () = model_probe.wait_started() => {}
    }
    drop(model_invocation);
    tokio::task::yield_now().await;
    assert_eq!(model_adapter.call_count(), 1);
    assert_eq!(model_probe.dropped_count(), 1);

    let tool_probe = Arc::new(PendingProbe::default());
    let tool_executions = Arc::new(AtomicUsize::new(0));
    let tool_runtime = runtime_with_pending_tool(
        "drop_tool",
        Arc::clone(&tool_executions),
        Arc::clone(&tool_probe),
    );
    let tool_adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(tool_response(vec![call(
            "drop-call",
            "drop_tool",
            "{}",
        )]))],
    );
    let tool_agent = agent_with_rounds(tool_adapter.facade(), tool_runtime, 2);
    let mut tool_invocation = Box::pin(tool_agent.invoke_with_control(
        vec![Message::user("question")],
        EventConfig::default(),
        RunControl::default(),
    ));
    tokio::select! {
        result = &mut tool_invocation => panic!("Tool unexpectedly completed: {result:?}"),
        () = tool_probe.wait_started() => {}
    }
    drop(tool_invocation);
    tokio::task::yield_now().await;
    assert_eq!(tool_adapter.call_count(), 1);
    assert_eq!(tool_executions.load(Ordering::SeqCst), 1);
    assert_eq!(tool_probe.dropped_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn pending_model_run_timeout_is_typed_and_drops_future() {
    let probe = Arc::new(PendingProbe::default());
    let adapter = PendingAdapter::new(Vec::new(), Arc::clone(&probe));
    let agent = agent(adapter.facade(), ToolRuntime::new(ToolRegistry::empty()));
    let sink = Arc::new(RecordingEventSink::default());
    let mut invocation = Box::pin(agent.invoke_with_control(
        vec![Message::user("SECRET_MODEL_RUN_TIMEOUT")],
        event_config(&sink),
        RunControl::new().with_run_timeout(std::time::Duration::from_secs(5)),
    ));

    tokio::select! {
        result = &mut invocation => panic!("model unexpectedly completed: {result:?}"),
        () = probe.wait_started() => {}
    }
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    let error = invocation.await.expect_err("run timeout fails invocation");

    assert!(matches!(
        graph_error(&error),
        GraphRunError::RunTimedOut { timeout, step: 1, .. }
            if *timeout == std::time::Duration::from_secs(5)
    ));
    assert_eq!(adapter.call_count(), 1);
    assert_eq!(probe.dropped_count(), 1);
    assert!(error.tool_batch_report().is_none());
    let events = sink.snapshot();
    assert_eq!(
        node_started_metadata(&events),
        [(NodeId::from("model").into(), 1)]
    );
    assert!(node_completed_metadata(&events).is_empty());
    assert_failed_lifecycle(
        &events,
        &RunFailure::RunTimedOut {
            timeout: std::time::Duration::from_secs(5),
            node_id: Some(NodeId::from("model").into()),
            step: 1,
        },
    );
    let markers = ["SECRET_MODEL_RUN_TIMEOUT"];
    assert_events_redacted(&events, &markers);
    assert_error_formats_redacted(&error, &markers);
}

#[tokio::test(start_paused = true)]
async fn pending_model_node_timeout_is_typed_and_drops_future() {
    let probe = Arc::new(PendingProbe::default());
    let adapter = PendingAdapter::new(Vec::new(), Arc::clone(&probe));
    let agent = agent(adapter.facade(), ToolRuntime::new(ToolRegistry::empty()));
    let sink = Arc::new(RecordingEventSink::default());
    let mut invocation = Box::pin(agent.invoke_with_control(
        vec![Message::user("SECRET_MODEL_NODE_TIMEOUT")],
        event_config(&sink),
        RunControl::new().with_node_timeout(std::time::Duration::from_secs(3)),
    ));

    tokio::select! {
        result = &mut invocation => panic!("model unexpectedly completed: {result:?}"),
        () = probe.wait_started() => {}
    }
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    let error = invocation.await.expect_err("node timeout fails invocation");

    assert!(matches!(
        graph_error(&error),
        GraphRunError::NodeTimedOut { timeout, node_id, step: 1, .. }
            if *timeout == std::time::Duration::from_secs(3)
                && node_id == &NodeId::from("model")
    ));
    assert_eq!(adapter.call_count(), 1);
    assert_eq!(probe.dropped_count(), 1);
    assert!(error.tool_batch_report().is_none());
    let events = sink.snapshot();
    assert_eq!(
        node_started_metadata(&events),
        [(NodeId::from("model").into(), 1)]
    );
    assert!(node_completed_metadata(&events).is_empty());
    assert_failed_lifecycle(
        &events,
        &RunFailure::NodeTimedOut {
            timeout: std::time::Duration::from_secs(3),
            node_id: NodeId::from("model").into(),
            step: 1,
        },
    );
    let markers = ["SECRET_MODEL_NODE_TIMEOUT"];
    assert_events_redacted(&events, &markers);
    assert_error_formats_redacted(&error, &markers);
}

#[tokio::test(start_paused = true)]
async fn pending_tool_run_timeout_is_typed_and_drops_future() {
    let probe = Arc::new(PendingProbe::default());
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_pending_tool(
        "run_timeout_tool",
        Arc::clone(&executions),
        Arc::clone(&probe),
    );
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(tool_response(vec![call(
            "run-timeout-call",
            "run_timeout_tool",
            r#"{"secret":"SECRET_TOOL_RUN_TIMEOUT"}"#,
        )]))],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 2);
    let mut invocation = Box::pin(agent.invoke_with_control(
        vec![Message::user("question")],
        EventConfig::default(),
        RunControl::new().with_run_timeout(std::time::Duration::from_secs(5)),
    ));

    tokio::select! {
        result = &mut invocation => panic!("Tool unexpectedly completed: {result:?}"),
        () = probe.wait_started() => {}
    }
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    let error = invocation.await.expect_err("run timeout fails Tool node");

    assert!(matches!(
        graph_error(&error),
        GraphRunError::RunTimedOut { step: 2, .. }
    ));
    assert_eq!(adapter.call_count(), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped_count(), 1);
    assert!(error.tool_batch_report().is_none());
    assert!(!format!("{error:?}").contains("SECRET_TOOL_RUN_TIMEOUT"));
}

#[tokio::test(start_paused = true)]
async fn pending_tool_node_timeout_is_typed_and_drops_future() {
    let probe = Arc::new(PendingProbe::default());
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_pending_tool(
        "node_timeout_tool",
        Arc::clone(&executions),
        Arc::clone(&probe),
    );
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![Ok(tool_response(vec![call(
            "node-timeout-call",
            "node_timeout_tool",
            r#"{"secret":"SECRET_TOOL_NODE_TIMEOUT"}"#,
        )]))],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 2);
    let mut invocation = Box::pin(agent.invoke_with_control(
        vec![Message::user("question")],
        EventConfig::default(),
        RunControl::new().with_node_timeout(std::time::Duration::from_secs(3)),
    ));

    tokio::select! {
        result = &mut invocation => panic!("Tool unexpectedly completed: {result:?}"),
        () = probe.wait_started() => {}
    }
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    let error = invocation.await.expect_err("node timeout fails Tool node");

    assert!(matches!(
        graph_error(&error),
        GraphRunError::NodeTimedOut { node_id, step: 2, .. }
            if node_id == &NodeId::from("tools")
    ));
    assert_eq!(adapter.call_count(), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped_count(), 1);
    assert!(error.tool_batch_report().is_none());
    assert!(!format!("{error:?}").contains("SECRET_TOOL_NODE_TIMEOUT"));
}

#[tokio::test(start_paused = true)]
async fn cancellation_precedes_ready_run_and_node_deadlines() {
    let probe = Arc::new(PendingProbe::default());
    let adapter = PendingAdapter::new(Vec::new(), Arc::clone(&probe));
    let agent = agent(adapter.facade(), ToolRuntime::new(ToolRegistry::empty()));
    let token = CancellationToken::new();
    let mut invocation = Box::pin(
        agent.invoke_with_control(
            vec![Message::user("question")],
            EventConfig::default(),
            RunControl::new()
                .with_cancellation_token(token.clone())
                .with_run_timeout(std::time::Duration::from_secs(5))
                .with_node_timeout(std::time::Duration::from_secs(5)),
        ),
    );

    tokio::select! {
        result = &mut invocation => panic!("model unexpectedly completed: {result:?}"),
        () = probe.wait_started() => {}
    }
    token.cancel();
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    let error = invocation
        .await
        .expect_err("control precedence stops invocation");

    assert!(matches!(
        graph_error(&error),
        GraphRunError::Cancelled { step: 1, .. }
    ));
    assert_eq!(adapter.call_count(), 1);
    assert_eq!(probe.dropped_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn run_timeout_precedes_node_timeout_when_deadlines_tie_without_cancellation() {
    let probe = Arc::new(PendingProbe::default());
    let adapter = PendingAdapter::new(Vec::new(), Arc::clone(&probe));
    let tool_executions = Arc::new(AtomicUsize::new(0));
    let agent = agent(
        adapter.facade(),
        nonempty_runtime(Arc::clone(&tool_executions)),
    );
    let sink = Arc::new(RecordingEventSink::default());
    let common_timeout = std::time::Duration::from_secs(5);
    let mut invocation = Box::pin(
        agent.invoke_with_control(
            vec![Message::user("SECRET_RUN_NODE_TIE")],
            event_config(&sink),
            RunControl::new()
                .with_run_timeout(common_timeout)
                .with_node_timeout(common_timeout),
        ),
    );

    tokio::select! {
        result = &mut invocation => panic!("model unexpectedly completed: {result:?}"),
        () = probe.wait_started() => {}
    }
    // Paused time has not advanced since invocation entry, so Core created the
    // run and model-node absolute deadlines from the same instant. Advancing
    // once to their shared deadline makes both ready with no cancellation.
    tokio::time::advance(common_timeout).await;
    let error = invocation
        .await
        .expect_err("run timeout wins the run/node deadline tie");

    assert!(matches!(
        graph_error(&error),
        GraphRunError::RunTimedOut {
            timeout,
            node_id: Some(node_id),
            step: 1,
            ..
        } if *timeout == common_timeout && node_id == &NodeId::from("model")
    ));
    assert_eq!(adapter.call_count(), 1);
    assert_eq!(probe.dropped_count(), 1);
    assert_eq!(tool_executions.load(Ordering::SeqCst), 0);
    assert!(error.tool_batch_report().is_none());
    let events = sink.snapshot();
    assert_eq!(
        node_started_metadata(&events),
        [(NodeId::from("model").into(), 1)]
    );
    assert!(node_completed_metadata(&events).is_empty());
    assert_failed_lifecycle(
        &events,
        &RunFailure::RunTimedOut {
            timeout: common_timeout,
            node_id: Some(NodeId::from("model").into()),
            step: 1,
        },
    );
    let markers = ["SECRET_RUN_NODE_TIE"];
    assert_events_redacted(&events, &markers);
    assert_error_formats_redacted(&error, &markers);
}

#[tokio::test]
async fn second_round_model_cancellation_preserves_prior_execution_facts() {
    let probe = Arc::new(PendingProbe::default());
    let adapter = PendingAdapter::new(
        vec![tool_response(vec![call("first-call", "first_tool", "{}")])],
        Arc::clone(&probe),
    );
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "first_tool",
        Arc::clone(&executions),
        TestToolOutcome::Success("first result"),
    )]);
    let agent = agent_with_rounds(adapter.facade(), runtime, 2);
    let token = CancellationToken::new();
    let sink = Arc::new(RecordingEventSink::default());
    let mut invocation = Box::pin(agent.invoke_with_control(
        vec![Message::user("question")],
        event_config(&sink),
        RunControl::new().with_cancellation_token(token.clone()),
    ));

    tokio::select! {
        result = &mut invocation => panic!("second model unexpectedly completed: {result:?}"),
        () = probe.wait_started() => {}
    }
    token.cancel();
    let error = invocation
        .await
        .expect_err("second round cancellation fails invocation");

    assert!(matches!(
        graph_error(&error),
        GraphRunError::Cancelled { step: 3, .. }
    ));
    assert_eq!(adapter.call_count(), 2);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped_count(), 1);
    assert_eq!(state_update_count(&sink.snapshot()), 2);
    assert!(error.tool_batch_report().is_none());
    assert_eq!(error.to_string(), "agent invocation failed");
}

#[tokio::test]
async fn event_sink_lifecycle_is_single_and_redacted() {
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_test_tools(vec![TestTool::new(
        "secret_tool",
        Arc::clone(&executions),
        TestToolOutcome::Success("SECRET_TOOL_RESULT"),
    )]);
    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new().with_tool_calling(true),
        vec![
            Ok(tool_response(vec![call(
                "secret-call",
                "secret_tool",
                r#"{"value":"SECRET_TOOL_ARGUMENT"}"#,
            )])),
            Ok(ChatResponse::new(
                AssistantMessage::text("SECRET_ASSISTANT_ANSWER"),
                FinishReason::Stop,
            )),
        ],
    );
    let agent = agent_with_rounds(adapter.facade(), runtime, 2);
    let sink = Arc::new(RecordingEventSink::default());

    let outcome = agent
        .invoke_with_control(
            vec![Message::user("SECRET_CALLER_PROMPT")],
            event_config(&sink),
            RunControl::default(),
        )
        .await
        .expect("observed invocation succeeds");

    assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
    let events = sink.snapshot();
    assert_eq!(
        event_count(&events, |event| matches!(
            event,
            GraphEvent::RunStarted { .. }
        )),
        1
    );
    assert_eq!(
        event_count(&events, |event| matches!(
            event,
            GraphEvent::RunCompleted { .. }
        )),
        1
    );
    assert_eq!(
        event_count(&events, |event| matches!(
            event,
            GraphEvent::RunFailed { .. }
        )),
        0
    );
    let expected_nodes = [
        (NodeId::from("model").into(), 1),
        (NodeId::from("tools").into(), 2),
        (NodeId::from("model").into(), 3),
    ];
    assert_eq!(node_started_metadata(&events), expected_nodes);
    assert_eq!(node_completed_metadata(&events), expected_nodes);
    assert_events_redacted(&events, &["SECRET_", "secret-call", "secret_tool"]);
}

#[test]
fn agent_error_debug_and_display_do_not_format_graph_source() {
    fn assert_error(error: &AgentError) {
        assert_eq!(error.to_string(), "agent invocation failed");
        assert_eq!(
            format!("{error:?}"),
            "AgentError { has_graph_source: true }"
        );
    }

    let adapter = ScriptedAdapter::new(
        ModelCapabilities::new(),
        vec![Err(ModelError::new(
            ModelErrorKind::Other,
            "SECRET_SOURCE_MESSAGE",
        ))],
    );
    let agent = agent(adapter.facade(), ToolRuntime::new(ToolRegistry::empty()));
    let error = block_on(agent.invoke(vec![Message::user("SECRET_REQUEST")]))
        .expect_err("scripted failure");

    assert_error(&error);
}

#[test]
fn agent_build_error_retains_core_source_without_default_source_formatting() {
    let error = AgentBuildError::from(GraphBuildError::ReservedNodeId {
        node_id: NodeId::new("SECRET_PRIVATE_NODE"),
    });

    assert_eq!(error.to_string(), "agent graph construction failed");
    assert_eq!(
        format!("{error:?}"),
        "AgentBuildError { phase: \"build\", has_source: true }"
    );
    assert!(
        error
            .source()
            .is_some_and(|source| source.is::<GraphBuildError>())
    );
    assert!(!error.to_string().contains("SECRET_PRIVATE_NODE"));
    assert!(!format!("{error:?}").contains("SECRET_PRIVATE_NODE"));
}
