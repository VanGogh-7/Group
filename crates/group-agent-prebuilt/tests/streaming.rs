use std::collections::VecDeque;
use std::error::Error;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_core::Stream;
use futures_util::StreamExt;
use group_agent_model::{
    AssistantMessage, ChatEventStream, ChatModel, ChatModelAdapter, ChatResponse, ChatStreamEvent,
    FinishReason, Message, ModelCapabilities, ModelCapability, ModelError, ModelErrorKind, ModelId,
    ModelMetadata, ProviderId, ToolCallDelta, ToolCallId, ToolDefinition, ToolName,
    ValidatedChatRequest,
};
use group_agent_prebuilt::{AgentConfig, AgentStopReason, AgentStreamEvent, ToolCallingAgent};
use group_agent_tool::{
    Tool, ToolBehavior, ToolError, ToolInput, ToolOutput, ToolRegistry, ToolRuntime,
};
use serde_json::json;

struct DropSentinel(Arc<AtomicBool>);
impl Drop for DropSentinel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct VectorStream {
    items: std::vec::IntoIter<Result<ChatStreamEvent, ModelError>>,
    _sentinel: Option<DropSentinel>,
}

impl Stream for VectorStream {
    type Item = Result<ChatStreamEvent, ModelError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.items.next())
    }
}

type StreamResult = Result<ChatStreamEvent, ModelError>;
type QueuedStreamTurns = Arc<Mutex<VecDeque<Vec<StreamResult>>>>;

struct VectorStreamAdapter {
    metadata: ModelMetadata,
    events_per_call: QueuedStreamTurns,
    drop_flag: Option<Arc<AtomicBool>>,
}

#[async_trait]
impl ChatModelAdapter for VectorStreamAdapter {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    async fn complete_raw(
        &self,
        _request: ValidatedChatRequest,
    ) -> Result<ChatResponse, ModelError> {
        Ok(ChatResponse::new(
            AssistantMessage::text("complete fallback"),
            FinishReason::Stop,
        ))
    }

    async fn stream_raw(
        &self,
        _request: ValidatedChatRequest,
    ) -> Result<ChatEventStream, ModelError> {
        let items = self
            .events_per_call
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_default();

        let sentinel = self.drop_flag.as_ref().map(|f| DropSentinel(f.clone()));

        Ok(Box::pin(VectorStream {
            items: items.into_iter(),
            _sentinel: sentinel,
        }))
    }
}

fn empty_tools() -> ToolRuntime {
    ToolRuntime::new(ToolRegistry::empty())
}

struct EchoLookupTool;

#[async_trait]
impl Tool for EchoLookupTool {
    fn name(&self) -> &ToolName {
        static NAME: std::sync::OnceLock<ToolName> = std::sync::OnceLock::new();
        NAME.get_or_init(|| ToolName::new("lookup").unwrap())
    }

    fn definition(&self) -> &ToolDefinition {
        static DEF: std::sync::OnceLock<ToolDefinition> = std::sync::OnceLock::new();
        DEF.get_or_init(|| {
            ToolDefinition::new(
                ToolName::new("lookup").unwrap(),
                "Lookup item",
                json!({
                    "type": "object",
                    "properties": {"item": {"type": "string"}},
                    "required": ["item"]
                }),
            )
        })
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    async fn execute(&self, _input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        Ok(ToolOutput::success_text("offline tool result"))
    }
}

fn lookup_runtime() -> ToolRuntime {
    let mut builder = ToolRegistry::builder();
    builder.register(EchoLookupTool).unwrap();
    ToolRuntime::new(builder.build())
}

#[tokio::test]
async fn streaming_model_only_yields_text_deltas_and_completion() {
    let events = vec![vec![
        Ok(ChatStreamEvent::ResponseStarted {
            response_id: None,
            model: None,
            extensions: group_agent_model::Extensions::new(),
        }),
        Ok(ChatStreamEvent::TextDelta("Hello".to_string())),
        Ok(ChatStreamEvent::TextDelta(" world!".to_string())),
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
    ]];

    let adapter = VectorStreamAdapter {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("stream-model").unwrap(),
            ModelCapabilities::new().with_streaming(true),
        ),
        events_per_call: Arc::new(Mutex::new(VecDeque::from(events))),
        drop_flag: None,
    };

    let model = ChatModel::from_adapter(adapter).unwrap();
    let agent = ToolCallingAgent::new(model, empty_tools(), AgentConfig::new(1).unwrap()).unwrap();

    let mut stream = agent.stream(vec![Message::user("hi")]);
    let mut collected = Vec::new();

    while let Some(event) = stream.next().await {
        collected.push(event.expect("stream item succeeds"));
    }

    assert_eq!(collected.len(), 5);
    assert_eq!(collected[0], AgentStreamEvent::ModelStarted { round: 1 });
    assert_eq!(
        collected[1],
        AgentStreamEvent::TextDelta {
            round: 1,
            delta: "Hello".to_string()
        }
    );
    assert_eq!(
        collected[2],
        AgentStreamEvent::TextDelta {
            round: 1,
            delta: " world!".to_string()
        }
    );
    assert_eq!(collected[3], AgentStreamEvent::ModelCompleted { round: 1 });

    match &collected[4] {
        AgentStreamEvent::Completed(outcome) => {
            assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
            assert_eq!(outcome.model_rounds(), 1);
            assert_eq!(
                outcome.final_message().unwrap().text_content(),
                "Hello world!"
            );
        }
        other => panic!("expected Completed event, got {other:?}"),
    }
}

#[tokio::test]
async fn invoke_with_stream_sink_dispatches_events_synchronously() {
    let events = vec![vec![
        Ok(ChatStreamEvent::ResponseStarted {
            response_id: None,
            model: None,
            extensions: group_agent_model::Extensions::new(),
        }),
        Ok(ChatStreamEvent::TextDelta("Synchronous".to_string())),
        Ok(ChatStreamEvent::TextDelta(" sink test".to_string())),
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
    ]];

    let adapter = VectorStreamAdapter {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("stream-model").unwrap(),
            ModelCapabilities::new().with_streaming(true),
        ),
        events_per_call: Arc::new(Mutex::new(VecDeque::from(events))),
        drop_flag: None,
    };

    let model = ChatModel::from_adapter(adapter).unwrap();
    let agent = ToolCallingAgent::new(model, empty_tools(), AgentConfig::new(1).unwrap()).unwrap();

    let observed = Arc::new(Mutex::new(Vec::new()));
    let sink_observed = Arc::clone(&observed);

    let outcome = agent
        .invoke_with_stream_sink(
            vec![Message::user("hi")],
            Arc::new(move |event: &AgentStreamEvent| {
                sink_observed.lock().unwrap().push(event.clone());
            }),
        )
        .await
        .expect("invoke_with_stream_sink succeeds");

    assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
    assert_eq!(
        outcome.final_message().unwrap().text_content(),
        "Synchronous sink test"
    );

    let captured = observed.lock().unwrap().clone();
    assert_eq!(captured.len(), 5);
    assert_eq!(captured[0], AgentStreamEvent::ModelStarted { round: 1 });
    assert_eq!(
        captured[1],
        AgentStreamEvent::TextDelta {
            round: 1,
            delta: "Synchronous".to_string()
        }
    );
    assert_eq!(
        captured[2],
        AgentStreamEvent::TextDelta {
            round: 1,
            delta: " sink test".to_string()
        }
    );
    assert_eq!(captured[3], AgentStreamEvent::ModelCompleted { round: 1 });
    assert_eq!(captured[4], AgentStreamEvent::Completed(outcome));
}

#[tokio::test]
async fn streaming_multi_round_with_tools_emits_tool_lifecycle_events() {
    let call_id = ToolCallId::new("call-1").unwrap();
    let tool_name = ToolName::new("lookup").unwrap();

    let round1_events = vec![
        Ok(ChatStreamEvent::ResponseStarted {
            response_id: None,
            model: None,
            extensions: group_agent_model::Extensions::new(),
        }),
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_id(call_id.clone())
                .with_name(tool_name.clone())
                .with_arguments_fragment(r#"{"item":"sample"}"#),
        )),
        Ok(ChatStreamEvent::Finished(FinishReason::ToolCalls)),
    ];

    let round2_events = vec![
        Ok(ChatStreamEvent::ResponseStarted {
            response_id: None,
            model: None,
            extensions: group_agent_model::Extensions::new(),
        }),
        Ok(ChatStreamEvent::TextDelta(
            "Tool completed successfully".to_string(),
        )),
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
    ];

    let adapter = VectorStreamAdapter {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("stream-model").unwrap(),
            ModelCapabilities::new()
                .with_streaming(true)
                .with_tool_calling(true),
        ),
        events_per_call: Arc::new(Mutex::new(VecDeque::from(vec![
            round1_events,
            round2_events,
        ]))),
        drop_flag: None,
    };

    let model = ChatModel::from_adapter(adapter).unwrap();
    let agent =
        ToolCallingAgent::new(model, lookup_runtime(), AgentConfig::new(2).unwrap()).unwrap();

    let mut stream = agent.stream(vec![Message::user("search for sample")]);
    let mut collected = Vec::new();

    while let Some(event) = stream.next().await {
        collected.push(event.expect("stream item succeeds"));
    }

    assert_eq!(collected.len(), 9);
    assert_eq!(collected[0], AgentStreamEvent::ModelStarted { round: 1 });
    assert!(matches!(
        collected[1],
        AgentStreamEvent::ToolCallDelta { round: 1, .. }
    ));
    assert_eq!(collected[2], AgentStreamEvent::ModelCompleted { round: 1 });
    assert_eq!(
        collected[3],
        AgentStreamEvent::ToolStarted {
            id: call_id.clone(),
            name: tool_name.clone(),
            round: 1,
        }
    );
    assert_eq!(
        collected[4],
        AgentStreamEvent::ToolCompleted {
            id: call_id,
            name: tool_name,
            round: 1,
            is_error: false,
        }
    );
    assert_eq!(collected[5], AgentStreamEvent::ModelStarted { round: 2 });
    assert_eq!(
        collected[6],
        AgentStreamEvent::TextDelta {
            round: 2,
            delta: "Tool completed successfully".to_string(),
        }
    );
    assert_eq!(collected[7], AgentStreamEvent::ModelCompleted { round: 2 });

    match &collected[8] {
        AgentStreamEvent::Completed(outcome) => {
            assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
            assert_eq!(outcome.model_rounds(), 2);
            assert_eq!(
                outcome.final_message().unwrap().text_content(),
                "Tool completed successfully"
            );
        }
        other => panic!("expected Completed event, got {other:?}"),
    }
}

#[tokio::test]
async fn model_without_streaming_capability_fails_closed() {
    let adapter = VectorStreamAdapter {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("no-stream").unwrap(),
            ModelCapabilities::new(), // streaming is false!
        ),
        events_per_call: Arc::new(Mutex::new(VecDeque::new())),
        drop_flag: None,
    };

    let model = ChatModel::from_adapter(adapter).unwrap();
    let agent = ToolCallingAgent::new(model, empty_tools(), AgentConfig::new(1).unwrap()).unwrap();

    let mut stream = agent.stream(vec![Message::user("hi")]);
    let first = stream.next().await.expect("stream yields an item");
    let error = first.expect_err("stream must fail closed without streaming capability");
    let mut source = error.source();
    let model_error = loop {
        let current = source.expect("typed ModelError remains source-reachable");
        if let Some(model_error) = current.downcast_ref::<ModelError>() {
            break model_error;
        }
        source = current.source();
    };
    assert_eq!(
        model_error.kind(),
        &ModelErrorKind::UnsupportedCapability(ModelCapability::Streaming)
    );
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn stream_protocol_error_terminates_stream_with_error() {
    // Protocol violation: finished without response_started or sending text after finished
    let events = vec![vec![
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
        Ok(ChatStreamEvent::TextDelta("after finished".to_string())),
    ]];

    let adapter = VectorStreamAdapter {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("stream-model").unwrap(),
            ModelCapabilities::new().with_streaming(true),
        ),
        events_per_call: Arc::new(Mutex::new(VecDeque::from(events))),
        drop_flag: None,
    };

    let model = ChatModel::from_adapter(adapter).unwrap();
    let agent = ToolCallingAgent::new(model, empty_tools(), AgentConfig::new(1).unwrap()).unwrap();

    let mut stream = agent.stream(vec![Message::user("hi")]);
    let mut saw_error = false;
    while let Some(event) = stream.next().await {
        if event.is_err() {
            saw_error = true;
            break;
        }
    }
    assert!(saw_error, "protocol error must terminate stream with Err");
}

#[tokio::test]
async fn approval_suspension_emits_approval_required_event() {
    let call_id = ToolCallId::new("call-1").unwrap();
    let tool_name = ToolName::new("lookup").unwrap();

    let round1_events = vec![
        Ok(ChatStreamEvent::ResponseStarted {
            response_id: None,
            model: None,
            extensions: group_agent_model::Extensions::new(),
        }),
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_id(call_id.clone())
                .with_name(tool_name.clone())
                .with_arguments_fragment(r#"{"item":"sample"}"#),
        )),
        Ok(ChatStreamEvent::Finished(FinishReason::ToolCalls)),
    ];

    let adapter = VectorStreamAdapter {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("stream-model").unwrap(),
            ModelCapabilities::new()
                .with_streaming(true)
                .with_tool_calling(true),
        ),
        events_per_call: Arc::new(Mutex::new(VecDeque::from(vec![round1_events]))),
        drop_flag: None,
    };

    let model = ChatModel::from_adapter(adapter).unwrap();
    let config = AgentConfig::new(2).unwrap().with_tool_approval(true);
    let agent = ToolCallingAgent::new(model, lookup_runtime(), config).unwrap();

    let observed = Arc::new(Mutex::new(Vec::new()));
    let sink_observed = Arc::clone(&observed);

    // Non-durable invoke fails closed with InterruptRequiresCheckpoint, but
    // ApprovalRequired event was emitted before attempting suspension!
    let res = agent
        .invoke_with_stream_sink(
            vec![Message::user("search for sample")],
            Arc::new(move |event: &AgentStreamEvent| {
                sink_observed.lock().unwrap().push(event.clone());
            }),
        )
        .await;

    assert!(
        res.is_err(),
        "non-durable invoke of approval agent fails closed"
    );

    let captured = observed.lock().unwrap().clone();
    let has_approval_event = captured
        .iter()
        .any(|e| matches!(e, AgentStreamEvent::ApprovalRequired { .. }));
    assert!(
        has_approval_event,
        "ApprovalRequired event must be emitted before suspension"
    );
}

#[tokio::test]
async fn dropping_stream_drops_underlying_model_stream_immediately() {
    let drop_flag = Arc::new(AtomicBool::new(false));

    // Multiple chunks
    let events: Vec<Result<ChatStreamEvent, ModelError>> = (0..100)
        .map(|i| Ok(ChatStreamEvent::TextDelta(format!("chunk-{i}"))))
        .collect();

    let adapter = VectorStreamAdapter {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("stream-model").unwrap(),
            ModelCapabilities::new().with_streaming(true),
        ),
        events_per_call: Arc::new(Mutex::new(VecDeque::from(vec![events]))),
        drop_flag: Some(drop_flag.clone()),
    };

    let model = ChatModel::from_adapter(adapter).unwrap();
    let agent = ToolCallingAgent::new(model, empty_tools(), AgentConfig::new(1).unwrap()).unwrap();

    let mut stream = agent.stream(vec![Message::user("hi")]);

    // Read initial event
    let event1 = stream.next().await.expect("has event");
    assert!(event1.is_ok());

    assert!(!drop_flag.load(Ordering::SeqCst));

    // Drop stream mid-run
    drop(stream);

    // Verify drop flag was set immediately
    assert!(drop_flag.load(Ordering::SeqCst));
}

#[tokio::test]
async fn stream_fuses_after_error_and_after_completion() {
    // 1. Test fusion after completion
    let events: Vec<Result<ChatStreamEvent, ModelError>> = vec![
        Ok(ChatStreamEvent::TextDelta("hello".to_string())),
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
    ];
    let adapter = VectorStreamAdapter {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("stream-model").unwrap(),
            ModelCapabilities::new().with_streaming(true),
        ),
        events_per_call: Arc::new(Mutex::new(VecDeque::from(vec![events]))),
        drop_flag: None,
    };
    let model = ChatModel::from_adapter(adapter).unwrap();
    let agent = ToolCallingAgent::new(model, empty_tools(), AgentConfig::new(1).unwrap()).unwrap();

    let mut stream = agent.stream(vec![Message::user("hi")]);
    let mut count = 0;
    while let Some(item) = stream.next().await {
        assert!(item.is_ok());
        count += 1;
    }
    assert!(count > 0);
    // After stream ended, subsequent polls return None
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());

    // 2. Test fusion after error
    let err_adapter = VectorStreamAdapter {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("stream-model").unwrap(),
            ModelCapabilities::new().with_streaming(false), // fails closed
        ),
        events_per_call: Arc::new(Mutex::new(VecDeque::new())),
        drop_flag: None,
    };
    let err_model = ChatModel::from_adapter(err_adapter).unwrap();
    let err_agent =
        ToolCallingAgent::new(err_model, empty_tools(), AgentConfig::new(1).unwrap()).unwrap();

    let mut err_stream = err_agent.stream(vec![Message::user("hi")]);
    let first = err_stream.next().await.expect("yields error item");
    assert!(first.is_err());
    // Fused: subsequent polls return None without yielding phantom events
    assert!(err_stream.next().await.is_none());
    assert!(err_stream.next().await.is_none());
}
