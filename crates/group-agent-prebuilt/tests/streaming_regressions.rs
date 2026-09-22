use std::collections::VecDeque;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use group_agent_model::{
    ChatEventStream, ChatModel, ChatModelAdapter, ChatResponse, ChatStreamEvent, FinishReason,
    Message, ModelCapabilities, ModelError, ModelId, ModelMetadata, ProviderId,
    StreamProtocolError, ToolCallDelta, ToolCallId, ToolDefinition, ToolName, ValidatedChatRequest,
};
use group_agent_prebuilt::{
    AgentConfig, AgentError, AgentOutcome, AgentStreamEvent, ToolCallingAgent,
};
use group_agent_tool::{
    Tool, ToolBehavior, ToolError, ToolErrorKind, ToolEvent, ToolInput, ToolObserverError,
    ToolObserverFailure, ToolObserverFailureKind, ToolOutput, ToolRegistry, ToolRuntime,
    ToolRuntimeErrorKind,
};
use serde_json::json;

struct ScriptedModel {
    metadata: ModelMetadata,
    turns: Mutex<VecDeque<Vec<ChatStreamEvent>>>,
}

#[async_trait]
impl ChatModelAdapter for ScriptedModel {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    async fn complete_raw(&self, _: ValidatedChatRequest) -> Result<ChatResponse, ModelError> {
        panic!("streaming must not call complete")
    }

    async fn stream_raw(&self, _: ValidatedChatRequest) -> Result<ChatEventStream, ModelError> {
        let events = self
            .turns
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted turn");
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}

fn model(turns: Vec<Vec<ChatStreamEvent>>) -> ChatModel {
    ChatModel::from_adapter(ScriptedModel {
        metadata: ModelMetadata::new(
            ProviderId::new("offline").unwrap(),
            ModelId::new("regression").unwrap(),
            ModelCapabilities::new()
                .with_streaming(true)
                .with_tool_calling(true),
        ),
        turns: Mutex::new(turns.into()),
    })
    .unwrap()
}

fn call_delta() -> ToolCallDelta {
    ToolCallDelta::new(0)
        .with_id(ToolCallId::new("call-1").unwrap())
        .with_name(ToolName::new("lookup").unwrap())
        .with_arguments_fragment("{}")
}

fn tool_turns() -> Vec<Vec<ChatStreamEvent>> {
    vec![
        vec![
            ChatStreamEvent::ToolCallDelta(call_delta()),
            ChatStreamEvent::Finished(FinishReason::ToolCalls),
        ],
        vec![
            ChatStreamEvent::TextDelta("done".into()),
            ChatStreamEvent::Finished(FinishReason::Stop),
        ],
    ]
}

struct Lookup {
    calls: Arc<AtomicUsize>,
    fail: bool,
}

#[async_trait]
impl Tool for Lookup {
    fn name(&self) -> &ToolName {
        static NAME: std::sync::OnceLock<ToolName> = std::sync::OnceLock::new();
        NAME.get_or_init(|| ToolName::new("lookup").unwrap())
    }

    fn definition(&self) -> &ToolDefinition {
        static DEF: std::sync::OnceLock<ToolDefinition> = std::sync::OnceLock::new();
        DEF.get_or_init(|| {
            ToolDefinition::new(
                self.name().clone(),
                "Offline lookup",
                json!({"type":"object"}),
            )
        })
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    async fn execute(&self, _: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            Err(ToolError::new(ToolErrorKind::Other, "offline failure"))
        } else {
            Ok(ToolOutput::success_text("found"))
        }
    }
}

fn runtime(fail: bool) -> (ToolRuntime, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ToolRegistry::builder();
    registry
        .register(Lookup {
            calls: calls.clone(),
            fail,
        })
        .unwrap();
    (ToolRuntime::new(registry.build()), calls)
}

async fn invoke(
    agent: &ToolCallingAgent,
    use_sink: bool,
    durable: bool,
) -> (Vec<AgentStreamEvent>, Result<AgentOutcome, AgentError>) {
    if use_sink {
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = events.clone();
        let sink = Arc::new(move |event: &AgentStreamEvent| {
            observed.lock().unwrap().push(event.clone());
        });
        let result = if durable {
            agent
                .invoke_with_checkpoint_stream_sink(
                    vec![Message::user("lookup")],
                    checkpoint_config(),
                    sink,
                )
                .await
                .map(|outcome| outcome.as_completed().unwrap().clone())
        } else {
            agent
                .invoke_with_stream_sink(vec![Message::user("lookup")], sink)
                .await
        };
        let captured = events.lock().unwrap().clone();
        (captured, result)
    } else {
        let mut events = Vec::new();
        let mut result = None;
        let mut stream = if durable {
            agent.stream_with_checkpoint(vec![Message::user("lookup")], checkpoint_config())
        } else {
            agent.stream(vec![Message::user("lookup")])
        };
        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => {
                    if let AgentStreamEvent::Completed(outcome) = &event {
                        result = Some(Ok(outcome.clone()));
                    }
                    events.push(event);
                }
                Err(error) => {
                    assert!(result.is_none());
                    result = Some(Err(error));
                }
            }
        }
        assert!(stream.next().await.is_none());
        (events, result.expect("terminal result"))
    }
}

fn source<'a, T: Error + 'static>(error: &'a (dyn Error + 'static)) -> Option<&'a T> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(found) = error.downcast_ref::<T>() {
            return Some(found);
        }
        current = error.source();
    }
    None
}

#[tokio::test]
async fn configured_observer_receives_streaming_tool_events() {
    for (use_sink, durable) in [(false, false), (true, false), (false, true), (true, true)] {
        let (runtime, calls) = runtime(false);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let runtime = runtime.with_event_sink(Arc::new(move |event: &ToolEvent| {
            captured.lock().unwrap().push(event.clone());
            Ok(())
        }));
        let agent =
            ToolCallingAgent::new(model(tool_turns()), runtime, AgentConfig::new(2).unwrap())
                .unwrap();
        let (_, result) = invoke(&agent, use_sink, durable).await;
        assert_eq!(
            result.unwrap().final_message().unwrap().text_content(),
            "done"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let events = observed.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], ToolEvent::ExecutionStarted { .. }));
        assert!(matches!(
            events[1],
            ToolEvent::ExecutionCompleted {
                is_error: false,
                ..
            }
        ));
    }
}

#[tokio::test]
async fn observer_start_error_or_panic_prevents_execution_during_streaming() {
    for (use_sink, durable) in [(false, false), (true, false), (false, true), (true, true)] {
        for panics in [false, true] {
            let (runtime, calls) = runtime(false);
            let runtime = runtime.with_event_sink(Arc::new(move |event: &ToolEvent| {
                if matches!(event, ToolEvent::ExecutionStarted { .. }) {
                    assert!(!panics, "offline observer panic");
                    return Err(ToolObserverError::new("offline observer failure"));
                }
                Ok(())
            }));
            let agent =
                ToolCallingAgent::new(model(tool_turns()), runtime, AgentConfig::new(2).unwrap())
                    .unwrap();
            let (events, result) = invoke(&agent, use_sink, durable).await;
            let error = result.expect_err("start observer failure must prevent execution");
            let report = error.tool_batch_report().unwrap();
            let failure = report.results()[0].as_ref().unwrap_err();
            assert_eq!(failure.kind(), ToolRuntimeErrorKind::ObserverFailed);
            assert_eq!(
                source::<ToolObserverFailure>(failure).unwrap().kind(),
                if panics {
                    ToolObserverFailureKind::Panicked
                } else {
                    ToolObserverFailureKind::ReturnedError
                }
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(!events.iter().any(|event| matches!(
                event,
                AgentStreamEvent::ToolStarted { .. }
                    | AgentStreamEvent::ToolCompleted { .. }
                    | AgentStreamEvent::Completed(_)
            )));
        }
    }
}

#[tokio::test]
async fn terminal_observer_failure_preserves_tool_outcome_and_failure_report() {
    for (use_sink, durable) in [(false, false), (true, false), (false, true), (true, true)] {
        for fail in [false, true] {
            let (runtime, calls) = runtime(fail);
            let terminals = Arc::new(AtomicUsize::new(0));
            let captured = terminals.clone();
            let runtime = runtime.with_event_sink(Arc::new(move |event: &ToolEvent| {
                if !matches!(event, ToolEvent::ExecutionStarted { .. }) {
                    captured.fetch_add(1, Ordering::SeqCst);
                    return Err(ToolObserverError::new("terminal observer failure"));
                }
                Ok(())
            }));
            let agent =
                ToolCallingAgent::new(model(tool_turns()), runtime, AgentConfig::new(2).unwrap())
                    .unwrap();
            let (events, result) = invoke(&agent, use_sink, durable).await;
            if fail {
                let error = result.unwrap_err();
                let report = error.tool_batch_report().unwrap();
                assert_eq!(
                    report.results()[0].as_ref().unwrap_err().kind(),
                    ToolRuntimeErrorKind::ExecutionFailed
                );
                assert!(source::<ToolError>(report.results()[0].as_ref().unwrap_err()).is_some());
                assert_eq!(
                    report.terminal_observer_failures()[0]
                        .as_ref()
                        .unwrap()
                        .kind(),
                    ToolObserverFailureKind::ReturnedError
                );
            } else {
                assert_eq!(
                    result.unwrap().final_message().unwrap().text_content(),
                    "done"
                );
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(terminals.load(Ordering::SeqCst), 1);
            let completed = events.iter().filter(|event| matches!(event, AgentStreamEvent::ToolCompleted { is_error, .. } if *is_error == fail)).count();
            assert_eq!(completed, 1);
        }
    }
}

#[tokio::test]
async fn failed_tool_emits_terminal_event_before_agent_error() {
    for (use_sink, durable) in [(false, false), (true, false), (false, true), (true, true)] {
        let (runtime, calls) = runtime(true);
        let agent =
            ToolCallingAgent::new(model(tool_turns()), runtime, AgentConfig::new(2).unwrap())
                .unwrap();
        let (events, result) = invoke(&agent, use_sink, durable).await;
        let error = result.unwrap_err();
        let report = error.tool_batch_report().unwrap();
        assert_eq!(
            report.results()[0].as_ref().unwrap_err().kind(),
            ToolRuntimeErrorKind::ExecutionFailed
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let lifecycle: Vec<_> = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    AgentStreamEvent::ToolStarted { .. } | AgentStreamEvent::ToolCompleted { .. }
                )
            })
            .collect();
        assert_eq!(
            lifecycle,
            vec![
                &AgentStreamEvent::ToolStarted {
                    id: ToolCallId::new("call-1").unwrap(),
                    name: ToolName::new("lookup").unwrap(),
                    round: 1
                },
                &AgentStreamEvent::ToolCompleted {
                    id: ToolCallId::new("call-1").unwrap(),
                    name: ToolName::new("lookup").unwrap(),
                    round: 1,
                    is_error: true
                },
            ]
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AgentStreamEvent::Completed(_)))
        );
    }
}

#[tokio::test]
async fn rejected_protocol_deltas_are_not_published() {
    for (use_sink, durable) in [(false, false), (true, false), (false, true), (true, true)] {
        for case in 0..3 {
            let (turn, accepted_deltas) = match case {
                0 => (
                    vec![
                        ChatStreamEvent::TextDelta("valid".into()),
                        ChatStreamEvent::Finished(FinishReason::Stop),
                        ChatStreamEvent::TextDelta("invalid".into()),
                    ],
                    1,
                ),
                1 => (
                    vec![
                        ChatStreamEvent::ToolCallDelta(call_delta()),
                        ChatStreamEvent::ToolCallDelta(
                            ToolCallDelta::new(0).with_id(ToolCallId::new("duplicate").unwrap()),
                        ),
                    ],
                    1,
                ),
                _ => (
                    vec![ChatStreamEvent::ToolCallDelta(ToolCallDelta::new(u32::MAX))],
                    0,
                ),
            };
            let agent = ToolCallingAgent::new(
                model(vec![turn]),
                ToolRuntime::new(ToolRegistry::empty()),
                AgentConfig::new(1).unwrap(),
            )
            .unwrap();
            let (events, result) = invoke(&agent, use_sink, durable).await;
            let error = result.unwrap_err();
            let protocol =
                source::<StreamProtocolError>(&error).expect("typed protocol error retained");
            assert!(matches!(
                (case, protocol),
                (0, StreamProtocolError::EventAfterFinished { .. })
                    | (1, StreamProtocolError::DuplicateToolCallField { .. })
                    | (2, StreamProtocolError::ToolCallIndexTooLarge { .. })
            ));
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(
                        event,
                        AgentStreamEvent::TextDelta { .. } | AgentStreamEvent::ToolCallDelta { .. }
                    ))
                    .count(),
                accepted_deltas
            );
            assert!(!events.iter().any(|event| matches!(
                event,
                AgentStreamEvent::ModelCompleted { .. } | AgentStreamEvent::Completed(_)
            )));
        }
    }
}

#[tokio::test]
async fn unstarted_tool_failure_does_not_emit_execution_lifecycle() {
    for (use_sink, durable) in [(false, false), (true, false), (false, true), (true, true)] {
        let agent = ToolCallingAgent::new(
            model(tool_turns()),
            ToolRuntime::new(ToolRegistry::empty()),
            AgentConfig::new(2).unwrap(),
        )
        .unwrap();
        let (events, result) = invoke(&agent, use_sink, durable).await;
        let error = result.unwrap_err();
        let report = error.tool_batch_report().unwrap();
        assert_eq!(
            report.results()[0].as_ref().unwrap_err().kind(),
            ToolRuntimeErrorKind::ToolNotFound
        );
        assert!(!events.iter().any(|event| matches!(
            event,
            AgentStreamEvent::ToolStarted { .. } | AgentStreamEvent::ToolCompleted { .. }
        )));
    }
}

fn checkpoint_config() -> group_agent_core::CheckpointConfig<group_agent_prebuilt::AgentSnapshot> {
    group_agent_core::CheckpointConfig::new(
        "stream-regression",
        Arc::new(group_agent_core::InMemoryCheckpointer::new(
            group_agent_prebuilt::AgentSnapshotCodec,
        )),
        group_agent_core::CheckpointPolicy::EverySuperstep,
    )
}
