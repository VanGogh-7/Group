use std::error::Error;
use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use group_agent_core::{EventConfig, GraphRunError, RunControl};
use group_agent_model::{
    ChatEventStream, ChatModel, ChatModelAdapter, ChatResponse, ChatStreamEvent, FinishReason,
    Message, ModelCapabilities, ModelError, ModelId, ModelMetadata, ProviderId, ToolCallDelta,
    ToolCallId, ToolDefinition, ToolName, ValidatedChatRequest,
};
use group_agent_prebuilt::{AgentConfig, AgentStreamEvent, ToolCallingAgent};
use group_agent_tool::{
    Tool, ToolBehavior, ToolError, ToolInput, ToolOutput, ToolRegistry, ToolRuntime,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Probe {
    started: Notify,
    entered: AtomicUsize,
    dropped: AtomicUsize,
}

struct Guard(Arc<Probe>);
impl Guard {
    fn enter(probe: &Arc<Probe>) -> Self {
        probe.entered.fetch_add(1, Ordering::SeqCst);
        probe.started.notify_one();
        Self(Arc::clone(probe))
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingModel {
    metadata: ModelMetadata,
    probe: Arc<Probe>,
    pending_model: bool,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl ChatModelAdapter for PendingModel {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }
    async fn complete_raw(&self, _: ValidatedChatRequest) -> Result<ChatResponse, ModelError> {
        panic!("streaming must never fall back to completion")
    }
    async fn stream_raw(&self, _: ValidatedChatRequest) -> Result<ChatEventStream, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.pending_model {
            let probe = Arc::clone(&self.probe);
            Ok(Box::pin(stream::once(async move {
                let _guard = Guard::enter(&probe);
                pending::<Result<ChatStreamEvent, ModelError>>().await
            })))
        } else {
            Ok(Box::pin(stream::iter([
                Ok(ChatStreamEvent::ToolCallDelta(
                    ToolCallDelta::new(0)
                        .with_id(ToolCallId::new("pending-call").unwrap())
                        .with_name(ToolName::new("pending-tool").unwrap())
                        .with_arguments_fragment("{}"),
                )),
                Ok(ChatStreamEvent::Finished(FinishReason::ToolCalls)),
            ])))
        }
    }
}

struct PendingTool {
    definition: ToolDefinition,
    probe: Arc<Probe>,
}
#[async_trait]
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
    async fn execute(&self, _: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        let _guard = Guard::enter(&self.probe);
        pending().await
    }
}

#[derive(Clone, Copy, Debug)]
enum Stop {
    Cancel,
    RunTimeout,
    NodeTimeout,
    Drop,
}

async fn exercise_control(stop: Stop) {
    for pending_model in [true, false] {
        for use_sink in [false, true] {
            let probe = Arc::new(Probe::default());
            let calls = Arc::new(AtomicUsize::new(0));
            let model = ChatModel::from_adapter(PendingModel {
                metadata: ModelMetadata::new(
                    ProviderId::new("offline").unwrap(),
                    ModelId::new("controlled-stream").unwrap(),
                    ModelCapabilities::new()
                        .with_streaming(true)
                        .with_tool_calling(true),
                ),
                probe: Arc::clone(&probe),
                pending_model,
                calls: Arc::clone(&calls),
            })
            .unwrap();
            let mut registry = ToolRegistry::builder();
            if !pending_model {
                registry
                    .register(PendingTool {
                        definition: ToolDefinition::new(
                            ToolName::new("pending-tool").unwrap(),
                            "Wait for cancellation",
                            serde_json::json!({"type":"object"}),
                        ),
                        probe: Arc::clone(&probe),
                    })
                    .unwrap();
            }
            let agent = ToolCallingAgent::new(
                model,
                ToolRuntime::new(registry.build()),
                AgentConfig::new(2).unwrap(),
            )
            .unwrap();
            let token = CancellationToken::new();
            let duration = Duration::from_secs(5);
            let control = match stop {
                Stop::Cancel => RunControl::new().with_cancellation_token(token.clone()),
                Stop::RunTimeout => RunControl::new().with_run_timeout(duration),
                Stop::NodeTimeout => RunControl::new().with_node_timeout(duration),
                Stop::Drop => RunControl::new(),
            };
            let events = Arc::new(Mutex::new(Vec::new()));
            let observed = Arc::clone(&events);
            let mut invocation = Box::pin(async {
                if use_sink {
                    agent
                        .invoke_with_stream_sink_control(
                            vec![Message::user("wait")],
                            Arc::new(move |event: &AgentStreamEvent| {
                                observed.lock().unwrap().push(event.clone());
                            }),
                            EventConfig::default(),
                            control,
                        )
                        .await
                        .map(|_| ())
                } else {
                    let mut stream = agent.stream_with_control(
                        vec![Message::user("wait")],
                        EventConfig::default(),
                        control,
                    );
                    while let Some(event) = stream.next().await {
                        observed.lock().unwrap().push(event?);
                    }
                    Ok(())
                }
            });
            tokio::select! {
                result = &mut invocation => panic!("unexpected completion: {result:?}"),
                () = probe.started.notified() => {}
            }
            assert_eq!(probe.entered.load(Ordering::SeqCst), 1);
            assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
            match stop {
                Stop::Drop => drop(invocation),
                _ => {
                    if matches!(stop, Stop::Cancel) {
                        token.cancel();
                    } else {
                        tokio::time::advance(duration).await;
                    }
                    let error = invocation.await.expect_err("control must fail the run");
                    let graph = error
                        .source()
                        .unwrap()
                        .downcast_ref::<GraphRunError>()
                        .unwrap();
                    let step = if pending_model { 1 } else { 2 };
                    match stop {
                        Stop::Cancel => assert!(
                            matches!(graph, GraphRunError::Cancelled { step: s, .. } if *s == step)
                        ),
                        Stop::RunTimeout => assert!(
                            matches!(graph, GraphRunError::RunTimedOut { step: s, timeout, .. } if *s == step && *timeout == duration)
                        ),
                        Stop::NodeTimeout => assert!(
                            matches!(graph, GraphRunError::NodeTimedOut { step: s, timeout, .. } if *s == step && *timeout == duration)
                        ),
                        Stop::Drop => unreachable!(),
                    }
                    assert!(error.tool_batch_report().is_none());
                }
            }
            assert_eq!(
                probe.dropped.load(Ordering::SeqCst),
                1,
                "{stop:?}, model={pending_model}, sink={use_sink}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1, "no subsequent model round");
            let events = events.lock().unwrap();
            assert!(!events.iter().any(|event| matches!(
                event,
                AgentStreamEvent::Completed(_) | AgentStreamEvent::ToolCompleted { .. }
            )));
            if pending_model {
                assert!(
                    !events
                        .iter()
                        .any(|event| matches!(event, AgentStreamEvent::ModelCompleted { .. }))
                );
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_drops_pending_model_and_tool_in_both_streaming_interfaces() {
    exercise_control(Stop::Cancel).await;
}
#[tokio::test(start_paused = true)]
async fn run_timeout_drops_pending_model_and_tool_with_typed_source() {
    exercise_control(Stop::RunTimeout).await;
}
#[tokio::test(start_paused = true)]
async fn node_timeout_drops_pending_model_and_tool_with_typed_source() {
    exercise_control(Stop::NodeTimeout).await;
}
#[tokio::test(start_paused = true)]
async fn dropping_stream_or_sink_future_drops_pending_model_and_tool_immediately() {
    exercise_control(Stop::Drop).await;
}
