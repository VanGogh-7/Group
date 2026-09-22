#[path = "../test_support/streaming_agent.rs"]
mod fixture;

use async_trait::async_trait;
use fixture::{Events, Probe, agent, config, model, store};
use futures_util::StreamExt;
use group_agent_core::{Checkpointer, ResumeConfig, ThreadId};
use group_agent_model::{Message, ToolDefinition, ToolName};
use group_agent_prebuilt::{
    AgentApprovalDecision, AgentConfig, AgentEventSink, AgentStreamEvent, ToolCallingAgent,
};
use group_agent_tool::{
    Tool, ToolBehavior, ToolError, ToolInput, ToolOutput, ToolRegistry, ToolRuntime,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn ordinary_and_streaming_checkpoints_can_switch_invocation_modes() {
    for start_streaming in [false, true] {
        let (agent, probe) = agent(true, 2);
        let store = store();
        let old_sink = Arc::new(Events::default());
        if start_streaming {
            agent
                .invoke_with_checkpoint_stream_sink(
                    vec![Message::user("question")],
                    config("switch", &store),
                    old_sink.clone(),
                )
                .await
                .unwrap();
        } else {
            agent
                .invoke_with_checkpoint(vec![Message::user("question")], config("switch", &store))
                .await
                .unwrap();
        }
        let before = old_sink.snapshot();
        let cfg =
            ResumeConfig::new("switch", store).with_resume_value(AgentApprovalDecision::Approve);
        if start_streaming {
            let result = agent.resume(cfg).await.unwrap();
            assert_eq!(result.as_completed().unwrap().model_rounds(), 2);
        } else {
            let events = agent.resume_stream(cfg).collect::<Vec<_>>().await;
            assert!(matches!(
                events.last(),
                Some(Ok(AgentStreamEvent::Completed(_)))
            ));
        }
        assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
        assert_eq!(probe.completions.load(Ordering::SeqCst), 1);
        assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
        assert_eq!(old_sink.snapshot(), before);
    }
}

#[tokio::test]
async fn interleaved_resumes_use_distinct_sinks_and_decisions_on_one_agent() {
    let (agent, probe) = agent(true, 2);
    let store = store();
    for thread in ["approved", "rejected"] {
        agent
            .invoke_with_checkpoint(vec![Message::user(thread)], config(thread, &store))
            .await
            .unwrap();
    }
    let sink = Arc::new(Events::default());
    let approved = agent.resume_with_stream_sink(
        ResumeConfig::new("approved", store.clone())
            .with_resume_value(AgentApprovalDecision::Approve),
        sink.clone(),
    );
    let rejected = agent.resume_stream(
        ResumeConfig::new("rejected", store.clone())
            .with_resume_value(AgentApprovalDecision::Reject),
    );
    let (approved, rejected) = tokio::join!(approved, rejected.collect::<Vec<_>>());
    let approved = approved.unwrap();
    assert!(
        !approved.as_completed().unwrap().messages()[2]
            .as_tool()
            .unwrap()
            .result()
            .is_error()
    );
    let rejected: Vec<_> = rejected.into_iter().map(Result::unwrap).collect();
    let AgentStreamEvent::Completed(rejected_outcome) = rejected.last().unwrap() else {
        panic!("completed")
    };
    assert!(
        rejected_outcome.messages()[2]
            .as_tool()
            .unwrap()
            .result()
            .is_error()
    );
    assert!(!rejected.iter().any(|e| matches!(
        e,
        AgentStreamEvent::ToolStarted { .. } | AgentStreamEvent::ToolCompleted { .. }
    )));
    assert_eq!(
        sink.snapshot()
            .iter()
            .filter(|e| matches!(e, AgentStreamEvent::ToolStarted { .. }))
            .count(),
        1
    );
    assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 2);
    assert_eq!(probe.completions.load(Ordering::SeqCst), 2);
    for thread in ["approved", "rejected"] {
        assert!(
            store
                .latest(&ThreadId::from(thread))
                .await
                .unwrap()
                .unwrap()
                .completed()
        );
    }
}

struct NestedTool {
    definition: ToolDefinition,
    inner: ToolCallingAgent,
    resume: ResumeConfig<group_agent_prebuilt::AgentSnapshot>,
    sink: Arc<Events>,
    outer_events: Arc<Events>,
    nested_streaming: bool,
    executions: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for NestedTool {
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
        self.executions.fetch_add(1, Ordering::SeqCst);
        let before = self.outer_events.snapshot();
        let outcome = if self.nested_streaming {
            self.inner
                .resume_with_stream_sink(self.resume.clone(), self.sink.clone())
                .await
                .unwrap()
        } else {
            self.inner.resume(self.resume.clone()).await.unwrap()
        };
        assert!(outcome.is_completed());
        assert_eq!(
            self.outer_events.snapshot(),
            before,
            "nested run never emits into outer sink"
        );
        Ok(ToolOutput::success_text("nested result"))
    }
}

#[tokio::test]
async fn nested_streaming_and_ordinary_resumes_do_not_inherit_outer_sink() {
    for nested_streaming in [false, true] {
        let (inner, inner_probe) = agent(true, 2);
        let inner_store = store();
        inner
            .invoke_with_checkpoint(vec![Message::user("inner")], config("inner", &inner_store))
            .await
            .unwrap();
        let inner_sink = Arc::new(Events::default());
        let outer_sink = Arc::new(Events::default());
        let executions = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::builder();
        registry
            .register(NestedTool {
                definition: ToolDefinition::new(
                    ToolName::new("lookup").unwrap(),
                    "Nested resume",
                    serde_json::json!({"type":"object"}),
                ),
                inner,
                resume: ResumeConfig::new("inner", inner_store)
                    .with_resume_value(AgentApprovalDecision::Approve),
                sink: inner_sink.clone(),
                outer_events: outer_sink.clone(),
                nested_streaming,
                executions: executions.clone(),
            })
            .unwrap();
        let outer = ToolCallingAgent::new(
            model(&Arc::new(Probe::default())),
            ToolRuntime::new(registry.build()),
            AgentConfig::new(2).unwrap().with_tool_approval(true),
        )
        .unwrap();
        let outer_store = store();
        outer
            .invoke_with_checkpoint(vec![Message::user("outer")], config("outer", &outer_store))
            .await
            .unwrap();
        let sink: Arc<dyn AgentEventSink> = outer_sink.clone();
        outer
            .resume_with_stream_sink(
                ResumeConfig::new("outer", outer_store)
                    .with_resume_value(AgentApprovalDecision::Approve),
                sink,
            )
            .await
            .unwrap();
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        assert_eq!(
            inner_probe.streams.load(Ordering::SeqCst),
            usize::from(nested_streaming)
        );
        assert_eq!(inner_sink.snapshot().is_empty(), !nested_streaming);
        assert_eq!(
            outer_sink
                .snapshot()
                .iter()
                .filter(|e| matches!(e, AgentStreamEvent::Completed(_)))
                .count(),
            1
        );
    }
}
