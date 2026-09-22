#[path = "../test_support/streaming_agent.rs"]
mod fixture;

use async_trait::async_trait;
use fixture::{Events, agent, config, store};
use futures_util::StreamExt;
use group_agent_core::{
    Checkpoint, CheckpointConfig, CheckpointId, CheckpointPolicy, CheckpointRequest,
    CheckpointWriteError, Checkpointer, CheckpointerError, GraphRunError, InMemoryCheckpointer,
    ResumeConfig, ThreadId,
};
use group_agent_model::Message;
use group_agent_prebuilt::{AgentApprovalDecision, AgentSnapshot, AgentStreamEvent};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[derive(Debug)]
struct SaveFailure;
impl fmt::Display for SaveFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fixture save failure")
    }
}
impl Error for SaveFailure {}

struct FailingStore {
    inner: Arc<InMemoryCheckpointer<AgentSnapshot>>,
    fail_interrupt: bool,
}
#[async_trait]
impl Checkpointer<AgentSnapshot> for FailingStore {
    async fn save(
        &self,
        request: CheckpointRequest<AgentSnapshot>,
    ) -> Result<Arc<Checkpoint<AgentSnapshot>>, CheckpointWriteError> {
        if (self.fail_interrupt && request.interrupt().is_some())
            || (!self.fail_interrupt && request.completed())
        {
            return Err(CheckpointerError::with_source("save failed", SaveFailure).into());
        }
        self.inner.save(request).await
    }
    async fn latest(
        &self,
        thread: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<AgentSnapshot>>>, CheckpointerError> {
        self.inner.latest(thread).await
    }
    async fn get(
        &self,
        thread: &ThreadId,
        id: CheckpointId,
    ) -> Result<Option<Arc<Checkpoint<AgentSnapshot>>>, CheckpointerError> {
        self.inner.get(thread, id).await
    }
    async fn history(
        &self,
        thread: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<AgentSnapshot>>>, CheckpointerError> {
        self.inner.history(thread).await
    }
}
fn has_source<T: Error + 'static>(error: &(dyn Error + 'static)) -> bool {
    let mut current = error.source();
    while let Some(error) = current {
        if error.is::<T>() {
            return true;
        }
        current = error.source();
    }
    false
}

#[tokio::test]
async fn failed_interrupt_or_final_save_never_publishes_durable_success() {
    for fail_interrupt in [false, true] {
        for use_sink in [false, true] {
            let (agent, probe) = agent(fail_interrupt, 2);
            let store = Arc::new(FailingStore {
                inner: store(),
                fail_interrupt,
            });
            let cfg =
                CheckpointConfig::new("save-fail", store.clone(), CheckpointPolicy::EverySuperstep);
            let (events, error) = if use_sink {
                let sink = Arc::new(Events::default());
                let error = agent
                    .invoke_with_checkpoint_stream_sink(
                        vec![Message::user("question")],
                        cfg,
                        sink.clone(),
                    )
                    .await
                    .unwrap_err();
                (sink.snapshot(), error)
            } else {
                let mut stream = agent.stream_with_checkpoint(vec![Message::user("question")], cfg);
                let mut events = Vec::new();
                let error = loop {
                    match stream.next().await.expect("must terminate with error") {
                        Ok(event) => events.push(event),
                        Err(error) => break error,
                    }
                };
                assert!(stream.next().await.is_none());
                (events, error)
            };
            assert!(has_source::<SaveFailure>(&error));
            assert!(matches!(
                error.source().unwrap().downcast_ref::<GraphRunError>(),
                Some(GraphRunError::CheckpointSaveFailed { .. })
            ));
            assert!(!events.iter().any(|e| matches!(
                e,
                AgentStreamEvent::Interrupted(_) | AgentStreamEvent::Completed(_)
            )));
            assert_eq!(
                events
                    .iter()
                    .any(|e| matches!(e, AgentStreamEvent::ApprovalRequired { .. })),
                fail_interrupt
            );
            assert_eq!(
                probe.executions.load(Ordering::SeqCst),
                usize::from(!fail_interrupt)
            );
            let head = store
                .latest(&ThreadId::from("save-fail"))
                .await
                .unwrap()
                .unwrap();
            assert!(!head.completed());
            assert!(head.interrupt().is_none());
            assert_eq!(
                store
                    .history(&ThreadId::from("save-fail"))
                    .await
                    .unwrap()
                    .len(),
                if fail_interrupt { 1 } else { 2 }
            );
        }
    }
}

#[tokio::test]
async fn bad_resume_decisions_and_stale_targets_do_not_execute_tools() {
    for use_sink in [false, true] {
        let (agent, probe) = agent(true, 2);
        let store = store();
        let outcome = agent
            .invoke_with_checkpoint(vec![Message::user("question")], config("decision", &store))
            .await
            .unwrap();
        let id = outcome.as_interrupted().unwrap().checkpoint_id();
        let history = store.history(&ThreadId::from("decision")).await.unwrap();
        for (case, cfg) in [
            ResumeConfig::new("decision", store.clone()),
            ResumeConfig::new("decision", store.clone()).with_resume_value(42u32),
            ResumeConfig::new("decision", store.clone()).with_checkpoint_id(history[0].id()),
        ]
        .into_iter()
        .enumerate()
        {
            let (events, error) = if use_sink {
                let sink = Arc::new(Events::default());
                let error = agent
                    .resume_with_stream_sink(cfg, sink.clone())
                    .await
                    .unwrap_err();
                (sink.snapshot(), error)
            } else {
                let mut stream = agent.resume_stream(cfg);
                let error = stream.next().await.unwrap().unwrap_err();
                assert!(stream.next().await.is_none());
                (Vec::new(), error)
            };
            assert!(events.is_empty());
            let graph = error
                .source()
                .unwrap()
                .downcast_ref::<GraphRunError>()
                .unwrap();
            match case {
                0 => assert!(matches!(graph, GraphRunError::MissingResumeValue { .. })),
                1 => assert!(has_source::<group_agent_core::ResumeValueError>(&error)),
                2 => assert!(matches!(graph, GraphRunError::ResumeConflict { .. })),
                _ => unreachable!(),
            }
            assert_eq!(probe.executions.load(Ordering::SeqCst), 0);
            assert_eq!(probe.streams.load(Ordering::SeqCst), 0);
            assert_eq!(
                store
                    .latest(&ThreadId::from("decision"))
                    .await
                    .unwrap()
                    .unwrap()
                    .id(),
                id
            );
        }
        agent
            .resume_with_stream_sink(
                ResumeConfig::new("decision", store)
                    .with_resume_value(AgentApprovalDecision::Approve),
                Arc::new(Events::default()),
            )
            .await
            .unwrap();
        assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn resumed_stream_preserves_start_observer_rejection() {
    use group_agent_prebuilt::{AgentConfig, ToolCallingAgent};
    use group_agent_tool::{ToolEvent, ToolObserverError, ToolObserverFailure};
    for use_sink in [false, true] {
        let probe = Arc::new(fixture::Probe::default());
        let runtime = fixture::runtime(&probe).with_event_sink(Arc::new(|event: &ToolEvent| {
            if matches!(event, ToolEvent::ExecutionStarted { .. }) {
                Err(ToolObserverError::new("start rejected"))
            } else {
                Ok(())
            }
        }));
        let agent = ToolCallingAgent::new(
            fixture::model(&probe),
            runtime,
            AgentConfig::new(2).unwrap().with_tool_approval(true),
        )
        .unwrap();
        let store = store();
        let initial = agent
            .invoke_with_checkpoint(vec![Message::user("question")], config("observer", &store))
            .await
            .unwrap();
        let saved = initial.as_interrupted().unwrap().checkpoint_id();
        let cfg = ResumeConfig::new("observer", store.clone())
            .with_resume_value(AgentApprovalDecision::Approve);
        let error = if use_sink {
            let sink = Arc::new(Events::default());
            let error = agent
                .resume_with_stream_sink(cfg, sink.clone())
                .await
                .unwrap_err();
            assert!(sink.snapshot().is_empty());
            error
        } else {
            let mut stream = agent.resume_stream(cfg);
            let error = stream.next().await.unwrap().unwrap_err();
            assert!(stream.next().await.is_none());
            error
        };
        let report = error
            .tool_batch_report()
            .expect("failure retains batch report");
        let failure = report.results()[0].as_ref().unwrap_err();
        assert_eq!(
            failure.kind(),
            group_agent_tool::ToolRuntimeErrorKind::ObserverFailed
        );
        assert!(has_source::<ToolObserverFailure>(failure));
        assert_eq!(probe.executions.load(Ordering::SeqCst), 0);
        assert_eq!(probe.streams.load(Ordering::SeqCst), 0);
        assert_eq!(
            store
                .latest(&ThreadId::from("observer"))
                .await
                .unwrap()
                .unwrap()
                .id(),
            saved
        );
    }
}
