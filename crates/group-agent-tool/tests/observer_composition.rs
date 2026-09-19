use std::error::Error;
use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use group_agent_model::{ToolCall, ToolCallId, ToolDefinition, ToolName};
use group_agent_tool::{
    Tool, ToolBatchConfig, ToolBehavior, ToolError, ToolErrorKind, ToolEvent, ToolEventSink,
    ToolExecutionOptions, ToolInput, ToolObserverError, ToolObserverFailure,
    ToolObserverFailureKind, ToolOutput, ToolRegistry, ToolRuntime, ToolRuntimeErrorKind,
};
use serde_json::json;

#[derive(Clone, Copy)]
enum Outcome {
    Success,
    BusinessError,
    InfrastructureError,
    Pending,
}

struct TestTool {
    definition: ToolDefinition,
    executions: Arc<AtomicUsize>,
    outcome: Outcome,
}

#[async_trait]
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

    async fn execute(&self, _input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        match self.outcome {
            Outcome::Success => Ok(ToolOutput::success_text("ok")),
            Outcome::BusinessError => Ok(ToolOutput::business_error_text("rejected")),
            Outcome::InfrastructureError => Err(ToolError::new(ToolErrorKind::Other, "backend")),
            Outcome::Pending => pending().await,
        }
    }
}

fn fixture(outcome: Outcome) -> (ToolRuntime, Arc<AtomicUsize>) {
    let executions = Arc::new(AtomicUsize::new(0));
    let mut builder = ToolRegistry::builder();
    builder
        .register(TestTool {
            definition: ToolDefinition::new(
                ToolName::new("test").unwrap(),
                "Observer composition test tool",
                json!({"type": "object"}),
            ),
            executions: Arc::clone(&executions),
            outcome,
        })
        .unwrap();
    (ToolRuntime::new(builder.build()), executions)
}

fn call(id: &str) -> ToolCall {
    ToolCall::new(
        ToolCallId::new(id).unwrap(),
        ToolName::new("test").unwrap(),
        json!({}),
    )
}

type Events = Arc<Mutex<Vec<(&'static str, ToolEvent)>>>;

#[derive(Clone, Copy)]
enum Failure {
    None,
    Error,
    Panic,
}

fn sink(
    name: &'static str,
    events: &Events,
    start: Failure,
    terminal: Failure,
) -> Arc<dyn ToolEventSink> {
    let events = Arc::clone(events);
    Arc::new(move |event: &ToolEvent| {
        events.lock().unwrap().push((name, event.clone()));
        match if matches!(event, ToolEvent::ExecutionStarted { .. }) {
            start
        } else {
            terminal
        } {
            Failure::None => Ok(()),
            Failure::Error => Err(ToolObserverError::with_source(
                name,
                std::io::Error::other("observer source"),
            )),
            Failure::Panic => panic!("observer panic"),
        }
    })
}

fn assert_failure(failure: &ToolObserverFailure, mode: Failure, name: &str) {
    match mode {
        Failure::Error => {
            assert_eq!(failure.kind(), ToolObserverFailureKind::ReturnedError);
            let source = failure
                .source()
                .unwrap()
                .downcast_ref::<ToolObserverError>()
                .unwrap();
            assert_eq!(source.as_message(), name);
            assert!(source.source().unwrap().is::<std::io::Error>());
        }
        Failure::Panic => {
            assert_eq!(failure.kind(), ToolObserverFailureKind::Panicked);
            assert!(failure.source().is_none());
        }
        Failure::None => panic!("expected a failure mode"),
    }
}

#[tokio::test]
async fn start_failure_short_circuits_later_observers_and_prevents_execution() {
    for mode in [Failure::Error, Failure::Panic] {
        for rejected_index in 0..3 {
            let (mut runtime, executions) = fixture(Outcome::Success);
            let events = Events::default();
            let names = ["first", "second", "third"];
            for (index, name) in names.iter().enumerate() {
                let observer = sink(
                    name,
                    &events,
                    if index == rejected_index {
                        mode
                    } else {
                        Failure::None
                    },
                    Failure::None,
                );
                runtime = if index == 0 {
                    runtime.with_event_sink(observer)
                } else {
                    runtime.with_additional_event_sink(observer)
                };
            }
            let report = runtime.execute_report(&call("blocked")).await;
            let error = report
                .primary()
                .as_ref()
                .expect_err("start failure blocks tool");
            assert_eq!(error.kind(), ToolRuntimeErrorKind::ObserverFailed);
            assert_failure(
                error.source().unwrap().downcast_ref().unwrap(),
                mode,
                names[rejected_index],
            );
            assert!(report.terminal_observer_failure().is_none());
            assert_eq!(executions.load(Ordering::SeqCst), 0);
            let captured = events.lock().unwrap();
            assert_eq!(captured.len(), rejected_index + 1);
            for ((name, event), expected_name) in captured.iter().zip(names) {
                assert_eq!(*name, expected_name);
                assert!(matches!(event, ToolEvent::ExecutionStarted { .. }));
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn terminal_failures_reach_every_observer_and_keep_primary_outcomes() {
    for outcome in [
        Outcome::Success,
        Outcome::BusinessError,
        Outcome::InfrastructureError,
        Outcome::Pending,
    ] {
        for (first, second) in [
            (Failure::Error, Failure::Panic),
            (Failure::Panic, Failure::Error),
        ] {
            let (runtime, executions) = fixture(outcome);
            let events = Events::default();
            let runtime = runtime
                .with_event_sink(sink("first", &events, Failure::None, first))
                .with_additional_event_sink(sink("second", &events, Failure::None, second))
                .with_additional_event_sink(sink("third", &events, Failure::None, Failure::None));
            let report = runtime
                .execute_report_with_options(
                    &call("terminal"),
                    ToolExecutionOptions::new().with_timeout(Duration::from_secs(1)),
                )
                .await;
            match outcome {
                Outcome::Success | Outcome::BusinessError => assert_eq!(
                    report.primary().as_ref().unwrap().is_error(),
                    matches!(outcome, Outcome::BusinessError)
                ),
                Outcome::InfrastructureError => {
                    let error = report.primary().as_ref().unwrap_err();
                    assert_eq!(error.kind(), ToolRuntimeErrorKind::ExecutionFailed);
                    assert!(error.source().unwrap().is::<ToolError>());
                }
                Outcome::Pending => assert_eq!(
                    report.primary().as_ref().unwrap_err().kind(),
                    ToolRuntimeErrorKind::TimedOut
                ),
            }
            assert_failure(report.terminal_observer_failure().unwrap(), first, "first");
            assert_eq!(executions.load(Ordering::SeqCst), 1);
            let captured = events.lock().unwrap();
            assert_eq!(
                captured.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
                ["first", "second", "third", "first", "second", "third"]
            );
            for (_, event) in &captured[3..] {
                assert_eq!(event, &captured[3].1);
                match outcome {
                    Outcome::Success | Outcome::BusinessError => assert!(
                        matches!(event, ToolEvent::ExecutionCompleted { is_error, .. } if *is_error == matches!(outcome, Outcome::BusinessError))
                    ),
                    Outcome::InfrastructureError => assert!(matches!(
                        event,
                        ToolEvent::ExecutionFailed {
                            kind: ToolRuntimeErrorKind::ExecutionFailed,
                            ..
                        }
                    )),
                    Outcome::Pending => {
                        assert!(matches!(event, ToolEvent::ExecutionTimedOut { .. }))
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn additions_are_local_to_runtime_clone_and_replacement_removes_all_observers() {
    let (runtime, executions) = fixture(Outcome::Success);
    let events = Events::default();
    let original =
        runtime.with_additional_event_sink(sink("original", &events, Failure::None, Failure::None));
    let composed = original.clone().with_additional_event_sink(sink(
        "added",
        &events,
        Failure::None,
        Failure::None,
    ));
    original.execute(&call("original-call")).await.unwrap();
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>(),
        ["original", "original"]
    );
    events.lock().unwrap().clear();
    composed.execute(&call("composed-call")).await.unwrap();
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>(),
        ["original", "added", "original", "added"]
    );
    events.lock().unwrap().clear();
    let replaced =
        composed.with_event_sink(sink("replacement", &events, Failure::None, Failure::None));
    replaced.execute(&call("replaced-call")).await.unwrap();
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>(),
        ["replacement", "replacement"]
    );
    assert_eq!(executions.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn batch_reports_keep_first_terminal_failure_for_each_input() {
    let (runtime, executions) = fixture(Outcome::Success);
    let events = Events::default();
    let runtime = runtime
        .with_event_sink(sink("first", &events, Failure::None, Failure::Panic))
        .with_additional_event_sink(sink("second", &events, Failure::None, Failure::Error))
        .with_additional_event_sink(sink("third", &events, Failure::None, Failure::None));
    let report = runtime
        .execute_batch(vec![call("one"), call("two")], ToolBatchConfig::new(2))
        .await
        .unwrap();
    assert_eq!(executions.load(Ordering::SeqCst), 2);
    assert_eq!(report.len(), 2);
    for (index, (result, failure)) in report
        .results()
        .iter()
        .zip(report.terminal_observer_failures())
        .enumerate()
    {
        assert!(!result.as_ref().unwrap().is_error());
        assert_failure(failure.as_ref().unwrap(), Failure::Panic, "first");
        let captured = events.lock().unwrap();
        let per_call = captured
            .iter()
            .filter(|(_, event)| event.context().batch_index() == Some(index))
            .collect::<Vec<_>>();
        assert_eq!(
            per_call.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            ["first", "second", "third", "first", "second", "third"]
        );
        assert!(per_call.iter().all(|(_, event)| event.context().call_id()
            == &ToolCallId::new(if index == 0 { "one" } else { "two" }).unwrap()));
    }
}
