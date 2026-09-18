#[path = "../test_support/offline_agent.rs"]
mod offline_agent;

use std::error::Error as StdError;
use std::sync::{Arc, Mutex};

use group_agent_core::{
    CheckpointConfig, CheckpointIncompatibility, CheckpointPolicy, Checkpointer, EventConfig,
    EventRetention, EventSink, ForkConfig, GraphEvent, GraphRunError, InMemoryCheckpointer,
    ReplayConfig, ResumeConfig, ResumeValueError, RunControl, ThreadId,
};
use group_agent_model::Message;
use group_agent_prebuilt::{
    AgentApprovalDecision, AgentConfig, AgentError, AgentSnapshot, AgentSnapshotCodec,
    AgentStopReason, ToolCallingAgent,
};
use offline_agent::{
    RecordedTranscripts, Script, ScriptedModel, ToolInvocations, counting_runtime,
};

fn checkpointer() -> Arc<InMemoryCheckpointer<AgentSnapshot>> {
    Arc::new(InMemoryCheckpointer::new(AgentSnapshotCodec))
}

fn as_dyn(
    store: &Arc<InMemoryCheckpointer<AgentSnapshot>>,
) -> Arc<dyn Checkpointer<AgentSnapshot>> {
    store.clone()
}

fn source_chain<T: StdError + 'static>(error: &AgentError) -> Option<&T> {
    let mut current: Option<&dyn StdError> = StdError::source(error);
    while let Some(source) = current {
        if let Some(typed) = source.downcast_ref::<T>() {
            return Some(typed);
        }
        current = source.source();
    }
    None
}

fn graph_run_error(error: &AgentError) -> &GraphRunError {
    source_chain(error).expect("AgentError retains a Core GraphRunError source")
}

fn approval_config() -> AgentConfig {
    AgentConfig::new(2)
        .expect("two model rounds are valid")
        .with_tool_approval(true)
}

fn approval_agent() -> (ToolCallingAgent, ToolInvocations) {
    let (runtime, invocations) = counting_runtime().expect("offline counting ToolRuntime is valid");
    let agent = ToolCallingAgent::new(
        ScriptedModel::one_tool_round().expect("offline scripted model is valid"),
        runtime,
        approval_config(),
    )
    .expect("offline agent construction succeeds");
    (agent, invocations)
}

fn base_agent() -> ToolCallingAgent {
    let (runtime, _invocations) =
        counting_runtime().expect("offline counting ToolRuntime is valid");
    ToolCallingAgent::new(
        ScriptedModel::one_tool_round().expect("offline scripted model is valid"),
        runtime,
        AgentConfig::new(2).expect("two model rounds are valid"),
    )
    .expect("offline agent construction succeeds")
}

fn rejection_agent() -> (ToolCallingAgent, ToolInvocations, RecordedTranscripts) {
    let (model, transcripts) =
        ScriptedModel::recording(Script::OneToolRoundAnyResult).expect("recording model is valid");
    let (runtime, invocations) = counting_runtime().expect("offline counting ToolRuntime is valid");
    let agent = ToolCallingAgent::new(model, runtime, approval_config())
        .expect("offline agent construction succeeds");
    (agent, invocations, transcripts)
}

fn checkpoint_config(
    thread_id: &ThreadId,
    store: &Arc<InMemoryCheckpointer<AgentSnapshot>>,
) -> CheckpointConfig<AgentSnapshot> {
    CheckpointConfig::new(
        thread_id.clone(),
        as_dyn(store),
        CheckpointPolicy::EverySuperstep,
    )
}

async fn interrupt(thread_id: &str) -> InterruptSetup {
    let store = checkpointer();
    let thread_id = ThreadId::from(thread_id);
    let (agent, invocations) = approval_agent();
    let outcome = agent
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            checkpoint_config(&thread_id, &store),
        )
        .await
        .expect("durable invocation succeeds");
    assert!(
        outcome.is_interrupted(),
        "the approval-enabled agent suspends"
    );
    InterruptSetup {
        store,
        thread_id,
        agent,
        invocations,
        interrupted: outcome,
    }
}

struct InterruptSetup {
    store: Arc<InMemoryCheckpointer<AgentSnapshot>>,
    thread_id: ThreadId,
    agent: ToolCallingAgent,
    invocations: ToolInvocations,
    interrupted: group_agent_prebuilt::AgentRunOutcome,
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

fn event_count(events: &[GraphEvent], predicate: impl Fn(&GraphEvent) -> bool) -> usize {
    events.iter().filter(|event| predicate(event)).count()
}

#[tokio::test]
async fn approval_interrupts_before_any_tool_side_effect() {
    let store = checkpointer();
    let thread_id = ThreadId::from("approval-interrupt");
    let (agent, invocations) = approval_agent();
    let sink = Arc::new(RecordingEventSink::default());

    let outcome = agent
        .invoke_with_checkpoint_control(
            vec![Message::user("Use the offline label tool.")],
            event_config(&sink),
            RunControl::default(),
            checkpoint_config(&thread_id, &store),
        )
        .await
        .expect("durable invocation succeeds");

    assert!(outcome.is_interrupted());
    let interrupted = outcome.as_interrupted().expect("interrupted outcome");
    assert_eq!(interrupted.thread_id(), &thread_id);
    let request = interrupted
        .approval_request()
        .expect("the suspension carries the approval request");
    let calls = request.pending_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].id().as_str(), "offline-call-1");
    assert_eq!(calls[0].name().as_str(), "lookup_label");
    assert_eq!(
        invocations.count(),
        0,
        "no Tool executed before the approval decision"
    );

    let head = store
        .latest(&thread_id)
        .await
        .expect("latest loads")
        .expect("head exists");
    assert!(head.interrupted(), "the head is the interrupted checkpoint");
    assert!(!head.completed());
    assert_eq!(head.id(), interrupted.checkpoint_id());

    let events = sink.snapshot();
    assert_eq!(
        event_count(&events, |event| matches!(
            event,
            GraphEvent::NodeInterrupted { .. }
        )),
        1
    );
    assert_eq!(
        event_count(&events, |event| matches!(
            event,
            GraphEvent::RunInterrupted { .. }
        )),
        1
    );
    assert_eq!(
        event_count(&events, |event| matches!(
            event,
            GraphEvent::RunCompleted { .. }
        )),
        0
    );
}

#[tokio::test]
async fn approve_resume_executes_the_pending_batch_once() {
    let setup = interrupt("approval-approve").await;

    let outcome = setup
        .agent
        .resume(
            ResumeConfig::new(setup.thread_id.clone(), as_dyn(&setup.store))
                .with_resume_value(AgentApprovalDecision::Approve),
        )
        .await
        .expect("approve resume completes");

    assert!(outcome.is_completed());
    assert_eq!(outcome.stop_reason(), Some(AgentStopReason::FinalAnswer));
    let completed = outcome.as_completed().expect("completed outcome");
    assert_eq!(completed.model_rounds(), 2);
    assert_eq!(
        completed
            .final_message()
            .expect("FinalAnswer includes a final assistant message")
            .text_content(),
        "Offline tool-assisted answer."
    );
    assert_eq!(
        setup.invocations.count(),
        1,
        "the approved batch executed exactly once"
    );

    let head = setup
        .store
        .latest(&setup.thread_id)
        .await
        .expect("latest loads")
        .expect("head exists");
    assert!(head.completed(), "the resumed run commits a completed head");
}

#[tokio::test]
async fn reject_resume_commits_business_error_tool_messages_and_continues() {
    let store = checkpointer();
    let thread_id = ThreadId::from("approval-reject");
    let (agent, invocations, transcripts) = rejection_agent();

    let outcome = agent
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            checkpoint_config(&thread_id, &store),
        )
        .await
        .expect("durable invocation succeeds");
    assert!(outcome.is_interrupted());

    let outcome = agent
        .resume(
            ResumeConfig::new(thread_id.clone(), as_dyn(&store))
                .with_resume_value(AgentApprovalDecision::Reject),
        )
        .await
        .expect("reject resume completes the loop");

    assert!(outcome.is_completed());
    assert_eq!(outcome.stop_reason(), Some(AgentStopReason::FinalAnswer));
    let completed = outcome.as_completed().expect("completed outcome");
    assert_eq!(completed.model_rounds(), 2);
    assert_eq!(
        completed
            .final_message()
            .expect("FinalAnswer includes a final assistant message")
            .text_content(),
        "Offline final answer."
    );
    assert_eq!(invocations.count(), 0, "a rejected batch executes no Tool");

    let transcripts = transcripts.all();
    assert_eq!(
        transcripts.len(),
        2,
        "one model round before and after the decision"
    );
    let tool_messages: Vec<_> = transcripts[1].iter().filter_map(Message::as_tool).collect();
    assert_eq!(
        tool_messages.len(),
        1,
        "one business-error ToolMessage per pending call"
    );
    assert_eq!(tool_messages[0].tool_call_id().as_str(), "offline-call-1");
    assert!(tool_messages[0].result().is_error());
    let content = tool_messages[0].result().content();
    assert_eq!(content.len(), 1);
    assert_eq!(content[0].as_text(), Some("tool call rejected by approval"));
}

#[tokio::test]
async fn resume_of_an_interrupted_head_without_a_value_fails_typed() {
    let setup = interrupt("approval-missing-value").await;

    let error = setup
        .agent
        .resume(ResumeConfig::new(
            setup.thread_id.clone(),
            as_dyn(&setup.store),
        ))
        .await
        .expect_err("an interrupted head requires a resume value");

    assert!(
        matches!(
            graph_run_error(&error),
            GraphRunError::MissingResumeValue { .. }
        ),
        "missing resume value is a typed Core failure"
    );
    let head = setup
        .store
        .latest(&setup.thread_id)
        .await
        .expect("latest loads")
        .expect("head exists");
    assert!(head.interrupted(), "the failed resume commits nothing new");
}

#[tokio::test]
async fn resume_of_a_completed_head_with_a_value_fails_typed() {
    let setup = interrupt("approval-unexpected-value").await;
    setup
        .agent
        .resume(
            ResumeConfig::new(setup.thread_id.clone(), as_dyn(&setup.store))
                .with_resume_value(AgentApprovalDecision::Approve),
        )
        .await
        .expect("approve resume completes");

    let error = setup
        .agent
        .resume(
            ResumeConfig::new(setup.thread_id.clone(), as_dyn(&setup.store))
                .with_resume_value(AgentApprovalDecision::Approve),
        )
        .await
        .expect_err("a completed head cannot consume a resume value");

    assert!(
        matches!(
            graph_run_error(&error),
            GraphRunError::UnexpectedResumeValue { .. }
        ),
        "a decision on a completed head is a typed Core failure"
    );
}

#[tokio::test]
async fn resume_with_a_wrong_typed_value_fails_and_keeps_the_interrupted_head() {
    let setup = interrupt("approval-wrong-type").await;

    let error = setup
        .agent
        .resume(
            ResumeConfig::new(setup.thread_id.clone(), as_dyn(&setup.store))
                .with_resume_value(String::from("yes")),
        )
        .await
        .expect_err("a wrong-typed resume value fails the run");

    assert!(
        matches!(graph_run_error(&error), GraphRunError::NodeFailed { .. }),
        "the wrong-typed decision surfaces as a node failure"
    );
    let resume_error = source_chain::<ResumeValueError>(&error)
        .expect("the node failure chain contains a ResumeValueError");
    assert!(
        matches!(resume_error, ResumeValueError::TypeMismatch { .. }),
        "the decision type mismatch is classified: {resume_error}"
    );

    let head = setup
        .store
        .latest(&setup.thread_id)
        .await
        .expect("latest loads")
        .expect("head exists");
    assert!(
        head.interrupted(),
        "single-attempt semantics leave the interrupted head intact"
    );

    let outcome = setup
        .agent
        .resume(
            ResumeConfig::new(setup.thread_id.clone(), as_dyn(&setup.store))
                .with_resume_value(AgentApprovalDecision::Approve),
        )
        .await
        .expect("a later correctly typed resume completes");
    assert!(outcome.is_completed());
    assert_eq!(setup.invocations.count(), 1);
}

#[tokio::test]
async fn non_durable_invoke_of_an_approval_agent_fails_closed() {
    let (agent, invocations) = approval_agent();

    let error = agent
        .invoke(vec![Message::user("Use the offline label tool.")])
        .await
        .expect_err("an approval-enabled agent cannot invoke non-durably");

    assert!(
        matches!(
            graph_run_error(&error),
            GraphRunError::InterruptRequiresCheckpoint { .. }
        ),
        "approval without checkpointing fails closed"
    );
    assert_eq!(
        invocations.count(),
        0,
        "no Tool executed on the failed path"
    );
}

#[tokio::test]
async fn replay_of_an_interrupted_checkpoint_with_a_decision_is_read_only() {
    let setup = interrupt("approval-replay").await;
    let interrupted_id = setup
        .interrupted
        .as_interrupted()
        .expect("interrupted outcome")
        .checkpoint_id();
    let ids_before: Vec<_> = setup
        .store
        .history(&setup.thread_id)
        .await
        .expect("checkpoint history loads")
        .iter()
        .map(|checkpoint| checkpoint.id())
        .collect();

    let report = setup
        .agent
        .replay(
            ReplayConfig::new(
                setup.thread_id.clone(),
                interrupted_id,
                as_dyn(&setup.store),
            )
            .with_resume_value(AgentApprovalDecision::Approve),
        )
        .await
        .expect("replay with an approval decision completes");

    assert_eq!(report.source_thread_id(), &setup.thread_id);
    assert_eq!(report.source_checkpoint_id(), interrupted_id);
    assert!(report.outcome().is_completed());
    assert_eq!(
        report
            .outcome()
            .final_message()
            .expect("replay reproduces the final assistant message")
            .text_content(),
        "Offline tool-assisted answer."
    );

    let ids_after: Vec<_> = setup
        .store
        .history(&setup.thread_id)
        .await
        .expect("checkpoint history loads")
        .iter()
        .map(|checkpoint| checkpoint.id())
        .collect();
    assert_eq!(ids_before, ids_after, "replay writes no lineage");

    let error = setup
        .agent
        .replay(ReplayConfig::new(
            setup.thread_id.clone(),
            interrupted_id,
            as_dyn(&setup.store),
        ))
        .await
        .expect_err("replaying an interrupted checkpoint requires a value");
    assert!(
        matches!(
            graph_run_error(&error),
            GraphRunError::MissingResumeValue { .. }
        ),
        "value-less replay of an interrupted checkpoint is a typed Core failure"
    );
}

#[tokio::test]
async fn fork_from_an_interrupted_checkpoint_branches_with_a_decision() {
    let setup = interrupt("approval-fork").await;
    let interrupted_id = setup
        .interrupted
        .as_interrupted()
        .expect("interrupted outcome")
        .checkpoint_id();

    let report = setup
        .agent
        .fork(
            ForkConfig::new(
                setup.thread_id.clone(),
                interrupted_id,
                as_dyn(&setup.store),
            )
            .with_resume_value(AgentApprovalDecision::Approve),
        )
        .await
        .expect("fork with an approval decision completes");

    assert_eq!(report.source_thread_id(), &setup.thread_id);
    assert_eq!(report.source_checkpoint_id(), interrupted_id);
    assert!(report.outcome().is_completed());
    assert_eq!(
        report
            .outcome()
            .final_message()
            .expect("fork reproduces the final assistant message")
            .text_content(),
        "Offline tool-assisted answer."
    );

    let branch_head = setup
        .store
        .branch_head(&setup.thread_id, report.branch_id())
        .await
        .expect("branch head loads")
        .expect("branch head exists");
    assert!(branch_head.completed(), "the branch commits its own head");
    assert_ne!(branch_head.id(), interrupted_id);

    let source_head = setup
        .store
        .latest(&setup.thread_id)
        .await
        .expect("latest loads")
        .expect("head exists");
    assert_eq!(source_head.id(), interrupted_id);
    assert!(
        source_head.interrupted(),
        "the source thread head remains the interrupted checkpoint"
    );
}

#[tokio::test]
async fn base_agent_resume_of_an_approval_interrupted_thread_fails_closed() {
    let setup = interrupt("approval-base-resume-mismatch").await;
    let interrupted_id = setup
        .interrupted
        .as_interrupted()
        .expect("interrupted outcome")
        .checkpoint_id();
    let base = base_agent();

    let error = base
        .resume(ResumeConfig::new(
            setup.thread_id.clone(),
            as_dyn(&setup.store),
        ))
        .await
        .expect_err("a base agent cannot resume an approval-suspended thread");

    let graph_error = graph_run_error(&error);
    assert!(
        matches!(
            graph_error,
            GraphRunError::CheckpointIncompatible {
                reason: CheckpointIncompatibility::GraphVersionMismatch { .. },
                ..
            }
        ),
        "the graph version split fails closed: {graph_error:?}"
    );

    let head = setup
        .store
        .latest(&setup.thread_id)
        .await
        .expect("latest loads")
        .expect("head exists");
    assert_eq!(
        head.id(),
        interrupted_id,
        "the failed resume commits nothing new"
    );
    assert!(head.interrupted());
}

#[tokio::test]
async fn approval_agent_resume_of_a_base_completed_thread_fails_closed() {
    let store = checkpointer();
    let thread_id = ThreadId::from("base-approval-resume-mismatch");
    let base = base_agent();
    let outcome = base
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            checkpoint_config(&thread_id, &store),
        )
        .await
        .expect("base durable invocation completes without approval");
    assert!(outcome.is_completed());
    let completed_id = store
        .latest(&thread_id)
        .await
        .expect("latest loads")
        .expect("head exists")
        .id();

    let (approval, _invocations) = approval_agent();
    let error = approval
        .resume(ResumeConfig::new(thread_id.clone(), as_dyn(&store)))
        .await
        .expect_err("an approval agent cannot resume a base-agent thread");

    let graph_error = graph_run_error(&error);
    assert!(
        matches!(
            graph_error,
            GraphRunError::CheckpointIncompatible {
                reason: CheckpointIncompatibility::GraphVersionMismatch { .. },
                ..
            }
        ),
        "the graph version split fails closed: {graph_error:?}"
    );

    let head = store
        .latest(&thread_id)
        .await
        .expect("latest loads")
        .expect("head exists");
    assert_eq!(
        head.id(),
        completed_id,
        "the failed resume commits nothing new"
    );
    assert!(head.completed());
    assert!(!head.interrupted());
}
