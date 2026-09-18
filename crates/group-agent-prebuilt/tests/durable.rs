#[path = "../test_support/offline_agent.rs"]
mod offline_agent;

use std::error::Error as StdError;
use std::sync::Arc;

use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, Checkpointer, ForkConfig, GraphRunError,
    InMemoryCheckpointer, ReplayConfig, ResumeConfig, RunConfig, ThreadId,
};
use group_agent_model::Message;
use group_agent_prebuilt::{
    AgentConfig, AgentError, AgentSnapshot, AgentSnapshotCodec, AgentStopReason, ToolCallingAgent,
};
use offline_agent::{
    RecordedTranscripts, Script, ScriptedModel, empty_runtime, flaky_local_runtime, local_runtime,
};

fn checkpointer() -> Arc<InMemoryCheckpointer<AgentSnapshot>> {
    Arc::new(InMemoryCheckpointer::new(AgentSnapshotCodec))
}

fn as_dyn(
    store: &Arc<InMemoryCheckpointer<AgentSnapshot>>,
) -> Arc<dyn Checkpointer<AgentSnapshot>> {
    store.clone()
}

fn graph_run_error(error: &AgentError) -> &GraphRunError {
    let mut current: Option<&dyn StdError> = StdError::source(error);
    loop {
        match current {
            Some(source) => match source.downcast_ref::<GraphRunError>() {
                Some(graph_error) => return graph_error,
                None => current = source.source(),
            },
            None => panic!("AgentError retains a Core GraphRunError source"),
        }
    }
}

fn tool_agent() -> ToolCallingAgent {
    ToolCallingAgent::new(
        ScriptedModel::one_tool_round().expect("offline scripted model is valid"),
        local_runtime().expect("offline local ToolRuntime is valid"),
        AgentConfig::new(2).expect("two model rounds are valid"),
    )
    .expect("offline agent construction succeeds")
}

fn flaky_tool_agent() -> (ToolCallingAgent, RecordedTranscripts) {
    let (model, transcripts) =
        ScriptedModel::fail_once(Script::OneToolRound, 2).expect("fail-once model is valid");
    let agent = ToolCallingAgent::new(
        model,
        local_runtime().expect("offline local ToolRuntime is valid"),
        AgentConfig::new(2).expect("two model rounds are valid"),
    )
    .expect("offline agent construction succeeds");
    (agent, transcripts)
}

#[tokio::test]
async fn durable_invoke_commits_one_checkpoint_per_superstep() {
    let store = checkpointer();
    let thread_id = ThreadId::from("durable-invoke");
    let agent = tool_agent();

    let outcome = agent
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            CheckpointConfig::new(
                thread_id.clone(),
                as_dyn(&store),
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect("durable invocation succeeds");

    assert!(outcome.is_completed());
    assert_eq!(outcome.stop_reason(), Some(AgentStopReason::FinalAnswer));
    let completed = outcome.as_completed().expect("completed outcome");
    assert_eq!(completed.model_rounds(), 2);

    let history = store
        .history(&thread_id)
        .await
        .expect("checkpoint history loads");
    assert_eq!(history.len(), 3, "model, tools, model super-steps");
    assert!(
        history[..2]
            .iter()
            .all(|checkpoint| !checkpoint.completed())
    );
    assert!(history.last().expect("head checkpoint").completed());
    let unique: std::collections::HashSet<_> =
        history.iter().map(|checkpoint| checkpoint.id()).collect();
    assert_eq!(unique.len(), history.len());
}

#[tokio::test]
async fn failed_durable_run_resumes_from_the_committed_head() {
    let store = checkpointer();
    let thread_id = ThreadId::from("resume-thread");
    let (agent, transcripts) = flaky_tool_agent();

    agent
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            CheckpointConfig::new(
                thread_id.clone(),
                as_dyn(&store),
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("the scripted model fails once on its second call");

    let interrupted_history = store
        .history(&thread_id)
        .await
        .expect("checkpoint history loads");
    assert_eq!(interrupted_history.len(), 2, "model and tools super-steps");
    assert!(
        interrupted_history
            .iter()
            .all(|checkpoint| !checkpoint.completed())
    );

    let outcome = agent
        .resume(ResumeConfig::new(thread_id.clone(), as_dyn(&store)))
        .await
        .expect("resume completes from the committed head");

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

    let transcripts = transcripts.all();
    assert_eq!(transcripts.len(), 3, "original two calls plus resumed call");
    assert_eq!(
        transcripts[1], transcripts[2],
        "the resumed model call receives the transcript committed before the failure"
    );
    assert_eq!(transcripts[2].len(), 3);
    assert!(matches!(transcripts[2][0], Message::User(_)));
    assert!(matches!(transcripts[2][1], Message::Assistant(_)));
    assert!(matches!(transcripts[2][2], Message::Tool(_)));

    let final_history = store
        .history(&thread_id)
        .await
        .expect("checkpoint history loads");
    assert_eq!(final_history.len(), 3);
    assert!(final_history.last().expect("head checkpoint").completed());
}

#[tokio::test]
async fn replay_reexecutes_read_only_from_a_historical_checkpoint() {
    let store = checkpointer();
    let thread_id = ThreadId::from("replay-thread");
    let agent = tool_agent();
    agent
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            CheckpointConfig::new(
                thread_id.clone(),
                as_dyn(&store),
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect("durable invocation succeeds");

    let history = store
        .history(&thread_id)
        .await
        .expect("checkpoint history loads");
    let ids_before: Vec<_> = history.iter().map(|checkpoint| checkpoint.id()).collect();
    let historical = &history[1];
    assert!(
        !historical.completed(),
        "the middle checkpoint is not the head"
    );

    let report = agent
        .replay(ReplayConfig::new(
            thread_id.clone(),
            historical.id(),
            as_dyn(&store),
        ))
        .await
        .expect("replay succeeds");

    assert_eq!(report.source_thread_id(), &thread_id);
    assert_eq!(report.source_checkpoint_id(), historical.id());
    assert_eq!(report.source_step(), historical.step());
    assert_eq!(report.source_superstep(), historical.superstep());
    assert!(report.outcome().is_completed());
    assert_eq!(
        report
            .outcome()
            .final_message()
            .expect("replay reproduces the final assistant message")
            .text_content(),
        "Offline tool-assisted answer."
    );

    let ids_after: Vec<_> = store
        .history(&thread_id)
        .await
        .expect("checkpoint history loads")
        .iter()
        .map(|checkpoint| checkpoint.id())
        .collect();
    assert_eq!(ids_before, ids_after, "replay writes no lineage");
}

#[tokio::test]
async fn fork_runs_a_writable_branch_without_touching_the_source_head() {
    let store = checkpointer();
    let thread_id = ThreadId::from("fork-thread");
    let agent = tool_agent();
    agent
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            CheckpointConfig::new(
                thread_id.clone(),
                as_dyn(&store),
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect("durable invocation succeeds");

    let head_before = store
        .latest(&thread_id)
        .await
        .expect("latest loads")
        .expect("head exists")
        .id();
    let history = store
        .history(&thread_id)
        .await
        .expect("checkpoint history loads");
    let historical = history[1].id();

    let report = agent
        .fork(ForkConfig::new(
            thread_id.clone(),
            historical,
            as_dyn(&store),
        ))
        .await
        .expect("fork succeeds");

    assert_eq!(report.source_thread_id(), &thread_id);
    assert_eq!(report.source_checkpoint_id(), historical);
    assert!(report.outcome().is_completed());
    assert_eq!(
        report
            .outcome()
            .final_message()
            .expect("fork reproduces the final assistant message")
            .text_content(),
        "Offline tool-assisted answer."
    );

    let branch_head = store
        .branch_head(&thread_id, report.branch_id())
        .await
        .expect("branch head loads")
        .expect("branch head exists");
    assert_ne!(
        branch_head.id(),
        head_before,
        "the branch commits under its own head"
    );
    let head_after = store
        .latest(&thread_id)
        .await
        .expect("latest loads")
        .expect("head exists")
        .id();
    assert_eq!(
        head_before, head_after,
        "the source thread head is unchanged"
    );
}

#[tokio::test]
async fn resume_of_an_unknown_thread_fails_with_typed_checkpoint_not_found() {
    let store = checkpointer();
    let agent = ToolCallingAgent::new(
        ScriptedModel::model_only().expect("offline scripted model is valid"),
        empty_runtime(),
        AgentConfig::new(1).expect("one model round is valid"),
    )
    .expect("offline agent construction succeeds");

    let error = agent
        .resume(ResumeConfig::new("no-such-thread", as_dyn(&store)))
        .await
        .expect_err("an unknown thread cannot resume");

    let mut current: Option<&dyn StdError> = StdError::source(&error);
    let graph_error = loop {
        match current {
            Some(error) => {
                if let Some(graph_error) = error.downcast_ref::<GraphRunError>() {
                    break graph_error;
                }
                current = error.source();
            }
            None => panic!("AgentError retains a Core GraphRunError source"),
        }
    };
    assert!(
        matches!(graph_error, GraphRunError::CheckpointNotFound { .. }),
        "unknown-thread resume is a typed CheckpointNotFound"
    );
    assert_eq!(error.to_string(), "agent invocation failed");
}

#[tokio::test]
async fn resume_with_an_explicit_run_config_overrides_the_agent_step_budget() {
    let store = checkpointer();
    let thread_id = ThreadId::from("resume-explicit-budget");
    let (agent, _transcripts) = flaky_tool_agent();

    agent
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            CheckpointConfig::new(
                thread_id.clone(),
                as_dyn(&store),
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("the scripted model fails once on its second call");

    let error = agent
        .resume(
            ResumeConfig::new(thread_id.clone(), as_dyn(&store)).with_run_config(RunConfig::new(0)),
        )
        .await
        .expect_err("an explicit zero-step budget stops the resume");
    let graph_error = graph_run_error(&error);
    assert!(
        matches!(
            graph_error,
            GraphRunError::MaxStepsExceeded {
                max_steps: 0,
                step: 3,
                ..
            }
        ),
        "the caller-set budget reaches Core unchanged: {graph_error:?}"
    );

    let head = store
        .latest(&thread_id)
        .await
        .expect("latest loads")
        .expect("head exists");
    assert_eq!(head.step(), 2, "the budget failure commits nothing new");

    let outcome = agent
        .resume(ResumeConfig::new(thread_id.clone(), as_dyn(&store)))
        .await
        .expect("a default resume inherits the Agent budget and completes");
    assert_eq!(outcome.stop_reason(), Some(AgentStopReason::FinalAnswer));
}

#[tokio::test]
async fn resume_inherits_the_agent_step_budget_beyond_the_core_default() {
    const MAX_ROUNDS: usize = 502;
    let store = checkpointer();
    let thread_id = ThreadId::from("resume-inherited-budget");
    let agent = ToolCallingAgent::new(
        ScriptedModel::tool_loop().expect("offline scripted model is valid"),
        flaky_local_runtime().expect("offline flaky ToolRuntime is valid"),
        AgentConfig::new(MAX_ROUNDS).expect("round cap is valid"),
    )
    .expect("offline agent construction succeeds");

    agent
        .invoke_with_checkpoint(
            vec![Message::user("Use the offline label tool.")],
            CheckpointConfig::new(
                thread_id.clone(),
                as_dyn(&store),
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("the flaky Tool fails its first execution");

    let head = store
        .latest(&thread_id)
        .await
        .expect("latest loads")
        .expect("head exists");
    assert_eq!(head.step(), 1, "only the model super-step committed");

    // The remaining loop needs 1003 additional nodes. Core's default
    // 1000-step budget would stop with MaxStepsExceeded at step 1002; the
    // inherited Agent budget of 2 * MAX_ROUNDS = 1004 covers the loop.
    let outcome = agent
        .resume(ResumeConfig::new(thread_id.clone(), as_dyn(&store)))
        .await
        .expect("the inherited Agent budget covers the remaining loop");
    assert!(outcome.is_completed());
    assert_eq!(outcome.stop_reason(), Some(AgentStopReason::MaxRounds));
    assert_eq!(
        outcome
            .as_completed()
            .expect("completed outcome")
            .model_rounds(),
        MAX_ROUNDS
    );
}
