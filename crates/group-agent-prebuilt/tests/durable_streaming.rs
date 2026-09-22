#[path = "../test_support/streaming_agent.rs"]
mod fixture;

use fixture::{Events, agent, config, store};
use futures_util::StreamExt;
use group_agent_core::{Checkpointer, ResumeConfig, ThreadId};
use group_agent_model::Message;
use group_agent_prebuilt::{AgentApprovalDecision, AgentStopReason, AgentStreamEvent};
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn checkpointed_stream_and_sink_finish_only_after_save() {
    for sink_mode in [false, true] {
        for rounds in [1, 2] {
            let (agent, probe) = agent(false, rounds);
            let store = store();
            let events = if sink_mode {
                let sink = Arc::new(Events::default());
                let result = agent
                    .invoke_with_checkpoint_stream_sink(
                        vec![Message::user("SECRET_PROMPT")],
                        config("complete", &store),
                        sink.clone(),
                    )
                    .await
                    .unwrap();
                assert!(result.is_completed());
                sink.snapshot()
            } else {
                let mut stream = agent.stream_with_checkpoint(
                    vec![Message::user("SECRET_PROMPT")],
                    config("complete", &store),
                );
                let mut events = Vec::new();
                while let Some(event) = stream.next().await {
                    events.push(event.unwrap());
                }
                assert!(stream.next().await.is_none());
                events
            };
            let AgentStreamEvent::Completed(outcome) = events.last().unwrap() else {
                panic!("terminal completion required")
            };
            assert_eq!(outcome.model_rounds(), rounds);
            assert_eq!(
                outcome.stop_reason(),
                if rounds == 1 {
                    AgentStopReason::MaxRounds
                } else {
                    AgentStopReason::FinalAnswer
                }
            );
            assert_eq!(outcome.usage_by_round().len(), rounds);
            assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
            assert_eq!(probe.streams.load(Ordering::SeqCst), rounds);
            assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
            assert!(
                store
                    .latest(&ThreadId::from("complete"))
                    .await
                    .unwrap()
                    .unwrap()
                    .completed()
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|e| matches!(e, AgentStreamEvent::Completed(_)))
                    .count(),
                1
            );
            for event in &events {
                assert!(!format!("{event:?}").contains("SECRET_"));
            }
        }
    }
}

#[tokio::test]
async fn approval_stream_suspends_then_resumes_approve_or_reject_with_new_sink() {
    for sink_mode in [false, true] {
        for decision in [
            AgentApprovalDecision::Approve,
            AgentApprovalDecision::Reject,
        ] {
            let (agent, probe) = agent(true, 2);
            let store = store();
            let initial = Arc::new(Events::default());
            let interrupted = agent
                .invoke_with_checkpoint_stream_sink(
                    vec![Message::user("question")],
                    config("approval", &store),
                    initial.clone(),
                )
                .await
                .unwrap();
            let saved = interrupted.as_interrupted().unwrap();
            assert_eq!(probe.executions.load(Ordering::SeqCst), 0);
            let before = initial.snapshot();
            assert!(matches!(
                before[before.len() - 2],
                AgentStreamEvent::ApprovalRequired { .. }
            ));
            assert!(
                matches!(before.last(), Some(AgentStreamEvent::Interrupted(i)) if i.checkpoint_id() == saved.checkpoint_id())
            );
            assert_eq!(
                store
                    .latest(&ThreadId::from("approval"))
                    .await
                    .unwrap()
                    .unwrap()
                    .id(),
                saved.checkpoint_id()
            );
            let resume = ResumeConfig::new("approval", store.clone())
                .with_checkpoint_id(saved.checkpoint_id())
                .with_resume_value(decision);
            let events = if sink_mode {
                let sink = Arc::new(Events::default());
                let outcome = agent
                    .resume_with_stream_sink(resume, sink.clone())
                    .await
                    .unwrap();
                assert!(outcome.is_completed());
                sink.snapshot()
            } else {
                let mut stream = agent.resume_stream(resume);
                let mut events = Vec::new();
                while let Some(event) = stream.next().await {
                    events.push(event.unwrap());
                }
                assert!(stream.next().await.is_none());
                events
            };
            assert_eq!(initial.snapshot(), before, "old sink must not be retained");
            assert_eq!(
                probe.executions.load(Ordering::SeqCst),
                usize::from(decision == AgentApprovalDecision::Approve)
            );
            assert_eq!(probe.streams.load(Ordering::SeqCst), 2);
            assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
            assert!(
                events
                    .iter()
                    .any(|e| matches!(e, AgentStreamEvent::TextDelta { round: 2, .. }))
            );
            assert!(!events.iter().any(|e| matches!(
                e,
                AgentStreamEvent::ModelStarted { round: 1 }
                    | AgentStreamEvent::ApprovalRequired { .. }
            )));
            let AgentStreamEvent::Completed(outcome) = events.last().unwrap() else {
                panic!("completed")
            };
            assert_eq!(outcome.messages().len(), 4);
            assert_eq!(
                outcome.messages()[2].as_tool().unwrap().result().is_error(),
                decision == AgentApprovalDecision::Reject
            );
            assert_eq!(outcome.usage_by_round().len(), 2);
            if decision == AgentApprovalDecision::Reject {
                assert!(!events.iter().any(|e| matches!(
                    e,
                    AgentStreamEvent::ToolStarted { .. } | AgentStreamEvent::ToolCompleted { .. }
                )));
            }
            let history = store.history(&ThreadId::from("approval")).await.unwrap();
            let terminal = Arc::new(Events::default());
            agent
                .resume_with_stream_sink(
                    ResumeConfig::new("approval", store.clone()),
                    terminal.clone(),
                )
                .await
                .unwrap();
            assert!(matches!(
                terminal.snapshot().as_slice(),
                [AgentStreamEvent::Completed(_)]
            ));
            let events = agent
                .resume_stream(ResumeConfig::new("approval", store.clone()))
                .collect::<Vec<_>>()
                .await;
            assert!(matches!(
                events.as_slice(),
                [Ok(AgentStreamEvent::Completed(_))]
            ));
            assert_eq!(
                store
                    .history(&ThreadId::from("approval"))
                    .await
                    .unwrap()
                    .len(),
                history.len()
            );
            assert_eq!(probe.streams.load(Ordering::SeqCst), 2);
        }
    }
}

#[tokio::test]
async fn failed_final_stream_resumes_committed_tools_without_reexecution() {
    for use_sink in [false, true] {
        let (agent, probe) = agent(false, 2);
        let store = store();
        probe.fail_final_once.store(true, Ordering::SeqCst);
        let initial = Arc::new(Events::default());
        let error = agent
            .invoke_with_checkpoint_stream_sink(
                vec![Message::user("question")],
                config("failed-final", &store),
                initial.clone(),
            )
            .await
            .unwrap_err();
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .is::<group_agent_core::GraphRunError>()
        );
        assert!(
            initial
                .snapshot()
                .iter()
                .any(|e| matches!(e, AgentStreamEvent::TextDelta { round: 2, .. }))
        );
        assert!(
            !initial
                .snapshot()
                .iter()
                .any(|e| matches!(e, AgentStreamEvent::Completed(_)))
        );
        let head = store
            .latest(&ThreadId::from("failed-final"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.step(), 2);
        let cfg = ResumeConfig::new("failed-final", store.clone()).with_checkpoint_id(head.id());
        let events = if use_sink {
            let sink = Arc::new(Events::default());
            agent
                .resume_with_stream_sink(cfg, sink.clone())
                .await
                .unwrap();
            sink.snapshot()
        } else {
            agent
                .resume_stream(cfg)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(Result::unwrap)
                .collect()
        };
        assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
        assert_eq!(probe.streams.load(Ordering::SeqCst), 3);
        assert!(!events.iter().any(|e| matches!(
            e,
            AgentStreamEvent::ToolStarted { .. } | AgentStreamEvent::ToolCompleted { .. }
        )));
        let AgentStreamEvent::Completed(outcome) = events.last().unwrap() else {
            panic!("completion")
        };
        assert_eq!(outcome.messages().len(), 4);
        assert_eq!(
            outcome.usage_by_round().len(),
            2,
            "failed round was never committed"
        );
    }
}
