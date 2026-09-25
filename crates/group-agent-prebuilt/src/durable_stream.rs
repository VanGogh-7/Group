use std::sync::Arc;

use group_agent_core::{CheckpointConfig, EventConfig, ResumeConfig, RunConfig, RunControl};

use crate::state::AgentState;
use crate::stream::ChannelEventSink;
use crate::{
    AgentError, AgentEventSink, AgentEventStream, AgentRunOutcome, AgentSnapshot, AgentStreamEvent,
    ToolCallingAgent,
};
use group_agent_model::Message;

impl ToolCallingAgent {
    /// Streams one conversation with opt-in durable checkpoints.
    ///
    /// Deltas are provisional until their State boundary commits. A saved
    /// approval suspension emits [`AgentStreamEvent::Interrupted`] and ends the
    /// stream; continue with [`Self::resume_stream`] and an explicit decision.
    /// Events themselves are not persisted or replayed. Dropping the stream
    /// drops owned execution but does not roll back remote Tool effects.
    pub fn stream_with_checkpoint(
        &self,
        messages: Vec<Message>,
        checkpoint_config: CheckpointConfig<AgentSnapshot>,
    ) -> AgentEventStream {
        self.stream_with_checkpoint_control(
            messages,
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config,
        )
    }

    /// Streams a checkpointed conversation with caller-supplied Core controls.
    ///
    /// Completion or interruption is emitted only after required checkpoint
    /// saves succeed. Model, Tool, Store, and control failures yield one typed
    /// error after buffered events and terminate the stream without retry.
    pub fn stream_with_checkpoint_control(
        &self,
        messages: Vec<Message>,
        event_config: EventConfig,
        run_control: RunControl,
        checkpoint_config: CheckpointConfig<AgentSnapshot>,
    ) -> AgentEventStream {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let sink = Arc::new(ChannelEventSink::new(sender));
        let graph = Arc::clone(&self.graph);
        let run_config = self.run_config.clone();
        let output = self.output.clone();
        let invocation = Box::pin(async move {
            let execution = graph
                .invoke_with_checkpoint(
                    AgentState::new(messages).with_sink(sink.clone()),
                    run_config,
                    event_config,
                    run_control,
                    checkpoint_config,
                )
                .await
                .map_err(AgentError::from_graph)?;
            dispatch_outcome(
                AgentRunOutcome::from_execution(execution, &output)?,
                sink.as_ref(),
            );
            Ok(())
        });
        AgentEventStream::new(receiver, invocation)
    }

    /// Runs a checkpointed streaming conversation with synchronous events.
    ///
    /// The sink belongs only to this invocation and is never checkpointed.
    /// [`AgentStreamEvent::ApprovalRequired`] is provisional; a successful
    /// interrupt save emits [`AgentStreamEvent::Interrupted`]. The returned
    /// outcome carries the same durable suspension or normal completion.
    ///
    /// # Errors
    ///
    /// Model, Tool, checkpoint, and control failures retain their typed sources.
    /// Earlier checkpoints or external effects may already exist. No retry or
    /// rollback is performed.
    pub async fn invoke_with_checkpoint_stream_sink(
        &self,
        messages: Vec<Message>,
        checkpoint_config: CheckpointConfig<AgentSnapshot>,
        sink: Arc<dyn AgentEventSink>,
    ) -> Result<AgentRunOutcome, AgentError> {
        self.invoke_with_checkpoint_stream_sink_control(
            messages,
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config,
            sink,
        )
        .await
    }

    /// Runs checkpointed streaming with synchronous events and Core controls.
    ///
    /// Controls and Core events are forwarded unchanged. Dropping the Future
    /// drops owned Model and Tool Futures, without proving remote rollback.
    /// Sink callbacks run inline and must remain lightweight.
    ///
    /// # Errors
    ///
    /// Returns typed Model, Tool, checkpoint, cancellation, and timeout failures
    /// without a successful terminal event or automatic retry.
    pub async fn invoke_with_checkpoint_stream_sink_control(
        &self,
        messages: Vec<Message>,
        event_config: EventConfig,
        run_control: RunControl,
        checkpoint_config: CheckpointConfig<AgentSnapshot>,
        sink: Arc<dyn AgentEventSink>,
    ) -> Result<AgentRunOutcome, AgentError> {
        let execution = self
            .graph
            .invoke_with_checkpoint(
                AgentState::new(messages).with_sink(sink.clone()),
                self.run_config.clone(),
                event_config,
                run_control,
                checkpoint_config,
            )
            .await
            .map_err(AgentError::from_graph)?;
        let outcome = AgentRunOutcome::from_execution(execution, &self.output)?;
        dispatch_outcome_ref(&outcome, sink.as_ref());
        Ok(outcome)
    }

    /// Streams newly executed work from the selected latest checkpoint.
    ///
    /// Controls, Core events, branch selection, and the one-attempt approval
    /// decision come from `resume_config`. A saved approval requires an explicit
    /// [`crate::AgentApprovalDecision`]. Historical deltas are not replayed.
    /// A completed checkpoint emits only Completed and causes no Model/Tool
    /// calls or checkpoint writes. Dropping this stream drops owned execution.
    pub fn resume_stream(&self, resume_config: ResumeConfig<AgentSnapshot>) -> AgentEventStream {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let sink = Arc::new(ChannelEventSink::new(sender));
        let graph = Arc::clone(&self.graph);
        let resume_config = self.streaming_resume_config(resume_config);
        let output = self.output.clone();
        let invocation = Box::pin(async move {
            let execution = graph
                .resume_with_state_initializer(resume_config, |state| {
                    state.set_sink(sink.clone());
                    Ok(())
                })
                .await
                .map_err(AgentError::from_graph)?;
            dispatch_outcome(
                AgentRunOutcome::from_execution(execution, &output)?,
                sink.as_ref(),
            );
            Ok(())
        });
        AgentEventStream::new(receiver, invocation)
    }

    /// Resumes the latest checkpoint with a new invocation-local streaming sink.
    ///
    /// The sink is attached only after Core validates and restores the selected
    /// checkpoint. It is never persisted. Resume configuration owns controls,
    /// Core events, and any required approval decision. Earlier deltas are not
    /// replayed; a completed checkpoint emits only its terminal outcome.
    ///
    /// # Errors
    ///
    /// Returns typed load, compatibility, decision, Model, Tool, Store, or
    /// control failures. A failure neither authorizes retry nor proves absence
    /// of external effects. No retry or rollback is performed.
    pub async fn resume_with_stream_sink(
        &self,
        resume_config: ResumeConfig<AgentSnapshot>,
        sink: Arc<dyn AgentEventSink>,
    ) -> Result<AgentRunOutcome, AgentError> {
        let execution = self
            .graph
            .resume_with_state_initializer(self.streaming_resume_config(resume_config), |state| {
                state.set_sink(sink.clone());
                Ok(())
            })
            .await
            .map_err(AgentError::from_graph)?;
        let outcome = AgentRunOutcome::from_execution(execution, &self.output)?;
        dispatch_outcome_ref(&outcome, sink.as_ref());
        Ok(outcome)
    }

    fn streaming_resume_config(
        &self,
        config: ResumeConfig<AgentSnapshot>,
    ) -> ResumeConfig<AgentSnapshot> {
        if *config.run_config() == RunConfig::default() {
            config.with_run_config(self.run_config.clone())
        } else {
            config
        }
    }
}

fn dispatch_outcome(outcome: AgentRunOutcome, sink: &dyn AgentEventSink) {
    let event = match outcome {
        AgentRunOutcome::Completed(outcome) => AgentStreamEvent::Completed(outcome),
        AgentRunOutcome::Interrupted(outcome) => AgentStreamEvent::Interrupted(outcome),
    };
    sink.on_event(&event);
}

fn dispatch_outcome_ref(outcome: &AgentRunOutcome, sink: &dyn AgentEventSink) {
    let event = match outcome {
        AgentRunOutcome::Completed(outcome) => AgentStreamEvent::Completed(outcome.clone()),
        AgentRunOutcome::Interrupted(outcome) => AgentStreamEvent::Interrupted(outcome.clone()),
    };
    sink.on_event(&event);
}
