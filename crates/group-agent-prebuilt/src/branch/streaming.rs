use super::{
    AgentBranch, BranchError, BranchErrorKind, BranchEventSink, BranchEventStream, BranchOutcome,
    BranchRunOutcome, BranchSnapshot, BranchStreamEvent,
    event::{ChannelEventSink, terminal},
    state::BranchState,
};
use group_agent_core::{CheckpointConfig, EventConfig, ResumeConfig, RunControl};
use group_agent_model::Message;
use std::sync::Arc;
impl AgentBranch {
    pub fn stream(&self, messages: Vec<Message>) -> BranchEventStream {
        self.stream_with_control(messages, EventConfig::default(), RunControl::default())
    }
    pub fn stream_with_control(
        &self,
        messages: Vec<Message>,
        events: EventConfig,
        control: RunControl,
    ) -> BranchEventStream {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = Arc::new(ChannelEventSink::new(tx));
        let branch = self.clone();
        BranchEventStream::new(
            rx,
            Box::pin(async move {
                branch
                    .invoke_with_stream_sink_control(messages, events, control, sink)
                    .await?;
                Ok(())
            }),
        )
    }
    pub async fn invoke_with_stream_sink(
        &self,
        messages: Vec<Message>,
        sink: Arc<dyn BranchEventSink>,
    ) -> Result<BranchOutcome, BranchError> {
        self.invoke_with_stream_sink_control(
            messages,
            EventConfig::default(),
            RunControl::default(),
            sink,
        )
        .await
    }
    pub async fn invoke_with_stream_sink_control(
        &self,
        messages: Vec<Message>,
        events: EventConfig,
        control: RunControl,
        sink: Arc<dyn BranchEventSink>,
    ) -> Result<BranchOutcome, BranchError> {
        if self.stages.iter().any(|s| s.config.tool_approval()) {
            return Err(BranchError::new(BranchErrorKind::Configuration));
        }
        let mut state = BranchState::new(messages)?;
        state.set_sink(sink.clone(), &self.stages);
        let report = self
            .graph
            .invoke_with_control(state, self.run_config.clone(), events, control)
            .await
            .map_err(BranchError::graph)?;
        let outcome = BranchOutcome::from_state(report.into_final_state(), &self.stages)?;
        sink.on_event(&BranchStreamEvent::Completed(outcome.clone()));
        Ok(outcome)
    }
    pub fn stream_with_checkpoint(
        &self,
        messages: Vec<Message>,
        config: CheckpointConfig<BranchSnapshot>,
    ) -> BranchEventStream {
        self.stream_with_checkpoint_control(
            messages,
            EventConfig::default(),
            RunControl::default(),
            config,
        )
    }
    pub fn stream_with_checkpoint_control(
        &self,
        messages: Vec<Message>,
        events: EventConfig,
        control: RunControl,
        config: CheckpointConfig<BranchSnapshot>,
    ) -> BranchEventStream {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = Arc::new(ChannelEventSink::new(tx));
        let branch = self.clone();
        BranchEventStream::new(
            rx,
            Box::pin(async move {
                branch
                    .invoke_with_checkpoint_stream_sink_control(
                        messages, events, control, config, sink,
                    )
                    .await?;
                Ok(())
            }),
        )
    }
    pub async fn invoke_with_checkpoint_stream_sink(
        &self,
        messages: Vec<Message>,
        config: CheckpointConfig<BranchSnapshot>,
        sink: Arc<dyn BranchEventSink>,
    ) -> Result<BranchRunOutcome, BranchError> {
        self.invoke_with_checkpoint_stream_sink_control(
            messages,
            EventConfig::default(),
            RunControl::default(),
            config,
            sink,
        )
        .await
    }
    pub async fn invoke_with_checkpoint_stream_sink_control(
        &self,
        messages: Vec<Message>,
        events: EventConfig,
        control: RunControl,
        config: CheckpointConfig<BranchSnapshot>,
        sink: Arc<dyn BranchEventSink>,
    ) -> Result<BranchRunOutcome, BranchError> {
        Self::check_policy(&config)?;
        let mut state = BranchState::new(messages)?;
        state.set_sink(sink.clone(), &self.stages);
        let result = self
            .graph
            .invoke_with_checkpoint(state, self.run_config.clone(), events, control, config)
            .await
            .map_err(BranchError::graph)?;
        let outcome = BranchRunOutcome::from_execution(result, &self.stages)?;
        terminal(&outcome, sink.as_ref());
        Ok(outcome)
    }
    /// Streams resumed work with fresh invocation-local sinks; historical deltas are absent.
    pub fn resume_stream(&self, config: ResumeConfig<BranchSnapshot>) -> BranchEventStream {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = Arc::new(ChannelEventSink::new(tx));
        let branch = self.clone();
        BranchEventStream::new(
            rx,
            Box::pin(async move {
                branch.resume_with_stream_sink(config, sink).await?;
                Ok(())
            }),
        )
    }
    pub async fn resume_with_stream_sink(
        &self,
        config: ResumeConfig<BranchSnapshot>,
        sink: Arc<dyn BranchEventSink>,
    ) -> Result<BranchRunOutcome, BranchError> {
        let result = self
            .graph
            .resume_with_state_initializer(self.resume_config(config)?, |state| {
                state.set_sink(sink.clone(), &self.stages);
                Ok(())
            })
            .await
            .map_err(BranchError::graph)?;
        let outcome = BranchRunOutcome::from_execution(result, &self.stages)?;
        terminal(&outcome, sink.as_ref());
        Ok(outcome)
    }
}
