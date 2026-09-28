use super::{
    AgentBranch, BranchError, BranchErrorKind, BranchForkReport, BranchReplayReport,
    BranchRunOutcome, BranchSnapshot, state::BranchState,
};
use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, EventConfig, ForkConfig, ReplayConfig, ResumeConfig,
    ResumeTarget, RunConfig, RunControl,
};
use group_agent_model::Message;
impl AgentBranch {
    pub async fn invoke_with_checkpoint(
        &self,
        messages: Vec<Message>,
        config: CheckpointConfig<BranchSnapshot>,
    ) -> Result<BranchRunOutcome, BranchError> {
        self.invoke_with_checkpoint_control(
            messages,
            EventConfig::default(),
            RunControl::default(),
            config,
        )
        .await
    }
    pub async fn invoke_with_checkpoint_control(
        &self,
        messages: Vec<Message>,
        events: EventConfig,
        control: RunControl,
        config: CheckpointConfig<BranchSnapshot>,
    ) -> Result<BranchRunOutcome, BranchError> {
        Self::check_policy(&config)?;
        let result = self
            .graph
            .invoke_with_checkpoint(
                BranchState::new(messages)?,
                self.run_config.clone(),
                events,
                control,
                config,
            )
            .await
            .map_err(BranchError::graph)?;
        BranchRunOutcome::from_execution(result, &self.stages)
    }
    pub(crate) fn check_policy(
        config: &CheckpointConfig<BranchSnapshot>,
    ) -> Result<(), BranchError> {
        if config.policy() != CheckpointPolicy::EverySuperstep {
            Err(BranchError::new(BranchErrorKind::Configuration))
        } else {
            Ok(())
        }
    }
    pub(crate) fn resume_config(
        &self,
        config: ResumeConfig<BranchSnapshot>,
    ) -> Result<ResumeConfig<BranchSnapshot>, BranchError> {
        if config.has_resume_value() && !matches!(config.target(), ResumeTarget::Checkpoint(_)) {
            return Err(BranchError::new(BranchErrorKind::Configuration));
        }
        let config = if *config.run_config() == RunConfig::default() {
            config.with_run_config(self.run_config.clone())
        } else {
            config
        };
        Ok(config.with_checkpoint_policy(CheckpointPolicy::EverySuperstep))
    }
    /// Resumes latest-only; approval decisions require an explicit checkpoint pin.
    /// Always saves EverySuperstep, overriding the supplied Core checkpoint policy.
    pub async fn resume(
        &self,
        config: ResumeConfig<BranchSnapshot>,
    ) -> Result<BranchRunOutcome, BranchError> {
        let result = self
            .graph
            .resume(self.resume_config(config)?)
            .await
            .map_err(BranchError::graph)?;
        BranchRunOutcome::from_execution(result, &self.stages)
    }
    /// Re-executes without lineage writes. Unfinished Replay may repeat external effects.
    pub async fn replay(
        &self,
        config: ReplayConfig<BranchSnapshot>,
    ) -> Result<BranchReplayReport, BranchError> {
        let config = if *config.run_config() == RunConfig::default() {
            config.with_run_config(self.run_config.clone())
        } else {
            config
        };
        let report = self
            .graph
            .replay(config)
            .await
            .map_err(BranchError::graph)?;
        BranchReplayReport::from_replay(report, &self.stages)
    }
    /// Creates a writable historical branch, always using EverySuperstep.
    /// Guards and output conversion can fail after branch creation; no rollback occurs.
    pub async fn fork(
        &self,
        config: ForkConfig<BranchSnapshot>,
    ) -> Result<BranchForkReport, BranchError> {
        let config = if *config.run_config() == RunConfig::default() {
            config.with_run_config(self.run_config.clone())
        } else {
            config
        };
        let report = self
            .graph
            .fork(config.with_checkpoint_policy(CheckpointPolicy::EverySuperstep))
            .await
            .map_err(BranchError::graph)?;
        BranchForkReport::from_fork(report, &self.stages)
    }
}
