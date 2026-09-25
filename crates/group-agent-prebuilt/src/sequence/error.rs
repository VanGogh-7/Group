use super::AgentStageId;
use std::{error::Error, fmt};

/// Application mapping failure. Default formatting never includes its source.
pub struct HandoffError(Box<dyn Error + Send + Sync>);
impl HandoffError {
    pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self {
        Self(Box::new(source))
    }
}
impl fmt::Debug for HandoffError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HandoffError")
    }
}
impl fmt::Display for HandoffError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("agent handoff failed")
    }
}
impl Error for HandoffError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Construction failure classification for an experimental sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SequenceErrorKind {
    Configuration,
    InvalidState,
    Graph,
    Output,
}
/// Payload-safe error retaining its concrete source and optional stage identity.
pub struct SequenceError {
    pub(crate) kind: SequenceErrorKind,
    pub(crate) stage: Option<AgentStageId>,
    source: Option<Box<dyn Error + Send + Sync>>,
}
/// Construction and invocation share the same typed classification and source rules.
pub type SequenceBuildError = SequenceError;
impl SequenceError {
    pub(crate) fn new(kind: SequenceErrorKind) -> Self {
        Self {
            kind,
            stage: None,
            source: None,
        }
    }
    pub(crate) fn source_error(
        kind: SequenceErrorKind,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            stage: None,
            source: Some(Box::new(source)),
        }
    }
    pub(crate) fn graph(source: group_agent_core::GraphRunError) -> Self {
        let mut stage = None;
        let mut current: Option<&(dyn Error + 'static)> = Some(&source);
        while let Some(error) = current {
            if let Some(error) = error.downcast_ref::<StageFailure>() {
                stage = Some(error.stage.clone());
                break;
            }
            current = error.source();
        }
        let mut error = Self::source_error(SequenceErrorKind::Graph, source);
        error.stage = stage;
        error
    }
    pub(crate) fn at_stage(mut self, stage: AgentStageId) -> Self {
        self.stage = Some(stage);
        self
    }
    pub(crate) fn node(kind: SequenceErrorKind, source: group_agent_core::NodeError) -> Self {
        let mut current: Option<&(dyn Error + 'static)> = Some(&source);
        let mut stage = None;
        while let Some(error) = current {
            if let Some(error) = error.downcast_ref::<StageFailure>() {
                stage = Some(error.stage.clone());
                break;
            }
            current = error.source();
        }
        let mut error = Self::source_error(kind, source);
        error.stage = stage;
        error
    }
    pub fn kind(&self) -> SequenceErrorKind {
        self.kind
    }
    pub fn stage(&self) -> Option<&AgentStageId> {
        self.stage.as_ref()
    }
}
impl fmt::Debug for SequenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SequenceError")
            .field("kind", &self.kind)
            .field("stage", &self.stage)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for SequenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("agent sequence failed")
    }
}
impl Error for SequenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref() as _)
    }
}
#[derive(Debug)]
pub(crate) struct StageFailure {
    pub stage: AgentStageId,
    pub source: group_agent_core::NodeError,
}
impl fmt::Display for StageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("agent stage failed")
    }
}
impl Error for StageFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
