use super::AgentStageId;
use std::{error::Error, fmt};

/// Application mapping failure. Default formatting never includes its source.
pub struct BranchSelectionError(Box<dyn Error + Send + Sync>);
impl BranchSelectionError {
    pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self {
        Self(Box::new(source))
    }
}
impl fmt::Debug for BranchSelectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BranchSelectionError")
    }
}
impl fmt::Display for BranchSelectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("agent selection failed")
    }
}
impl Error for BranchSelectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Construction failure classification for an experimental branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BranchErrorKind {
    Configuration,
    InvalidState,
    Graph,
    Output,
    SelectorPanicked,
}
/// Payload-safe error retaining its concrete source and optional stage identity.
pub struct BranchError {
    pub(crate) kind: BranchErrorKind,
    pub(crate) stage: Option<AgentStageId>,
    source: Option<Box<dyn Error + Send + Sync>>,
}
/// Construction and invocation share the same typed classification and source rules.
pub type BranchBuildError = BranchError;
impl BranchError {
    pub(crate) fn new(kind: BranchErrorKind) -> Self {
        Self {
            kind,
            stage: None,
            source: None,
        }
    }
    pub(crate) fn source_error(
        kind: BranchErrorKind,
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
        let mut scan: Option<&(dyn Error + 'static)> = Some(&source);
        let mut kind = BranchErrorKind::Graph;
        while let Some(error) = scan {
            if error.is::<SelectorPanicked>() {
                kind = BranchErrorKind::SelectorPanicked;
                break;
            }
            scan = error.source();
        }
        let mut error = Self::source_error(kind, source);
        error.stage = stage;
        error
    }
    pub(crate) fn at_stage(mut self, stage: AgentStageId) -> Self {
        self.stage = Some(stage);
        self
    }
    pub(crate) fn node(kind: BranchErrorKind, source: group_agent_core::NodeError) -> Self {
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
    pub fn kind(&self) -> BranchErrorKind {
        self.kind
    }
    pub fn stage(&self) -> Option<&AgentStageId> {
        self.stage.as_ref()
    }
}
impl fmt::Debug for BranchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BranchError")
            .field("kind", &self.kind)
            .field("stage", &self.stage)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for BranchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("agent branch failed")
    }
}
impl Error for BranchError {
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

#[derive(Debug)]
pub(crate) struct SelectorPanicked;
impl fmt::Display for SelectorPanicked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("branch selector panicked")
    }
}
impl Error for SelectorPanicked {}
