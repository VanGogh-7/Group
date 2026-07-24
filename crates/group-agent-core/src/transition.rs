use std::collections::HashSet;
use std::sync::Arc;

use indexmap::IndexMap;
use petgraph::stable_graph::NodeIndex;

use crate::edge::{FanOutRouter, Router};
use crate::{GraphPath, GraphState, NodeId, NodePath, RouteError};

pub(crate) enum CompiledTransition<S>
where
    S: GraphState,
{
    Fixed(Option<NodeIndex>),
    StaticFanOut(Vec<NodeIndex>),
    Conditional {
        router: Router<S>,
        allowed_targets: IndexMap<NodeId, Option<NodeIndex>>,
    },
    ConditionalFanOut {
        router: FanOutRouter<S>,
        allowed_targets: IndexMap<NodeId, Option<NodeIndex>>,
    },
}

pub(crate) enum RouteDecision {
    Single(NodePath),
    Multiple(Vec<NodePath>),
}

pub(crate) enum TransitionError {
    Router(RouteError),
    InvalidTarget(NodeId),
    EmptyTargets,
    DuplicateTarget(NodeId),
}

impl<S> CompiledTransition<S>
where
    S: GraphState,
{
    #[inline]
    pub(crate) fn resolve_into<F>(
        &self,
        state: &S,
        graph_path: &GraphPath,
        targets: &mut Vec<NodeIndex>,
        path_at: F,
    ) -> Result<Option<RouteDecision>, TransitionError>
    where
        F: Fn(NodeIndex) -> NodePath,
    {
        match self {
            Self::Fixed(target) => {
                targets.extend(*target);
                Ok(None)
            }
            Self::StaticFanOut(compiled_targets) => {
                targets.extend(compiled_targets.iter().copied());
                Ok(None)
            }
            Self::Conditional {
                router,
                allowed_targets,
            } => {
                let target_id = router(state).map_err(TransitionError::Router)?;
                let target = allowed_targets
                    .get(&target_id)
                    .copied()
                    .ok_or_else(|| TransitionError::InvalidTarget(target_id.clone()))?;
                let target_path =
                    target.map_or_else(|| NodePath::new(graph_path, target_id), &path_at);
                targets.extend(target);
                Ok(Some(RouteDecision::Single(target_path)))
            }
            Self::ConditionalFanOut {
                router,
                allowed_targets,
            } => {
                let selected = router(state).map_err(TransitionError::Router)?;
                if selected.is_empty() {
                    return Err(TransitionError::EmptyTargets);
                }

                let mut seen = HashSet::with_capacity(selected.len());
                let mut resolved = Vec::with_capacity(selected.len());
                for target_id in selected {
                    if !seen.insert(target_id.clone()) {
                        return Err(TransitionError::DuplicateTarget(target_id));
                    }
                    let target = allowed_targets
                        .get(&target_id)
                        .copied()
                        .ok_or_else(|| TransitionError::InvalidTarget(target_id.clone()))?;
                    resolved.push((target, target_id));
                }
                resolved.sort_unstable_by_key(|(target, _)| {
                    target.map_or((1, usize::MAX), |index| (0, index.index()))
                });

                let mut selected_paths = Vec::with_capacity(resolved.len());
                for (target, target_id) in resolved {
                    selected_paths.push(
                        target.map_or_else(|| NodePath::new(graph_path, target_id), &path_at),
                    );
                    targets.extend(target);
                }
                Ok(Some(RouteDecision::Multiple(selected_paths)))
            }
        }
    }

    pub(crate) fn remap(&self, mapping: &[NodeIndex]) -> Self {
        let target = |target: Option<NodeIndex>| target.map(|index| mapping[index.index()]);
        let remap_allowed = |allowed_targets: &IndexMap<NodeId, Option<NodeIndex>>| {
            allowed_targets
                .iter()
                .map(|(node_id, index)| (node_id.clone(), target(*index)))
                .collect()
        };
        match self {
            Self::Fixed(index) => Self::Fixed(target(*index)),
            Self::StaticFanOut(indices) => {
                Self::StaticFanOut(indices.iter().map(|index| mapping[index.index()]).collect())
            }
            Self::Conditional {
                router,
                allowed_targets,
            } => Self::Conditional {
                router: Arc::clone(router),
                allowed_targets: remap_allowed(allowed_targets),
            },
            Self::ConditionalFanOut {
                router,
                allowed_targets,
            } => Self::ConditionalFanOut {
                router: Arc::clone(router),
                allowed_targets: remap_allowed(allowed_targets),
            },
        }
    }
}
