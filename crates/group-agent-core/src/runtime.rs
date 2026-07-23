use tracing::debug;

use crate::graph::CompiledTransition;
use crate::{
    CompiledGraph, EventConfig, EventRetention, GraphEvent, GraphRunError, GraphState, NodeContext,
    NodeId, RunConfig, RunFailure, RunId,
};

/// The outcome of a successful graph invocation.
#[derive(Clone, Debug)]
pub struct RunReport<S>
where
    S: GraphState,
{
    run_id: RunId,
    final_state: S,
    steps: usize,
    visited_nodes: Vec<NodeId>,
    events: Vec<GraphEvent>,
}

impl<S> RunReport<S>
where
    S: GraphState,
{
    /// Returns the identifier assigned to this invocation.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the state after all updates were applied.
    #[must_use]
    pub const fn final_state(&self) -> &S {
        &self.final_state
    }

    /// Consumes the report and returns the final state.
    #[must_use]
    pub fn into_final_state(self) -> S {
        self.final_state
    }

    /// Returns the number of nodes executed.
    #[must_use]
    pub const fn steps(&self) -> usize {
        self.steps
    }

    /// Returns executed node identifiers in execution order.
    #[must_use]
    pub fn visited_nodes(&self) -> &[NodeId] {
        &self.visited_nodes
    }

    /// Returns lifecycle events in emission order.
    #[must_use]
    pub fn events(&self) -> &[GraphEvent] {
        &self.events
    }
}

impl<S> CompiledGraph<S>
where
    S: GraphState,
{
    /// Invokes the graph with [`RunConfig::default`].
    pub async fn invoke(&self, initial_state: S) -> Result<RunReport<S>, GraphRunError> {
        self.invoke_with_config(initial_state, RunConfig::default())
            .await
    }

    /// Invokes the graph with an explicit run configuration.
    pub async fn invoke_with_config(
        &self,
        initial_state: S,
        config: RunConfig,
    ) -> Result<RunReport<S>, GraphRunError> {
        self.invoke_with_events(initial_state, config, EventConfig::default())
            .await
    }

    /// Invokes the graph with explicit run and event-delivery configuration.
    pub async fn invoke_with_events(
        &self,
        initial_state: S,
        config: RunConfig,
        event_config: EventConfig,
    ) -> Result<RunReport<S>, GraphRunError> {
        let run_id = RunId::next();
        let mut state = initial_state;
        let mut steps = 0;
        let mut visited_nodes = Vec::new();
        let mut events = EventEmitter::new(run_id, &event_config);
        events.emit(GraphEvent::RunStarted {
            run_id,
            max_steps: config.max_steps,
        });
        let mut current = self.entry_index;

        debug!(%run_id, max_steps = config.max_steps, "graph run started");

        while current != self.end_index {
            let compiled_node = self.node_at(current);
            let step = steps + 1;
            if steps >= config.max_steps {
                return events.fail(GraphRunError::MaxStepsExceeded {
                    max_steps: config.max_steps,
                    node_id: compiled_node.id.clone(),
                    step,
                });
            }

            let node_id = compiled_node.id.clone();
            let context = NodeContext::new(step, node_id.clone());
            events.emit(GraphEvent::NodeStarted {
                run_id,
                node_id: node_id.clone(),
                step,
            });
            visited_nodes.push(node_id.clone());
            steps = step;
            debug!(node_id = %node_id, step, "node started");

            let update = match compiled_node.node.run(&state, &context).await {
                Ok(update) => update,
                Err(source) => {
                    return events.fail(GraphRunError::NodeFailed {
                        node_id: node_id.clone(),
                        step,
                        source,
                    });
                }
            };

            events.emit(GraphEvent::NodeCompleted {
                run_id,
                node_id: node_id.clone(),
                step,
            });

            if let Err(source) = state.apply(update) {
                return events.fail(GraphRunError::StateUpdateFailed {
                    node_id: node_id.clone(),
                    step,
                    source,
                });
            }

            events.emit(GraphEvent::StateUpdated {
                run_id,
                node_id: node_id.clone(),
                step,
            });
            debug!(node_id = %node_id, step, "state updated");

            current = match &compiled_node.transition {
                CompiledTransition::Fixed(target) => *target,
                CompiledTransition::Conditional {
                    router,
                    allowed_targets,
                } => {
                    let target_id = match router(&state) {
                        Ok(target_id) => target_id,
                        Err(source) => {
                            return events.fail(GraphRunError::RouteFailed {
                                node_id: node_id.clone(),
                                step,
                                source,
                            });
                        }
                    };
                    let Some(target_index) = allowed_targets.get(&target_id).copied() else {
                        return events.fail(GraphRunError::InvalidRouteTarget {
                            node_id: node_id.clone(),
                            target: target_id,
                            step,
                        });
                    };
                    events.emit(GraphEvent::RouteSelected {
                        run_id,
                        source: node_id,
                        target: target_id,
                        step,
                    });
                    target_index
                }
            };
        }

        events.emit(GraphEvent::RunCompleted { run_id, steps });
        debug!(%run_id, steps, "graph run completed");

        Ok(RunReport {
            run_id,
            final_state: state,
            steps,
            visited_nodes,
            events: events.into_retained(),
        })
    }
}

struct EventEmitter<'a> {
    run_id: RunId,
    config: &'a EventConfig,
    retained: Vec<GraphEvent>,
}

impl<'a> EventEmitter<'a> {
    fn new(run_id: RunId, config: &'a EventConfig) -> Self {
        Self {
            run_id,
            config,
            retained: Vec::new(),
        }
    }

    fn emit(&mut self, event: GraphEvent) {
        if let Some(sink) = self.config.sink() {
            sink.on_event(&event);
        }
        if self.config.retention() == EventRetention::All {
            self.retained.push(event);
        }
    }

    fn fail<T>(&mut self, error: GraphRunError) -> Result<T, GraphRunError> {
        let failure = RunFailure::from(&error);
        self.emit(GraphEvent::RunFailed {
            run_id: self.run_id,
            failure,
        });
        Err(error)
    }

    fn into_retained(self) -> Vec<GraphEvent> {
        self.retained
    }
}

impl From<&GraphRunError> for RunFailure {
    fn from(error: &GraphRunError) -> Self {
        match error {
            GraphRunError::MaxStepsExceeded {
                max_steps,
                node_id,
                step,
            } => Self::MaxStepsExceeded {
                max_steps: *max_steps,
                node_id: node_id.clone(),
                step: *step,
            },
            GraphRunError::NodeFailed { node_id, step, .. } => Self::NodeFailed {
                node_id: node_id.clone(),
                step: *step,
            },
            GraphRunError::StateUpdateFailed { node_id, step, .. } => Self::StateUpdateFailed {
                node_id: node_id.clone(),
                step: *step,
            },
            GraphRunError::RouteFailed { node_id, step, .. } => Self::RouteFailed {
                node_id: node_id.clone(),
                step: *step,
            },
            GraphRunError::InvalidRouteTarget {
                node_id,
                target,
                step,
            } => Self::InvalidRouteTarget {
                node_id: node_id.clone(),
                target: target.clone(),
                step: *step,
            },
        }
    }
}
