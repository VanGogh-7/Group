use tracing::debug;

use crate::{CompiledGraph, GraphEvent, GraphRunError, GraphState, NodeContext, NodeId, RunConfig};

/// The outcome of a successful graph invocation.
#[derive(Clone, Debug)]
pub struct RunReport<S>
where
    S: GraphState,
{
    final_state: S,
    steps: usize,
    visited_nodes: Vec<NodeId>,
    events: Vec<GraphEvent>,
}

impl<S> RunReport<S>
where
    S: GraphState,
{
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
        let mut state = initial_state;
        let mut steps = 0;
        let mut visited_nodes = Vec::new();
        let mut events = vec![GraphEvent::RunStarted {
            max_steps: config.max_steps,
        }];
        let mut current = self.first_node_id();

        debug!(max_steps = config.max_steps, "graph run started");

        while current != NodeId::end() {
            let step = steps + 1;
            if steps >= config.max_steps {
                return Err(GraphRunError::MaxStepsExceeded {
                    max_steps: config.max_steps,
                    node_id: current,
                    step,
                });
            }

            let context = NodeContext::new(step, current.clone());
            events.push(GraphEvent::NodeStarted {
                node_id: current.clone(),
                step,
            });
            debug!(node_id = %current, step, "node started");

            let update = self
                .node(&current)
                .run(&state, &context)
                .await
                .map_err(|source| GraphRunError::NodeFailed {
                    node_id: current.clone(),
                    step,
                    source,
                })?;

            events.push(GraphEvent::NodeCompleted {
                node_id: current.clone(),
                step,
            });

            state
                .apply(update)
                .map_err(|source| GraphRunError::StateUpdateFailed {
                    node_id: current.clone(),
                    step,
                    source,
                })?;

            events.push(GraphEvent::StateUpdated {
                node_id: current.clone(),
                step,
            });
            debug!(node_id = %current, step, "state updated");

            visited_nodes.push(current.clone());
            steps = step;
            current = self.successor_id(&current);
        }

        events.push(GraphEvent::RunCompleted { steps });
        debug!(steps, "graph run completed");

        Ok(RunReport {
            final_state: state,
            steps,
            visited_nodes,
            events,
        })
    }
}
