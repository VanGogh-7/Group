use std::future::pending;
use std::time::Duration;

use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::graph::CompiledTransition;
use crate::{
    CompiledGraph, EventConfig, EventRetention, GraphEvent, GraphRunError, GraphState, NodeContext,
    NodeId, RunConfig, RunControl, RunFailure, RunId,
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
        self.invoke_with_control(initial_state, config, event_config, RunControl::default())
            .await
    }

    /// Invokes the graph with explicit run, event, and execution controls.
    pub async fn invoke_with_control(
        &self,
        initial_state: S,
        config: RunConfig,
        event_config: EventConfig,
        control: RunControl,
    ) -> Result<RunReport<S>, GraphRunError> {
        let invocation_started = Instant::now();
        let run_id = RunId::next();
        let control = ActiveControl::new(control, invocation_started);
        let mut state = initial_state;
        let mut steps = 0;
        let mut visited_nodes = Vec::new();
        let mut events = EventEmitter::new(run_id, &event_config);
        events.emit(|| GraphEvent::RunStarted {
            run_id,
            max_steps: config.max_steps,
        });
        let mut current = self.entry_index;

        debug!(%run_id, max_steps = config.max_steps, "graph run started");

        let initial_node = (current != self.end_index).then(|| &self.node_at(current).id);
        let initial_step = usize::from(initial_node.is_some());
        if let Some(error) =
            control.check(run_id, initial_node, initial_step, control.deadline(None))
        {
            return events.fail(error);
        }

        while current != self.end_index {
            let compiled_node = self.node_at(current);
            let step = steps + 1;
            if let Some(error) = control.check(
                run_id,
                Some(&compiled_node.id),
                step,
                control.deadline(None),
            ) {
                return events.fail(error);
            }
            if steps >= config.max_steps {
                return events.fail(GraphRunError::MaxStepsExceeded {
                    max_steps: config.max_steps,
                    node_id: compiled_node.id.clone(),
                    step,
                });
            }

            let node_id = compiled_node.id.clone();
            let node_started = Instant::now();
            let node_deadline = control.node_deadline(node_started);
            let active_deadline = control.deadline(node_deadline);
            let context = NodeContext::new(
                step,
                node_id.clone(),
                control.cancellation_token.clone(),
                control.run_deadline,
            );
            events.emit(|| GraphEvent::NodeStarted {
                run_id,
                node_id: node_id.clone(),
                step,
            });
            visited_nodes.push(node_id.clone());
            steps = step;
            debug!(node_id = %node_id, step, "node started");

            if let Some(error) = control.check(run_id, Some(&node_id), step, active_deadline) {
                return events.fail(error);
            }

            let node_result = if control.is_disabled() {
                compiled_node.node.run(&state, &context).await
            } else {
                let node_future = compiled_node.node.run(&state, &context);
                tokio::pin!(node_future);
                tokio::select! {
                    biased;
                    () = control.cancellation_token.cancelled() => {
                        return events.fail(control.cancelled_error(
                            run_id,
                            Some(&node_id),
                            step,
                        ));
                    }
                    deadline = wait_for_deadline(active_deadline) => {
                        return events.fail(control.deadline_error(
                            run_id,
                            Some(&node_id),
                            step,
                            deadline,
                        ));
                    }
                    result = &mut node_future => result,
                }
            };

            if let Some(error) = control.check(run_id, Some(&node_id), step, control.deadline(None))
            {
                return events.fail(error);
            }

            let update = match node_result {
                Ok(update) => update,
                Err(source) => {
                    return events.fail(GraphRunError::NodeFailed {
                        node_id: node_id.clone(),
                        step,
                        source,
                    });
                }
            };

            events.emit(|| GraphEvent::NodeCompleted {
                run_id,
                node_id: node_id.clone(),
                step,
            });

            if let Some(error) = control.check(run_id, Some(&node_id), step, control.deadline(None))
            {
                return events.fail(error);
            }

            if let Err(source) = state.apply(update) {
                return events.fail(GraphRunError::StateUpdateFailed {
                    node_id: node_id.clone(),
                    step,
                    source,
                });
            }

            events.emit(|| GraphEvent::StateUpdated {
                run_id,
                node_id: node_id.clone(),
                step,
            });
            debug!(node_id = %node_id, step, "state updated");

            if let Some(error) = control.check(run_id, Some(&node_id), step, control.deadline(None))
            {
                return events.fail(error);
            }

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
                    events.emit(|| GraphEvent::RouteSelected {
                        run_id,
                        source: node_id.clone(),
                        target: target_id,
                        step,
                    });
                    target_index
                }
            };

            if let Some(error) = control.check(run_id, Some(&node_id), step, control.deadline(None))
            {
                return events.fail(error);
            }
        }

        if let Some(error) = control.check(run_id, None, steps, control.deadline(None)) {
            return events.fail(error);
        }

        events.emit(|| GraphEvent::RunCompleted { run_id, steps });
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
    enabled: bool,
    retained: Vec<GraphEvent>,
}

impl<'a> EventEmitter<'a> {
    fn new(run_id: RunId, config: &'a EventConfig) -> Self {
        Self {
            run_id,
            config,
            enabled: config.is_enabled(),
            retained: Vec::new(),
        }
    }

    fn emit<F>(&mut self, make_event: F)
    where
        F: FnOnce() -> GraphEvent,
    {
        if !self.enabled {
            return;
        }
        let event = make_event();
        if let Some(sink) = self.config.sink() {
            sink.on_event(&event);
        }
        if self.config.retention() == EventRetention::All {
            self.retained.push(event);
        }
    }

    fn fail<T>(&mut self, error: GraphRunError) -> Result<T, GraphRunError> {
        if self.enabled {
            let failure = RunFailure::from(&error);
            let run_id = self.run_id;
            self.emit(|| GraphEvent::RunFailed { run_id, failure });
        }
        Err(error)
    }

    fn into_retained(self) -> Vec<GraphEvent> {
        self.retained
    }
}

struct ActiveControl {
    cancellation_token: CancellationToken,
    cancellation_enabled: bool,
    run_timeout: Option<Duration>,
    run_deadline: Option<Instant>,
    node_timeout: Option<Duration>,
}

#[derive(Clone, Copy)]
enum ActiveDeadline {
    Run(Instant),
    Node(Instant),
}

impl ActiveDeadline {
    const fn instant(self) -> Instant {
        match self {
            Self::Run(deadline) | Self::Node(deadline) => deadline,
        }
    }
}

impl ActiveControl {
    fn new(control: RunControl, invocation_started: Instant) -> Self {
        let run_timeout = control.run_timeout();
        let cancellation_token = control.cancellation_token().cloned();
        Self {
            cancellation_enabled: cancellation_token.is_some(),
            cancellation_token: cancellation_token.unwrap_or_default(),
            run_timeout,
            run_deadline: run_timeout.map(|timeout| invocation_started + timeout),
            node_timeout: control.node_timeout(),
        }
    }

    fn is_disabled(&self) -> bool {
        !self.cancellation_enabled && self.run_deadline.is_none() && self.node_timeout.is_none()
    }

    fn node_deadline(&self, node_started: Instant) -> Option<Instant> {
        self.node_timeout.map(|timeout| node_started + timeout)
    }

    fn deadline(&self, node_deadline: Option<Instant>) -> Option<ActiveDeadline> {
        match (self.run_deadline, node_deadline) {
            (Some(run), Some(node)) if run <= node => Some(ActiveDeadline::Run(run)),
            (Some(_), Some(node)) => Some(ActiveDeadline::Node(node)),
            (Some(run), None) => Some(ActiveDeadline::Run(run)),
            (None, Some(node)) => Some(ActiveDeadline::Node(node)),
            (None, None) => None,
        }
    }

    fn check(
        &self,
        run_id: RunId,
        node_id: Option<&NodeId>,
        step: usize,
        deadline: Option<ActiveDeadline>,
    ) -> Option<GraphRunError> {
        if self.cancellation_token.is_cancelled() {
            return Some(self.cancelled_error(run_id, node_id, step));
        }
        if let Some(deadline) = deadline.filter(|deadline| Instant::now() >= deadline.instant()) {
            return Some(self.deadline_error(run_id, node_id, step, deadline));
        }
        None
    }

    fn deadline_error(
        &self,
        run_id: RunId,
        node_id: Option<&NodeId>,
        step: usize,
        deadline: ActiveDeadline,
    ) -> GraphRunError {
        match deadline {
            ActiveDeadline::Run(_) => self.run_timeout_error(run_id, node_id, step),
            ActiveDeadline::Node(_) => self.node_timeout_error(
                run_id,
                node_id.expect("node deadline always has node context"),
                step,
            ),
        }
    }

    fn cancelled_error(
        &self,
        run_id: RunId,
        node_id: Option<&NodeId>,
        step: usize,
    ) -> GraphRunError {
        GraphRunError::Cancelled {
            run_id,
            node_id: node_id.cloned(),
            step,
        }
    }

    fn run_timeout_error(
        &self,
        run_id: RunId,
        node_id: Option<&NodeId>,
        step: usize,
    ) -> GraphRunError {
        GraphRunError::RunTimedOut {
            run_id,
            timeout: self
                .run_timeout
                .expect("run deadline exists only with a configured timeout"),
            node_id: node_id.cloned(),
            step,
        }
    }

    fn node_timeout_error(&self, run_id: RunId, node_id: &NodeId, step: usize) -> GraphRunError {
        GraphRunError::NodeTimedOut {
            run_id,
            timeout: self
                .node_timeout
                .expect("node deadline exists only with a configured timeout"),
            node_id: node_id.clone(),
            step,
        }
    }
}

async fn wait_for_deadline(deadline: Option<ActiveDeadline>) -> ActiveDeadline {
    if let Some(deadline) = deadline {
        sleep_until(deadline.instant()).await;
        deadline
    } else {
        pending::<ActiveDeadline>().await
    }
}

impl From<&GraphRunError> for RunFailure {
    fn from(error: &GraphRunError) -> Self {
        match error {
            GraphRunError::Cancelled { node_id, step, .. } => Self::Cancelled {
                node_id: node_id.clone(),
                step: *step,
            },
            GraphRunError::RunTimedOut {
                timeout,
                node_id,
                step,
                ..
            } => Self::RunTimedOut {
                timeout: *timeout,
                node_id: node_id.clone(),
                step: *step,
            },
            GraphRunError::NodeTimedOut {
                timeout,
                node_id,
                step,
                ..
            } => Self::NodeTimedOut {
                timeout: *timeout,
                node_id: node_id.clone(),
                step: *step,
            },
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
