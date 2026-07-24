use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::checkpoint::CheckpointLineage;
use crate::graph::{CompiledNode, CompiledTransition};
use crate::{
    CheckpointConfig, CheckpointId, CheckpointPolicy, CheckpointRequest, CheckpointState,
    CheckpointWriteError, CompiledGraph, EventConfig, EventRetention, GraphEvent, GraphRunError,
    GraphState, NodeContext, NodeId, NodeUpdate, RunConfig, RunControl, RunFailure, RunId,
    ThreadId,
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
        self.invoke_internal(
            initial_state,
            config,
            event_config,
            control,
            DisabledCheckpoint,
        )
        .await
    }

    async fn invoke_internal<C>(
        &self,
        initial_state: S,
        config: RunConfig,
        event_config: EventConfig,
        control: RunControl,
        mut checkpoints: C,
    ) -> Result<RunReport<S>, GraphRunError>
    where
        C: RuntimeCheckpoint<S>,
    {
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
        let mut frontier = if self.entry_index == self.end_index {
            Vec::new()
        } else {
            vec![self.entry_index]
        };
        let mut next_frontier = Vec::new();
        let mut superstep = 0;

        debug!(%run_id, max_steps = config.max_steps, "graph run started");

        let initial_node = frontier.first().map(|index| &self.node_at(*index).id);
        let initial_step = usize::from(initial_node.is_some());
        if let Some(error) =
            control.check(run_id, initial_node, initial_step, control.deadline(None))
        {
            return events.fail(error);
        }

        if frontier.is_empty() && checkpoints.should_save(true) {
            let checkpoint_future = checkpoints.save(run_id, 0, 0, &state, Vec::new(), true);
            let save_result = if control.is_disabled() {
                checkpoint_future.await
            } else {
                tokio::pin!(checkpoint_future);
                let run_deadline = control.deadline(None);
                tokio::select! {
                    biased;
                    () = control.cancellation_token.cancelled() => {
                        return events.fail(control.cancelled_error(run_id, None, 0));
                    }
                    deadline = wait_for_deadline(run_deadline) => {
                        return events.fail(control.deadline_error(run_id, None, 0, deadline));
                    }
                    result = &mut checkpoint_future => result,
                }
            };
            let saved = match save_result {
                Ok(saved) => saved,
                Err(error) => return events.fail(error),
            };
            events.emit(|| GraphEvent::CheckpointSaved {
                run_id,
                checkpoint_id: saved.id,
                thread_id: saved.thread_id,
                superstep: 0,
                step: 0,
                completed: true,
            });
        }

        while !frontier.is_empty() {
            let first_node = &self.node_at(frontier[0]).id;
            let first_step = steps + 1;
            if let Some(error) =
                control.check(run_id, Some(first_node), first_step, control.deadline(None))
            {
                return events.fail(error);
            }

            let remaining_steps = config.max_steps.saturating_sub(steps);
            if frontier.len() > remaining_steps {
                let blocked_offset = remaining_steps;
                let blocked_node = &self.node_at(frontier[blocked_offset]).id;
                return events.fail(GraphRunError::MaxStepsExceeded {
                    max_steps: config.max_steps,
                    node_id: blocked_node.clone(),
                    step: steps + blocked_offset + 1,
                });
            }

            superstep += 1;
            let is_parallel = frontier.len() > 1;
            if is_parallel {
                events.emit(|| GraphEvent::SuperstepStarted {
                    run_id,
                    superstep,
                    node_ids: frontier
                        .iter()
                        .map(|index| self.node_at(*index).id.clone())
                        .collect(),
                });
            }

            let step_base = steps;
            if !is_parallel {
                let compiled_node = self.node_at(frontier[0]);
                let step = first_step;
                if let Some(error) = control.check(
                    run_id,
                    Some(&compiled_node.id),
                    step,
                    control.deadline(None),
                ) {
                    return events.fail(error);
                }

                let node_deadline = control.node_deadline(Instant::now());
                let context = NodeContext::new(
                    step,
                    compiled_node.id.clone(),
                    control.cancellation_token.clone(),
                    control.run_deadline,
                );
                events.emit(|| GraphEvent::NodeStarted {
                    run_id,
                    node_id: compiled_node.id.clone(),
                    step,
                });
                visited_nodes.push(compiled_node.id.clone());
                debug!(node_id = %compiled_node.id, step, superstep, "node started");

                if let Some(error) = control.check(
                    run_id,
                    Some(&compiled_node.id),
                    step,
                    control.deadline(node_deadline),
                ) {
                    return events.fail(error);
                }
                steps = step;
                let update = match execute_node(
                    &control,
                    run_id,
                    compiled_node,
                    &state,
                    &context,
                    node_deadline,
                )
                .await
                {
                    Ok(update) => update,
                    Err(error) => return events.fail(error),
                };
                events.emit(|| GraphEvent::NodeCompleted {
                    run_id,
                    node_id: compiled_node.id.clone(),
                    step: step_base + 1,
                });
                if let Some(error) = control.check(
                    run_id,
                    Some(&compiled_node.id),
                    step_base + 1,
                    control.deadline(None),
                ) {
                    return events.fail(error);
                }

                if let Some(error) =
                    control.check(run_id, Some(first_node), first_step, control.deadline(None))
                {
                    return events.fail(error);
                }
                if let Err(source) = state.apply(update) {
                    return events.fail(GraphRunError::StateUpdateFailed {
                        node_id: first_node.clone(),
                        step: first_step,
                        source,
                    });
                }
            } else {
                let mut contexts = Vec::with_capacity(frontier.len());
                let mut node_deadlines = Vec::with_capacity(frontier.len());
                for (offset, index) in frontier.iter().copied().enumerate() {
                    let compiled_node = self.node_at(index);
                    let step = step_base + offset + 1;
                    if let Some(error) = control.check(
                        run_id,
                        Some(&compiled_node.id),
                        step,
                        control.deadline(None),
                    ) {
                        return events.fail(error);
                    }

                    let node_deadline = control.node_deadline(Instant::now());
                    let context = NodeContext::new(
                        step,
                        compiled_node.id.clone(),
                        control.cancellation_token.clone(),
                        control.run_deadline,
                    );
                    events.emit(|| GraphEvent::NodeStarted {
                        run_id,
                        node_id: compiled_node.id.clone(),
                        step,
                    });
                    visited_nodes.push(compiled_node.id.clone());
                    debug!(node_id = %compiled_node.id, step, superstep, "node started");

                    if let Some(error) = control.check(
                        run_id,
                        Some(&compiled_node.id),
                        step,
                        control.deadline(node_deadline),
                    ) {
                        return events.fail(error);
                    }
                    contexts.push(context);
                    node_deadlines.push(node_deadline);
                }
                steps += frontier.len();

                let mut pending_nodes = FuturesUnordered::new();
                for (offset, index) in frontier.iter().copied().enumerate() {
                    let compiled_node = self.node_at(index);
                    let context = &contexts[offset];
                    let node_deadline = node_deadlines[offset];
                    let control = &control;
                    let state = &state;
                    pending_nodes.push(async move {
                        (
                            offset,
                            execute_node(
                                control,
                                run_id,
                                compiled_node,
                                state,
                                context,
                                node_deadline,
                            )
                            .await,
                        )
                    });
                }

                let mut update_slots = std::iter::repeat_with(|| None)
                    .take(frontier.len())
                    .collect::<Vec<_>>();
                while let Some((offset, result)) = pending_nodes.next().await {
                    let compiled_node = self.node_at(frontier[offset]);
                    let step = step_base + offset + 1;
                    let update = match result {
                        Ok(update) => update,
                        Err(error) => return events.fail(error),
                    };
                    events.emit(|| GraphEvent::NodeCompleted {
                        run_id,
                        node_id: compiled_node.id.clone(),
                        step,
                    });
                    if let Some(error) = control.check(
                        run_id,
                        Some(&compiled_node.id),
                        step,
                        control.deadline(None),
                    ) {
                        return events.fail(error);
                    }
                    update_slots[offset] = Some(update);
                }
                drop(pending_nodes);

                if let Some(error) =
                    control.check(run_id, Some(first_node), first_step, control.deadline(None))
                {
                    return events.fail(error);
                }
                let node_ids = frontier
                    .iter()
                    .map(|index| self.node_at(*index).id.clone())
                    .collect::<Vec<_>>();
                let batch = node_ids
                    .iter()
                    .cloned()
                    .zip(update_slots)
                    .map(|(node_id, update)| {
                        NodeUpdate::new(
                            node_id,
                            update.expect("every parallel node completed successfully"),
                        )
                    })
                    .collect();
                if let Err(source) = state.apply_batch(batch) {
                    return events.fail(GraphRunError::StateBatchUpdateFailed {
                        node_ids,
                        step: first_step,
                        source,
                    });
                }
            }

            for (offset, index) in frontier.iter().copied().enumerate() {
                let compiled_node = self.node_at(index);
                let step = step_base + offset + 1;
                events.emit(|| GraphEvent::StateUpdated {
                    run_id,
                    node_id: compiled_node.id.clone(),
                    step,
                });
                debug!(node_id = %compiled_node.id, step, superstep, "state updated");
                if let Some(error) = control.check(
                    run_id,
                    Some(&compiled_node.id),
                    step,
                    control.deadline(None),
                ) {
                    return events.fail(error);
                }
            }

            next_frontier.clear();
            for (offset, index) in frontier.iter().copied().enumerate() {
                let compiled_node = self.node_at(index);
                let node_id = &compiled_node.id;
                let step = step_base + offset + 1;
                match &compiled_node.transition {
                    CompiledTransition::Fixed(target) => {
                        if *target != self.end_index {
                            next_frontier.push(*target);
                        }
                    }
                    CompiledTransition::FanOut(targets) => {
                        next_frontier.extend(
                            targets
                                .iter()
                                .copied()
                                .filter(|target| *target != self.end_index),
                        );
                    }
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
                        if target_index != self.end_index {
                            next_frontier.push(target_index);
                        }
                    }
                }

                if let Some(error) =
                    control.check(run_id, Some(node_id), step, control.deadline(None))
                {
                    return events.fail(error);
                }
            }

            next_frontier.sort_unstable_by_key(|index| index.index());
            next_frontier.dedup();
            let completed = next_frontier.is_empty();
            if checkpoints.should_save(completed) {
                let next_node_ids = next_frontier
                    .iter()
                    .map(|index| self.node_at(*index).id.clone())
                    .collect();
                let checkpoint_future =
                    checkpoints.save(run_id, superstep, steps, &state, next_node_ids, completed);
                let save_result = if control.is_disabled() {
                    checkpoint_future.await
                } else {
                    tokio::pin!(checkpoint_future);
                    let run_deadline = control.deadline(None);
                    tokio::select! {
                        biased;
                        () = control.cancellation_token.cancelled() => {
                            return events.fail(control.cancelled_error(run_id, None, steps));
                        }
                        deadline = wait_for_deadline(run_deadline) => {
                            return events.fail(control.deadline_error(run_id, None, steps, deadline));
                        }
                        result = &mut checkpoint_future => result,
                    }
                };
                let saved = match save_result {
                    Ok(saved) => saved,
                    Err(error) => return events.fail(error),
                };
                events.emit(|| GraphEvent::CheckpointSaved {
                    run_id,
                    checkpoint_id: saved.id,
                    thread_id: saved.thread_id,
                    superstep,
                    step: steps,
                    completed,
                });
                if let Some(error) = control.check(run_id, None, steps, control.deadline(None)) {
                    return events.fail(error);
                }
            }
            if is_parallel {
                events.emit(|| GraphEvent::SuperstepCompleted { run_id, superstep });
            }
            if let Some(error) =
                control.check(run_id, Some(first_node), first_step, control.deadline(None))
            {
                return events.fail(error);
            }
            std::mem::swap(&mut frontier, &mut next_frontier);
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

impl<S> CompiledGraph<S>
where
    S: CheckpointState,
{
    /// Invokes the graph with explicit execution and checkpoint configuration.
    pub async fn invoke_with_checkpoint(
        &self,
        initial_state: S,
        config: RunConfig,
        event_config: EventConfig,
        control: RunControl,
        checkpoint_config: CheckpointConfig<S::Snapshot>,
    ) -> Result<RunReport<S>, GraphRunError> {
        self.invoke_internal(
            initial_state,
            config,
            event_config,
            control,
            EnabledCheckpoint::new(checkpoint_config),
        )
        .await
    }
}

struct SavedCheckpoint {
    id: CheckpointId,
    thread_id: ThreadId,
}

#[async_trait]
trait RuntimeCheckpoint<S>: Send
where
    S: GraphState,
{
    fn should_save(&self, completed: bool) -> bool;

    async fn save(
        &mut self,
        run_id: RunId,
        superstep: usize,
        step: usize,
        state: &S,
        next_frontier: Vec<NodeId>,
        completed: bool,
    ) -> Result<SavedCheckpoint, GraphRunError>;
}

struct DisabledCheckpoint;

#[async_trait]
impl<S> RuntimeCheckpoint<S> for DisabledCheckpoint
where
    S: GraphState,
{
    fn should_save(&self, _completed: bool) -> bool {
        false
    }

    async fn save(
        &mut self,
        _run_id: RunId,
        _superstep: usize,
        _step: usize,
        _state: &S,
        _next_frontier: Vec<NodeId>,
        _completed: bool,
    ) -> Result<SavedCheckpoint, GraphRunError> {
        unreachable!("disabled checkpointing never enters the storage path")
    }
}

struct EnabledCheckpoint<S>
where
    S: CheckpointState,
{
    config: CheckpointConfig<S::Snapshot>,
    expected_parent: Option<CheckpointId>,
}

impl<S> EnabledCheckpoint<S>
where
    S: CheckpointState,
{
    fn new(config: CheckpointConfig<S::Snapshot>) -> Self {
        let expected_parent = config.expected_parent();
        Self {
            config,
            expected_parent,
        }
    }
}

#[async_trait]
impl<S> RuntimeCheckpoint<S> for EnabledCheckpoint<S>
where
    S: CheckpointState,
{
    fn should_save(&self, completed: bool) -> bool {
        self.config.policy() == CheckpointPolicy::EverySuperstep || completed
    }

    async fn save(
        &mut self,
        run_id: RunId,
        superstep: usize,
        step: usize,
        state: &S,
        next_frontier: Vec<NodeId>,
        completed: bool,
    ) -> Result<SavedCheckpoint, GraphRunError> {
        let thread_id = self.config.thread_id().clone();
        let snapshot =
            state
                .snapshot()
                .map(Arc::new)
                .map_err(|source| GraphRunError::SnapshotFailed {
                    run_id,
                    thread_id: thread_id.clone(),
                    superstep,
                    step,
                    source,
                })?;
        let request = CheckpointRequest::new(
            CheckpointLineage::new(
                CheckpointId::next(),
                self.expected_parent,
                thread_id.clone(),
                run_id,
            ),
            superstep,
            step,
            snapshot,
            next_frontier,
            completed,
        );
        let checkpoint = self
            .config
            .checkpointer()
            .save(request)
            .await
            .map_err(|source| match source {
                CheckpointWriteError::Conflict {
                    expected_parent,
                    actual_parent,
                } => GraphRunError::CheckpointConflict {
                    run_id,
                    thread_id: thread_id.clone(),
                    superstep,
                    step,
                    expected_parent,
                    actual_parent,
                },
                CheckpointWriteError::Failed(source) => GraphRunError::CheckpointSaveFailed {
                    run_id,
                    thread_id: thread_id.clone(),
                    superstep,
                    step,
                    source,
                },
            })?;
        self.expected_parent = Some(checkpoint.id());
        Ok(SavedCheckpoint {
            id: checkpoint.id(),
            thread_id,
        })
    }
}

async fn execute_node<S>(
    control: &ActiveControl,
    run_id: RunId,
    compiled_node: &CompiledNode<S>,
    state: &S,
    context: &NodeContext,
    node_deadline: Option<Instant>,
) -> Result<S::Update, GraphRunError>
where
    S: GraphState,
{
    let node_id = &compiled_node.id;
    let step = context.step();
    let active_deadline = control.deadline(node_deadline);
    if let Some(error) = control.check(run_id, Some(node_id), step, active_deadline) {
        return Err(error);
    }

    let node_result = if control.is_disabled() {
        compiled_node.node.run(state, context).await
    } else {
        let node_future = compiled_node.node.run(state, context);
        tokio::pin!(node_future);
        tokio::select! {
            biased;
            () = control.cancellation_token.cancelled() => {
                return Err(control.cancelled_error(run_id, Some(node_id), step));
            }
            deadline = wait_for_deadline(active_deadline) => {
                return Err(control.deadline_error(run_id, Some(node_id), step, deadline));
            }
            result = &mut node_future => result,
        }
    };

    if let Some(error) = control.check(run_id, Some(node_id), step, control.deadline(None)) {
        return Err(error);
    }

    node_result.map_err(|source| GraphRunError::NodeFailed {
        node_id: node_id.clone(),
        step,
        source,
    })
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
            GraphRunError::StateBatchUpdateFailed { node_ids, step, .. } => {
                Self::StateBatchUpdateFailed {
                    node_ids: node_ids.clone(),
                    step: *step,
                }
            }
            GraphRunError::SnapshotFailed {
                thread_id,
                superstep,
                step,
                ..
            } => Self::SnapshotFailed {
                thread_id: thread_id.clone(),
                superstep: *superstep,
                step: *step,
            },
            GraphRunError::CheckpointConflict {
                thread_id,
                superstep,
                step,
                expected_parent,
                actual_parent,
                ..
            } => Self::CheckpointConflict {
                thread_id: thread_id.clone(),
                superstep: *superstep,
                step: *step,
                expected_parent: *expected_parent,
                actual_parent: *actual_parent,
            },
            GraphRunError::CheckpointSaveFailed {
                thread_id,
                superstep,
                step,
                ..
            } => Self::CheckpointSaveFailed {
                thread_id: thread_id.clone(),
                superstep: *superstep,
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
