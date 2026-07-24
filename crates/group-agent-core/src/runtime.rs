use std::future::{Future, pending};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};
use petgraph::stable_graph::NodeIndex;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::checkpoint::{CheckpointLineage, ResumeParts};
use crate::graph::{CompiledItem, CompiledNode, CompiledTransition};
use crate::node::NodeKind;
use crate::{
    Checkpoint, CheckpointConfig, CheckpointId, CheckpointIncompatibility, CheckpointInterrupt,
    CheckpointPolicy, CheckpointRequest, CheckpointState, CheckpointWriteError, CompiledGraph,
    EventConfig, EventRetention, ExecutionOutcome, GraphEvent, GraphRunError, GraphState,
    InterruptReport, NodeContext, NodeOutcome, NodePath, NodeUpdate, ResumeConfig, ResumeTarget,
    ResumeValue, RunConfig, RunControl, RunFailure, RunId, ThreadId,
};

/// The completed report produced when a graph reaches an empty frontier.
#[derive(Clone, Debug)]
pub struct RunReport<S>
where
    S: GraphState,
{
    run_id: RunId,
    final_state: S,
    steps: usize,
    visited_nodes: Vec<NodePath>,
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

    /// Returns the cumulative number of nodes executed in this lineage.
    #[must_use]
    pub const fn steps(&self) -> usize {
        self.steps
    }

    /// Returns executed node identifiers in execution order.
    #[must_use]
    pub fn visited_nodes(&self) -> &[NodePath] {
        &self.visited_nodes
    }

    /// Returns lifecycle events in emission order.
    #[must_use]
    pub fn events(&self) -> &[GraphEvent] {
        &self.events
    }
}

struct Execution<'a, S>
where
    S: GraphState,
{
    run_id: RunId,
    state: S,
    steps: usize,
    visited_nodes: Vec<NodePath>,
    events: EventEmitter<'a>,
    frontier: Vec<NodeIndex>,
    superstep: usize,
    save_empty_checkpoint: bool,
    resume_node: Option<NodeIndex>,
    resume_value: Option<ResumeValue>,
}

enum SubgraphBoundary {
    Started(crate::GraphPath),
    Completed(crate::GraphPath),
}

impl SubgraphBoundary {
    fn emit(self, events: &mut EventEmitter<'_>, run_id: RunId) {
        match self {
            Self::Started(graph_path) => {
                events.emit(|| GraphEvent::SubgraphStarted { run_id, graph_path });
            }
            Self::Completed(graph_path) => {
                events.emit(|| GraphEvent::SubgraphCompleted { run_id, graph_path });
            }
        }
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
        let outcome = self
            .invoke_internal(
                initial_state,
                config,
                event_config,
                control,
                DisabledCheckpoint,
            )
            .await?;
        match outcome {
            ExecutionOutcome::Completed(report) => Ok(report),
            ExecutionOutcome::Interrupted(_) => {
                unreachable!("checkpoint-disabled invocation cannot return an interrupt outcome")
            }
        }
    }

    async fn invoke_internal<C>(
        &self,
        initial_state: S,
        config: RunConfig,
        event_config: EventConfig,
        control: RunControl,
        checkpoints: C,
    ) -> Result<ExecutionOutcome<S>, GraphRunError>
    where
        C: RuntimeCheckpoint<S>,
    {
        let invocation_started = Instant::now();
        let run_id = RunId::next();
        let control = ActiveControl::new(control, invocation_started);
        let mut events = EventEmitter::new(run_id, &event_config);
        events.emit(|| GraphEvent::RunStarted {
            run_id,
            max_steps: config.max_steps,
        });
        let mut frontier = self.entry_index.into_iter().collect::<Vec<_>>();

        debug!(%run_id, max_steps = config.max_steps, "graph run started");

        match self.normalize_frontier(&mut frontier, &initial_state, &mut events, run_id, 0) {
            Ok(boundaries) => {
                for boundary in boundaries {
                    boundary.emit(&mut events, run_id);
                }
            }
            Err(error) => return events.fail(error),
        }
        let initial_node = frontier.first().map(|index| &self.node_at(*index).path);
        let initial_step = usize::from(initial_node.is_some());
        if let Some(error) =
            control.check(run_id, initial_node, initial_step, control.deadline(None))
        {
            return events.fail(error);
        }

        self.execute_internal(
            Execution {
                run_id,
                state: initial_state,
                steps: 0,
                visited_nodes: Vec::new(),
                events,
                frontier,
                superstep: 0,
                save_empty_checkpoint: true,
                resume_node: None,
                resume_value: None,
            },
            config,
            control,
            checkpoints,
        )
        .await
    }

    async fn execute_internal<C>(
        &self,
        execution: Execution<'_, S>,
        config: RunConfig,
        control: ActiveControl,
        mut checkpoints: C,
    ) -> Result<ExecutionOutcome<S>, GraphRunError>
    where
        C: RuntimeCheckpoint<S>,
    {
        let Execution {
            run_id,
            mut state,
            mut steps,
            mut visited_nodes,
            mut events,
            mut frontier,
            mut superstep,
            save_empty_checkpoint,
            mut resume_node,
            mut resume_value,
        } = execution;
        let mut next_frontier = Vec::new();
        let absolute_step_limit = steps.saturating_add(config.max_steps);

        if save_empty_checkpoint && frontier.is_empty() && checkpoints.should_save(true) {
            let save_result = await_run_boundary(
                &control,
                run_id,
                steps,
                checkpoints.save(
                    run_id,
                    &state,
                    CheckpointBoundary::new(superstep, steps, Vec::new(), true, None),
                ),
            )
            .await;
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
                completed: true,
            });
        }

        while !frontier.is_empty() {
            let first_node = &self.node_at(frontier[0]).path;
            let first_step = steps + 1;
            if let Some(error) =
                control.check(run_id, Some(first_node), first_step, control.deadline(None))
            {
                return events.fail(error);
            }

            let remaining_steps = absolute_step_limit.saturating_sub(steps);
            if frontier.len() > remaining_steps {
                let blocked_offset = remaining_steps;
                let blocked_node = &self.node_at(frontier[blocked_offset]).path;
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
                        .map(|index| self.node_at(*index).path.clone())
                        .collect(),
                });
            }

            let step_base = steps;
            if !is_parallel {
                let compiled_node = self.node_at(frontier[0]);
                let step = first_step;
                if let Some(error) = control.check(
                    run_id,
                    Some(&compiled_node.path),
                    step,
                    control.deadline(None),
                ) {
                    return events.fail(error);
                }

                let node_deadline = control.node_deadline(Instant::now());
                let context = NodeContext::new(
                    step,
                    compiled_node.path.clone(),
                    control.cancellation_token.clone(),
                    control.run_deadline,
                    (resume_node == Some(frontier[0]))
                        .then(|| resume_value.clone())
                        .flatten(),
                );
                events.emit(|| GraphEvent::NodeStarted {
                    run_id,
                    node_id: compiled_node.path.clone(),
                    step,
                });
                visited_nodes.push(compiled_node.path.clone());
                debug!(node_path = %compiled_node.path, step, superstep, "node started");

                if let Some(error) = control.check(
                    run_id,
                    Some(&compiled_node.path),
                    step,
                    control.deadline(node_deadline),
                ) {
                    return events.fail(error);
                }
                let outcome = match execute_node(
                    &control,
                    run_id,
                    compiled_node,
                    &state,
                    &context,
                    node_deadline,
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(error) => return events.fail(error),
                };
                let update = match outcome {
                    NodeOutcome::Update(update) => update,
                    NodeOutcome::Interrupt(request) => {
                        let interrupt = request.into_checkpoint(compiled_node.path.clone());
                        events.emit(|| GraphEvent::NodeInterrupted {
                            run_id,
                            interrupt_id: interrupt.id(),
                            node_id: compiled_node.path.clone(),
                            step,
                        });
                        if !checkpoints.is_enabled() {
                            return events.fail(GraphRunError::InterruptRequiresCheckpoint {
                                interrupt_id: interrupt.id(),
                                node_id: compiled_node.path.clone(),
                                step,
                            });
                        }

                        let committed_superstep = superstep - 1;
                        let save_result = await_run_boundary(
                            &control,
                            run_id,
                            step_base,
                            checkpoints.save(
                                run_id,
                                &state,
                                CheckpointBoundary::new(
                                    committed_superstep,
                                    step_base,
                                    vec![compiled_node.path.clone()],
                                    false,
                                    Some(interrupt.clone()),
                                ),
                            ),
                        )
                        .await;
                        let saved = match save_result {
                            Ok(saved) => saved,
                            Err(error) => return events.fail(error),
                        };
                        events.emit(|| GraphEvent::CheckpointSaved {
                            run_id,
                            checkpoint_id: saved.id,
                            thread_id: saved.thread_id.clone(),
                            superstep: committed_superstep,
                            step: step_base,
                            completed: false,
                        });
                        if let Some(error) =
                            control.check(run_id, None, step_base, control.deadline(None))
                        {
                            return events.fail(error);
                        }
                        events.emit(|| GraphEvent::RunInterrupted {
                            run_id,
                            interrupt_id: interrupt.id(),
                            checkpoint_id: saved.id,
                            thread_id: saved.thread_id.clone(),
                            node_id: compiled_node.path.clone(),
                            superstep: committed_superstep,
                            step: step_base,
                        });
                        debug!(
                            %run_id,
                            node_path = %compiled_node.path,
                            interrupt_id = %interrupt.id(),
                            step = step_base,
                            "graph run interrupted"
                        );
                        return Ok(ExecutionOutcome::Interrupted(InterruptReport {
                            run_id,
                            state,
                            steps: step_base,
                            superstep: committed_superstep,
                            visited_nodes,
                            events: events.into_retained(),
                            checkpoint_id: saved.id,
                            thread_id: saved.thread_id,
                            interrupt,
                        }));
                    }
                };
                events.emit(|| GraphEvent::NodeCompleted {
                    run_id,
                    node_id: compiled_node.path.clone(),
                    step: step_base + 1,
                });
                if let Some(error) = control.check(
                    run_id,
                    Some(&compiled_node.path),
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
                steps = step;
                if let Err(source) = state.apply(update) {
                    return events.fail(GraphRunError::StateUpdateFailed {
                        node_id: first_node.clone(),
                        step: first_step,
                        source,
                    });
                }
                resume_node = None;
                resume_value = None;
            } else {
                let mut contexts = Vec::with_capacity(frontier.len());
                let mut node_deadlines = Vec::with_capacity(frontier.len());
                for (offset, index) in frontier.iter().copied().enumerate() {
                    let compiled_node = self.node_at(index);
                    let step = step_base + offset + 1;
                    if let Some(error) = control.check(
                        run_id,
                        Some(&compiled_node.path),
                        step,
                        control.deadline(None),
                    ) {
                        return events.fail(error);
                    }

                    let node_deadline = control.node_deadline(Instant::now());
                    let context = NodeContext::new(
                        step,
                        compiled_node.path.clone(),
                        control.cancellation_token.clone(),
                        control.run_deadline,
                        (resume_node == Some(index))
                            .then(|| resume_value.clone())
                            .flatten(),
                    );
                    events.emit(|| GraphEvent::NodeStarted {
                        run_id,
                        node_id: compiled_node.path.clone(),
                        step,
                    });
                    visited_nodes.push(compiled_node.path.clone());
                    debug!(node_path = %compiled_node.path, step, superstep, "node started");

                    if let Some(error) = control.check(
                        run_id,
                        Some(&compiled_node.path),
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
                    let outcome = match result {
                        Ok(outcome) => outcome,
                        Err(error) => return events.fail(error),
                    };
                    let update = match outcome {
                        NodeOutcome::Update(update) => update,
                        NodeOutcome::Interrupt(request) => {
                            let interrupt = request.into_checkpoint(compiled_node.path.clone());
                            events.emit(|| GraphEvent::NodeInterrupted {
                                run_id,
                                interrupt_id: interrupt.id(),
                                node_id: compiled_node.path.clone(),
                                step,
                            });
                            return events.fail(GraphRunError::UnsupportedParallelInterrupt {
                                interrupt_id: interrupt.id(),
                                node_id: compiled_node.path.clone(),
                                step,
                            });
                        }
                    };
                    events.emit(|| GraphEvent::NodeCompleted {
                        run_id,
                        node_id: compiled_node.path.clone(),
                        step,
                    });
                    if let Some(error) = control.check(
                        run_id,
                        Some(&compiled_node.path),
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
                    .map(|index| self.node_at(*index).path.clone())
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
                resume_node = None;
                resume_value = None;
            }

            for (offset, index) in frontier.iter().copied().enumerate() {
                let compiled_node = self.node_at(index);
                let step = step_base + offset + 1;
                events.emit(|| GraphEvent::StateUpdated {
                    run_id,
                    node_id: compiled_node.path.clone(),
                    step,
                });
                debug!(node_path = %compiled_node.path, step, superstep, "state updated");
                if let Some(error) = control.check(
                    run_id,
                    Some(&compiled_node.path),
                    step,
                    control.deadline(None),
                ) {
                    return events.fail(error);
                }
            }

            next_frontier.clear();
            let current_graph_path = self.node_at(frontier[0]).graph_path.clone();
            for (offset, index) in frontier.iter().copied().enumerate() {
                let compiled_node = self.node_at(index);
                let node_id = &compiled_node.path;
                let step = step_base + offset + 1;
                match &compiled_node.transition {
                    CompiledTransition::Fixed(target) => {
                        if let Some(target) = target {
                            next_frontier.push(*target);
                        }
                    }
                    CompiledTransition::FanOut(targets) => {
                        next_frontier.extend(targets.iter().flatten().copied());
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
                                target: NodePath::new(&compiled_node.graph_path, target_id),
                                step,
                            });
                        };
                        let target_path = target_index.map_or_else(
                            || NodePath::new(&compiled_node.graph_path, target_id.clone()),
                            |index| self.path_at(index),
                        );
                        events.emit(|| GraphEvent::RouteSelected {
                            run_id,
                            source: node_id.clone(),
                            target: target_path,
                            step,
                        });
                        if let Some(target_index) = target_index {
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
            if next_frontier.is_empty() {
                if let Some(exit) = self.scope_exits.get(&current_graph_path) {
                    next_frontier.push(*exit);
                }
            }
            let subgraph_boundaries = match self.normalize_frontier(
                &mut next_frontier,
                &state,
                &mut events,
                run_id,
                steps,
            ) {
                Ok(boundaries) => boundaries,
                Err(error) => return events.fail(error),
            };
            let completed = next_frontier.is_empty();
            if checkpoints.should_save(completed) {
                let next_node_ids = next_frontier
                    .iter()
                    .map(|index| self.node_at(*index).path.clone())
                    .collect();
                let save_result = await_run_boundary(
                    &control,
                    run_id,
                    steps,
                    checkpoints.save(
                        run_id,
                        &state,
                        CheckpointBoundary::new(superstep, steps, next_node_ids, completed, None),
                    ),
                )
                .await;
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
            for boundary in subgraph_boundaries {
                boundary.emit(&mut events, run_id);
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

        Ok(ExecutionOutcome::Completed(RunReport {
            run_id,
            final_state: state,
            steps,
            visited_nodes,
            events: events.into_retained(),
        }))
    }

    fn path_at(&self, index: NodeIndex) -> NodePath {
        match self.item_at(index) {
            CompiledItem::Node(node) => node.path.clone(),
            CompiledItem::EnterSubgraph { graph_path, .. } => graph_path.mount_path(),
            CompiledItem::ExitSubgraph { mount_path, .. } => mount_path.clone(),
        }
    }

    fn normalize_frontier(
        &self,
        frontier: &mut Vec<NodeIndex>,
        state: &S,
        events: &mut EventEmitter<'_>,
        run_id: RunId,
        step: usize,
    ) -> Result<Vec<SubgraphBoundary>, GraphRunError> {
        let mut boundaries = Vec::new();
        loop {
            if frontier.is_empty() {
                return Ok(boundaries);
            }
            if frontier.len() != 1 {
                debug_assert!(
                    frontier
                        .iter()
                        .all(|index| matches!(self.item_at(*index), CompiledItem::Node(_))),
                    "compiled parent frontier cannot mix subgraphs and parallel nodes"
                );
                return Ok(boundaries);
            }
            let index = frontier[0];
            match self.item_at(index) {
                CompiledItem::Node(_) => return Ok(boundaries),
                CompiledItem::EnterSubgraph { graph_path, target } => {
                    let graph_path = graph_path.clone();
                    let target = *target;
                    boundaries.push(SubgraphBoundary::Started(graph_path));
                    frontier.clear();
                    frontier.extend(target);
                }
                CompiledItem::ExitSubgraph {
                    graph_path,
                    mount_path,
                    transition,
                } => {
                    let graph_path = graph_path.clone();
                    let mount_path = mount_path.clone();
                    frontier.clear();
                    match transition {
                        CompiledTransition::Fixed(target) => frontier.extend(*target),
                        CompiledTransition::FanOut(targets) => {
                            frontier.extend(targets.iter().flatten().copied());
                        }
                        CompiledTransition::Conditional {
                            router,
                            allowed_targets,
                        } => {
                            let target_id =
                                router(state).map_err(|source| GraphRunError::RouteFailed {
                                    node_id: mount_path.clone(),
                                    step,
                                    source,
                                })?;
                            let Some(target) = allowed_targets.get(&target_id).copied() else {
                                return Err(GraphRunError::InvalidRouteTarget {
                                    node_id: mount_path.clone(),
                                    target: NodePath::new(&mount_path.graph_path(), target_id),
                                    step,
                                });
                            };
                            let target_path = target.map_or_else(
                                || NodePath::new(&mount_path.graph_path(), target_id.clone()),
                                |target| self.path_at(target),
                            );
                            events.emit(|| GraphEvent::RouteSelected {
                                run_id,
                                source: mount_path.clone(),
                                target: target_path,
                                step,
                            });
                            frontier.extend(target);
                        }
                    }
                    boundaries.push(SubgraphBoundary::Completed(graph_path));
                    frontier.sort_unstable_by_key(|target| target.index());
                    frontier.dedup();
                    if frontier.is_empty() {
                        let parent_path = mount_path.graph_path();
                        if let Some(exit) = self.scope_exits.get(&parent_path) {
                            frontier.push(*exit);
                        }
                    }
                }
            }
        }
    }
}

impl<S> CompiledGraph<S>
where
    S: CheckpointState,
{
    /// Restores a checkpoint and either completes or saves another interrupt.
    pub async fn resume(
        &self,
        resume_config: ResumeConfig<S::Snapshot>,
    ) -> Result<ExecutionOutcome<S>, GraphRunError> {
        let invocation_started = Instant::now();
        let ResumeParts {
            thread_id,
            checkpointer,
            target,
            checkpoint_policy,
            run_config,
            event_config,
            control: run_control,
            resume_value,
        } = resume_config.into_parts();
        let run_id = RunId::next();
        let control = ActiveControl::new(run_control, invocation_started);
        let mut events = EventEmitter::new(run_id, &event_config);
        events.emit(|| GraphEvent::RunStarted {
            run_id,
            max_steps: run_config.max_steps,
        });
        debug!(%run_id, %thread_id, max_steps = run_config.max_steps, "graph resume started");

        if let Some(error) = control.check(run_id, None, 0, control.deadline(None)) {
            return events.fail(error);
        }

        let requested_id = match target {
            ResumeTarget::Latest => None,
            ResumeTarget::Checkpoint(checkpoint_id) => Some(checkpoint_id),
        };
        let load = async {
            let checkpoint = match target {
                ResumeTarget::Latest => checkpointer.latest(&thread_id).await,
                ResumeTarget::Checkpoint(checkpoint_id) => {
                    checkpointer.get(&thread_id, checkpoint_id).await
                }
            }
            .map_err(|source| GraphRunError::CheckpointLoadFailed {
                run_id,
                thread_id: thread_id.clone(),
                checkpoint_id: requested_id,
                source,
            })?;
            checkpoint.ok_or_else(|| GraphRunError::CheckpointNotFound {
                run_id,
                thread_id: thread_id.clone(),
                checkpoint_id: requested_id,
            })
        };
        let checkpoint = match await_run_boundary(&control, run_id, 0, load).await {
            Ok(checkpoint) => checkpoint,
            Err(error) => return events.fail(error),
        };
        let checkpoint_step = checkpoint.step();

        if checkpoint.thread_id() != &thread_id {
            return events.fail(GraphRunError::CheckpointIncompatible {
                run_id,
                thread_id,
                checkpoint_id: checkpoint.id(),
                step: checkpoint_step,
                reason: CheckpointIncompatibility::ThreadMismatch {
                    actual_thread: checkpoint.thread_id().clone(),
                },
            });
        }

        if matches!(target, ResumeTarget::Checkpoint(_)) {
            let latest = async {
                checkpointer.latest(&thread_id).await.map_err(|source| {
                    GraphRunError::CheckpointLoadFailed {
                        run_id,
                        thread_id: thread_id.clone(),
                        checkpoint_id: Some(checkpoint.id()),
                        source,
                    }
                })
            };
            let latest = match await_run_boundary(&control, run_id, checkpoint_step, latest).await {
                Ok(latest) => latest,
                Err(error) => return events.fail(error),
            };
            let latest_checkpoint_id = latest.as_ref().map(|checkpoint| checkpoint.id());
            if latest_checkpoint_id != Some(checkpoint.id()) {
                return events.fail(GraphRunError::ResumeConflict {
                    run_id,
                    thread_id,
                    checkpoint_id: checkpoint.id(),
                    latest_checkpoint_id,
                    step: checkpoint_step,
                });
            }
        }

        let frontier = match self.validate_and_resolve_resume_frontier(&checkpoint) {
            Ok(frontier) => frontier,
            Err(reason) => {
                return events.fail(GraphRunError::CheckpointIncompatible {
                    run_id,
                    thread_id,
                    checkpoint_id: checkpoint.id(),
                    step: checkpoint_step,
                    reason,
                });
            }
        };
        let (resume_node, resume_value) = match (checkpoint.interrupt(), resume_value) {
            (Some(_), Some(value)) => (frontier.first().copied(), Some(value)),
            (Some(interrupt), None) => {
                return events.fail(GraphRunError::MissingResumeValue {
                    run_id,
                    thread_id,
                    checkpoint_id: checkpoint.id(),
                    interrupt_id: interrupt.id(),
                    node_id: interrupt.node_path().clone(),
                    step: checkpoint_step,
                });
            }
            (None, Some(_)) => {
                return events.fail(GraphRunError::UnexpectedResumeValue {
                    run_id,
                    thread_id,
                    checkpoint_id: checkpoint.id(),
                    step: checkpoint_step,
                });
            }
            (None, None) => (None, None),
        };

        if let Some(error) = control.check(run_id, None, checkpoint_step, control.deadline(None)) {
            return events.fail(error);
        }
        let state = match S::restore(checkpoint.snapshot()) {
            Ok(state) => state,
            Err(source) => {
                return events.fail(GraphRunError::RestoreFailed {
                    run_id,
                    thread_id,
                    checkpoint_id: checkpoint.id(),
                    superstep: checkpoint.superstep(),
                    step: checkpoint_step,
                    source,
                });
            }
        };
        if let Some(error) = control.check(run_id, None, checkpoint_step, control.deadline(None)) {
            return events.fail(error);
        }

        events.emit(|| GraphEvent::RunResumed {
            run_id,
            thread_id: thread_id.clone(),
            checkpoint_id: checkpoint.id(),
            step: checkpoint_step,
            superstep: checkpoint.superstep(),
        });
        if let Some(first) = frontier.first() {
            let graph_path = &self.node_at(*first).graph_path;
            debug_assert!(
                frontier
                    .iter()
                    .all(|index| self.node_at(*index).graph_path == *graph_path),
                "one resumable frontier cannot span graph namespaces"
            );
            for graph_path in graph_path.prefixes() {
                events.emit(|| GraphEvent::SubgraphStarted { run_id, graph_path });
            }
        }

        let checkpoint_config = CheckpointConfig::new(thread_id, checkpointer, checkpoint_policy)
            .with_expected_parent(Some(checkpoint.id()));
        self.execute_internal(
            Execution {
                run_id,
                state,
                steps: checkpoint_step,
                visited_nodes: Vec::new(),
                events,
                frontier,
                superstep: checkpoint.superstep(),
                save_empty_checkpoint: false,
                resume_node,
                resume_value,
            },
            run_config,
            control,
            EnabledCheckpoint::new(checkpoint_config, self.version.clone()),
        )
        .await
    }

    /// Invokes with checkpointing and returns completion or saved suspension.
    pub async fn invoke_with_checkpoint(
        &self,
        initial_state: S,
        config: RunConfig,
        event_config: EventConfig,
        control: RunControl,
        checkpoint_config: CheckpointConfig<S::Snapshot>,
    ) -> Result<ExecutionOutcome<S>, GraphRunError> {
        self.invoke_internal(
            initial_state,
            config,
            event_config,
            control,
            EnabledCheckpoint::new(checkpoint_config, self.version.clone()),
        )
        .await
    }

    fn validate_and_resolve_resume_frontier(
        &self,
        checkpoint: &Checkpoint<S::Snapshot>,
    ) -> Result<Vec<NodeIndex>, CheckpointIncompatibility> {
        match (checkpoint.graph_version(), self.version()) {
            (None, _) => return Err(CheckpointIncompatibility::UnversionedCheckpoint),
            (Some(_), None) => return Err(CheckpointIncompatibility::UnversionedGraph),
            (Some(checkpoint_version), Some(compiled_version))
                if checkpoint_version != compiled_version =>
            {
                return Err(CheckpointIncompatibility::GraphVersionMismatch {
                    checkpoint: checkpoint_version.clone(),
                    compiled: compiled_version.clone(),
                });
            }
            (Some(_), Some(_)) => {}
        }
        if checkpoint.completed() && !checkpoint.next_frontier().is_empty() {
            return Err(CheckpointIncompatibility::CompletedWithFrontier);
        }
        if !checkpoint.completed() && checkpoint.next_frontier().is_empty() {
            return Err(CheckpointIncompatibility::IncompleteWithoutFrontier);
        }
        if checkpoint.completed() && checkpoint.interrupted() {
            return Err(CheckpointIncompatibility::CompletedInterrupt);
        }
        if let Some(interrupt) = checkpoint.interrupt() {
            let frontier = checkpoint.next_frontier();
            if frontier.len() != 1 || frontier.first() != Some(interrupt.node_path()) {
                return Err(CheckpointIncompatibility::InvalidInterruptFrontier {
                    interrupt_node: interrupt.node_path().clone(),
                    frontier: frontier.to_vec(),
                });
            }
        }
        checkpoint
            .next_frontier()
            .iter()
            .map(|node_path| {
                if node_path.leaf().is_start() {
                    return Err(CheckpointIncompatibility::StartInFrontier);
                }
                if node_path.leaf().is_end() {
                    return Err(CheckpointIncompatibility::EndInFrontier);
                }
                self.node_paths.get(node_path).copied().ok_or_else(|| {
                    CheckpointIncompatibility::UnknownFrontierNode {
                        node_id: node_path.clone(),
                    }
                })
            })
            .collect()
    }
}

struct SavedCheckpoint {
    id: CheckpointId,
    thread_id: ThreadId,
}

struct CheckpointBoundary {
    superstep: usize,
    step: usize,
    next_frontier: Vec<NodePath>,
    completed: bool,
    interrupt: Option<CheckpointInterrupt>,
}

impl CheckpointBoundary {
    fn new(
        superstep: usize,
        step: usize,
        next_frontier: Vec<NodePath>,
        completed: bool,
        interrupt: Option<CheckpointInterrupt>,
    ) -> Self {
        Self {
            superstep,
            step,
            next_frontier,
            completed,
            interrupt,
        }
    }
}

#[async_trait]
trait RuntimeCheckpoint<S>: Send
where
    S: GraphState,
{
    fn is_enabled(&self) -> bool;

    fn should_save(&self, completed: bool) -> bool;

    async fn save(
        &mut self,
        run_id: RunId,
        state: &S,
        boundary: CheckpointBoundary,
    ) -> Result<SavedCheckpoint, GraphRunError>;
}

struct DisabledCheckpoint;

#[async_trait]
impl<S> RuntimeCheckpoint<S> for DisabledCheckpoint
where
    S: GraphState,
{
    fn is_enabled(&self) -> bool {
        false
    }

    fn should_save(&self, _completed: bool) -> bool {
        false
    }

    async fn save(
        &mut self,
        _run_id: RunId,
        _state: &S,
        _boundary: CheckpointBoundary,
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
    graph_version: Option<crate::GraphVersion>,
}

impl<S> EnabledCheckpoint<S>
where
    S: CheckpointState,
{
    fn new(
        config: CheckpointConfig<S::Snapshot>,
        graph_version: Option<crate::GraphVersion>,
    ) -> Self {
        let expected_parent = config.expected_parent();
        Self {
            config,
            expected_parent,
            graph_version,
        }
    }
}

#[async_trait]
impl<S> RuntimeCheckpoint<S> for EnabledCheckpoint<S>
where
    S: CheckpointState,
{
    fn is_enabled(&self) -> bool {
        true
    }

    fn should_save(&self, completed: bool) -> bool {
        self.config.policy() == CheckpointPolicy::EverySuperstep || completed
    }

    async fn save(
        &mut self,
        run_id: RunId,
        state: &S,
        boundary: CheckpointBoundary,
    ) -> Result<SavedCheckpoint, GraphRunError> {
        let CheckpointBoundary {
            superstep,
            step,
            next_frontier,
            completed,
            interrupt,
        } = boundary;
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
                self.graph_version.clone(),
                thread_id.clone(),
                run_id,
            ),
            superstep,
            step,
            snapshot,
            next_frontier,
            completed,
            interrupt,
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
                CheckpointWriteError::IdempotencyConflict { checkpoint_id } => {
                    GraphRunError::CheckpointIdConflict {
                        run_id,
                        thread_id: thread_id.clone(),
                        checkpoint_id,
                        superstep,
                        step,
                    }
                }
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
) -> Result<NodeOutcome<S::Update>, GraphRunError>
where
    S: GraphState,
{
    let node_id = &compiled_node.path;
    let step = context.step();
    let active_deadline = control.deadline(node_deadline);
    if let Some(error) = control.check(run_id, Some(node_id), step, active_deadline) {
        return Err(error);
    }

    let node_result = if control.is_disabled() {
        match &compiled_node.kind {
            NodeKind::Normal(node) => node.run(state, context).await.map(NodeOutcome::Update),
            NodeKind::Interruptible(node) => node.run(state, context).await,
        }
    } else {
        let node_future = async {
            match &compiled_node.kind {
                NodeKind::Normal(node) => node.run(state, context).await.map(NodeOutcome::Update),
                NodeKind::Interruptible(node) => node.run(state, context).await,
            }
        };
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
        node_id: Option<&NodePath>,
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
        node_id: Option<&NodePath>,
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
        node_id: Option<&NodePath>,
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
        node_id: Option<&NodePath>,
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

    fn node_timeout_error(&self, run_id: RunId, node_id: &NodePath, step: usize) -> GraphRunError {
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

async fn await_run_boundary<F, T>(
    control: &ActiveControl,
    run_id: RunId,
    step: usize,
    future: F,
) -> Result<T, GraphRunError>
where
    F: Future<Output = Result<T, GraphRunError>>,
{
    if control.is_disabled() {
        return future.await;
    }

    tokio::pin!(future);
    let run_deadline = control.deadline(None);
    tokio::select! {
        biased;
        () = control.cancellation_token.cancelled() => {
            Err(control.cancelled_error(run_id, None, step))
        }
        deadline = wait_for_deadline(run_deadline) => {
            Err(control.deadline_error(run_id, None, step, deadline))
        }
        result = &mut future => result,
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
            GraphRunError::CheckpointIdConflict {
                thread_id,
                checkpoint_id,
                superstep,
                step,
                ..
            } => Self::CheckpointIdConflict {
                thread_id: thread_id.clone(),
                checkpoint_id: *checkpoint_id,
                superstep: *superstep,
                step: *step,
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
            GraphRunError::CheckpointLoadFailed {
                thread_id,
                checkpoint_id,
                ..
            } => Self::CheckpointLoadFailed {
                thread_id: thread_id.clone(),
                checkpoint_id: *checkpoint_id,
            },
            GraphRunError::CheckpointNotFound {
                thread_id,
                checkpoint_id,
                ..
            } => Self::CheckpointNotFound {
                thread_id: thread_id.clone(),
                checkpoint_id: *checkpoint_id,
            },
            GraphRunError::ResumeConflict {
                thread_id,
                checkpoint_id,
                latest_checkpoint_id,
                step,
                ..
            } => Self::ResumeConflict {
                thread_id: thread_id.clone(),
                checkpoint_id: *checkpoint_id,
                latest_checkpoint_id: *latest_checkpoint_id,
                step: *step,
            },
            GraphRunError::CheckpointIncompatible {
                thread_id,
                checkpoint_id,
                step,
                reason,
                ..
            } => Self::CheckpointIncompatible {
                thread_id: thread_id.clone(),
                checkpoint_id: *checkpoint_id,
                step: *step,
                reason: reason.clone(),
            },
            GraphRunError::RestoreFailed {
                thread_id,
                checkpoint_id,
                superstep,
                step,
                ..
            } => Self::RestoreFailed {
                thread_id: thread_id.clone(),
                checkpoint_id: *checkpoint_id,
                superstep: *superstep,
                step: *step,
            },
            GraphRunError::InterruptRequiresCheckpoint {
                interrupt_id,
                node_id,
                step,
            } => Self::InterruptRequiresCheckpoint {
                interrupt_id: *interrupt_id,
                node_id: node_id.clone(),
                step: *step,
            },
            GraphRunError::UnsupportedParallelInterrupt {
                interrupt_id,
                node_id,
                step,
            } => Self::UnsupportedParallelInterrupt {
                interrupt_id: *interrupt_id,
                node_id: node_id.clone(),
                step: *step,
            },
            GraphRunError::MissingResumeValue {
                thread_id,
                checkpoint_id,
                interrupt_id,
                node_id,
                step,
                ..
            } => Self::MissingResumeValue {
                thread_id: thread_id.clone(),
                checkpoint_id: *checkpoint_id,
                interrupt_id: *interrupt_id,
                node_id: node_id.clone(),
                step: *step,
            },
            GraphRunError::UnexpectedResumeValue {
                thread_id,
                checkpoint_id,
                step,
                ..
            } => Self::UnexpectedResumeValue {
                thread_id: thread_id.clone(),
                checkpoint_id: *checkpoint_id,
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

#[cfg(test)]
mod resume_validation_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::{
        Checkpointer, CheckpointerError, END, GraphPath, GraphVersion, Node, NodeError, START,
        SnapshotError, StateError, StateGraph,
    };

    #[derive(Debug, Default)]
    struct ValidationState {
        restore_calls: Arc<AtomicUsize>,
    }

    struct ValidationSnapshot {
        restore_calls: Arc<AtomicUsize>,
        fail_restore: bool,
    }

    impl GraphState for ValidationState {
        type Update = ();

        fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
            Ok(())
        }
    }

    impl CheckpointState for ValidationState {
        type Snapshot = ValidationSnapshot;

        fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
            Ok(ValidationSnapshot {
                restore_calls: Arc::clone(&self.restore_calls),
                fail_restore: false,
            })
        }

        fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
            snapshot.restore_calls.fetch_add(1, Ordering::SeqCst);
            if snapshot.fail_restore {
                return Err(SnapshotError::message("restore should not run"));
            }
            Ok(Self {
                restore_calls: Arc::clone(&snapshot.restore_calls),
            })
        }
    }

    struct NoopNode;

    #[async_trait]
    impl Node<ValidationState> for NoopNode {
        async fn run(
            &self,
            _state: &ValidationState,
            _context: &NodeContext,
        ) -> Result<(), NodeError> {
            Ok(())
        }
    }

    struct StaticCheckpointer {
        checkpoint: Arc<Checkpoint<ValidationSnapshot>>,
    }

    #[async_trait]
    impl Checkpointer<ValidationSnapshot> for StaticCheckpointer {
        async fn save(
            &self,
            request: CheckpointRequest<ValidationSnapshot>,
        ) -> Result<Arc<Checkpoint<ValidationSnapshot>>, CheckpointWriteError> {
            Ok(Arc::new(request.into_checkpoint()))
        }

        async fn latest(
            &self,
            thread_id: &ThreadId,
        ) -> Result<Option<Arc<Checkpoint<ValidationSnapshot>>>, CheckpointerError> {
            Ok((self.checkpoint.thread_id() == thread_id).then(|| Arc::clone(&self.checkpoint)))
        }

        async fn history(
            &self,
            thread_id: &ThreadId,
        ) -> Result<Vec<Arc<Checkpoint<ValidationSnapshot>>>, CheckpointerError> {
            Ok(if self.checkpoint.thread_id() == thread_id {
                vec![Arc::clone(&self.checkpoint)]
            } else {
                Vec::new()
            })
        }
    }

    fn validation_graph() -> CompiledGraph<ValidationState> {
        let mut graph = StateGraph::new();
        graph.set_version("validation-v1");
        graph
            .add_node("valid", NoopNode)
            .expect("node should register");
        graph.add_edge(START, "valid").add_edge("valid", END);
        graph.compile().expect("graph should compile")
    }

    fn nested_validation_graph() -> CompiledGraph<ValidationState> {
        let mut child = StateGraph::new();
        child
            .add_node("valid", NoopNode)
            .expect("child node should register");
        child.add_edge(START, "valid").add_edge("valid", END);
        let mut parent = StateGraph::new();
        parent.set_version("validation-v1");
        parent
            .add_subgraph("child", child.compile().expect("child should compile"))
            .expect("child should mount");
        parent.add_edge(START, "child").add_edge("child", END);
        parent.compile().expect("parent should compile")
    }

    fn forged_checkpoint(
        frontier: Vec<NodePath>,
        completed: bool,
    ) -> (Arc<Checkpoint<ValidationSnapshot>>, Arc<AtomicUsize>) {
        forged_checkpoint_with_interrupt(frontier, completed, None)
    }

    fn forged_checkpoint_with_interrupt(
        frontier: Vec<NodePath>,
        completed: bool,
        interrupt: Option<CheckpointInterrupt>,
    ) -> (Arc<Checkpoint<ValidationSnapshot>>, Arc<AtomicUsize>) {
        let restore_calls = Arc::new(AtomicUsize::new(0));
        let request = CheckpointRequest::new(
            CheckpointLineage::new(
                CheckpointId::next(),
                None,
                Some(GraphVersion::from("validation-v1")),
                ThreadId::from("validation-thread"),
                RunId::next(),
            ),
            1,
            1,
            Arc::new(ValidationSnapshot {
                restore_calls: Arc::clone(&restore_calls),
                fail_restore: true,
            }),
            frontier,
            completed,
            interrupt,
        );
        (Arc::new(request.into_checkpoint()), restore_calls)
    }

    async fn assert_incompatible_before_restore(
        frontier: Vec<NodePath>,
        completed: bool,
        expected: CheckpointIncompatibility,
    ) {
        assert_incompatible_before_restore_with(validation_graph(), frontier, completed, expected)
            .await;
    }

    async fn assert_incompatible_before_restore_with(
        graph: CompiledGraph<ValidationState>,
        frontier: Vec<NodePath>,
        completed: bool,
        expected: CheckpointIncompatibility,
    ) {
        let (checkpoint, restore_calls) = forged_checkpoint(frontier, completed);
        let store: Arc<dyn Checkpointer<ValidationSnapshot>> =
            Arc::new(StaticCheckpointer { checkpoint });
        let error = graph
            .resume(ResumeConfig::new("validation-thread", store))
            .await
            .expect_err("forged checkpoint should be incompatible");
        match error {
            GraphRunError::CheckpointIncompatible { reason, .. } => {
                assert_eq!(reason, expected);
            }
            other => panic!("unexpected resume error: {other}"),
        }
        assert_eq!(restore_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn start_frontier_is_rejected_before_restore() {
        assert_incompatible_before_restore(
            vec![NodePath::from(crate::NodeId::start())],
            false,
            CheckpointIncompatibility::StartInFrontier,
        )
        .await;
    }

    #[tokio::test]
    async fn end_frontier_is_rejected_before_restore() {
        assert_incompatible_before_restore(
            vec![NodePath::from(crate::NodeId::end())],
            false,
            CheckpointIncompatibility::EndInFrontier,
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_completed_frontier_combinations_are_rejected_before_restore() {
        assert_incompatible_before_restore(
            vec![NodePath::from("valid")],
            true,
            CheckpointIncompatibility::CompletedWithFrontier,
        )
        .await;
        assert_incompatible_before_restore(
            Vec::new(),
            false,
            CheckpointIncompatibility::IncompleteWithoutFrontier,
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_interrupt_metadata_is_rejected_before_restore() {
        let interrupt = crate::InterruptRequest::new("approval")
            .into_checkpoint(NodePath::new(&GraphPath::new(["child"]), "valid"));
        let (checkpoint, restore_calls) =
            forged_checkpoint_with_interrupt(vec![NodePath::from("valid")], false, Some(interrupt));
        let store: Arc<dyn Checkpointer<ValidationSnapshot>> =
            Arc::new(StaticCheckpointer { checkpoint });
        let error = validation_graph()
            .resume(ResumeConfig::new("validation-thread", store))
            .await
            .expect_err("mismatched interrupt metadata should be incompatible");
        assert!(matches!(
            error,
            GraphRunError::CheckpointIncompatible {
                reason: CheckpointIncompatibility::InvalidInterruptFrontier { .. },
                ..
            }
        ));
        assert_eq!(restore_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unknown_nested_frontier_and_wrong_namespace_are_rejected_before_restore() {
        let unknown_nested = NodePath::new(&GraphPath::new(["child"]), "missing");
        assert_incompatible_before_restore_with(
            nested_validation_graph(),
            vec![unknown_nested.clone()],
            false,
            CheckpointIncompatibility::UnknownFrontierNode {
                node_id: unknown_nested,
            },
        )
        .await;

        let wrong_namespace = NodePath::new(&GraphPath::new(["wrong"]), "valid");
        assert_incompatible_before_restore_with(
            nested_validation_graph(),
            vec![wrong_namespace.clone()],
            false,
            CheckpointIncompatibility::UnknownFrontierNode {
                node_id: wrong_namespace,
            },
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_nested_interrupt_metadata_is_rejected_before_restore() {
        let valid = NodePath::new(&GraphPath::new(["child"]), "valid");
        let invalid_interrupt_path = NodePath::new(&GraphPath::new(["child"]), "other");
        let interrupt = crate::InterruptRequest::new("approval")
            .into_checkpoint(invalid_interrupt_path.clone());
        let (checkpoint, restore_calls) =
            forged_checkpoint_with_interrupt(vec![valid.clone()], false, Some(interrupt));
        let store: Arc<dyn Checkpointer<ValidationSnapshot>> =
            Arc::new(StaticCheckpointer { checkpoint });
        let error = nested_validation_graph()
            .resume(ResumeConfig::new("validation-thread", store))
            .await
            .expect_err("nested interrupt metadata should be incompatible");
        assert!(matches!(
            error,
            GraphRunError::CheckpointIncompatible {
                reason:
                    CheckpointIncompatibility::InvalidInterruptFrontier {
                        interrupt_node,
                        frontier,
                    },
                ..
            } if interrupt_node == invalid_interrupt_path && frontier == vec![valid]
        ));
        assert_eq!(restore_calls.load(Ordering::SeqCst), 0);
    }
}
