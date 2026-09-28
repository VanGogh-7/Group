use super::{
    AgentStage, AgentStageId, BranchApprovalDecision, BranchApprovalRequest, BranchError,
    BranchErrorKind, BranchSelection, BranchSelector, BranchTarget,
    error::StageFailure,
    state::{BranchState, BranchUpdate, admit},
};
use crate::{
    AgentApprovalDecision, AgentApprovalRequest, AgentStopReason,
    agent_nodes::{ModelNode, ToolNode},
};
use group_agent_core::{
    CompiledGraph, END, InterruptibleNode, Node, NodeContext, NodeError, NodeId, NodeOutcome,
    START, StateGraph,
};
use group_agent_model::{ChatResponse, FinishReason};
use sha2::{Digest, Sha256};
use std::{future::Future, pin::Pin, sync::Arc};
const MODELS: [&str; 3] = ["first_model", "b_model", "c_model"];
const TOOLS: [&str; 3] = ["first_tools", "b_tools", "c_tools"];
pub(crate) fn guard(
    state: &BranchState,
    stages: &[AgentStage; 3],
    validate_output: bool,
) -> Result<(), NodeError> {
    for (index, stage) in stages.iter().enumerate() {
        let result = (|| {
            if let Ok(saved) = state.stage(index) {
                if saved.model_rounds() > stage.config.max_rounds()
                    || !saved.usage_is_aligned()
                    || (saved.stop_reason() == Some(AgentStopReason::MaxRounds)
                        && saved.model_rounds() != stage.config.max_rounds())
                {
                    return Err(NodeError::message("invalid saved stage budget"));
                }
                if validate_output && saved.stop_reason() == Some(AgentStopReason::FinalAnswer) {
                    let message = saved
                        .messages()
                        .last()
                        .and_then(group_agent_model::Message::as_assistant)
                        .ok_or_else(|| NodeError::message("missing saved final answer"))?;
                    stage
                        .output
                        .validate_response(&ChatResponse::new(message.clone(), FinishReason::Stop))
                        .map_err(|e| NodeError::with_source("saved output invalid", e))?;
                }
            }
            Ok(())
        })();
        result.map_err(|e| attributed(&stage.id, e))?;
    }
    Ok(())
}
fn active(
    state: &BranchState,
    stages: &[AgentStage; 3],
    index: usize,
    tools: bool,
) -> Result<(), NodeError> {
    guard(state, stages, true)?;
    let bad = || {
        NodeError::with_source(
            "invalid branch node frontier",
            BranchError::new(BranchErrorKind::InvalidState),
        )
    };
    if state.stopped
        || (index == 0 && state.selection.is_some())
        || (index != 0
            && (state.selection.and_then(BranchTarget::index) != Some(index)
                || state.downstream.is_none()
                || state.first.stop_reason() != Some(AgentStopReason::FinalAnswer)))
    {
        return Err(bad());
    }
    let saved = state.stage(index).map_err(|_| bad())?;
    if saved.stop_reason().is_some() {
        return Err(bad());
    }
    let pending = saved.pending_tool_calls().is_some_and(|c| !c.is_empty());
    if tools {
        if !pending || saved.model_rounds() == 0 {
            return Err(bad());
        }
    } else if pending || saved.model_rounds() >= stages[index].config.max_rounds() {
        return Err(bad());
    }
    Ok(())
}
fn attributed(stage: &AgentStageId, source: NodeError) -> NodeError {
    if std::error::Error::source(&source).is_some_and(|e| e.is::<StageFailure>()) {
        return source;
    }

    NodeError::with_source(
        "branch stage failed",
        StageFailure {
            stage: stage.clone(),
            source,
        },
    )
}
struct StageModel {
    index: usize,
    stages: Arc<[AgentStage; 3]>,
    node: ModelNode,
}
struct StageTools {
    index: usize,
    stages: Arc<[AgentStage; 3]>,
    node: ToolNode,
}
struct Select {
    stages: Arc<[AgentStage; 3]>,
    selector: Arc<dyn BranchSelector>,
}
impl Node<BranchState> for StageModel {
    fn run<'a, 'b, 'c, 'async_trait>(
        &'a self,
        state: &'b BranchState,
        context: &'c NodeContext,
    ) -> Pin<Box<dyn Future<Output = Result<BranchUpdate, NodeError>> + Send + 'async_trait>>
    where
        'a: 'async_trait,
        'b: 'async_trait,
        'c: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let result = async {
                active(state, &self.stages, self.index, false)?;
                let state = state
                    .stage(self.index)
                    .map_err(|e| NodeError::with_source("invalid stage", e))?;
                let update = Node::run(&self.node, state, context).await?;
                Ok(BranchUpdate::Stage {
                    index: self.index,
                    update,
                })
            }
            .await;
            result.map_err(|e| attributed(&self.stages[self.index].id, e))
        })
    }
}
impl InterruptibleNode<BranchState> for StageTools {
    fn run<'a, 'b, 'c, 'async_trait>(
        &'a self,
        state: &'b BranchState,
        context: &'c NodeContext,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<NodeOutcome<BranchUpdate>, NodeError>> + Send + 'async_trait,
        >,
    >
    where
        'a: 'async_trait,
        'b: 'async_trait,
        'c: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let result = async {
                active(state, &self.stages, self.index, true)?;
                let state = state
                    .stage(self.index)
                    .map_err(|e| NodeError::with_source("invalid stage", e))?;
                let calls = self.node.validated_pending_calls(state)?;
                let stage = &self.stages[self.index];
                let update = if stage.config.tool_approval() {
                    if !context.has_resume_value() {
                        if let Some(sink) = state.sink() {
                            sink.on_event(&crate::AgentStreamEvent::ApprovalRequired {
                                request: AgentApprovalRequest::new(calls.to_vec()),
                            });
                        }
                        return Ok(NodeOutcome::interrupt(BranchApprovalRequest {
                            stage: stage.id.clone(),
                            request: AgentApprovalRequest::new(calls.to_vec()),
                        }));
                    }
                    let decision = context
                        .require_resume_value::<BranchApprovalDecision>()
                        .map_err(|e| NodeError::with_source("invalid branch decision", e))?;
                    if decision.stage != stage.id {
                        return Err(NodeError::message("approval targets a different stage"));
                    }
                    match decision.decision {
                        AgentApprovalDecision::Approve => {
                            self.node.execute_pending(state, calls).await?
                        }
                        AgentApprovalDecision::Reject => self.node.rejected_update(state, calls),
                    }
                } else {
                    self.node.execute_pending(state, calls).await?
                };
                Ok(NodeOutcome::update(BranchUpdate::Stage {
                    index: self.index,
                    update,
                }))
            }
            .await;
            result.map_err(|e| attributed(&self.stages[self.index].id, e))
        })
    }
}
impl Node<BranchState> for Select {
    fn run<'a, 'b, 'c, 'async_trait>(
        &'a self,
        state: &'b BranchState,
        context: &'c NodeContext,
    ) -> Pin<Box<dyn Future<Output = Result<BranchUpdate, NodeError>> + Send + 'async_trait>>
    where
        'a: 'async_trait,
        'b: 'async_trait,
        'c: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let _ = context;
            let result = async {
                guard(state, &self.stages, true)?;
                if state.stopped
                    || state.selection.is_some()
                    || state.downstream.is_some()
                    || state.first.stop_reason() != Some(AgentStopReason::FinalAnswer)
                {
                    return Err(NodeError::message("invalid selection phase"));
                }
                let message = state
                    .first
                    .messages()
                    .last()
                    .and_then(group_agent_model::Message::as_assistant)
                    .ok_or_else(|| NodeError::message("missing first result"))?;
                let output = self.stages[0]
                    .output
                    .validate_response(&ChatResponse::new(message.clone(), FinishReason::Stop))
                    .map_err(|e| NodeError::with_source("selection output invalid", e))?
                    .ok_or_else(|| NodeError::message("missing selection output"))?;
                let selection = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.selector.select(&output)
                }))
                .map_err(|_| {
                    NodeError::with_source(
                        "branch selector panicked",
                        super::error::SelectorPanicked,
                    )
                })?
                .map_err(|e| NodeError::with_source("branch selector failed", e))?;
                match &selection {
                    BranchSelection::B(messages) | BranchSelection::C(messages) => admit(messages)
                        .map_err(|e| NodeError::with_source("invalid selected messages", e))?,
                    BranchSelection::Complete => {}
                }
                if let Some(sink) = &state.sink {
                    sink.on_event(&super::BranchStreamEvent::Selection {
                        from: self.stages[0].id.clone(),
                        target: selection.target(),
                    });
                }
                Ok(BranchUpdate::Select(selection))
            }
            .await;
            result.map_err(|e| attributed(&self.stages[0].id, e))
        })
    }
}
pub(crate) fn compile(
    revision: &AgentStageId,
    stages: Arc<[AgentStage; 3]>,
    selector: Arc<dyn BranchSelector>,
) -> Result<CompiledGraph<BranchState>, BranchError> {
    let build = || -> Result<_, crate::AgentBuildError> {
        let mut graph = StateGraph::new();
        let parts: Vec<_> = stages
            .iter()
            .map(|s| {
                serde_json::json!([
                    s.id.as_str(),
                    s.config.max_rounds(),
                    s.config.tool_approval(),
                    s.output.contract_id()
                ])
            })
            .collect();
        let bytes = serde_json::to_vec(&serde_json::json!([
            "group-agent-branch/1",
            revision.as_str(),
            parts
        ]))
        .expect("identity JSON");
        graph.set_version(format!(
            "group-agent-prebuilt/agent-branch/1/{:x}",
            Sha256::digest(bytes)
        ));
        for index in 0..3 {
            let stage = &stages[index];
            graph.add_node(
                MODELS[index],
                StageModel {
                    index,
                    stages: stages.clone(),
                    node: ModelNode {
                        output: stage.contract(),
                        model: stage.model.clone(),
                        tools: stage.tools.clone(),
                    },
                },
            )?;
            graph.add_interruptible_node(
                TOOLS[index],
                StageTools {
                    index,
                    stages: stages.clone(),
                    node: ToolNode {
                        runtime: stage.tools.clone(),
                        max_rounds: stage.config.max_rounds(),
                    },
                },
            )?;
            let next = if index == 0 { "select" } else { END };
            graph.add_conditional_edges(
                MODELS[index],
                [TOOLS[index], next],
                move |state: &BranchState| {
                    let stage = state.stage(index).map_err(|e| {
                        group_agent_core::RouteError::with_source("stage routing failed", e)
                    })?;
                    Ok(NodeId::from(
                        if stage.stop_reason() == Some(AgentStopReason::FinalAnswer) {
                            next
                        } else {
                            TOOLS[index]
                        },
                    ))
                },
            )?;
            graph.add_conditional_edges(
                TOOLS[index],
                [MODELS[index], END],
                move |state: &BranchState| {
                    Ok(NodeId::from(if state.stopped {
                        END
                    } else {
                        MODELS[index]
                    }))
                },
            )?;
        }
        graph.add_node("select", Select { stages, selector })?;
        graph.add_edge(START, MODELS[0]);
        graph.add_conditional_edges(
            "select",
            [MODELS[1], MODELS[2], END],
            |state: &BranchState| {
                Ok(NodeId::from(match state.selection {
                    Some(BranchTarget::B) => MODELS[1],
                    Some(BranchTarget::C) => MODELS[2],
                    Some(BranchTarget::Complete) => END,
                    None => return Err(group_agent_core::RouteError::message("missing selection")),
                }))
            },
        )?;
        Ok(graph.compile()?)
    };
    build().map_err(|e| BranchError::source_error(BranchErrorKind::Configuration, e))
}
