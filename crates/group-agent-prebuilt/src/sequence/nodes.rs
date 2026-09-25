use super::{
    AgentStage, AgentStageId, HandoffMapper, SequenceApprovalDecision, SequenceApprovalRequest,
    SequenceError, SequenceErrorKind,
    error::StageFailure,
    state::{SequenceState, SequenceUpdate, admit},
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
const MODELS: [&str; 2] = ["first_model", "second_model"];
const TOOLS: [&str; 2] = ["first_tools", "second_tools"];
pub(crate) fn guard(
    state: &SequenceState,
    stages: &[AgentStage; 2],
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
    state: &SequenceState,
    stages: &[AgentStage; 2],
    index: usize,
    tools: bool,
) -> Result<(), NodeError> {
    guard(state, stages, true)?;
    let bad = || {
        NodeError::with_source(
            "invalid sequence node frontier",
            SequenceError::new(SequenceErrorKind::InvalidState),
        )
    };
    if state.stopped
        || (index == 0 && state.second.is_some())
        || (index == 1
            && (state.second.is_none()
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
        "sequence stage failed",
        StageFailure {
            stage: stage.clone(),
            source,
        },
    )
}
struct StageModel {
    index: usize,
    stages: Arc<[AgentStage; 2]>,
    node: ModelNode,
}
struct StageTools {
    index: usize,
    stages: Arc<[AgentStage; 2]>,
    node: ToolNode,
}
struct Handoff {
    stages: Arc<[AgentStage; 2]>,
    mapper: Arc<dyn HandoffMapper>,
}
impl Node<SequenceState> for StageModel {
    fn run<'a, 'b, 'c, 'async_trait>(
        &'a self,
        state: &'b SequenceState,
        context: &'c NodeContext,
    ) -> Pin<Box<dyn Future<Output = Result<SequenceUpdate, NodeError>> + Send + 'async_trait>>
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
                Ok(SequenceUpdate::Stage {
                    index: self.index,
                    update,
                })
            }
            .await;
            result.map_err(|e| attributed(&self.stages[self.index].id, e))
        })
    }
}
impl InterruptibleNode<SequenceState> for StageTools {
    fn run<'a, 'b, 'c, 'async_trait>(
        &'a self,
        state: &'b SequenceState,
        context: &'c NodeContext,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<NodeOutcome<SequenceUpdate>, NodeError>>
                + Send
                + 'async_trait,
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
                        return Ok(NodeOutcome::interrupt(SequenceApprovalRequest {
                            stage: stage.id.clone(),
                            request: AgentApprovalRequest::new(calls.to_vec()),
                        }));
                    }
                    let decision = context
                        .require_resume_value::<SequenceApprovalDecision>()
                        .map_err(|e| NodeError::with_source("invalid sequence decision", e))?;
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
                Ok(NodeOutcome::update(SequenceUpdate::Stage {
                    index: self.index,
                    update,
                }))
            }
            .await;
            result.map_err(|e| attributed(&self.stages[self.index].id, e))
        })
    }
}
impl Node<SequenceState> for Handoff {
    fn run<'a, 'b, 'c, 'async_trait>(
        &'a self,
        state: &'b SequenceState,
        context: &'c NodeContext,
    ) -> Pin<Box<dyn Future<Output = Result<SequenceUpdate, NodeError>> + Send + 'async_trait>>
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
                if state.second.is_some()
                    || state.first.stop_reason() != Some(AgentStopReason::FinalAnswer)
                {
                    return Err(NodeError::message("invalid handoff phase"));
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
                    .map_err(|e| NodeError::with_source("handoff output invalid", e))?
                    .ok_or_else(|| NodeError::message("missing handoff output"))?;
                let messages = self
                    .mapper
                    .map(&output)
                    .map_err(|e| NodeError::with_source("handoff mapper failed", e))?;
                admit(&messages)
                    .map_err(|e| NodeError::with_source("invalid handoff messages", e))?;
                if let Some(sink) = &state.sink {
                    sink.on_event(&super::SequenceStreamEvent::Handoff {
                        from: self.stages[0].id.clone(),
                        to: self.stages[1].id.clone(),
                    });
                }
                Ok(SequenceUpdate::Handoff(messages))
            }
            .await;
            result.map_err(|e| attributed(&self.stages[0].id, e))
        })
    }
}
pub(crate) fn compile(
    revision: &AgentStageId,
    stages: Arc<[AgentStage; 2]>,
    mapper: Arc<dyn HandoffMapper>,
) -> Result<CompiledGraph<SequenceState>, SequenceError> {
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
            "group-agent-sequence/1",
            revision.as_str(),
            parts
        ]))
        .expect("identity JSON");
        graph.set_version(format!(
            "group-agent-prebuilt/agent-sequence/1/{:x}",
            Sha256::digest(bytes)
        ));
        for index in 0..2 {
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
            let next = if index == 0 { "handoff" } else { END };
            graph.add_conditional_edges(
                MODELS[index],
                [TOOLS[index], next],
                move |state: &SequenceState| {
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
                move |state: &SequenceState| {
                    Ok(NodeId::from(if state.stopped {
                        END
                    } else {
                        MODELS[index]
                    }))
                },
            )?;
        }
        graph.add_node("handoff", Handoff { stages, mapper })?;
        graph.add_edge(START, MODELS[0]);
        graph.add_edge("handoff", MODELS[1]);
        Ok(graph.compile()?)
    };
    build().map_err(|e| SequenceError::source_error(SequenceErrorKind::Configuration, e))
}
