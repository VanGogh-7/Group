use crate::error::AgentToolBatchFailure;
use crate::state::{AgentState, AgentUpdate};
use crate::stream::{AgentStreamEvent, YieldNow};
use crate::structured_output::OutputContract;
use crate::{AgentApprovalDecision, AgentApprovalRequest};
use futures_util::StreamExt;
use group_agent_core::{InterruptibleNode, Node, NodeContext, NodeError, NodeOutcome};
use group_agent_model::{
    ChatModel, ChatRequest, ChatStreamCollector, ChatStreamEvent, Message, ToolCall, ToolChoice,
    ToolMessage, ToolResult,
};
use group_agent_tool::{ToolBatchConfig, ToolRuntime};
use std::{error::Error as StdError, fmt, future::Future, pin::Pin, sync::Arc};

pub(crate) struct ModelNode {
    pub(crate) output: OutputContract,
    pub(crate) model: ChatModel,
    pub(crate) tools: ToolRuntime,
}

impl Node<AgentState> for ModelNode {
    fn run<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        state: &'life1 AgentState,
        _context: &'life2 NodeContext,
    ) -> Pin<Box<dyn Future<Output = Result<AgentUpdate, NodeError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let definitions = self
                .tools
                .registry()
                .definitions()
                .cloned()
                .collect::<Vec<_>>();
            let tool_choice = if definitions.is_empty() {
                ToolChoice::None
            } else {
                ToolChoice::Auto
            };
            let request = ChatRequest::new(state.messages().to_vec())
                .with_tools(definitions)
                .with_tool_choice(tool_choice);
            let request = self.output.request(request);
            let round = state.model_rounds() + 1;
            let (message, usage) = if let Some(sink) = state.sink() {
                let mut stream = self
                    .model
                    .stream(request)
                    .await
                    .map_err(|source| NodeError::with_source("model stream failed", source))?;

                sink.on_event(&AgentStreamEvent::ModelStarted { round });

                let mut collector = ChatStreamCollector::new();
                while let Some(event) = stream.next().await {
                    let event = event.map_err(|source| {
                        NodeError::with_source("model stream chunk failed", source)
                    })?;
                    let delta = match &event {
                        ChatStreamEvent::TextDelta(delta) => Some(AgentStreamEvent::TextDelta {
                            round,
                            delta: delta.clone(),
                        }),
                        ChatStreamEvent::ToolCallDelta(delta) => {
                            Some(AgentStreamEvent::ToolCallDelta {
                                round,
                                delta: delta.clone(),
                            })
                        }
                        _ => None,
                    };
                    collector.push(event).map_err(|source| {
                        NodeError::with_source("stream collection failed", source)
                    })?;
                    if let Some(delta) = delta {
                        sink.on_event(&delta);
                        YieldNow(false).await;
                    }
                }

                let response = collector.finish().map_err(|source| {
                    NodeError::with_source("stream collection finish failed", source)
                })?;

                sink.on_event(&AgentStreamEvent::ModelCompleted { round });

                (response.message().clone(), response.usage().cloned())
            } else {
                let response =
                    self.model.complete(request).await.map_err(|source| {
                        NodeError::with_source("model completion failed", source)
                    })?;
                (response.message().clone(), response.usage().cloned())
            };

            Ok(AgentUpdate::ModelCompleted { message, usage })
        })
    }
}

pub(crate) struct ToolNode {
    pub(crate) runtime: ToolRuntime,
    pub(crate) max_rounds: usize,
}

/// Fixed payload-free ToolMessage content committed for a rejected call.
const TOOL_REJECTION_NOTICE: &str = "tool call rejected by approval";

impl ToolNode {
    pub(crate) fn validated_pending_calls<'a>(
        &self,
        state: &'a AgentState,
    ) -> Result<&'a [ToolCall], NodeError> {
        let calls = state.pending_tool_calls().ok_or_else(|| {
            NodeError::with_source("tool node invariant failed", ToolNodeInvariant)
        })?;
        if calls.is_empty() {
            return Err(NodeError::with_source(
                "tool node invariant failed",
                ToolNodeInvariant,
            ));
        }
        if state.model_rounds() > self.max_rounds {
            return Err(NodeError::with_source(
                "tool node invariant failed",
                ToolNodeInvariant,
            ));
        }
        Ok(calls)
    }

    pub(crate) async fn execute_pending(
        &self,
        state: &AgentState,
        calls: &[ToolCall],
    ) -> Result<AgentUpdate, NodeError> {
        let observed_runtime;
        let runtime = if let Some(sink) = state.sink() {
            let agent_sink = Arc::clone(sink);
            let round = state.model_rounds();
            observed_runtime = self.runtime.clone().with_additional_event_sink(Arc::new(
                move |event: &group_agent_tool::ToolEvent| {
                    match event {
                        group_agent_tool::ToolEvent::ExecutionStarted { context } => {
                            agent_sink.on_event(&AgentStreamEvent::ToolStarted {
                                id: context.call_id().clone(),
                                name: context.tool_name().clone(),
                                round,
                            });
                        }
                        group_agent_tool::ToolEvent::ExecutionCompleted { context, is_error } => {
                            agent_sink.on_event(&AgentStreamEvent::ToolCompleted {
                                id: context.call_id().clone(),
                                name: context.tool_name().clone(),
                                round,
                                is_error: *is_error,
                            });
                        }
                        group_agent_tool::ToolEvent::ExecutionFailed {
                            context,
                            kind:
                                group_agent_tool::ToolRuntimeErrorKind::ExecutionFailed
                                | group_agent_tool::ToolRuntimeErrorKind::Cancelled,
                        }
                        | group_agent_tool::ToolEvent::ExecutionTimedOut { context, .. } => {
                            agent_sink.on_event(&AgentStreamEvent::ToolCompleted {
                                id: context.call_id().clone(),
                                name: context.tool_name().clone(),
                                round,
                                is_error: true,
                            });
                        }
                        _ => {}
                    }
                    Ok(())
                },
            ));
            &observed_runtime
        } else {
            &self.runtime
        };

        let report = runtime
            .execute_batch(calls.to_vec(), ToolBatchConfig::default())
            .await
            .map_err(|source| NodeError::with_source("tool batch rejected", source))?;
        if report.results().iter().any(Result::is_err) {
            return Err(NodeError::with_source(
                "tool batch execution failed",
                AgentToolBatchFailure::new(report),
            ));
        }

        let messages = report
            .into_tool_messages()
            .into_iter()
            .map(|message| match message {
                Ok(Message::Tool(message)) => message,
                Ok(_) | Err(_) => unreachable!("validated Tool batch message conversion"),
            })
            .collect::<Vec<ToolMessage>>();
        Ok(self.tools_completed(state, messages))
    }

    pub(crate) fn rejected_update(&self, state: &AgentState, calls: &[ToolCall]) -> AgentUpdate {
        let messages = calls
            .iter()
            .map(|call| {
                ToolMessage::new(
                    call.id().clone(),
                    ToolResult::error_text(TOOL_REJECTION_NOTICE),
                )
            })
            .collect();
        self.tools_completed(state, messages)
    }

    fn tools_completed(&self, state: &AgentState, messages: Vec<ToolMessage>) -> AgentUpdate {
        AgentUpdate::ToolsCompleted {
            messages,
            reached_max_rounds: state.model_rounds() == self.max_rounds,
        }
    }
}

impl Node<AgentState> for ToolNode {
    fn run<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        state: &'life1 AgentState,
        _context: &'life2 NodeContext,
    ) -> Pin<Box<dyn Future<Output = Result<AgentUpdate, NodeError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let calls = self.validated_pending_calls(state)?;
            self.execute_pending(state, calls).await
        })
    }
}

impl InterruptibleNode<AgentState> for ToolNode {
    fn run<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        state: &'life1 AgentState,
        context: &'life2 NodeContext,
    ) -> Pin<
        Box<dyn Future<Output = Result<NodeOutcome<AgentUpdate>, NodeError>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let calls = self.validated_pending_calls(state)?;
            if !context.has_resume_value() {
                let request = AgentApprovalRequest::new(calls.to_vec());
                if let Some(sink) = state.sink() {
                    sink.on_event(&AgentStreamEvent::ApprovalRequired {
                        request: request.clone(),
                    });
                }
                return Ok(NodeOutcome::interrupt(request));
            }
            let decision = context
                .require_resume_value::<AgentApprovalDecision>()
                .map_err(|source| NodeError::with_source("invalid approval decision", source))?;
            // Exhaustive on purpose: introducing a future decision variant
            // fails this build instead of ever falling through to approval.
            match decision {
                AgentApprovalDecision::Approve => self
                    .execute_pending(state, calls)
                    .await
                    .map(NodeOutcome::update),
                AgentApprovalDecision::Reject => {
                    Ok(NodeOutcome::update(self.rejected_update(state, calls)))
                }
            }
        })
    }
}

#[derive(Debug)]
struct ToolNodeInvariant;

impl fmt::Display for ToolNodeInvariant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("tool node state invariant failed")
    }
}

impl StdError for ToolNodeInvariant {}
