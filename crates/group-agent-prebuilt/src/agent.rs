use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::StreamExt;
#[cfg(test)]
use group_agent_core::GraphVersion;
use group_agent_core::{
    CheckpointConfig, CompiledGraph, END, EventConfig, ForkConfig, InterruptibleNode, Node,
    NodeContext, NodeError, NodeId, NodeOutcome, ReplayConfig, ResumeConfig, RouteError, RunConfig,
    RunControl, START, StateGraph,
};
use group_agent_model::{
    AssistantMessage, ChatModel, ChatRequest, ChatStreamCollector, ChatStreamEvent, Message,
    TokenUsage, ToolCall, ToolChoice, ToolMessage, ToolResult,
};
use group_agent_tool::{ToolBatchConfig, ToolRuntime};
use serde::{Deserialize, Serialize};

use crate::error::AgentToolBatchFailure;
use crate::outcome::{AgentForkReport, AgentReplayReport, AgentRunOutcome};
use crate::snapshot::AgentSnapshot;
use crate::state::{AgentState, AgentUpdate};
use crate::stream::{
    AgentEventSink, AgentEventStream, AgentStreamEvent, ChannelEventSink, YieldNow,
};
use crate::structured_output::OutputContract;
use crate::{
    AgentApprovalDecision, AgentApprovalRequest, AgentBuildError, AgentConfig, AgentError,
};

const MODEL_NODE_ID: &str = "model";
const TOOL_NODE_ID: &str = "tools";

/// Durable compatibility identity of the private Tool-calling graph.
///
/// The version is recorded in every durable checkpoint written for this
/// graph. It must change whenever node identifiers, node paths, or the graph
/// topology change.
const AGENT_GRAPH_VERSION: &str = "group-agent-prebuilt/tool-calling-agent/1";

/// Durable compatibility identity of the approval-enabled Tool-calling graph.
///
/// Approval changes the Tool node's kind, so approval-enabled and base agents
/// resume only from their own checkpoints; cross-configuration resume fails
/// closed at this identity boundary. The version is pinned by tests and must
/// change whenever node identifiers, node paths, or the topology change.
const AGENT_APPROVAL_GRAPH_VERSION: &str = "group-agent-prebuilt/tool-calling-agent/approval/1";

/// Experimental normal stop classification for a prebuilt Agent invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AgentStopReason {
    /// The model returned an assistant message containing no ToolCalls.
    FinalAnswer,
    /// The final allowed model round requested Tools, and its complete bounded
    /// Tool batch and paired ToolMessages were committed without another model
    /// turn.
    MaxRounds,
}

/// Experimental owned result from a successful prebuilt Agent invocation.
///
/// The transcript is canonical. The final assistant message is derived from
/// it and is not stored a second time.
///
/// A Tool round uses the same public [`ChatModel`] facade and [`ToolRuntime`]
/// boundary as an application. With two allowed model rounds the ToolMessage
/// enters the next model request before `FinalAnswer`; with one allowed round
/// the Tool still executes and the normal result is `MaxRounds` with no final
/// assistant message:
///
/// ```
/// # use async_trait::async_trait;
/// # use group_agent_model::{AssistantMessage, ChatModel, ChatModelAdapter, ChatResponse, FinishReason, Message, ModelCapabilities, ModelError, ModelId, ModelMetadata, ProviderId, ToolCall, ToolCallId, ToolDefinition, ToolName, ValidatedChatRequest};
/// # use group_agent_prebuilt::{AgentConfig, AgentStopReason, ToolCallingAgent};
/// # use group_agent_tool::{Tool, ToolBehavior, ToolError, ToolInput, ToolOutput, ToolRegistry, ToolRuntime};
/// # use serde_json::json;
/// # struct Scripted { metadata: ModelMetadata }
/// # #[async_trait]
/// # impl ChatModelAdapter for Scripted {
/// #     fn metadata(&self) -> &ModelMetadata { &self.metadata }
/// #     async fn complete_raw(&self, request: ValidatedChatRequest) -> Result<ChatResponse, ModelError> {
/// #         let saw_tool = request.messages().iter().any(|message| matches!(message, Message::Tool(_)));
/// #         let message = if saw_tool {
/// #             AssistantMessage::text("tool-assisted answer")
/// #         } else {
/// #             AssistantMessage::new(Vec::new(), vec![ToolCall::new(
/// #                 ToolCallId::new("call-1").unwrap(),
/// #                 ToolName::new("lookup").unwrap(),
/// #                 json!({"item": "sample"}),
/// #             )])
/// #         };
/// #         let finish = if saw_tool { FinishReason::Stop } else { FinishReason::ToolCalls };
/// #         Ok(ChatResponse::new(message, finish))
/// #     }
/// # }
/// # struct Lookup { definition: ToolDefinition }
/// # #[async_trait]
/// # impl Tool for Lookup {
/// #     fn name(&self) -> &ToolName { self.definition.name() }
/// #     fn definition(&self) -> &ToolDefinition { &self.definition }
/// #     fn behavior(&self) -> ToolBehavior { ToolBehavior::read_only() }
/// #     async fn execute(&self, _input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
/// #         Ok(ToolOutput::success_text("offline result"))
/// #     }
/// # }
/// # fn model() -> Result<ChatModel, Box<dyn std::error::Error>> {
/// #     Ok(ChatModel::from_adapter(Scripted { metadata: ModelMetadata::new(
/// #         ProviderId::new("offline")?, ModelId::new("scripted")?,
/// #         ModelCapabilities::new().with_tool_calling(true),
/// #     )})?)
/// # }
/// # fn tools() -> Result<ToolRuntime, Box<dyn std::error::Error>> {
/// #     let mut builder = ToolRegistry::builder();
/// #     builder.register(Lookup { definition: ToolDefinition::new(
/// #         ToolName::new("lookup")?, "Offline lookup", json!({
/// #             "type": "object", "properties": {"item": {"type": "string"}},
/// #             "required": ["item"], "additionalProperties": false
/// #         }),
/// #     )})?;
/// #     Ok(ToolRuntime::new(builder.build()))
/// # }
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let completed = ToolCallingAgent::new(model()?, tools()?, AgentConfig::new(2)?)?
///     .invoke(vec![Message::user("use the tool")])
///     .await?;
/// assert_eq!(completed.stop_reason(), AgentStopReason::FinalAnswer);
/// assert_eq!(completed.model_rounds(), 2);
/// assert_eq!(completed.final_message().unwrap().text_content(), "tool-assisted answer");
///
/// let capped = ToolCallingAgent::new(model()?, tools()?, AgentConfig::new(1)?)?
///     .invoke(vec![Message::user("use the tool")])
///     .await?;
/// assert_eq!(capped.stop_reason(), AgentStopReason::MaxRounds);
/// assert_eq!(capped.model_rounds(), 1);
/// assert!(capped.final_message().is_none());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, PartialEq)]
pub struct AgentOutcome {
    #[cfg(feature = "structured-output")]
    pub(crate) output: Option<group_agent_model::ValidatedJsonOutput>,
    messages: Vec<Message>,
    model_rounds: usize,
    usage_by_round: Vec<Option<TokenUsage>>,
    stop_reason: AgentStopReason,
}

impl AgentOutcome {
    pub(crate) fn from_completed_state(state: AgentState) -> Self {
        let (messages, model_rounds, usage_by_round, stop_reason) = state.into_parts();
        let stop_reason = stop_reason.expect("a completed model graph commits a stop reason");
        Self {
            #[cfg(feature = "structured-output")]
            output: None,
            messages,
            model_rounds,
            usage_by_round,
            stop_reason,
        }
    }

    /// Returns locally validated JSON only for a structured final answer.
    #[cfg(feature = "structured-output")]
    pub fn structured_output(&self) -> Option<&group_agent_model::ValidatedJsonOutput> {
        self.output.as_ref()
    }

    /// Returns the complete ordered conversation transcript.
    #[must_use]
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// Returns successfully committed model rounds.
    #[must_use]
    pub const fn model_rounds(&self) -> usize {
        self.model_rounds
    }

    /// Returns usage aligned one-to-one with committed model rounds.
    #[must_use]
    pub fn usage_by_round(&self) -> &[Option<TokenUsage>] {
        &self.usage_by_round
    }

    /// Returns why the invocation stopped normally.
    #[must_use]
    pub const fn stop_reason(&self) -> AgentStopReason {
        self.stop_reason
    }

    /// Derives the final assistant answer from the canonical transcript.
    ///
    /// This is `None` when [`AgentStopReason::MaxRounds`] follows a Tool batch,
    /// because the transcript ends in ToolMessages that the model did not read.
    #[must_use]
    pub fn final_message(&self) -> Option<&AssistantMessage> {
        if self.stop_reason != AgentStopReason::FinalAnswer {
            return None;
        }
        self.messages.last().and_then(Message::as_assistant)
    }
}

impl fmt::Debug for AgentOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentOutcome")
            .field("message_count", &self.messages.len())
            .field("model_rounds", &self.model_rounds)
            .field("usage_rounds", &self.usage_by_round.len())
            .field("stop_reason", &self.stop_reason)
            .finish()
    }
}

/// Experimental reusable prebuilt Tool-calling Agent.
///
/// The current experimental behavior alternates non-streaming Model turns and
/// bounded Tool batches until a final answer or the configured round limit.
/// The graph is compiled once by [`Self::new`] and reused by every
/// [`Self::invoke`] and [`Self::invoke_with_control`].
///
/// This entirely offline example constructs the Agent through the public
/// [`ChatModel`] facade and invokes the model-only path with both default and
/// caller-supplied Core controls:
///
/// ```
/// # use async_trait::async_trait;
/// # use group_agent_core::{EventConfig, RunControl};
/// # use group_agent_model::{AssistantMessage, ChatModel, ChatModelAdapter, ChatResponse, FinishReason, Message, ModelCapabilities, ModelError, ModelId, ModelMetadata, ProviderId, ValidatedChatRequest};
/// # use group_agent_prebuilt::{AgentConfig, AgentStopReason, ToolCallingAgent};
/// # use group_agent_tool::{ToolRegistry, ToolRuntime};
/// # struct FinalModel { metadata: ModelMetadata }
/// # #[async_trait]
/// # impl ChatModelAdapter for FinalModel {
/// #     fn metadata(&self) -> &ModelMetadata { &self.metadata }
/// #     async fn complete_raw(&self, _request: ValidatedChatRequest) -> Result<ChatResponse, ModelError> {
/// #         Ok(ChatResponse::new(AssistantMessage::text("offline answer"), FinishReason::Stop))
/// #     }
/// # }
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let model = ChatModel::from_adapter(FinalModel {
///     metadata: ModelMetadata::new(
///         ProviderId::new("offline")?,
///         ModelId::new("scripted")?,
///         ModelCapabilities::new(),
///     ),
/// })?;
/// let agent = ToolCallingAgent::new(
///     model,
///     ToolRuntime::new(ToolRegistry::empty()),
///     AgentConfig::new(1)?,
/// )?;
/// let outcome = agent.invoke(vec![Message::user("hello")]).await?;
/// assert_eq!(outcome.stop_reason(), AgentStopReason::FinalAnswer);
/// assert_eq!(outcome.model_rounds(), 1);
/// assert_eq!(outcome.final_message().unwrap().text_content(), "offline answer");
///
/// let controlled = agent
///     .invoke_with_control(
///         vec![Message::user("hello again")],
///         EventConfig::default(),
///         RunControl::default(),
///     )
///     .await?;
/// assert_eq!(controlled.stop_reason(), AgentStopReason::FinalAnswer);
/// # Ok(())
/// # }
/// ```
pub struct ToolCallingAgent {
    pub(crate) output: OutputContract,
    pub(crate) graph: Arc<CompiledGraph<AgentState>>,
    pub(crate) run_config: RunConfig,
    #[cfg(test)]
    compile_probe: CountingModelGraphCompiler,
}

impl ToolCallingAgent {
    /// Builds and compiles the experimental Tool-calling graph.
    ///
    /// The graph is compiled once during construction and reused across
    /// invocations. It loops between Model turns and bounded Tool batches
    /// requested by those turns through the supplied [`ToolRuntime`] until a
    /// normal stop condition is committed.
    pub fn new(
        model: ChatModel,
        tools: ToolRuntime,
        config: AgentConfig,
    ) -> Result<Self, AgentBuildError> {
        Self::new_inner(model, tools, config, OutputContract::default())
    }

    /// Constructs an agent whose final answers must satisfy an output contract.
    /// Structured checkpoints require this same contract on every restore.
    #[cfg(feature = "structured-output")]
    pub fn new_with_output(
        model: ChatModel,
        tools: ToolRuntime,
        config: AgentConfig,
        output: group_agent_model::StructuredOutput,
    ) -> Result<Self, AgentBuildError> {
        if !model.metadata().capabilities().structured_output() {
            return Err(AgentBuildError::OutputConfiguration(
                group_agent_model::ModelError::unsupported(
                    group_agent_model::ModelCapability::StructuredOutput,
                    model.metadata(),
                ),
            ));
        }
        Self::new_inner(
            model,
            tools,
            config,
            OutputContract {
                value: Some(output),
            },
        )
    }

    fn new_inner(
        model: ChatModel,
        tools: ToolRuntime,
        config: AgentConfig,
        output: OutputContract,
    ) -> Result<Self, AgentBuildError> {
        let max_steps = config
            .max_rounds()
            .checked_mul(2)
            .expect("AgentConfig validates the private Core step bound");
        #[cfg(test)]
        let compile_probe = CountingModelGraphCompiler::default();
        #[cfg(test)]
        let graph = compile_agent_graph(
            model,
            tools,
            config.max_rounds(),
            config.tool_approval(),
            output.clone(),
            &compile_probe,
        )?;
        #[cfg(not(test))]
        let graph = compile_agent_graph(
            model,
            tools,
            config.max_rounds(),
            config.tool_approval(),
            output.clone(),
            &CoreModelGraphCompiler,
        )?;
        Ok(Self {
            output,
            graph: Arc::new(graph),
            run_config: RunConfig::new(max_steps),
            #[cfg(test)]
            compile_probe,
        })
    }

    /// Experimentally invokes one isolated non-streaming Tool-calling
    /// conversation with Core's default event and execution-control
    /// configuration.
    ///
    /// Each Model request owns a clone of the current canonical transcript. A
    /// response without ToolCalls returns a normal
    /// [`AgentStopReason::FinalAnswer`] outcome. Otherwise, the requested
    /// bounded Tool batch executes and its paired ToolMessages commit before
    /// another Model turn. If the final allowed Model round requests Tools,
    /// that complete batch still executes and commits before a normal
    /// [`AgentStopReason::MaxRounds`] outcome; [`AgentOutcome::final_message`]
    /// is then `None` because no later Model turn reads those ToolMessages.
    ///
    /// # Errors
    ///
    /// Model, Tool infrastructure, graph, State, cancellation, and timeout
    /// failures return [`AgentError`]. An error can occur after earlier Tool
    /// batches executed and committed their ToolMessages to internal Agent
    /// State. The error does not expose that committed transcript or those
    /// ToolMessages.
    ///
    /// # Side effects
    ///
    /// Tools may produce external side effects before an error is returned.
    /// Dropping the invocation Future releases local Model and Tool Futures but
    /// does not prove that a remote operation was cancelled or its effects were
    /// undone. Receiving `AgentError` does not prove that Tools did not execute
    /// and must not be treated as permission to retry blindly. This
    /// non-durable path persists nothing; durability is opt-in through
    /// [`Self::invoke_with_checkpoint`]. There is no rollback, exactly-once,
    /// or automatic-retry guarantee.
    pub async fn invoke(&self, messages: Vec<Message>) -> Result<AgentOutcome, AgentError> {
        self.invoke_inner(
            messages,
            None,
            EventConfig::default(),
            RunControl::default(),
        )
        .await
    }

    /// Experimentally invokes one isolated conversation with caller-supplied
    /// Core events and execution controls.
    ///
    /// The supplied [`EventConfig`] and [`RunControl`] are forwarded unchanged
    /// to the existing Core graph invocation. Cancellation, run timeout, and
    /// node timeout retain Core's typed [`group_agent_core::GraphRunError`]
    /// classifications and precedence.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError`] for Model, Tool infrastructure, graph, State,
    /// cancellation, or timeout failure. The error does not expose internal
    /// committed State or transcript, including earlier successfully committed
    /// Tool rounds.
    ///
    /// # Side effects and cancellation
    ///
    /// Earlier Tools may already have produced external side effects before a
    /// later failure. Cancellation, timeout, or dropping this Future releases
    /// locally owned pending Futures; it does not prove that remote work was
    /// cancelled or rolled back. This non-durable path persists nothing;
    /// durability is opt-in through [`Self::invoke_with_checkpoint_control`].
    /// There is no exactly-once, hidden-retry, or rollback guarantee.
    pub async fn invoke_with_control(
        &self,
        messages: Vec<Message>,
        event_config: EventConfig,
        run_control: RunControl,
    ) -> Result<AgentOutcome, AgentError> {
        self.invoke_inner(messages, None, event_config, run_control)
            .await
    }

    /// Experimentally invokes one conversation with streaming token and
    /// lifecycle events dispatched synchronously to a caller-supplied sink.
    ///
    /// The underlying model execution uses streaming when supported by the
    /// configured model adapter, emitting [`AgentStreamEvent::TextDelta`] and
    /// [`AgentStreamEvent::ToolCallDelta`] fragments as they arrive.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError`] if the model lacks streaming capability, or if
    /// model streaming, tool execution, or graph execution fails.
    pub async fn invoke_with_stream_sink(
        &self,
        messages: Vec<Message>,
        sink: Arc<dyn AgentEventSink>,
    ) -> Result<AgentOutcome, AgentError> {
        self.invoke_with_stream_sink_control(
            messages,
            sink,
            EventConfig::default(),
            RunControl::default(),
        )
        .await
    }

    /// Experimentally invokes one conversation with streaming events and
    /// caller-supplied Core execution controls.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError`] for model, tool, graph, cancellation, or timeout failure.
    pub async fn invoke_with_stream_sink_control(
        &self,
        messages: Vec<Message>,
        sink: Arc<dyn AgentEventSink>,
        event_config: EventConfig,
        run_control: RunControl,
    ) -> Result<AgentOutcome, AgentError> {
        self.invoke_inner(messages, Some(sink), event_config, run_control)
            .await
    }

    /// Experimentally invokes one conversation as an asynchronous event stream.
    ///
    /// Emits [`AgentStreamEvent`] items including incremental text tokens,
    /// tool call fragments, tool lifecycle events, and the final completion
    /// outcome.
    ///
    /// Dropping the returned [`AgentEventStream`] drops the underlying
    /// invocation and terminates provider streaming and tool execution
    /// without leaving detached tasks.
    pub fn stream(&self, messages: Vec<Message>) -> AgentEventStream {
        self.stream_with_control(messages, EventConfig::default(), RunControl::default())
    }

    /// Experimentally invokes one conversation as an asynchronous event stream
    /// with caller-supplied Core execution controls.
    pub fn stream_with_control(
        &self,
        messages: Vec<Message>,
        event_config: EventConfig,
        run_control: RunControl,
    ) -> AgentEventStream {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let sink = Arc::new(ChannelEventSink::new(sender));
        let graph = Arc::clone(&self.graph);
        let run_config = self.run_config.clone();
        let output = self.output.clone();
        let invocation = Box::pin(async move {
            let state = AgentState::new(messages).with_sink(sink.clone());
            let report = graph
                .invoke_with_control(state, run_config, event_config, run_control)
                .await
                .map_err(AgentError::from_graph)?;
            let outcome = output.validate(AgentOutcome::from_completed_state(
                report.into_final_state(),
            ))?;
            sink.on_event(&AgentStreamEvent::Completed(outcome));
            Ok(())
        });
        AgentEventStream::new(receiver, invocation)
    }

    async fn invoke_inner(
        &self,
        messages: Vec<Message>,
        sink: Option<Arc<dyn AgentEventSink>>,
        event_config: EventConfig,
        run_control: RunControl,
    ) -> Result<AgentOutcome, AgentError> {
        let mut state = AgentState::new(messages);
        if let Some(ref s) = sink {
            state = state.with_sink(Arc::clone(s));
        }
        let report = self
            .graph
            .invoke_with_control(state, self.run_config.clone(), event_config, run_control)
            .await
            .map_err(AgentError::from_graph)?;
        let outcome = self.output.validate(AgentOutcome::from_completed_state(
            report.into_final_state(),
        ))?;
        if let Some(ref s) = sink {
            s.on_event(&AgentStreamEvent::Completed(outcome.clone()));
        }
        Ok(outcome)
    }

    /// Experimentally invokes one conversation with opt-in durable
    /// checkpoints.
    ///
    /// Every committed super-step boundary is written to the configured
    /// checkpointer under the configured logical thread, recorded with the
    /// graph's stable compatibility version. The checkpoint configuration is
    /// typed over the public opaque [`AgentSnapshot`]; the private Agent State
    /// never appears in this API. The result converts eagerly into
    /// [`AgentRunOutcome`].
    ///
    /// This variant uses Core's default event and execution-control
    /// configuration and delegates to [`Self::invoke_with_checkpoint_control`].
    ///
    /// # Errors
    ///
    /// Returns [`AgentError`] for Model, Tool infrastructure, graph, State,
    /// checkpoint load/store, cancellation, or timeout failure. Earlier
    /// committed super-steps remain durably stored in the configured
    /// checkpointer. The error does not expose the committed transcript.
    ///
    /// # Side effects
    ///
    /// Tools may produce external side effects before an error is returned.
    /// Durable checkpoints record committed Agent State only; they do not roll
    /// back Tool effects. This method performs no hidden retry and provides no
    /// exactly-once guarantee.
    pub async fn invoke_with_checkpoint(
        &self,
        messages: Vec<Message>,
        checkpoint_config: CheckpointConfig<AgentSnapshot>,
    ) -> Result<AgentRunOutcome, AgentError> {
        self.invoke_with_checkpoint_control(
            messages,
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config,
        )
        .await
    }

    /// Experimentally invokes one conversation with caller-supplied Core
    /// events, execution controls, and opt-in durable checkpoints.
    ///
    /// Behaves like [`Self::invoke_with_checkpoint`], forwarding the supplied
    /// [`EventConfig`] and [`RunControl`] unchanged to the Core graph
    /// invocation.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError`] for Model, Tool infrastructure, graph, State,
    /// checkpoint load/store, cancellation, or timeout failure. The error does
    /// not expose internal committed State or transcript. Earlier committed
    /// super-steps remain durably stored.
    ///
    /// # Side effects
    ///
    /// Tools may produce external side effects before a later failure.
    /// Durable checkpoints record committed Agent State only; they do not roll
    /// back Tool effects. There is no hidden-retry, exactly-once, or rollback
    /// guarantee.
    pub async fn invoke_with_checkpoint_control(
        &self,
        messages: Vec<Message>,
        event_config: EventConfig,
        run_control: RunControl,
        checkpoint_config: CheckpointConfig<AgentSnapshot>,
    ) -> Result<AgentRunOutcome, AgentError> {
        let outcome = self
            .graph
            .invoke_with_checkpoint(
                AgentState::new(messages),
                self.run_config.clone(),
                event_config,
                run_control,
                checkpoint_config,
            )
            .await
            .map_err(AgentError::from_graph)?;
        AgentRunOutcome::from_execution(outcome, &self.output)
    }

    /// Experimentally resumes the latest committed checkpoint of one logical
    /// thread.
    ///
    /// Resume is latest-head-only: it loads the thread's current latest
    /// checkpoint — or the explicitly targeted checkpoint when it is still the
    /// latest — and continues execution from that committed boundary. Use
    /// [`Self::replay`] for read-only historical re-execution or [`Self::fork`]
    /// for a writable branch. New super-step boundaries commit back to the same
    /// thread through the checkpointer in the resume configuration.
    ///
    /// The additional node budget comes from
    /// [`ResumeConfig::with_run_config`]; when unset, this Agent's
    /// construction-time step bound applies instead of Core's
    /// [`RunConfig::default`]. An explicit caller-set value equal to the Core
    /// default is treated as unset.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError`] when the thread has no checkpoint, when the
    /// stored snapshot fails compatibility or codec validation, and for Model,
    /// Tool infrastructure, graph, State, checkpoint load/store, cancellation,
    /// or timeout failure. The error does not expose the committed transcript.
    /// This method performs no hidden retry.
    pub async fn resume(
        &self,
        resume_config: ResumeConfig<AgentSnapshot>,
    ) -> Result<AgentRunOutcome, AgentError> {
        let resume_config = if *resume_config.run_config() == RunConfig::default() {
            resume_config.with_run_config(self.run_config.clone())
        } else {
            resume_config
        };
        let outcome = self
            .graph
            .resume(resume_config)
            .await
            .map_err(AgentError::from_graph)?;
        AgentRunOutcome::from_execution(outcome, &self.output)
    }

    /// Experimentally re-executes from one exact historical checkpoint without
    /// writing lineage.
    ///
    /// Replay is exact and read-only: it never saves a checkpoint, advances a
    /// thread head, or creates a branch. Nodes execute normally and therefore
    /// may repeat external side effects, including Tool execution.
    ///
    /// The additional node budget comes from
    /// [`ReplayConfig::with_run_config`]; when unset, this Agent's
    /// construction-time step bound applies instead of Core's
    /// [`RunConfig::default`]. An explicit caller-set value equal to the Core
    /// default is treated as unset.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError`] when the checkpoint is missing or incompatible,
    /// and for Model, Tool infrastructure, graph, State, cancellation, or
    /// timeout failure. The error does not expose the committed transcript.
    /// This method performs no hidden retry.
    pub async fn replay(
        &self,
        replay_config: ReplayConfig<AgentSnapshot>,
    ) -> Result<AgentReplayReport, AgentError> {
        let replay_config = if *replay_config.run_config() == RunConfig::default() {
            replay_config.with_run_config(self.run_config.clone())
        } else {
            replay_config
        };
        let report = self
            .graph
            .replay(replay_config)
            .await
            .map_err(AgentError::from_graph)?;
        AgentReplayReport::from_replay(report, &self.output)
    }

    /// Experimentally creates a writable branch from one exact historical
    /// checkpoint and runs it.
    ///
    /// Fork is the only writable historical branch operation. The source
    /// thread's head and history stay read-only; every checkpoint produced by
    /// the branch run commits under the branch's independent head.
    ///
    /// The additional node budget comes from
    /// [`ForkConfig::with_run_config`]; when unset, this Agent's
    /// construction-time step bound applies instead of Core's
    /// [`RunConfig::default`]. An explicit caller-set value equal to the Core
    /// default is treated as unset.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError`] when the checkpoint is missing or incompatible,
    /// when the branch identifier already exists, and for Model, Tool
    /// infrastructure, graph, State, checkpoint load/store, cancellation, or
    /// timeout failure. The error does not expose the committed transcript.
    /// This method performs no hidden retry.
    pub async fn fork(
        &self,
        fork_config: ForkConfig<AgentSnapshot>,
    ) -> Result<AgentForkReport, AgentError> {
        let fork_config = if *fork_config.run_config() == RunConfig::default() {
            fork_config.with_run_config(self.run_config.clone())
        } else {
            fork_config
        };
        let report = self
            .graph
            .fork(fork_config)
            .await
            .map_err(AgentError::from_graph)?;
        AgentForkReport::from_fork(report, &self.output)
    }

    #[cfg(test)]
    pub(crate) fn observed_graph_compiles(&self) -> usize {
        self.compile_probe.observed()
    }

    #[cfg(test)]
    pub(crate) fn graph_version(&self) -> Option<&GraphVersion> {
        self.graph.version()
    }
}

struct ModelNode {
    output: OutputContract,
    model: ChatModel,
    tools: ToolRuntime,
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

struct ToolNode {
    runtime: ToolRuntime,
    max_rounds: usize,
}

/// Fixed payload-free ToolMessage content committed for a rejected call.
const TOOL_REJECTION_NOTICE: &str = "tool call rejected by approval";

impl ToolNode {
    fn validated_pending_calls<'a>(
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

    async fn execute_pending(
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

    fn rejected_update(&self, state: &AgentState, calls: &[ToolCall]) -> AgentUpdate {
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

#[derive(Debug, Eq, PartialEq)]
enum ToolRouteInvariant {
    NoCommittedModelRound,
    UsageRoundMismatch,
    EmptyTranscript,
    TranscriptTailNotTool,
    ContinuationAtRoundLimit,
    MaxRoundsMismatch,
    FinalAnswerAtToolRoute,
}

impl fmt::Display for ToolRouteInvariant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let classification = match self {
            Self::NoCommittedModelRound => "tool route requires a committed model round",
            Self::UsageRoundMismatch => "tool route usage alignment failed",
            Self::EmptyTranscript => "tool route requires a transcript",
            Self::TranscriptTailNotTool => "tool route requires a ToolMessage tail",
            Self::ContinuationAtRoundLimit => "tool continuation round invariant failed",
            Self::MaxRoundsMismatch => "tool max-rounds invariant failed",
            Self::FinalAnswerAtToolRoute => "tool route cannot follow a final answer",
        };
        formatter.write_str(classification)
    }
}

impl StdError for ToolRouteInvariant {}

fn route_after_model(state: &AgentState) -> Result<NodeId, RouteError> {
    let calls = state
        .pending_tool_calls()
        .ok_or_else(|| RouteError::message("model route requires a committed assistant turn"))?;
    if calls.is_empty() {
        Ok(NodeId::end())
    } else {
        Ok(NodeId::from(TOOL_NODE_ID))
    }
}

fn tools_route_target(state: &AgentState, max_rounds: usize) -> Result<NodeId, ToolRouteInvariant> {
    if state.model_rounds() == 0 {
        return Err(ToolRouteInvariant::NoCommittedModelRound);
    }
    if !state.usage_is_aligned() {
        return Err(ToolRouteInvariant::UsageRoundMismatch);
    }
    match state.messages().last() {
        None => return Err(ToolRouteInvariant::EmptyTranscript),
        Some(Message::Tool(_)) => {}
        Some(_) => return Err(ToolRouteInvariant::TranscriptTailNotTool),
    }

    match state.stop_reason() {
        Some(AgentStopReason::MaxRounds) if state.model_rounds() == max_rounds => Ok(NodeId::end()),
        None if state.model_rounds() < max_rounds => Ok(NodeId::from(MODEL_NODE_ID)),
        None => Err(ToolRouteInvariant::ContinuationAtRoundLimit),
        Some(AgentStopReason::MaxRounds) => Err(ToolRouteInvariant::MaxRoundsMismatch),
        Some(AgentStopReason::FinalAnswer) => Err(ToolRouteInvariant::FinalAnswerAtToolRoute),
    }
}

fn route_after_tools(state: &AgentState, max_rounds: usize) -> Result<NodeId, RouteError> {
    tools_route_target(state, max_rounds)
        .map_err(|source| RouteError::with_source("tool route invariant failed", source))
}

trait ModelGraphCompiler {
    fn compile(
        &self,
        graph: StateGraph<AgentState>,
    ) -> Result<CompiledGraph<AgentState>, AgentBuildError>;
}

struct CoreModelGraphCompiler;

impl ModelGraphCompiler for CoreModelGraphCompiler {
    fn compile(
        &self,
        graph: StateGraph<AgentState>,
    ) -> Result<CompiledGraph<AgentState>, AgentBuildError> {
        graph.compile().map_err(AgentBuildError::from)
    }
}

#[cfg(test)]
#[derive(Default)]
struct CountingModelGraphCompiler {
    observed: AtomicUsize,
}

#[cfg(test)]
impl CountingModelGraphCompiler {
    fn observed(&self) -> usize {
        self.observed.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
impl ModelGraphCompiler for CountingModelGraphCompiler {
    fn compile(
        &self,
        graph: StateGraph<AgentState>,
    ) -> Result<CompiledGraph<AgentState>, AgentBuildError> {
        self.observed.fetch_add(1, Ordering::SeqCst);
        CoreModelGraphCompiler.compile(graph)
    }
}

fn compile_agent_graph<C: ModelGraphCompiler>(
    model: ChatModel,
    tools: ToolRuntime,
    max_rounds: usize,
    tool_approval: bool,
    output: OutputContract,
    compiler: &C,
) -> Result<CompiledGraph<AgentState>, AgentBuildError> {
    let mut graph = StateGraph::new();
    graph.set_version(if tool_approval {
        AGENT_APPROVAL_GRAPH_VERSION
    } else {
        AGENT_GRAPH_VERSION
    });
    if let Some(version) = output.version(tool_approval) {
        graph.set_version(version);
    }
    graph.add_node(
        MODEL_NODE_ID,
        ModelNode {
            output,
            model,
            tools: tools.clone(),
        },
    )?;
    let tool_node = ToolNode {
        runtime: tools,
        max_rounds,
    };
    if tool_approval {
        graph.add_interruptible_node(TOOL_NODE_ID, tool_node)?;
    } else {
        graph.add_node(TOOL_NODE_ID, tool_node)?;
    }
    graph.add_edge(START, MODEL_NODE_ID);
    graph.add_conditional_edges(MODEL_NODE_ID, [END, TOOL_NODE_ID], route_after_model)?;
    graph.add_conditional_edges(TOOL_NODE_ID, [END, MODEL_NODE_ID], move |state| {
        route_after_tools(state, max_rounds)
    })?;
    compiler.compile(graph)
}

#[cfg(test)]
mod tests {
    use group_agent_model::{ToolCallId, ToolResult};

    use super::*;

    fn tool_tail() -> Message {
        Message::Tool(ToolMessage::new(
            ToolCallId::new("route-call").expect("valid ToolCall ID"),
            ToolResult::text("SECRET_TOOL_RESULT"),
        ))
    }

    fn route_state(
        tail: Option<Message>,
        model_rounds: usize,
        usage_rounds: usize,
        stop_reason: Option<AgentStopReason>,
    ) -> AgentState {
        AgentState::from_parts(
            tail.into_iter().collect(),
            model_rounds,
            vec![None; usage_rounds],
            stop_reason,
        )
    }

    fn assert_typed_route_error(
        state: &AgentState,
        max_rounds: usize,
        expected: ToolRouteInvariant,
    ) {
        let error = route_after_tools(state, max_rounds).expect_err("invalid State must not route");
        let source = error
            .source()
            .and_then(|source| source.downcast_ref::<ToolRouteInvariant>())
            .expect("RouteError retains the private typed invariant source");

        assert_eq!(source, &expected);
        for formatted in [format!("{error}"), format!("{error:?}")] {
            assert!(!formatted.contains("SECRET_"));
            assert!(!formatted.contains("route-call"));
        }
    }

    #[test]
    fn tools_router_accepts_consistent_continuation_state() {
        let state = route_state(Some(tool_tail()), 1, 1, None);

        let target = route_after_tools(&state, 2).expect("consistent State continues");

        assert_eq!(target, NodeId::from(MODEL_NODE_ID));
    }

    #[test]
    fn tools_router_accepts_consistent_max_rounds_state() {
        let state = route_state(Some(tool_tail()), 2, 2, Some(AgentStopReason::MaxRounds));

        let target = route_after_tools(&state, 2).expect("consistent limit State ends");

        assert_eq!(target, NodeId::end());
    }

    #[test]
    fn tools_router_rejects_inconsistent_private_states_with_typed_errors() {
        assert_typed_route_error(
            &route_state(Some(tool_tail()), 0, 0, None),
            2,
            ToolRouteInvariant::NoCommittedModelRound,
        );
        assert_typed_route_error(
            &route_state(Some(tool_tail()), 2, 1, None),
            3,
            ToolRouteInvariant::UsageRoundMismatch,
        );
        assert_typed_route_error(
            &route_state(Some(tool_tail()), 1, 2, None),
            2,
            ToolRouteInvariant::UsageRoundMismatch,
        );
        assert_typed_route_error(
            &route_state(Some(tool_tail()), 2, 1, Some(AgentStopReason::MaxRounds)),
            2,
            ToolRouteInvariant::UsageRoundMismatch,
        );
        assert_typed_route_error(
            &route_state(None, 1, 1, None),
            2,
            ToolRouteInvariant::EmptyTranscript,
        );
        assert_typed_route_error(
            &route_state(Some(Message::assistant("SECRET_ASSISTANT")), 1, 1, None),
            2,
            ToolRouteInvariant::TranscriptTailNotTool,
        );
        assert_typed_route_error(
            &route_state(Some(Message::user("SECRET_USER")), 1, 1, None),
            2,
            ToolRouteInvariant::TranscriptTailNotTool,
        );
        assert_typed_route_error(
            &route_state(Some(tool_tail()), 2, 2, None),
            2,
            ToolRouteInvariant::ContinuationAtRoundLimit,
        );
        assert_typed_route_error(
            &route_state(Some(tool_tail()), 1, 1, Some(AgentStopReason::MaxRounds)),
            2,
            ToolRouteInvariant::MaxRoundsMismatch,
        );
        assert_typed_route_error(
            &route_state(Some(tool_tail()), 1, 1, Some(AgentStopReason::FinalAnswer)),
            2,
            ToolRouteInvariant::FinalAnswerAtToolRoute,
        );
    }
}
