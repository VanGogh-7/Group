use std::error::Error as StdError;
use std::fmt;
use std::future::{Future, pending, poll_fn};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    END, EventConfig, GraphRunError, GraphState, Node, NodeContext, NodeError, RunConfig,
    RunControl, START, StateError, StateGraph,
};
use group_agent_model::{Extensions, Message, ToolCall, ToolCallId, ToolDefinition, ToolName};
use group_agent_tool::{
    IdempotencyKey, SchemaViolation, Tool, ToolBatchConfig, ToolBatchError, ToolBatchFailurePolicy,
    ToolBatchItem, ToolBehavior, ToolDefinitionError, ToolError, ToolErrorKind, ToolEvent,
    ToolExecutionOptions, ToolInput, ToolObserverError, ToolObserverFailureKind, ToolOutput,
    ToolRegistry, ToolRegistryError, ToolRuntime, ToolRuntimeErrorKind,
};
use serde_json::{Value, json};
use tokio::sync::{Barrier, Notify, Semaphore};
use tokio_util::sync::CancellationToken;

fn tool_name(value: &str) -> ToolName {
    ToolName::new(value).expect("test tool name is non-empty")
}

fn call_id(value: &str) -> ToolCallId {
    ToolCallId::new(value).expect("test call id is non-empty")
}

fn definition(name: &str, schema: Value) -> ToolDefinition {
    ToolDefinition::new(tool_name(name), format!("{name} test tool"), schema)
}

fn object_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "value": {"type": "integer"}
        },
        "required": ["value"],
        "additionalProperties": false
    })
}

fn call(id: &str, name: &str, arguments: Value) -> ToolCall {
    ToolCall::new(call_id(id), tool_name(name), arguments)
}

fn result_text(result: &group_agent_tool::ToolResult) -> String {
    result
        .content()
        .iter()
        .filter_map(|part| part.as_text())
        .collect()
}

#[derive(Clone)]
enum ImmediateOutcome {
    Success(&'static str),
    BusinessError(&'static str),
    InfrastructureError,
    Cancelled,
}

struct ImmediateTool {
    definition: ToolDefinition,
    advertised_name: ToolName,
    behavior: ToolBehavior,
    calls: Arc<AtomicUsize>,
    outcome: ImmediateOutcome,
}

impl ImmediateTool {
    fn new(name: &str, behavior: ToolBehavior, outcome: ImmediateOutcome) -> Self {
        Self {
            definition: definition(name, object_schema()),
            advertised_name: tool_name(name),
            behavior,
            calls: Arc::new(AtomicUsize::new(0)),
            outcome,
        }
    }

    fn with_schema(name: &str, schema: Value) -> Self {
        Self {
            definition: definition(name, schema),
            advertised_name: tool_name(name),
            behavior: ToolBehavior::read_only(),
            calls: Arc::new(AtomicUsize::new(0)),
            outcome: ImmediateOutcome::Success("ok"),
        }
    }
}

#[async_trait]
impl Tool for ImmediateTool {
    fn name(&self) -> &ToolName {
        &self.advertised_name
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        self.behavior
    }

    async fn execute(&self, _input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.outcome {
            ImmediateOutcome::Success(text) => Ok(ToolOutput::success_text(text)),
            ImmediateOutcome::BusinessError(text) => Ok(ToolOutput::business_error_text(text)),
            ImmediateOutcome::InfrastructureError => Err(ToolError::with_source(
                ToolErrorKind::Other,
                "SECRET_TOOL_MESSAGE",
                SecretSource,
            )),
            ImmediateOutcome::Cancelled => Err(ToolError::with_source(
                ToolErrorKind::Cancelled,
                "SECRET_CANCELLATION_MESSAGE",
                SecretSource,
            )),
        }
    }
}

#[derive(Debug)]
struct SecretSource;

impl fmt::Display for SecretSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SECRET_SOURCE_VALUE")
    }
}

impl StdError for SecretSource {}

struct DropGuard {
    dropped: Arc<AtomicUsize>,
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingTool {
    definition: ToolDefinition,
    behavior: ToolBehavior,
    started: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for PendingTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        self.behavior
    }

    async fn execute(&self, _input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        let _guard = DropGuard {
            dropped: Arc::clone(&self.dropped),
        };
        self.started.fetch_add(1, Ordering::SeqCst);
        pending().await
    }
}

struct ActiveGuard {
    active: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct GateTool {
    definition: ToolDefinition,
    behavior: ToolBehavior,
    gate: Arc<Semaphore>,
    entered: Arc<Notify>,
    started: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for GateTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        self.behavior
    }

    async fn execute(&self, _input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let _guard = ActiveGuard {
            active: Arc::clone(&self.active),
            dropped: Arc::clone(&self.dropped),
        };
        self.started.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_waiters();
        let permit = Arc::clone(&self.gate)
            .acquire_owned()
            .await
            .expect("test semaphore remains open");
        permit.forget();
        Ok(ToolOutput::success_text("released"))
    }
}

struct BarrierTool {
    definition: ToolDefinition,
    barrier: Arc<Barrier>,
}

#[async_trait]
impl Tool for BarrierTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    async fn execute(&self, _input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        self.barrier.wait().await;
        Ok(ToolOutput::success_text("parallel"))
    }
}

struct FailFastDrainTool {
    definition: ToolDefinition,
    barrier: Arc<Barrier>,
    gate: Arc<Semaphore>,
    entered: Arc<Notify>,
    started: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for FailFastDrainTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    async fn execute(&self, input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        let mode = input
            .arguments()
            .get("mode")
            .and_then(Value::as_str)
            .expect("validated fail-fast mode");
        self.started.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_waiters();
        self.barrier.wait().await;

        if mode == "fail" {
            return Err(ToolError::with_source(
                ToolErrorKind::Other,
                "SECRET_FAIL_FAST_MESSAGE",
                SecretSource,
            ));
        }

        let permit = Arc::clone(&self.gate)
            .acquire_owned()
            .await
            .expect("test semaphore remains open");
        permit.forget();
        Ok(ToolOutput::success_text("drained sibling"))
    }
}

struct OrderedTool {
    definition: ToolDefinition,
    releases: Vec<Arc<Notify>>,
    started: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    completion_order: Arc<Mutex<Vec<usize>>>,
    completed: Arc<Notify>,
}

#[async_trait]
impl Tool for OrderedTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    async fn execute(&self, input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        let slot = input
            .arguments()
            .get("slot")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .expect("validated test slot");
        self.started.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_waiters();
        self.releases[slot].notified().await;
        self.completion_order
            .lock()
            .expect("completion order mutex")
            .push(slot);
        self.completed.notify_waiters();
        Ok(ToolOutput::success_text(slot.to_string()))
    }
}

struct InspectTool {
    definition: ToolDefinition,
    input_debug: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl Tool for InspectTool {
    fn name(&self) -> &ToolName {
        self.definition.name()
    }

    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::read_only()
    }

    async fn execute(&self, input: ToolInput<'_>) -> Result<ToolOutput, ToolError> {
        *self.input_debug.lock().expect("input debug mutex") = Some(format!("{input:?}"));
        Ok(ToolOutput::success_text("SECRET_OUTPUT_VALUE"))
    }
}

fn runtime_with<T>(tool: T) -> ToolRuntime
where
    T: Tool + 'static,
{
    let mut builder = ToolRegistry::builder();
    builder.register(tool).expect("test tool registers");
    ToolRuntime::new(builder.build())
}

struct GateFixture {
    runtime: ToolRuntime,
    gate: Arc<Semaphore>,
    entered: Arc<Notify>,
    started: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

fn gate_runtime(behavior: ToolBehavior) -> GateFixture {
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Notify::new());
    let started = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with(GateTool {
        definition: definition("gate", object_schema()),
        behavior,
        gate: Arc::clone(&gate),
        entered: Arc::clone(&entered),
        started: Arc::clone(&started),
        active,
        peak: Arc::clone(&peak),
        dropped: Arc::clone(&dropped),
    });
    GateFixture {
        runtime,
        gate,
        entered,
        started,
        peak,
        dropped,
    }
}

async fn drive_until_count<F>(
    future: &mut std::pin::Pin<Box<F>>,
    counter: &AtomicUsize,
    target: usize,
    notification: &Notify,
) where
    F: Future,
{
    while counter.load(Ordering::SeqCst) < target {
        tokio::select! {
            () = notification.notified() => {}
            _ = future.as_mut() => panic!("future completed before reaching count {target}"),
        }
    }
}

#[test]
fn registry_registers_and_orders_definitions_deterministically() {
    let mut builder = ToolRegistry::builder();
    builder
        .register(ImmediateTool::new(
            "zeta",
            ToolBehavior::read_only(),
            ImmediateOutcome::Success("z"),
        ))
        .expect("zeta registers")
        .register(ImmediateTool::new(
            "alpha",
            ToolBehavior::read_only(),
            ImmediateOutcome::Success("a"),
        ))
        .expect("alpha registers");
    let registry = builder.build();

    let names = registry
        .definitions()
        .map(|definition| definition.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["alpha", "zeta"]);
    assert_eq!(registry.len(), 2);
    assert_eq!(registry.compiled_schema_count(), 2);
    assert!(registry.get(&tool_name("alpha")).is_some());
}

#[test]
fn registry_rejects_duplicate_name_before_recompiling_schema() {
    let mut builder = ToolRegistry::builder();
    assert_eq!(builder.schema_compilation_count(), 0);
    builder
        .register(ImmediateTool::new(
            "duplicate",
            ToolBehavior::read_only(),
            ImmediateOutcome::Success("first"),
        ))
        .expect("first registration succeeds");
    assert_eq!(builder.schema_compilation_count(), 1);
    let error = builder
        .register(ImmediateTool::new(
            "duplicate",
            ToolBehavior::read_only(),
            ImmediateOutcome::Success("second"),
        ))
        .expect_err("duplicate registration fails");

    assert_eq!(error.kind(), ToolRuntimeErrorKind::DuplicateTool);
    assert!(matches!(error, ToolRegistryError::DuplicateTool { .. }));
    assert_eq!(builder.schema_compilation_count(), 1);
}

#[test]
fn registry_rejects_invalid_definition_shapes() {
    let mut mismatched = ImmediateTool::new(
        "defined",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("ok"),
    );
    mismatched.advertised_name = tool_name("advertised");
    let mut builder = ToolRegistry::builder();
    let mismatch = builder
        .register(mismatched)
        .expect_err("name mismatch fails");
    assert!(matches!(
        mismatch,
        ToolRegistryError::InvalidDefinition {
            source: ToolDefinitionError::NameMismatch { .. },
            ..
        }
    ));

    let empty_description = ImmediateTool {
        definition: ToolDefinition::new(tool_name("empty_description"), "   ", object_schema()),
        advertised_name: tool_name("empty_description"),
        behavior: ToolBehavior::read_only(),
        calls: Arc::new(AtomicUsize::new(0)),
        outcome: ImmediateOutcome::Success("ok"),
    };
    let mut builder = ToolRegistry::builder();
    assert!(matches!(
        builder.register(empty_description),
        Err(ToolRegistryError::InvalidDefinition {
            source: ToolDefinitionError::EmptyDescription,
            ..
        })
    ));

    let invalid_name = ImmediateTool {
        definition: ToolDefinition::new(tool_name(" invalid "), "invalid name", object_schema()),
        advertised_name: tool_name(" invalid "),
        behavior: ToolBehavior::read_only(),
        calls: Arc::new(AtomicUsize::new(0)),
        outcome: ImmediateOutcome::Success("ok"),
    };
    let mut builder = ToolRegistry::builder();
    assert!(matches!(
        builder.register(invalid_name),
        Err(ToolRegistryError::InvalidDefinition {
            source: ToolDefinitionError::InvalidName,
            ..
        })
    ));
}

#[test]
fn registry_rejects_invalid_json_schema_with_redacted_location() {
    let tool = ImmediateTool::with_schema("invalid_schema", json!({"type": 42}));
    let mut builder = ToolRegistry::builder();
    let error = builder.register(tool).expect_err("invalid schema fails");
    assert!(!format!("{error}").contains("42"));
    assert!(!format!("{error:?}").contains("42"));

    let definition_error = error.source().expect("definition source");
    assert!(definition_error.is::<ToolDefinitionError>());
    let schema_error = definition_error.source().expect("jsonschema source");
    assert!(schema_error.is::<jsonschema::ValidationError<'static>>());

    let ToolRegistryError::InvalidDefinition {
        source: ToolDefinitionError::InvalidSchema {
            violation, source, ..
        },
        ..
    } = error
    else {
        panic!("unexpected registry error");
    };
    assert!(!violation.keyword().is_empty());
    assert!(!violation.schema_path().is_empty());
    assert!(!format!("{violation:?}").contains("42"));
    assert!(!format!("{source:?}").is_empty());
    assert_eq!(builder.schema_compilation_count(), 1);
}

#[test]
fn schema_compiler_instrumentation_counts_real_attempts_not_entries() {
    let mut builder = ToolRegistry::builder();
    builder
        .register(ImmediateTool::new(
            "compiled_once",
            ToolBehavior::read_only(),
            ImmediateOutcome::Success("ok"),
        ))
        .expect("valid schema compiles");
    assert_eq!(builder.schema_compilation_count(), 1);

    let duplicate = builder.register(ImmediateTool::new(
        "compiled_once",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("duplicate"),
    ));
    assert!(matches!(
        duplicate,
        Err(ToolRegistryError::DuplicateTool { .. })
    ));
    assert_eq!(builder.schema_compilation_count(), 1);

    let invalid = builder.register(ImmediateTool::with_schema(
        "invalid_compilation",
        json!({"type": "SECRET_INVALID_SCHEMA_SENTINEL"}),
    ));
    assert!(invalid.is_err());
    assert_eq!(builder.schema_compilation_count(), 2);

    let registry = builder.build();
    assert_eq!(registry.len(), 1);
    assert_eq!(registry.schema_compilation_count(), 2);
}

#[test]
fn registry_rejects_inconsistent_behavior() {
    let read_only = ImmediateTool::new(
        "read_only_key",
        ToolBehavior::read_only().with_required_idempotency_key(true),
        ImmediateOutcome::Success("ok"),
    );
    let mut builder = ToolRegistry::builder();
    assert!(matches!(
        builder.register(read_only),
        Err(ToolRegistryError::InvalidDefinition {
            source: ToolDefinitionError::InvalidBehavior { .. },
            ..
        })
    ));

    let non_idempotent = ImmediateTool::new(
        "non_idempotent_key",
        ToolBehavior::non_idempotent_write().with_required_idempotency_key(true),
        ImmediateOutcome::Success("ok"),
    );
    let mut builder = ToolRegistry::builder();
    assert!(matches!(
        builder.register(non_idempotent),
        Err(ToolRegistryError::InvalidDefinition {
            source: ToolDefinitionError::InvalidBehavior { .. },
            ..
        })
    ));
}

#[tokio::test]
async fn schema_is_compiled_once_and_reused_by_execution() {
    let runtime = runtime_with(ImmediateTool::new(
        "cached",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("ok"),
    ));
    assert_eq!(runtime.registry().compiled_schema_count(), 1);

    for index in 0..3 {
        let result = runtime
            .execute(&call(
                &format!("call-{index}"),
                "cached",
                json!({"value": index}),
            ))
            .await
            .expect("cached schema accepts arguments");
        assert_eq!(result_text(&result), "ok");
    }
    assert_eq!(runtime.registry().compiled_schema_count(), 1);
}

#[tokio::test]
async fn single_execution_returns_success_and_business_error_distinctly() {
    let success = runtime_with(ImmediateTool::new(
        "success",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("done"),
    ));
    let result = success
        .execute(&call("success-call", "success", json!({"value": 1})))
        .await
        .expect("success result");
    assert!(!result.is_error());
    assert_eq!(result_text(&result), "done");

    let business_error = runtime_with(ImmediateTool::new(
        "business",
        ToolBehavior::read_only(),
        ImmediateOutcome::BusinessError("expected business rejection"),
    ));
    let result = business_error
        .execute(&call("business-call", "business", json!({"value": 1})))
        .await
        .expect("business error remains a ToolResult");
    assert!(result.is_error());
    assert_eq!(result_text(&result), "expected business rejection");
}

#[tokio::test]
async fn tool_not_found_and_invalid_call_are_structured() {
    let runtime = ToolRuntime::new(ToolRegistry::empty());
    let missing = runtime
        .execute(&call("missing-call", "missing", json!({})))
        .await
        .expect_err("missing tool fails");
    assert_eq!(missing.kind(), ToolRuntimeErrorKind::ToolNotFound);

    let invalid = runtime
        .execute(&call(" invalid-call ", "missing", json!({})))
        .await
        .expect_err("non-canonical call id fails");
    assert_eq!(invalid.kind(), ToolRuntimeErrorKind::InvalidToolCall);
}

#[tokio::test]
async fn invalid_arguments_never_enter_tool() {
    let tool = ImmediateTool::new(
        "validated",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("ok"),
    );
    let calls = Arc::clone(&tool.calls);
    let runtime = runtime_with(tool);
    let error = runtime
        .execute(&call(
            "invalid-arguments",
            "validated",
            json!({"value": "SECRET_INVALID_ARGUMENT_SENTINEL"}),
        ))
        .await
        .expect_err("schema rejects argument");

    assert_eq!(error.kind(), ToolRuntimeErrorKind::InvalidArguments);
    let violation = error.schema_violation().expect("schema location");
    assert_eq!(violation.instance_path(), "/value");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!format!("{error}").contains("SECRET_INVALID_ARGUMENT_SENTINEL"));
    assert!(!format!("{error:?}").contains("SECRET_INVALID_ARGUMENT_SENTINEL"));
    assert!(
        error
            .source()
            .expect("owned jsonschema source")
            .is::<jsonschema::ValidationError<'static>>()
    );
}

#[tokio::test]
async fn infrastructure_error_preserves_source_chain_without_default_secret_output() {
    let runtime = runtime_with(ImmediateTool::new(
        "failing",
        ToolBehavior::read_only(),
        ImmediateOutcome::InfrastructureError,
    ));
    let error = runtime
        .execute(&call("failing-call", "failing", json!({"value": 1})))
        .await
        .expect_err("infrastructure failure");

    assert_eq!(error.kind(), ToolRuntimeErrorKind::ExecutionFailed);
    assert!(!format!("{error}").contains("SECRET"));
    assert!(!format!("{error:?}").contains("SECRET"));
    let tool_error = error.source().expect("tool error source");
    assert!(tool_error.is::<ToolError>());
    let root = tool_error.source().expect("root source");
    assert!(root.is::<SecretSource>());
}

#[tokio::test]
async fn tool_reported_cancellation_is_structured_and_preserves_its_source() {
    let runtime = runtime_with(ImmediateTool::new(
        "cancelled",
        ToolBehavior::read_only(),
        ImmediateOutcome::Cancelled,
    ));
    let error = runtime
        .execute(&call("cancelled-call", "cancelled", json!({"value": 1})))
        .await
        .expect_err("tool reports cancellation");

    assert_eq!(error.kind(), ToolRuntimeErrorKind::Cancelled);
    let tool_error = error.source().expect("tool cancellation source");
    assert!(tool_error.is::<ToolError>());
    assert!(
        tool_error
            .source()
            .expect("root cancellation source")
            .is::<SecretSource>()
    );
    assert!(!format!("{error:?}").contains("SECRET"));
}

#[tokio::test(start_paused = true)]
async fn timeout_drops_tool_future_and_retains_safe_context() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with(PendingTool {
        definition: definition("timeout", object_schema()),
        behavior: ToolBehavior::read_only(),
        started: Arc::new(AtomicUsize::new(0)),
        dropped: Arc::clone(&dropped),
    });
    let error = runtime
        .execute_with_options(
            &call("timeout-call", "timeout", json!({"value": 1})),
            ToolExecutionOptions::new().with_timeout(Duration::from_secs(5)),
        )
        .await
        .expect_err("timeout expires");

    assert_eq!(error.kind(), ToolRuntimeErrorKind::TimedOut);
    assert_eq!(error.timeout(), Some(Duration::from_secs(5)));
    assert_eq!(error.context().call_id().as_str(), "timeout-call");
    assert_eq!(error.context().tool_name().as_str(), "timeout");
    assert!(error.source().is_some());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropping_single_execution_drops_tool_future() {
    let started = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with(PendingTool {
        definition: definition("pending", object_schema()),
        behavior: ToolBehavior::read_only(),
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
    });
    let call = call("pending-call", "pending", json!({"value": 1}));
    let mut execution = Box::pin(runtime.execute(&call));

    poll_fn(|context| match execution.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("pending tool unexpectedly completed"),
    })
    .await;
    assert_eq!(started.load(Ordering::SeqCst), 1);
    drop(execution);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn idempotency_and_side_effect_policies_are_enforced() {
    let requires_key_tool = ImmediateTool::new(
        "write",
        ToolBehavior::idempotent_write().with_required_idempotency_key(true),
        ImmediateOutcome::Success("written"),
    );
    let requires_key_calls = Arc::clone(&requires_key_tool.calls);
    let requires_key = runtime_with(requires_key_tool);
    let write_call = call("write-call", "write", json!({"value": 1}));
    let missing = requires_key
        .execute(&write_call)
        .await
        .expect_err("missing key fails");
    assert_eq!(missing.kind(), ToolRuntimeErrorKind::MissingIdempotencyKey);
    assert_eq!(requires_key_calls.load(Ordering::SeqCst), 0);
    let result = requires_key
        .execute_with_options(
            &write_call,
            ToolExecutionOptions::new()
                .with_idempotency_key(IdempotencyKey::new("operation-1").expect("valid key")),
        )
        .await
        .expect("key allows execution");
    assert_eq!(result_text(&result), "written");
    assert_eq!(requires_key_calls.load(Ordering::SeqCst), 1);

    let non_idempotent = runtime_with(ImmediateTool::new(
        "charge",
        ToolBehavior::non_idempotent_write(),
        ImmediateOutcome::Success("charged"),
    ));
    let denied = non_idempotent
        .execute_with_options(
            &call("charge-call", "charge", json!({"value": 1})),
            ToolExecutionOptions::new().with_non_idempotent_writes(false),
        )
        .await
        .expect_err("caller policy rejects side effect");
    assert_eq!(denied.kind(), ToolRuntimeErrorKind::UnsupportedExecution);
}

#[tokio::test]
async fn batch_respects_max_concurrency_and_collects_all() {
    let fixture = gate_runtime(ToolBehavior::read_only());
    let calls = (0..4)
        .map(|index| call(&format!("gate-{index}"), "gate", json!({"value": index})))
        .collect();
    let mut batch = Box::pin(
        fixture
            .runtime
            .execute_batch(calls, ToolBatchConfig::new(2)),
    );

    drive_until_count(&mut batch, &fixture.started, 2, &fixture.entered).await;
    assert_eq!(fixture.peak.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.started.load(Ordering::SeqCst), 2);
    fixture.gate.add_permits(2);

    drive_until_count(&mut batch, &fixture.started, 4, &fixture.entered).await;
    assert_eq!(fixture.peak.load(Ordering::SeqCst), 2);
    fixture.gate.add_permits(2);
    let report = batch.await.expect("batch completes");
    assert_eq!(report.len(), 4);
    assert!(report.results().iter().all(Result::is_ok));
    assert_eq!(fixture.dropped.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn batch_results_remain_in_input_order_when_completion_order_differs() {
    let releases = vec![Arc::new(Notify::new()), Arc::new(Notify::new())];
    let started = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let completion_order = Arc::new(Mutex::new(Vec::new()));
    let completed = Arc::new(Notify::new());
    let runtime = runtime_with(OrderedTool {
        definition: definition(
            "ordered",
            json!({
                "type": "object",
                "properties": {"slot": {"type": "integer", "minimum": 0, "maximum": 1}},
                "required": ["slot"],
                "additionalProperties": false
            }),
        ),
        releases: releases.clone(),
        started: Arc::clone(&started),
        entered: Arc::clone(&entered),
        completion_order: Arc::clone(&completion_order),
        completed: Arc::clone(&completed),
    });
    let calls = vec![
        call("ordered-0", "ordered", json!({"slot": 0})),
        call("ordered-1", "ordered", json!({"slot": 1})),
    ];
    let mut batch = Box::pin(runtime.execute_batch(calls, ToolBatchConfig::new(2)));
    drive_until_count(&mut batch, &started, 2, &entered).await;

    releases[1].notify_one();
    while completion_order
        .lock()
        .expect("completion order mutex")
        .is_empty()
    {
        tokio::select! {
            () = completed.notified() => {}
            _ = batch.as_mut() => panic!("batch completed before slot zero was released"),
        }
    }
    releases[0].notify_one();
    let report = batch.await.expect("batch completes");

    assert_eq!(
        *completion_order.lock().expect("completion order mutex"),
        [1, 0]
    );
    let ordered_results = report
        .results()
        .iter()
        .map(|result| result_text(result.as_ref().expect("successful result")))
        .collect::<Vec<_>>();
    assert_eq!(ordered_results, ["0", "1"]);
}

#[tokio::test]
async fn duplicate_batch_call_ids_fail_before_any_tool_runs() {
    let tool = ImmediateTool::new(
        "duplicate_call",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("ok"),
    );
    let calls = Arc::clone(&tool.calls);
    let runtime = runtime_with(tool);
    let error = runtime
        .execute_batch(
            vec![
                call("same-id", "duplicate_call", json!({"value": 1})),
                call("same-id", "duplicate_call", json!({"value": 2})),
            ],
            ToolBatchConfig::new(2),
        )
        .await
        .expect_err("duplicate id fails batch");

    assert!(matches!(
        error,
        ToolBatchError::DuplicateToolCallId {
            first_index: 0,
            duplicate_index: 1,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn collect_all_keeps_independent_runtime_failures() {
    let mut builder = ToolRegistry::builder();
    builder
        .register(ImmediateTool::new(
            "good",
            ToolBehavior::read_only(),
            ImmediateOutcome::Success("good"),
        ))
        .expect("good registers")
        .register(ImmediateTool::new(
            "bad",
            ToolBehavior::read_only(),
            ImmediateOutcome::InfrastructureError,
        ))
        .expect("bad registers");
    let runtime = ToolRuntime::new(builder.build());
    let report = runtime
        .execute_batch(
            vec![
                call("good-call", "good", json!({"value": 1})),
                call("bad-call", "bad", json!({"value": 1})),
                call("missing-call", "missing", json!({})),
            ],
            ToolBatchConfig::new(3),
        )
        .await
        .expect("batch structure is valid");

    assert!(report.results()[0].is_ok());
    assert_eq!(
        report.results()[1].as_ref().expect_err("bad fails").kind(),
        ToolRuntimeErrorKind::ExecutionFailed
    );
    assert_eq!(
        report.results()[2]
            .as_ref()
            .expect_err("missing fails")
            .kind(),
        ToolRuntimeErrorKind::ToolNotFound
    );
}

#[tokio::test]
async fn fail_fast_drains_started_calls_and_marks_only_unscheduled_calls_not_started() {
    let succeeds = ImmediateTool::new(
        "started_success",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("started"),
    );
    let success_calls = Arc::clone(&succeeds.calls);
    let fails = ImmediateTool::new(
        "started_failure",
        ToolBehavior::read_only(),
        ImmediateOutcome::InfrastructureError,
    );
    let failure_calls = Arc::clone(&fails.calls);
    let mut builder = ToolRegistry::builder();
    builder.register(fails).expect("failing registers");
    builder.register(succeeds).expect("success registers");
    let runtime = ToolRuntime::new(builder.build());
    let report = runtime
        .execute_batch(
            vec![
                call("fail-fast", "started_failure", json!({"value": 1})),
                call("started-success", "started_success", json!({"value": 2})),
                call("never-started", "started_success", json!({"value": 3})),
            ],
            ToolBatchConfig::new(2).with_failure_policy(ToolBatchFailurePolicy::FailFast),
        )
        .await
        .expect("batch structure is valid");

    assert_eq!(failure_calls.load(Ordering::SeqCst), 1);
    assert_eq!(success_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        report.results()[0]
            .as_ref()
            .expect_err("trigger fails")
            .kind(),
        ToolRuntimeErrorKind::ExecutionFailed
    );
    assert_eq!(
        result_text(report.results()[1].as_ref().expect("started call succeeds")),
        "started"
    );
    assert_eq!(
        report.results()[2]
            .as_ref()
            .expect_err("queued call was never started")
            .kind(),
        ToolRuntimeErrorKind::NotStartedDueToFailFast
    );
}

#[tokio::test]
async fn fail_fast_waits_for_a_gated_started_sibling_before_finalizing_the_ordered_report() {
    let barrier = Arc::new(Barrier::new(2));
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Notify::new());
    let started = Arc::new(AtomicUsize::new(0));
    let never = ImmediateTool::new(
        "never_scheduled_after_failure",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("must not run"),
    );
    let never_calls = Arc::clone(&never.calls);
    let mut builder = ToolRegistry::builder();
    builder
        .register(FailFastDrainTool {
            definition: definition(
                "fail_fast_drain",
                json!({
                    "type": "object",
                    "properties": {
                        "mode": {"type": "string", "enum": ["sibling", "fail"]}
                    },
                    "required": ["mode"],
                    "additionalProperties": false
                }),
            ),
            barrier,
            gate: Arc::clone(&gate),
            entered: Arc::clone(&entered),
            started: Arc::clone(&started),
        })
        .expect("drain tool registers")
        .register(never)
        .expect("never-scheduled tool registers");

    let failure_events = Arc::new(AtomicUsize::new(0));
    let failure_observed = Arc::new(Notify::new());
    let observed_count = Arc::clone(&failure_events);
    let observed_notify = Arc::clone(&failure_observed);
    let runtime =
        ToolRuntime::new(builder.build()).with_event_sink(Arc::new(move |event: &ToolEvent| {
            if matches!(
                event,
                ToolEvent::ExecutionFailed {
                    kind: ToolRuntimeErrorKind::ExecutionFailed,
                    ..
                }
            ) {
                observed_count.fetch_add(1, Ordering::SeqCst);
                observed_notify.notify_waiters();
            }
            Ok(())
        }));
    let mut batch = Box::pin(runtime.execute_batch(
        vec![
            call(
                "gated-sibling",
                "fail_fast_drain",
                json!({"mode": "sibling"}),
            ),
            call(
                "observed-failure",
                "fail_fast_drain",
                json!({"mode": "fail"}),
            ),
            call(
                "never-scheduled",
                "never_scheduled_after_failure",
                json!({"value": 3}),
            ),
        ],
        ToolBatchConfig::new(2).with_failure_policy(ToolBatchFailurePolicy::FailFast),
    ));

    drive_until_count(&mut batch, &started, 2, &entered).await;
    while failure_events.load(Ordering::SeqCst) == 0 {
        tokio::select! {
            () = failure_observed.notified() => {}
            _ = batch.as_mut() => panic!("batch finalized while a started sibling was gated"),
        }
    }
    poll_fn(|context| match batch.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("batch finalized before the gated sibling completed"),
    })
    .await;
    assert_eq!(started.load(Ordering::SeqCst), 2);
    assert_eq!(never_calls.load(Ordering::SeqCst), 0);

    gate.add_permits(1);
    let report = batch.await.expect("drained fail-fast batch completes");
    assert_eq!(report.len(), 3);
    assert_eq!(
        result_text(
            report.results()[0]
                .as_ref()
                .expect("started sibling keeps its real success"),
        ),
        "drained sibling"
    );
    assert_eq!(
        report.results()[1]
            .as_ref()
            .expect_err("observed trigger keeps its real error")
            .kind(),
        ToolRuntimeErrorKind::ExecutionFailed
    );
    assert_eq!(
        report.results()[2]
            .as_ref()
            .expect_err("unscheduled call has an explicit state")
            .kind(),
        ToolRuntimeErrorKind::NotStartedDueToFailFast
    );
    assert_ne!(
        report.results()[0]
            .as_ref()
            .map_err(group_agent_tool::ToolRuntimeError::kind),
        Err(ToolRuntimeErrorKind::Cancelled)
    );
}

#[tokio::test]
async fn fail_fast_preserves_each_started_failure_and_input_order() {
    let first = ImmediateTool::new(
        "first_failure",
        ToolBehavior::read_only(),
        ImmediateOutcome::InfrastructureError,
    );
    let first_calls = Arc::clone(&first.calls);
    let second = ImmediateTool::new(
        "second_failure",
        ToolBehavior::read_only(),
        ImmediateOutcome::Cancelled,
    );
    let second_calls = Arc::clone(&second.calls);
    let never = ImmediateTool::new(
        "never_after_fail_fast",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("never"),
    );
    let never_calls = Arc::clone(&never.calls);
    let mut builder = ToolRegistry::builder();
    builder
        .register(first)
        .expect("first registers")
        .register(second)
        .expect("second registers")
        .register(never)
        .expect("never registers");
    let runtime = ToolRuntime::new(builder.build());

    let report = runtime
        .execute_batch(
            vec![
                call("first", "first_failure", json!({"value": 1})),
                call("second", "second_failure", json!({"value": 2})),
                call("third", "never_after_fail_fast", json!({"value": 3})),
            ],
            ToolBatchConfig::new(2).with_failure_policy(ToolBatchFailurePolicy::FailFast),
        )
        .await
        .expect("batch structure is valid");

    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(never_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        report.results()[0]
            .as_ref()
            .expect_err("first failure")
            .kind(),
        ToolRuntimeErrorKind::ExecutionFailed
    );
    assert_eq!(
        report.results()[1]
            .as_ref()
            .expect_err("second failure")
            .kind(),
        ToolRuntimeErrorKind::Cancelled
    );
    assert!(
        report.results()[1]
            .as_ref()
            .expect_err("second failure")
            .source()
            .expect("original ToolError")
            .is::<ToolError>()
    );
    assert_eq!(
        report.results()[2]
            .as_ref()
            .expect_err("third not started")
            .kind(),
        ToolRuntimeErrorKind::NotStartedDueToFailFast
    );
}

#[tokio::test]
async fn fail_fast_does_not_relabel_an_explicit_parallel_non_idempotent_side_effect() {
    let failure = ImmediateTool::new(
        "fail_before_side_effect_report",
        ToolBehavior::read_only(),
        ImmediateOutcome::InfrastructureError,
    );
    let write = ImmediateTool::new(
        "parallel_side_effect",
        ToolBehavior::non_idempotent_write().with_parallel(true),
        ImmediateOutcome::Success("side effect happened"),
    );
    let write_calls = Arc::clone(&write.calls);
    let mut builder = ToolRegistry::builder();
    builder
        .register(failure)
        .expect("failure registers")
        .register(write)
        .expect("write registers");
    let runtime = ToolRuntime::new(builder.build());

    let report = runtime
        .execute_batch(
            vec![
                call(
                    "failure-before-write",
                    "fail_before_side_effect_report",
                    json!({"value": 1}),
                ),
                call(
                    "side-effect-call",
                    "parallel_side_effect",
                    json!({"value": 2}),
                ),
            ],
            ToolBatchConfig::new(2).with_failure_policy(ToolBatchFailurePolicy::FailFast),
        )
        .await
        .expect("batch structure is valid");

    assert_eq!(write_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        result_text(
            report.results()[1]
                .as_ref()
                .expect("write outcome retained")
        ),
        "side effect happened"
    );
}

#[tokio::test]
async fn dropping_batch_drops_all_running_tool_futures() {
    let started = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with(PendingTool {
        definition: definition("pending_batch", object_schema()),
        behavior: ToolBehavior::read_only(),
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
    });
    let mut batch = Box::pin(runtime.execute_batch(
        vec![
            call("pending-batch-0", "pending_batch", json!({"value": 0})),
            call("pending-batch-1", "pending_batch", json!({"value": 1})),
        ],
        ToolBatchConfig::new(2),
    ));

    poll_fn(|context| match batch.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("pending batch unexpectedly completed"),
    })
    .await;
    assert_eq!(started.load(Ordering::SeqCst), 2);
    drop(batch);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn non_idempotent_writes_are_sequential_unless_explicitly_parallel() {
    let fixture = gate_runtime(ToolBehavior::non_idempotent_write());
    let mut batch = Box::pin(fixture.runtime.execute_batch(
        vec![
            call("write-0", "gate", json!({"value": 0})),
            call("write-1", "gate", json!({"value": 1})),
        ],
        ToolBatchConfig::new(2),
    ));

    drive_until_count(&mut batch, &fixture.started, 1, &fixture.entered).await;
    assert_eq!(fixture.started.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.peak.load(Ordering::SeqCst), 1);
    fixture.gate.add_permits(1);
    drive_until_count(&mut batch, &fixture.started, 2, &fixture.entered).await;
    assert_eq!(fixture.peak.load(Ordering::SeqCst), 1);
    fixture.gate.add_permits(1);
    assert!(
        batch
            .await
            .expect("batch completes")
            .results()
            .iter()
            .all(Result::is_ok)
    );

    let parallel_fixture = gate_runtime(ToolBehavior::non_idempotent_write().with_parallel(true));
    let mut parallel_batch = Box::pin(parallel_fixture.runtime.execute_batch(
        vec![
            call("parallel-write-0", "gate", json!({"value": 0})),
            call("parallel-write-1", "gate", json!({"value": 1})),
        ],
        ToolBatchConfig::new(2),
    ));
    drive_until_count(
        &mut parallel_batch,
        &parallel_fixture.started,
        2,
        &parallel_fixture.entered,
    )
    .await;
    assert_eq!(parallel_fixture.peak.load(Ordering::SeqCst), 2);
    parallel_fixture.gate.add_permits(2);
    assert!(
        parallel_batch
            .await
            .expect("parallel batch completes")
            .results()
            .iter()
            .all(Result::is_ok)
    );
}

#[tokio::test]
async fn mixed_read_only_and_non_idempotent_writes_respect_exclusive_order() {
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Notify::new());
    let started = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let make_tool = |name, behavior| GateTool {
        definition: definition(name, object_schema()),
        behavior,
        gate: Arc::clone(&gate),
        entered: Arc::clone(&entered),
        started: Arc::clone(&started),
        active: Arc::clone(&active),
        peak: Arc::clone(&peak),
        dropped: Arc::clone(&dropped),
    };
    let mut builder = ToolRegistry::builder();
    builder
        .register(make_tool("mixed_read", ToolBehavior::read_only()))
        .expect("read registers")
        .register(make_tool(
            "mixed_write",
            ToolBehavior::non_idempotent_write(),
        ))
        .expect("write registers");
    let runtime = ToolRuntime::new(builder.build());
    let mut batch = Box::pin(runtime.execute_batch(
        vec![
            call("mixed-read-0", "mixed_read", json!({"value": 0})),
            call("mixed-write-1", "mixed_write", json!({"value": 1})),
            call("mixed-read-2", "mixed_read", json!({"value": 2})),
        ],
        ToolBatchConfig::new(3),
    ));

    drive_until_count(&mut batch, &started, 1, &entered).await;
    assert_eq!(started.load(Ordering::SeqCst), 1);
    gate.add_permits(1);
    drive_until_count(&mut batch, &started, 2, &entered).await;
    assert_eq!(started.load(Ordering::SeqCst), 2);
    gate.add_permits(1);
    drive_until_count(&mut batch, &started, 3, &entered).await;
    assert_eq!(started.load(Ordering::SeqCst), 3);
    gate.add_permits(1);

    let report = batch.await.expect("mixed batch completes");
    assert!(report.results().iter().all(Result::is_ok));
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn read_only_tools_can_reach_a_parallel_barrier() {
    let barrier = Arc::new(Barrier::new(3));
    let runtime = runtime_with(BarrierTool {
        definition: definition("barrier", object_schema()),
        barrier: Arc::clone(&barrier),
    });
    let batch = runtime.execute_batch(
        vec![
            call("barrier-0", "barrier", json!({"value": 0})),
            call("barrier-1", "barrier", json!({"value": 1})),
        ],
        ToolBatchConfig::new(2),
    );
    let (_, report) = tokio::join!(barrier.wait(), batch);
    assert!(
        report
            .expect("batch completes")
            .results()
            .iter()
            .all(Result::is_ok)
    );
}

#[tokio::test]
async fn invalid_batch_calls_never_enter_tools_while_valid_calls_still_run() {
    let tool = ImmediateTool::new(
        "batch_validated",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("ok"),
    );
    let calls = Arc::clone(&tool.calls);
    let runtime = runtime_with(tool);
    let report = runtime
        .execute_batch(
            vec![
                call(
                    "invalid-batch",
                    "batch_validated",
                    json!({"value": "wrong"}),
                ),
                call("valid-batch", "batch_validated", json!({"value": 1})),
            ],
            ToolBatchConfig::new(2),
        )
        .await
        .expect("batch structure is valid");

    assert_eq!(
        report.results()[0]
            .as_ref()
            .expect_err("invalid arguments fail")
            .kind(),
        ToolRuntimeErrorKind::InvalidArguments
    );
    assert!(report.results()[1].is_ok());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn debug_display_and_events_do_not_leak_payloads() {
    let input_debug = Arc::new(Mutex::new(None));
    let events = Arc::new(Mutex::new(Vec::<ToolEvent>::new()));
    let event_capture = Arc::clone(&events);
    let runtime = runtime_with(InspectTool {
        definition: definition(
            "inspect",
            json!({
                "type": "object",
                "properties": {"secret": {"type": "string"}},
                "required": ["secret"],
                "additionalProperties": false
            }),
        ),
        input_debug: Arc::clone(&input_debug),
    })
    .with_event_sink(Arc::new(move |event: &ToolEvent| {
        event_capture
            .lock()
            .expect("event capture mutex")
            .push(event.clone());
        Ok(())
    }));
    let metadata = Extensions::new()
        .with("metadata-key", json!("SECRET_METADATA_VALUE"))
        .expect("metadata");
    let output = runtime
        .execute_output_with_options(
            &call(
                "inspect-call",
                "inspect",
                json!({"secret": "SECRET_ARGUMENT_VALUE"}),
            ),
            ToolExecutionOptions::new().with_metadata(metadata),
        )
        .await
        .expect("inspection succeeds");

    let input_debug = input_debug
        .lock()
        .expect("input debug mutex")
        .clone()
        .expect("input debug captured");
    assert!(!input_debug.contains("SECRET_ARGUMENT_VALUE"));
    assert!(!input_debug.contains("SECRET_METADATA_VALUE"));
    assert!(!format!("{output:?}").contains("SECRET_OUTPUT_VALUE"));
    let events = events.lock().expect("event capture mutex");
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0], ToolEvent::ExecutionStarted { .. }));
    assert!(matches!(
        events[1],
        ToolEvent::ExecutionCompleted {
            is_error: false,
            ..
        }
    ));
    let event_debug = format!("{events:?}");
    assert!(!event_debug.contains("SECRET_ARGUMENT_VALUE"));
    assert!(!event_debug.contains("SECRET_METADATA_VALUE"));
    assert!(!event_debug.contains("SECRET_OUTPUT_VALUE"));
}

#[tokio::test]
async fn started_observer_error_prevents_tool_execution_and_is_source_preserving() {
    let tool = ImmediateTool::new(
        "started_observer_error",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("must not run"),
    );
    let calls = Arc::clone(&tool.calls);
    let runtime = runtime_with(tool).with_event_sink(Arc::new(|event: &ToolEvent| {
        if matches!(event, ToolEvent::ExecutionStarted { .. }) {
            return Err(ToolObserverError::with_source(
                "SECRET_OBSERVER_MESSAGE",
                SecretSource,
            ));
        }
        Ok(())
    }));

    let error = runtime
        .execute(&call(
            "started-observer-error-call",
            "started_observer_error",
            json!({"value": 1}),
        ))
        .await
        .expect_err("started observer rejects execution");

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(error.kind(), ToolRuntimeErrorKind::ObserverFailed);
    assert!(!format!("{error}").contains("SECRET"));
    assert!(!format!("{error:?}").contains("SECRET"));
    let observer_failure = error.source().expect("observer failure source");
    assert!(observer_failure.is::<group_agent_tool::ToolObserverFailure>());
    let observer_error = observer_failure.source().expect("returned observer error");
    assert!(observer_error.is::<ToolObserverError>());
    assert!(
        observer_error
            .source()
            .expect("observer root source")
            .is::<SecretSource>()
    );
}

#[tokio::test]
async fn started_observer_panic_is_caught_and_prevents_tool_execution() {
    let tool = ImmediateTool::new(
        "started_observer_panic",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("must not run"),
    );
    let calls = Arc::clone(&tool.calls);
    let runtime = runtime_with(tool).with_event_sink(Arc::new(|event: &ToolEvent| {
        if matches!(event, ToolEvent::ExecutionStarted { .. }) {
            panic!("SECRET_PANIC_PAYLOAD");
        }
        Ok(())
    }));

    let error = runtime
        .execute(&call(
            "started-observer-panic-call",
            "started_observer_panic",
            json!({"value": 1}),
        ))
        .await
        .expect_err("started observer panic becomes an error");

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(error.kind(), ToolRuntimeErrorKind::ObserverFailed);
    assert!(!format!("{error:?}").contains("SECRET_PANIC_PAYLOAD"));
    let failure = error
        .source()
        .expect("observer failure")
        .downcast_ref::<group_agent_tool::ToolObserverFailure>()
        .expect("concrete observer failure");
    assert_eq!(failure.kind(), ToolObserverFailureKind::Panicked);
}

#[tokio::test]
async fn completed_observer_error_and_panic_do_not_replace_success() {
    for panics in [false, true] {
        let runtime = runtime_with(ImmediateTool::new(
            "completed_observer",
            ToolBehavior::read_only(),
            ImmediateOutcome::Success("real success"),
        ))
        .with_event_sink(Arc::new(move |event: &ToolEvent| {
            if matches!(event, ToolEvent::ExecutionCompleted { .. }) {
                if panics {
                    panic!("SECRET_COMPLETED_PANIC");
                }
                return Err(ToolObserverError::new("SECRET_COMPLETED_ERROR"));
            }
            Ok(())
        }));

        let report = runtime
            .execute_report(&call(
                &format!("completed-observer-{panics}"),
                "completed_observer",
                json!({"value": 1}),
            ))
            .await;
        assert_eq!(
            result_text(report.primary().as_ref().expect("success stays primary")),
            "real success"
        );
        assert_eq!(
            report
                .terminal_observer_failure()
                .expect("terminal diagnostic")
                .kind(),
            if panics {
                ToolObserverFailureKind::Panicked
            } else {
                ToolObserverFailureKind::ReturnedError
            }
        );
        assert!(!format!("{report:?}").contains("SECRET"));
    }
}

#[tokio::test]
async fn failed_observer_error_and_panic_do_not_replace_tool_error_or_source() {
    for panics in [false, true] {
        let runtime = runtime_with(ImmediateTool::new(
            "failed_observer",
            ToolBehavior::read_only(),
            ImmediateOutcome::InfrastructureError,
        ))
        .with_event_sink(Arc::new(move |event: &ToolEvent| {
            if matches!(event, ToolEvent::ExecutionFailed { .. }) {
                if panics {
                    panic!("SECRET_FAILED_PANIC");
                }
                return Err(ToolObserverError::new("SECRET_FAILED_ERROR"));
            }
            Ok(())
        }));

        let report = runtime
            .execute_report(&call(
                &format!("failed-observer-{panics}"),
                "failed_observer",
                json!({"value": 1}),
            ))
            .await;
        let error = report
            .primary()
            .as_ref()
            .expect_err("tool failure stays primary");
        assert_eq!(error.kind(), ToolRuntimeErrorKind::ExecutionFailed);
        assert!(error.source().expect("ToolError source").is::<ToolError>());
        assert!(
            error
                .source()
                .expect("ToolError source")
                .source()
                .expect("root source")
                .is::<SecretSource>()
        );
        assert!(report.terminal_observer_failure().is_some());
    }
}

#[tokio::test(start_paused = true)]
async fn timed_out_observer_panic_does_not_replace_timeout() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with(PendingTool {
        definition: definition("timeout_observer", object_schema()),
        behavior: ToolBehavior::read_only(),
        started: Arc::new(AtomicUsize::new(0)),
        dropped: Arc::clone(&dropped),
    })
    .with_event_sink(Arc::new(|event: &ToolEvent| {
        if matches!(event, ToolEvent::ExecutionTimedOut { .. }) {
            panic!("SECRET_TIMEOUT_PANIC");
        }
        Ok(())
    }));

    let report = runtime
        .execute_report_with_options(
            &call(
                "timeout-observer-call",
                "timeout_observer",
                json!({"value": 1}),
            ),
            ToolExecutionOptions::new().with_timeout(Duration::from_secs(1)),
        )
        .await;
    let error = report
        .primary()
        .as_ref()
        .expect_err("timeout stays primary");
    assert_eq!(error.kind(), ToolRuntimeErrorKind::TimedOut);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        report
            .terminal_observer_failure()
            .expect("panic retained")
            .kind(),
        ToolObserverFailureKind::Panicked
    );
}

#[tokio::test]
async fn terminal_observer_panic_cannot_erase_non_idempotent_execution_fact() {
    let tool = ImmediateTool::new(
        "observer_side_effect",
        ToolBehavior::non_idempotent_write(),
        ImmediateOutcome::Success("committed"),
    );
    let calls = Arc::clone(&tool.calls);
    let runtime = runtime_with(tool).with_event_sink(Arc::new(|event: &ToolEvent| {
        if matches!(event, ToolEvent::ExecutionCompleted { .. }) {
            panic!("SECRET_AFTER_SIDE_EFFECT");
        }
        Ok(())
    }));

    let report = runtime
        .execute_report(&call(
            "observer-side-effect-call",
            "observer_side_effect",
            json!({"value": 1}),
        ))
        .await;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        result_text(report.primary().as_ref().expect("success remains")),
        "committed"
    );
    assert!(report.terminal_observer_failure().is_some());
}

#[tokio::test]
async fn execution_helpers_pair_results_with_original_call_ids() {
    let runtime = runtime_with(ImmediateTool::new(
        "message_tool",
        ToolBehavior::read_only(),
        ImmediateOutcome::BusinessError("model-visible failure"),
    ));
    let single_call = call("message-call", "message_tool", json!({"value": 1}));
    let message = runtime
        .execute_message(&single_call)
        .await
        .expect("business error is a Tool message");
    let tool_message = message.as_tool().expect("tool message");
    assert_eq!(tool_message.tool_call_id(), single_call.id());
    assert!(tool_message.result().is_error());
    assert_eq!(result_text(tool_message.result()), "model-visible failure");

    let calls = vec![
        call("batch-message-0", "message_tool", json!({"value": 0})),
        call("batch-message-1", "message_tool", json!({"value": 1})),
    ];
    let report = runtime
        .execute_batch(calls, ToolBatchConfig::new(2))
        .await
        .expect("batch executes");
    let messages = report.into_tool_messages();
    let ids = messages
        .into_iter()
        .map(|message| {
            let message = message.expect("Tool message");
            match message {
                Message::Tool(message) => message.tool_call_id().as_str().to_owned(),
                _ => panic!("expected Tool message"),
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, ["batch-message-0", "batch-message-1"]);
}

#[tokio::test]
async fn safe_message_report_helpers_preserve_primary_secondary_and_runtime_errors() {
    let runtime = runtime_with(ImmediateTool::new(
        "reported_message",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("primary result"),
    ))
    .with_event_sink(Arc::new(|event: &ToolEvent| {
        if matches!(event, ToolEvent::ExecutionCompleted { .. }) {
            return Err(ToolObserverError::new(
                "SECRET_TERMINAL_OBSERVER_DIAGNOSTIC",
            ));
        }
        Ok(())
    }));

    let reported_call = call(
        "reported-message-call",
        "reported_message",
        json!({"value": 1}),
    );
    let report = runtime.execute_message_report(&reported_call).await;
    let message = report
        .primary()
        .as_ref()
        .expect("terminal observer failure cannot replace the Tool message");
    let tool_message = message.as_tool().expect("primary is a Tool message");
    assert_eq!(tool_message.tool_call_id(), reported_call.id());
    assert_eq!(result_text(tool_message.result()), "primary result");
    assert_eq!(
        report
            .terminal_observer_failure()
            .expect("secondary diagnostic remains inspectable")
            .kind(),
        ToolObserverFailureKind::ReturnedError
    );

    let ordinary = runtime
        .execute(&call(
            "ordinary-primary-call",
            "reported_message",
            json!({"value": 2}),
        ))
        .await
        .expect("ordinary execute returns the primary outcome");
    assert_eq!(result_text(&ordinary), "primary result");

    let mut builder = ToolRegistry::builder();
    builder
        .register(ImmediateTool::new(
            "batch_message_success",
            ToolBehavior::read_only(),
            ImmediateOutcome::Success("message"),
        ))
        .expect("success tool registers")
        .register(ImmediateTool::new(
            "batch_message_failure",
            ToolBehavior::read_only(),
            ImmediateOutcome::InfrastructureError,
        ))
        .expect("failure tool registers");
    let batch_runtime = ToolRuntime::new(builder.build());
    let converted = batch_runtime
        .execute_batch(
            vec![
                call(
                    "batch-message-success",
                    "batch_message_success",
                    json!({"value": 1}),
                ),
                call(
                    "batch-message-runtime-error",
                    "batch_message_failure",
                    json!({"value": 2}),
                ),
            ],
            ToolBatchConfig::new(2),
        )
        .await
        .expect("batch structure is valid")
        .into_tool_messages();

    let first = converted[0]
        .as_ref()
        .expect("successful outcome becomes a Tool message")
        .as_tool()
        .expect("message role is Tool");
    assert_eq!(first.tool_call_id().as_str(), "batch-message-success");
    assert_eq!(
        converted[1]
            .as_ref()
            .expect_err("Runtime error remains Err instead of a fake Tool message")
            .kind(),
        ToolRuntimeErrorKind::ExecutionFailed
    );
}

#[test]
fn schema_violation_display_contains_locations_but_not_instance_value() {
    fn assert_redacted(violation: &SchemaViolation) {
        let text = format!("{violation}");
        assert!(text.contains("instance path"));
        assert!(!text.contains("SECRET"));
    }

    let runtime = runtime_with(ImmediateTool::new(
        "schema_redaction",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("ok"),
    ));
    let invalid_call = call(
        "schema-redaction-call",
        "schema_redaction",
        json!({"value": "SECRET_INVALID_VALUE"}),
    );
    let execution = runtime.execute(&invalid_call);
    let error = futures_executor::block_on(execution).expect_err("schema fails");
    assert_redacted(error.schema_violation().expect("violation"));
}

#[derive(Debug)]
struct ToolNodeState {
    call: ToolCall,
    result: Option<group_agent_tool::ToolResult>,
}

struct ToolNodeUpdate {
    result: group_agent_tool::ToolResult,
}

impl GraphState for ToolNodeState {
    type Update = ToolNodeUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.result = Some(update.result);
        Ok(())
    }
}

struct RuntimeNode {
    runtime: ToolRuntime,
}

#[async_trait]
impl Node<ToolNodeState> for RuntimeNode {
    async fn run(
        &self,
        state: &ToolNodeState,
        _context: &NodeContext,
    ) -> Result<ToolNodeUpdate, NodeError> {
        let result = self
            .runtime
            .execute(&state.call)
            .await
            .map_err(|source| NodeError::with_source("tool execution failed", source))?;
        Ok(ToolNodeUpdate { result })
    }
}

fn tool_graph(runtime: ToolRuntime) -> group_agent_core::CompiledGraph<ToolNodeState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("tool", RuntimeNode { runtime })
        .expect("tool node registers");
    graph.add_edge(START, "tool").add_edge("tool", END);
    graph.compile().expect("tool graph compiles")
}

#[tokio::test]
async fn tool_runtime_runs_as_an_ordinary_group_node() {
    let graph = tool_graph(runtime_with(ImmediateTool::new(
        "node_tool",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("node result"),
    )));
    let report = graph
        .invoke(ToolNodeState {
            call: call("node-call", "node_tool", json!({"value": 1})),
            result: None,
        })
        .await
        .expect("graph completes");
    let result = report
        .final_state()
        .result
        .as_ref()
        .expect("node stores result");
    assert_eq!(result_text(result), "node result");
}

#[tokio::test]
async fn tool_runtime_error_chain_survives_node_and_graph_layers() {
    let graph = tool_graph(runtime_with(ImmediateTool::new(
        "node_failure",
        ToolBehavior::read_only(),
        ImmediateOutcome::InfrastructureError,
    )));
    let error = graph
        .invoke(ToolNodeState {
            call: call("node-failure-call", "node_failure", json!({"value": 1})),
            result: None,
        })
        .await
        .expect_err("graph fails");

    assert!(matches!(error, GraphRunError::NodeFailed { .. }));
    let node_error = error.source().expect("node source");
    let runtime_error = node_error.source().expect("runtime source");
    assert!(runtime_error.is::<group_agent_tool::ToolRuntimeError>());
    let tool_error = runtime_error.source().expect("tool source");
    assert!(tool_error.is::<ToolError>());
    assert!(
        tool_error
            .source()
            .expect("root source")
            .is::<SecretSource>()
    );
}

#[tokio::test]
async fn invalid_arguments_source_chain_reaches_jsonschema_through_group_without_execution() {
    let tool = ImmediateTool::new(
        "node_schema_failure",
        ToolBehavior::read_only(),
        ImmediateOutcome::Success("must not run"),
    );
    let calls = Arc::clone(&tool.calls);
    let graph = tool_graph(runtime_with(tool));
    let sentinel = "SECRET_GROUP_SCHEMA_ARGUMENT";
    let error = graph
        .invoke(ToolNodeState {
            call: call(
                "node-schema-failure-call",
                "node_schema_failure",
                json!({"value": sentinel}),
            ),
            result: None,
        })
        .await
        .expect_err("invalid arguments fail through the ordinary Tool Node");

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(error, GraphRunError::NodeFailed { .. }));
    assert!(!format!("{error}").contains(sentinel));
    assert!(!format!("{error:?}").contains(sentinel));

    let node_error = error.source().expect("GraphRunError -> NodeError");
    assert!(node_error.is::<NodeError>());
    assert!(!format!("{node_error}").contains(sentinel));
    assert!(!format!("{node_error:?}").contains(sentinel));

    let runtime_error = node_error.source().expect("NodeError -> ToolRuntimeError");
    assert!(runtime_error.is::<group_agent_tool::ToolRuntimeError>());
    let runtime_error = runtime_error
        .downcast_ref::<group_agent_tool::ToolRuntimeError>()
        .expect("concrete ToolRuntimeError");
    assert_eq!(runtime_error.kind(), ToolRuntimeErrorKind::InvalidArguments);
    assert!(!format!("{runtime_error}").contains(sentinel));
    assert!(!format!("{runtime_error:?}").contains(sentinel));

    let schema_error = runtime_error
        .source()
        .expect("ToolRuntimeError -> jsonschema::ValidationError");
    assert!(schema_error.is::<jsonschema::ValidationError<'static>>());
}

#[tokio::test]
async fn group_cancellation_drops_tool_runtime_future() {
    let started = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let graph = tool_graph(runtime_with(PendingTool {
        definition: definition("cancelled_node", object_schema()),
        behavior: ToolBehavior::read_only(),
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
    }));
    let token = CancellationToken::new();
    let run_token = token.clone();
    let task = tokio::spawn(async move {
        graph
            .invoke_with_control(
                ToolNodeState {
                    call: call("cancelled-node-call", "cancelled_node", json!({"value": 1})),
                    result: None,
                },
                RunConfig::default(),
                EventConfig::default(),
                RunControl::new().with_cancellation_token(run_token),
            )
            .await
    });

    while started.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    token.cancel();
    let error = task
        .await
        .expect("graph task joins")
        .expect_err("run cancels");
    assert!(matches!(error, GraphRunError::Cancelled { .. }));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn group_node_timeout_drops_tool_runtime_future() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let graph = tool_graph(runtime_with(PendingTool {
        definition: definition("timed_node", object_schema()),
        behavior: ToolBehavior::read_only(),
        started: Arc::new(AtomicUsize::new(0)),
        dropped: Arc::clone(&dropped),
    }));
    let error = graph
        .invoke_with_control(
            ToolNodeState {
                call: call("timed-node-call", "timed_node", json!({"value": 1})),
                result: None,
            },
            RunConfig::default(),
            EventConfig::default(),
            RunControl::new().with_node_timeout(Duration::from_secs(2)),
        )
        .await
        .expect_err("node times out");

    assert!(matches!(error, GraphRunError::NodeTimedOut { .. }));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn batch_item_keeps_independent_options() {
    let item = ToolBatchItem::new(call("item-call", "item", json!({})))
        .with_options(ToolExecutionOptions::new().with_timeout(Duration::from_millis(25)));
    assert_eq!(item.call().id().as_str(), "item-call");
    assert_eq!(item.options().timeout(), Some(Duration::from_millis(25)));
}

#[test]
fn idempotency_key_debug_and_display_are_redacted() {
    let key = IdempotencyKey::new("SECRET_IDEMPOTENCY_VALUE").expect("valid key");
    assert!(!format!("{key}").contains("SECRET"));
    assert!(!format!("{key:?}").contains("SECRET"));
    assert_eq!(key.as_str(), "SECRET_IDEMPOTENCY_VALUE");
}

#[test]
fn zero_batch_concurrency_is_rejected() {
    let runtime = ToolRuntime::new(ToolRegistry::empty());
    let error =
        futures_executor::block_on(runtime.execute_batch(Vec::new(), ToolBatchConfig::new(0)))
            .expect_err("zero concurrency fails");
    assert_eq!(error, ToolBatchError::ZeroConcurrency);
}

#[test]
fn tool_error_message_is_explicit_but_not_printed_by_default() {
    let error = ToolError::new(ToolErrorKind::Other, "SECRET_BUSINESS_DETAIL");
    assert_eq!(error.as_message(), "SECRET_BUSINESS_DETAIL");
    assert!(!format!("{error}").contains("SECRET"));
    assert!(!format!("{error:?}").contains("SECRET"));
}
