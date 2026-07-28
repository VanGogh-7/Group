use std::collections::HashMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use group_agent_model::{Extensions, Message, ToolCall, ToolCallId, ToolResult};

use crate::event::SharedToolEventSink;
use crate::registry::RegisteredTool;
use crate::{
    IdempotencyKey, SchemaViolation, ToolBatchError, ToolCallContext, ToolErrorKind, ToolEvent,
    ToolEventSink, ToolInput, ToolObserverFailure, ToolOutput, ToolRegistry, ToolRuntimeError,
    ToolRuntimeErrorKind, ToolSideEffect,
};

/// One primary execution outcome plus an optional terminal-observer diagnostic.
///
/// A failure from `ExecutionStarted` is a primary `ObserverFailed` error because
/// the Tool was not entered. Once Tool execution has produced success, failure,
/// or timeout, that primary fact is immutable; a terminal observer failure is
/// retained separately here.
pub struct ToolExecutionReport<T> {
    primary: Result<T, ToolRuntimeError>,
    terminal_observer_failure: Option<ToolObserverFailure>,
}

impl<T> ToolExecutionReport<T> {
    fn new(
        primary: Result<T, ToolRuntimeError>,
        terminal_observer_failure: Option<ToolObserverFailure>,
    ) -> Self {
        Self {
            primary,
            terminal_observer_failure,
        }
    }

    /// Returns the primary Tool outcome.
    pub const fn primary(&self) -> &Result<T, ToolRuntimeError> {
        &self.primary
    }

    /// Returns a terminal observer failure without changing the primary outcome.
    #[must_use]
    pub const fn terminal_observer_failure(&self) -> Option<&ToolObserverFailure> {
        self.terminal_observer_failure.as_ref()
    }

    /// Consumes the report and returns only the primary Tool outcome.
    pub fn into_primary(self) -> Result<T, ToolRuntimeError> {
        self.primary
    }

    /// Consumes the report into its primary and secondary components.
    pub fn into_parts(self) -> (Result<T, ToolRuntimeError>, Option<ToolObserverFailure>) {
        (self.primary, self.terminal_observer_failure)
    }

    fn map<U>(self, map: impl FnOnce(T) -> U) -> ToolExecutionReport<U> {
        ToolExecutionReport::new(self.primary.map(map), self.terminal_observer_failure)
    }
}

impl<T> fmt::Debug for ToolExecutionReport<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolExecutionReport")
            .field("primary", &self.primary)
            .field("terminal_observer_failure", &self.terminal_observer_failure)
            .finish()
    }
}

/// Per-call timeout, idempotency, metadata, and side-effect policy.
#[derive(Clone)]
pub struct ToolExecutionOptions {
    timeout: Option<Duration>,
    idempotency_key: Option<IdempotencyKey>,
    metadata: Extensions,
    allow_non_idempotent_writes: bool,
}

impl ToolExecutionOptions {
    /// Creates default options with no timeout or key.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            timeout: None,
            idempotency_key: None,
            metadata: Extensions::new(),
            allow_non_idempotent_writes: true,
        }
    }

    /// Applies one caller-runtime timeout to the tool future.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Supplies an opaque idempotency key.
    #[must_use]
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    /// Supplies provider-neutral execution metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Extensions) -> Self {
        self.metadata = metadata;
        self
    }

    /// Allows or rejects tools classified as non-idempotent writes.
    #[must_use]
    pub const fn with_non_idempotent_writes(mut self, allowed: bool) -> Self {
        self.allow_non_idempotent_writes = allowed;
        self
    }

    /// Returns the optional timeout.
    #[must_use]
    pub const fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Returns the optional idempotency key.
    #[must_use]
    pub const fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.idempotency_key.as_ref()
    }

    /// Returns execution metadata.
    #[must_use]
    pub const fn metadata(&self) -> &Extensions {
        &self.metadata
    }
}

impl Default for ToolExecutionOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ToolExecutionOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolExecutionOptions")
            .field("timeout", &self.timeout)
            .field("idempotency_key", &self.idempotency_key)
            .field("metadata", &self.metadata)
            .field(
                "allow_non_idempotent_writes",
                &self.allow_non_idempotent_writes,
            )
            .finish()
    }
}

/// Failure policy for a batch after all calls have been prevalidated.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolBatchFailurePolicy {
    #[default]
    CollectAll,
    FailFast,
}

/// Batch scheduling configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ToolBatchConfig {
    max_concurrency: usize,
    failure_policy: ToolBatchFailurePolicy,
}

impl ToolBatchConfig {
    /// Creates a collect-all configuration.
    #[must_use]
    pub const fn new(max_concurrency: usize) -> Self {
        Self {
            max_concurrency,
            failure_policy: ToolBatchFailurePolicy::CollectAll,
        }
    }

    /// Selects collect-all or fail-fast behavior.
    #[must_use]
    pub const fn with_failure_policy(mut self, failure_policy: ToolBatchFailurePolicy) -> Self {
        self.failure_policy = failure_policy;
        self
    }

    /// Returns the maximum number of overlapping calls.
    #[must_use]
    pub const fn max_concurrency(self) -> usize {
        self.max_concurrency
    }

    /// Returns the selected failure policy.
    #[must_use]
    pub const fn failure_policy(self) -> ToolBatchFailurePolicy {
        self.failure_policy
    }
}

impl Default for ToolBatchConfig {
    fn default() -> Self {
        Self::new(8)
    }
}

/// One owned batch call and its independent execution options.
#[derive(Clone, Debug)]
pub struct ToolBatchItem {
    call: ToolCall,
    options: ToolExecutionOptions,
}

impl ToolBatchItem {
    /// Creates a batch item with default execution options.
    #[must_use]
    pub const fn new(call: ToolCall) -> Self {
        Self {
            call,
            options: ToolExecutionOptions::new(),
        }
    }

    /// Applies independent execution options.
    #[must_use]
    pub fn with_options(mut self, options: ToolExecutionOptions) -> Self {
        self.options = options;
        self
    }

    /// Returns the model-produced call.
    #[must_use]
    pub const fn call(&self) -> &ToolCall {
        &self.call
    }

    /// Returns the independent execution options.
    #[must_use]
    pub const fn options(&self) -> &ToolExecutionOptions {
        &self.options
    }
}

/// Ordered per-call results from one batch.
pub struct ToolBatchReport {
    call_ids: Vec<ToolCallId>,
    results: Vec<Result<ToolResult, ToolRuntimeError>>,
    terminal_observer_failures: Vec<Option<ToolObserverFailure>>,
}

impl ToolBatchReport {
    fn new(
        call_ids: Vec<ToolCallId>,
        results: Vec<Result<ToolResult, ToolRuntimeError>>,
        terminal_observer_failures: Vec<Option<ToolObserverFailure>>,
    ) -> Self {
        Self {
            call_ids,
            results,
            terminal_observer_failures,
        }
    }

    /// Returns results in original input order.
    pub fn results(&self) -> &[Result<ToolResult, ToolRuntimeError>] {
        &self.results
    }

    /// Consumes the report into ordered results.
    #[must_use]
    pub fn into_results(self) -> Vec<Result<ToolResult, ToolRuntimeError>> {
        self.results
    }

    /// Returns terminal observer failures in original input order.
    ///
    /// Each entry is secondary to the corresponding primary result.
    #[must_use]
    pub fn terminal_observer_failures(&self) -> &[Option<ToolObserverFailure>] {
        &self.terminal_observer_failures
    }

    /// Consumes the report into correctly paired model Tool messages.
    ///
    /// Successful infrastructure outcomes, including business-error
    /// `ToolResult` values, use the original model-produced `ToolCallId`.
    /// Infrastructure errors remain errors and do not become Tool messages.
    #[must_use]
    pub fn into_tool_messages(self) -> Vec<Result<Message, ToolRuntimeError>> {
        self.call_ids
            .into_iter()
            .zip(self.results)
            .map(|(call_id, result)| result.map(|result| Message::tool(call_id, result)))
            .collect()
    }

    /// Returns the number of input calls.
    #[must_use]
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Returns whether the batch contained no calls.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }
}

impl fmt::Debug for ToolBatchReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolBatchReport")
            .field("results", &self.results)
            .field(
                "terminal_observer_failures",
                &self.terminal_observer_failures,
            )
            .finish()
    }
}

/// Validating, immutable local tool execution facade.
#[derive(Clone)]
pub struct ToolRuntime {
    registry: ToolRegistry,
    event_sink: Option<SharedToolEventSink>,
}

impl ToolRuntime {
    /// Creates a runtime over an immutable registry.
    #[must_use]
    pub const fn new(registry: ToolRegistry) -> Self {
        Self {
            registry,
            event_sink: None,
        }
    }

    /// Installs a lightweight synchronous observer.
    #[must_use]
    pub fn with_event_sink(mut self, event_sink: Arc<dyn ToolEventSink>) -> Self {
        self.event_sink = Some(event_sink);
        self
    }

    /// Returns the shared registry.
    #[must_use]
    pub const fn registry(&self) -> &ToolRegistry {
        &self.registry
    }

    /// Executes one call and returns the existing model-facing result type.
    pub async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolRuntimeError> {
        self.execute_report(call).await.into_primary()
    }

    /// Executes one call and retains a terminal observer failure separately.
    pub async fn execute_report(&self, call: &ToolCall) -> ToolExecutionReport<ToolResult> {
        self.execute_report_with_options(call, ToolExecutionOptions::default())
            .await
    }

    /// Executes one call with explicit policy and returns a model-facing result.
    pub async fn execute_with_options(
        &self,
        call: &ToolCall,
        options: ToolExecutionOptions,
    ) -> Result<ToolResult, ToolRuntimeError> {
        self.execute_report_with_options(call, options)
            .await
            .into_primary()
    }

    /// Executes one call with explicit policy and retains observer diagnostics.
    pub async fn execute_report_with_options(
        &self,
        call: &ToolCall,
        options: ToolExecutionOptions,
    ) -> ToolExecutionReport<ToolResult> {
        self.execute_output_report_with_options(call, options)
            .await
            .map(ToolOutput::into_result)
    }

    /// Executes one call while retaining tool output metadata.
    pub async fn execute_output_with_options(
        &self,
        call: &ToolCall,
        options: ToolExecutionOptions,
    ) -> Result<ToolOutput, ToolRuntimeError> {
        self.execute_output_report_with_options(call, options)
            .await
            .into_primary()
    }

    /// Executes one call while retaining output metadata and observer diagnostics.
    pub async fn execute_output_report_with_options(
        &self,
        call: &ToolCall,
        options: ToolExecutionOptions,
    ) -> ToolExecutionReport<ToolOutput> {
        let context = call_context(call, None);
        let entry = match self.prepare(call, &options, &context) {
            Ok(entry) => entry,
            Err(error) => {
                let observer_failure = self.emit_failure(&error);
                return ToolExecutionReport::new(Err(error), observer_failure);
            }
        };
        self.execute_prepared(call, &options, context, entry).await
    }

    /// Executes one call and constructs a correctly paired model Tool message.
    pub async fn execute_message(&self, call: &ToolCall) -> Result<Message, ToolRuntimeError> {
        self.execute(call)
            .await
            .map(|result| Message::tool(call.id().clone(), result))
    }

    /// Executes one call, pairs its result, and retains observer diagnostics.
    pub async fn execute_message_report(&self, call: &ToolCall) -> ToolExecutionReport<Message> {
        let call_id = call.id().clone();
        self.execute_report(call)
            .await
            .map(|result| Message::tool(call_id, result))
    }

    /// Executes calls with default per-call options.
    pub async fn execute_batch(
        &self,
        calls: Vec<ToolCall>,
        config: ToolBatchConfig,
    ) -> Result<ToolBatchReport, ToolBatchError> {
        self.execute_batch_items(calls.into_iter().map(ToolBatchItem::new).collect(), config)
            .await
    }

    /// Executes independently configured calls with bounded, spawn-free concurrency.
    pub async fn execute_batch_items(
        &self,
        items: Vec<ToolBatchItem>,
        config: ToolBatchConfig,
    ) -> Result<ToolBatchReport, ToolBatchError> {
        if config.max_concurrency == 0 {
            return Err(ToolBatchError::ZeroConcurrency);
        }
        validate_unique_call_ids(&items)?;

        let mut prepared = Vec::with_capacity(items.len());
        let mut results = std::iter::repeat_with(|| None)
            .take(items.len())
            .collect::<Vec<Option<Result<ToolResult, ToolRuntimeError>>>>();
        let mut observer_failures = std::iter::repeat_with(|| None)
            .take(items.len())
            .collect::<Vec<Option<ToolObserverFailure>>>();

        for (index, item) in items.iter().enumerate() {
            let context = call_context(&item.call, Some(index));
            match self.prepare(&item.call, &item.options, &context) {
                Ok(entry) => prepared.push(Some(entry)),
                Err(error) => {
                    observer_failures[index] = self.emit_failure(&error);
                    results[index] = Some(Err(error));
                    prepared.push(None);
                }
            }
        }

        if matches!(config.failure_policy, ToolBatchFailurePolicy::FailFast)
            && results.iter().any(Option::is_some)
        {
            self.mark_not_started(&items, &mut results);
            return Ok(finish_report(&items, results, observer_failures));
        }

        let mut running = FuturesUnordered::new();
        let mut next = 0;
        let mut exclusive_running = false;
        let mut stop_scheduling = false;

        loop {
            while !stop_scheduling
                && next < items.len()
                && running.len() < config.max_concurrency
                && !exclusive_running
            {
                if results[next].is_some() {
                    next += 1;
                    continue;
                }
                let entry = prepared[next].expect("prepared entry exists for pending result");
                if !entry.behavior.allows_parallel() {
                    if running.is_empty() {
                        running.push(self.execute_batch_item(next, &items[next], entry));
                        exclusive_running = true;
                        next += 1;
                    }
                    break;
                }
                running.push(self.execute_batch_item(next, &items[next], entry));
                next += 1;
            }

            if running.is_empty() {
                if stop_scheduling {
                    self.mark_not_started(&items, &mut results);
                    break;
                }
                if next >= items.len() {
                    break;
                }
                continue;
            }

            let (index, report) = running.next().await.expect("running batch future");
            let (result, observer_failure) = report.into_parts();
            let failed = result.is_err();
            results[index] = Some(result);
            observer_failures[index] = observer_failure;
            if exclusive_running {
                exclusive_running = false;
            }

            if failed && matches!(config.failure_policy, ToolBatchFailurePolicy::FailFast) {
                stop_scheduling = true;
            }
        }

        Ok(finish_report(&items, results, observer_failures))
    }

    fn prepare<'a>(
        &'a self,
        call: &ToolCall,
        options: &ToolExecutionOptions,
        context: &ToolCallContext,
    ) -> Result<&'a RegisteredTool, ToolRuntimeError> {
        if !is_valid_call_identifier(call.id().as_str())
            || !is_valid_call_identifier(call.name().as_str())
        {
            return Err(ToolRuntimeError::new(
                ToolRuntimeErrorKind::InvalidToolCall,
                context.clone(),
            ));
        }

        let entry = self.registry.entry(call.name()).ok_or_else(|| {
            ToolRuntimeError::new(ToolRuntimeErrorKind::ToolNotFound, context.clone())
        })?;

        if let Err(error) = entry.validator.validate(call.arguments()) {
            let violation = SchemaViolation::from_error(&error);
            return Err(ToolRuntimeError::invalid_arguments(
                context.clone(),
                violation,
                error.to_owned(),
            ));
        }
        if entry.behavior.requires_idempotency_key() && options.idempotency_key.is_none() {
            return Err(ToolRuntimeError::new(
                ToolRuntimeErrorKind::MissingIdempotencyKey,
                context.clone(),
            ));
        }
        if matches!(
            entry.behavior.side_effect(),
            ToolSideEffect::NonIdempotentWrite
        ) && !options.allow_non_idempotent_writes
        {
            return Err(ToolRuntimeError::new(
                ToolRuntimeErrorKind::UnsupportedExecution,
                context.clone(),
            ));
        }
        Ok(entry)
    }

    async fn execute_batch_item(
        &self,
        index: usize,
        item: &ToolBatchItem,
        entry: &RegisteredTool,
    ) -> (usize, ToolExecutionReport<ToolResult>) {
        let context = call_context(&item.call, Some(index));
        let report = self
            .execute_prepared(&item.call, &item.options, context, entry)
            .await
            .map(ToolOutput::into_result);
        (index, report)
    }

    async fn execute_prepared(
        &self,
        call: &ToolCall,
        options: &ToolExecutionOptions,
        context: ToolCallContext,
        entry: &RegisteredTool,
    ) -> ToolExecutionReport<ToolOutput> {
        if let Err(source) = self.emit(ToolEvent::ExecutionStarted {
            context: context.clone(),
        }) {
            return ToolExecutionReport::new(
                Err(ToolRuntimeError::observer_failed(context, source)),
                None,
            );
        }
        let input = ToolInput::new(
            call.id(),
            call.name(),
            call.arguments(),
            options.idempotency_key.as_ref(),
            &options.metadata,
        );

        let execution = entry.tool.execute(input);
        let output = if let Some(timeout) = options.timeout {
            match tokio::time::timeout(timeout, execution).await {
                Ok(result) => result,
                Err(source) => {
                    let error = ToolRuntimeError::timed_out(context.clone(), timeout, source);
                    let observer_failure = self
                        .emit(ToolEvent::ExecutionTimedOut { context, timeout })
                        .err();
                    return ToolExecutionReport::new(Err(error), observer_failure);
                }
            }
        } else {
            execution.await
        };

        match output {
            Ok(output) => {
                let observer_failure = self
                    .emit(ToolEvent::ExecutionCompleted {
                        context,
                        is_error: output.result().is_error(),
                    })
                    .err();
                ToolExecutionReport::new(Ok(output), observer_failure)
            }
            Err(source) => {
                let kind = match source.kind() {
                    ToolErrorKind::Cancelled => ToolRuntimeErrorKind::Cancelled,
                    ToolErrorKind::Other => ToolRuntimeErrorKind::ExecutionFailed,
                };
                let error = ToolRuntimeError::with_source(kind, context.clone(), source);
                let observer_failure = self
                    .emit(ToolEvent::ExecutionFailed { context, kind })
                    .err();
                ToolExecutionReport::new(Err(error), observer_failure)
            }
        }
    }

    fn mark_not_started(
        &self,
        items: &[ToolBatchItem],
        results: &mut [Option<Result<ToolResult, ToolRuntimeError>>],
    ) {
        for (index, (item, result)) in items.iter().zip(results.iter_mut()).enumerate() {
            if result.is_none() {
                let context = call_context(&item.call, Some(index));
                let error =
                    ToolRuntimeError::new(ToolRuntimeErrorKind::NotStartedDueToFailFast, context);
                *result = Some(Err(error));
            }
        }
    }

    fn emit_failure(&self, error: &ToolRuntimeError) -> Option<ToolObserverFailure> {
        self.emit(ToolEvent::ExecutionFailed {
            context: error.context().clone(),
            kind: error.kind(),
        })
        .err()
    }

    fn emit(&self, event: ToolEvent) -> Result<(), ToolObserverFailure> {
        if let Some(sink) = &self.event_sink {
            match catch_unwind(AssertUnwindSafe(|| sink.on_event(&event))) {
                Ok(Ok(())) => {}
                Ok(Err(source)) => return Err(ToolObserverFailure::returned(source)),
                Err(_panic_payload) => return Err(ToolObserverFailure::panicked()),
            }
        }
        Ok(())
    }
}

impl fmt::Debug for ToolRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolRuntime")
            .field("registry", &self.registry)
            .field("has_event_sink", &self.event_sink.is_some())
            .finish()
    }
}

fn call_context(call: &ToolCall, batch_index: Option<usize>) -> ToolCallContext {
    ToolCallContext::new(call.id().clone(), call.name().clone(), batch_index)
}

fn is_valid_call_identifier(value: &str) -> bool {
    value.trim() == value && !value.chars().any(char::is_control)
}

fn validate_unique_call_ids(items: &[ToolBatchItem]) -> Result<(), ToolBatchError> {
    let mut seen = HashMap::<ToolCallId, usize>::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        if let Some(first_index) = seen.insert(item.call.id().clone(), index) {
            return Err(ToolBatchError::DuplicateToolCallId {
                call_id: item.call.id().clone(),
                first_index,
                duplicate_index: index,
            });
        }
    }
    Ok(())
}

fn finish_report(
    items: &[ToolBatchItem],
    results: Vec<Option<Result<ToolResult, ToolRuntimeError>>>,
    terminal_observer_failures: Vec<Option<ToolObserverFailure>>,
) -> ToolBatchReport {
    ToolBatchReport::new(
        items.iter().map(|item| item.call.id().clone()).collect(),
        results
            .into_iter()
            .map(|result| result.expect("every batch result is finalized"))
            .collect(),
        terminal_observer_failures,
    )
}
