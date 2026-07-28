mod support;

use std::error::Error as StdError;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use group_agent_mcp::{
    McpAdapterError, McpAdapterErrorKind, McpClientSession, McpConfigError, McpDiscoveryConfig,
    McpServerConfig, McpServerId, McpToolNamePolicy, McpToolPrefix, McpToolSet,
};
use group_agent_model::{Message, ToolCall, ToolCallId, ToolName, ToolResult};
use group_agent_tool::{
    ToolBatchConfig, ToolBatchFailurePolicy, ToolBehavior, ToolExecutionOptions,
    ToolRuntimeErrorKind, ToolSideEffect,
};
use serde_json::{Value, json};
use support::{ServerScenario, ServerState, in_process_session, in_process_session_with_id};

fn call(id: &str, name: &str, arguments: Value) -> ToolCall {
    ToolCall::new(
        ToolCallId::new(id).expect("valid call id"),
        ToolName::new(name).expect("valid tool name"),
        arguments,
    )
}

fn text_parts(result: &ToolResult) -> Vec<&str> {
    result
        .content()
        .iter()
        .filter_map(group_agent_model::ContentPart::as_text)
        .collect()
}

fn source_of<'a, T>(error: &'a (dyn StdError + 'static)) -> Option<&'a T>
where
    T: StdError + 'static,
{
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(concrete) = source.downcast_ref::<T>() {
            return Some(concrete);
        }
        current = source.source();
    }
    None
}

async fn wait_for_count(state: &ServerState, target: usize) {
    while state.tool_calls.load(Ordering::SeqCst) < target {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn initialization_paginates_stably_reuses_session_and_refreshes_by_snapshot() {
    let (session, state, server) = in_process_session(ServerScenario::Standard).await;
    let first = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("discovery succeeds");
    let names = first
        .registry()
        .definitions()
        .map(|definition| definition.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "business_error",
            "calculator",
            "child_pid",
            "echo",
            "malformed",
            "multi_text",
            "pending",
            "protocol_error",
            "structured",
            "text_and_structured",
            "unsupported_audio",
            "unsupported_image",
            "unsupported_resource",
            "unsupported_resource_link",
        ]
    );
    assert_eq!(first.registry().schema_compilation_count(), first.len());
    assert_eq!(state.connections.load(Ordering::SeqCst), 1);
    assert_eq!(state.list_calls.load(Ordering::SeqCst), 2);

    let refreshed = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("explicit refresh creates a second snapshot");
    assert_eq!(refreshed.len(), first.len());
    assert_eq!(first.len(), 14);
    assert_eq!(state.connections.load(Ordering::SeqCst), 1);
    assert_eq!(state.list_calls.load(Ordering::SeqCst), 4);

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[tokio::test]
async fn pagination_cycles_protocol_failures_disconnects_and_limits_never_publish_partial_sets() {
    for scenario in [
        ServerScenario::SameCursor,
        ServerScenario::TwoCursorCycle,
        ServerScenario::MultiCursorCycle,
    ] {
        let (session, state, server) = in_process_session(scenario).await;
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            session.discover(McpDiscoveryConfig::new()),
        )
        .await
        .expect("cursor loop detection must not hang")
        .expect_err("cursor cycle cannot publish a Tool set");
        assert_eq!(error.kind(), McpAdapterErrorKind::Protocol);
        assert!(state.list_calls.load(Ordering::SeqCst) >= 2);
        session.shutdown().await.expect("shutdown succeeds");
        server.await.expect("server joins");
    }

    let (protocol, state, protocol_server) =
        in_process_session(ServerScenario::SecondPageProtocolError).await;
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        protocol.discover(McpDiscoveryConfig::new()),
    )
    .await
    .expect("second-page protocol failure must not hang")
    .expect_err("a failed second page cannot publish its first page");
    assert_eq!(error.kind(), McpAdapterErrorKind::Protocol);
    assert_eq!(state.list_calls.load(Ordering::SeqCst), 2);
    assert!(source_of::<rmcp::ServiceError>(&error).is_some());
    protocol.shutdown().await.expect("shutdown succeeds");
    protocol_server.await.expect("server joins");

    let (disconnected, state, disconnected_server) =
        in_process_session(ServerScenario::SecondPageDisconnect).await;
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        disconnected.discover(McpDiscoveryConfig::new()),
    )
    .await
    .expect("second-page disconnect must not hang")
    .expect_err("a disconnected traversal cannot publish its first page");
    assert_eq!(error.kind(), McpAdapterErrorKind::Transport);
    assert_eq!(state.list_calls.load(Ordering::SeqCst), 2);
    assert!(source_of::<rmcp::ServiceError>(&error).is_some());
    disconnected.shutdown().await.expect("shutdown succeeds");
    disconnected_server.await.expect("server joins");

    let (page_limited, state, page_server) =
        in_process_session(ServerScenario::EndlessPagination).await;
    let page_config = McpDiscoveryConfig::new()
        .with_max_pages(3)
        .expect("positive page limit");
    let error = tokio::time::timeout(Duration::from_secs(1), page_limited.discover(page_config))
        .await
        .expect("page limit must not hang")
        .expect_err("page limit cannot publish a partial set");
    assert_eq!(error.kind(), McpAdapterErrorKind::DiscoveryFailed);
    assert_eq!(state.list_calls.load(Ordering::SeqCst), 3);
    page_limited.shutdown().await.expect("shutdown succeeds");
    page_server.await.expect("server joins");

    let (tool_limited, state, tool_server) =
        in_process_session(ServerScenario::EndlessPagination).await;
    let tool_config = McpDiscoveryConfig::new()
        .with_max_tools(2)
        .expect("positive tool limit");
    let error = tokio::time::timeout(Duration::from_secs(1), tool_limited.discover(tool_config))
        .await
        .expect("tool limit must not hang")
        .expect_err("tool limit cannot publish a partial set");
    assert_eq!(error.kind(), McpAdapterErrorKind::DiscoveryFailed);
    assert_eq!(state.list_calls.load(Ordering::SeqCst), 3);
    tool_limited.shutdown().await.expect("shutdown succeeds");
    tool_server.await.expect("server joins");

    let (duplicate, _, duplicate_server) =
        in_process_session(ServerScenario::DuplicateRemoteTool).await;
    let error = duplicate
        .discover(McpDiscoveryConfig::new())
        .await
        .expect_err("duplicate remote Tool cannot publish a snapshot");
    assert_eq!(error.kind(), McpAdapterErrorKind::ToolNameConflict);
    duplicate.shutdown().await.expect("shutdown succeeds");
    duplicate_server.await.expect("server joins");
}

#[tokio::test]
async fn missing_capability_invalid_name_and_invalid_schema_are_structured() {
    let (missing, _, missing_server) = in_process_session(ServerScenario::CapabilityMissing).await;
    let error = missing
        .discover(McpDiscoveryConfig::new())
        .await
        .expect_err("tools capability is required");
    assert_eq!(error.kind(), McpAdapterErrorKind::CapabilityMissing);
    missing.shutdown().await.expect("shutdown succeeds");
    missing_server.await.expect("server joins");

    let (invalid_name, _, name_server) = in_process_session(ServerScenario::InvalidName).await;
    let error = invalid_name
        .discover(McpDiscoveryConfig::new())
        .await
        .expect_err("invalid remote name fails discovery");
    assert_eq!(error.kind(), McpAdapterErrorKind::InvalidToolDefinition);
    invalid_name.shutdown().await.expect("shutdown succeeds");
    name_server.await.expect("server joins");

    let (invalid_schema, _, schema_server) =
        in_process_session(ServerScenario::InvalidSchema).await;
    let error = invalid_schema
        .discover(McpDiscoveryConfig::new())
        .await
        .expect_err("registry rejects invalid schema");
    assert_eq!(error.kind(), McpAdapterErrorKind::InvalidToolDefinition);
    assert!(source_of::<jsonschema::ValidationError<'static>>(&error).is_some());
    invalid_schema.shutdown().await.expect("shutdown succeeds");
    schema_server.await.expect("server joins");
}

#[tokio::test]
async fn stdio_initialization_failure_preserves_io_source_and_redacts_executable() {
    let sentinel = "SECRET_MISSING_MCP_EXECUTABLE";
    let error = McpClientSession::connect_stdio(
        McpServerConfig::new(
            McpServerId::new("missing-child").expect("valid id"),
            sentinel,
        )
        .expect("syntactically valid config"),
    )
    .await
    .expect_err("missing executable fails initialization");
    assert_eq!(error.kind(), McpAdapterErrorKind::InitializationFailed);
    assert!(source_of::<std::io::Error>(&error).is_some());
    assert!(!format!("{error}").contains(sentinel));
    assert!(!format!("{error:?}").contains(sentinel));
}

#[tokio::test]
async fn initialization_json_rpc_error_is_exactly_protocol_and_source_preserving() {
    let state = Arc::new(ServerState::default());
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (server_read, server_write) = tokio::io::split(server);
    let server_state = Arc::clone(&state);
    let server = tokio::spawn(async move {
        support::server::serve(
            server_read,
            server_write,
            ServerScenario::InitializationProtocolError,
            server_state,
        )
        .await;
    });
    let error =
        McpClientSession::connect(McpServerId::new("init-protocol").expect("valid id"), client)
            .await
            .expect_err("JSON-RPC initialization error fails");
    assert_eq!(error.kind(), McpAdapterErrorKind::Protocol);
    assert!(source_of::<rmcp::service::ClientInitializeError>(&error).is_some());
    assert!(!format!("{error}").contains("SECRET_REMOTE_PROTOCOL_ERROR"));
    assert!(!format!("{error:?}").contains("SECRET_PROTOCOL_PAYLOAD"));
    server.await.expect("server joins");
}

#[tokio::test]
async fn namespaces_are_reversible_and_unprefixed_collisions_never_overwrite() {
    let (first_session, _, first_server) =
        in_process_session_with_id("alpha", ServerScenario::Standard).await;
    let (second_session, _, second_server) =
        in_process_session_with_id("beta", ServerScenario::Standard).await;
    let first = first_session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("first discovery");
    let second = second_session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("second discovery");
    let error = McpToolSet::combine([first, second]).expect_err("collision is explicit");
    assert_eq!(error.kind(), McpAdapterErrorKind::ToolNameConflict);

    let alpha = first_session
        .discover(McpDiscoveryConfig::new().with_name_policy(McpToolNamePolicy::ServerNamespace))
        .await
        .expect("namespaced alpha");
    let beta = second_session
        .discover(
            McpDiscoveryConfig::new().with_name_policy(McpToolNamePolicy::Prefix(
                McpToolPrefix::new("stable_beta").expect("valid prefix"),
            )),
        )
        .await
        .expect("namespaced beta");
    let combined = McpToolSet::combine([alpha, beta]).expect("namespaces avoid collision");
    assert_eq!(combined.len(), 28);
    let local = ToolName::new("alpha__echo").expect("valid local");
    assert_eq!(combined.remote_name(&local), Some("echo"));

    first_session.shutdown().await.expect("first shutdown");
    second_session.shutdown().await.expect("second shutdown");
    first_server.await.expect("first server joins");
    second_server.await.expect("second server joins");
}

#[tokio::test]
async fn behavior_overrides_are_validated_and_frozen_at_discovery() {
    let (session, _, server) = in_process_session(ServerScenario::Standard).await;
    let config = McpDiscoveryConfig::new()
        .with_behavior_override("echo", ToolBehavior::read_only())
        .expect("valid override");
    let set = session.discover(config).await.expect("discovery succeeds");
    let echo = ToolName::new("echo").expect("valid name");
    assert_eq!(
        set.registry()
            .behavior(&echo)
            .expect("echo behavior")
            .side_effect(),
        ToolSideEffect::ReadOnly
    );
    let calculator = ToolName::new("calculator").expect("valid name");
    assert_eq!(
        set.registry()
            .behavior(&calculator)
            .expect("calculator behavior")
            .side_effect(),
        ToolSideEffect::NonIdempotentWrite
    );

    let unknown = McpDiscoveryConfig::new()
        .with_behavior_override("not_discovered", ToolBehavior::read_only())
        .expect("override is syntactically valid");
    let error = session
        .discover(unknown)
        .await
        .expect_err("unknown override is rejected");
    assert_eq!(error.kind(), McpAdapterErrorKind::InvalidConfig);

    let inconsistent = McpDiscoveryConfig::new()
        .with_behavior_override(
            "echo",
            ToolBehavior::non_idempotent_write().with_required_idempotency_key(true),
        )
        .expect("override key is valid");
    let error = session
        .discover(inconsistent)
        .await
        .expect_err("registry validates behavior");
    assert_eq!(error.kind(), McpAdapterErrorKind::InvalidToolDefinition);

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[test]
fn duplicate_behavior_overrides_are_rejected_even_when_values_match() {
    let base = McpDiscoveryConfig::new()
        .with_behavior_override("echo", ToolBehavior::read_only())
        .expect("first override is accepted");
    let duplicate = base
        .clone()
        .with_behavior_override("echo", ToolBehavior::read_only())
        .expect_err("same-value duplicate is ambiguous");
    assert_eq!(duplicate, McpConfigError::DuplicateBehaviorOverride);
    assert_eq!(
        McpAdapterError::from(duplicate).kind(),
        McpAdapterErrorKind::InvalidConfig
    );

    let contradictory = base
        .with_behavior_override("echo", ToolBehavior::non_idempotent_write())
        .expect_err("conflicting duplicate is ambiguous");
    assert_eq!(contradictory, McpConfigError::DuplicateBehaviorOverride);
    assert_eq!(
        McpAdapterError::from(contradictory).kind(),
        McpAdapterErrorKind::InvalidConfig
    );
}

#[tokio::test]
async fn tool_set_debug_exposes_only_safe_summary_and_names_remain_explicit_accessors() {
    let (session, _, server) = in_process_session(ServerScenario::Standard).await;
    let set = session
        .discover(
            McpDiscoveryConfig::new().with_name_policy(McpToolNamePolicy::Prefix(
                McpToolPrefix::new("SECRET_LOCAL_PREFIX").expect("valid prefix"),
            )),
        )
        .await
        .expect("discovery succeeds");
    let local_name =
        ToolName::new("SECRET_LOCAL_PREFIX__protocol_error").expect("valid local name");
    assert_eq!(set.remote_name(&local_name), Some("protocol_error"));

    let debug = format!("{set:?}");
    assert!(debug.contains("tool_count"));
    assert!(debug.contains("name_policy_kinds"));
    assert!(!debug.contains("SECRET_LOCAL_PREFIX"));
    assert!(!debug.contains("protocol_error"));
    assert!(!debug.contains("echo"));

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[tokio::test]
async fn execution_maps_text_structured_order_business_error_and_call_id() {
    let (session, state, server) = in_process_session(ServerScenario::Standard).await;
    let runtime = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("discovery")
        .runtime();

    let echo_call = call("mcp-echo-id", "echo", json!({"text": "hello"}));
    let message = runtime
        .execute_message(&echo_call)
        .await
        .expect("echo succeeds");
    let Message::Tool(message) = message else {
        panic!("expected Tool message");
    };
    assert_eq!(message.tool_call_id(), echo_call.id());
    assert_eq!(text_parts(message.result()), ["hello"]);

    let structured = runtime
        .execute(&call("structured-id", "structured", json!({})))
        .await
        .expect("structured succeeds");
    assert_eq!(text_parts(&structured), [r#"{"answer":42,"stable":true}"#]);

    let multiple = runtime
        .execute(&call("multi-id", "multi_text", json!({})))
        .await
        .expect("multi text succeeds");
    assert_eq!(text_parts(&multiple), ["first", "second"]);

    let combined = runtime
        .execute(&call(
            "text-structured-id",
            "text_and_structured",
            json!({}),
        ))
        .await
        .expect("text and structured content succeed");
    assert_eq!(
        text_parts(&combined),
        ["text-first", r#"{"answer":42,"stable":true}"#]
    );

    let business = runtime
        .execute(&call("business-id", "business_error", json!({})))
        .await
        .expect("business failure is a ToolResult");
    assert!(business.is_error());
    assert_eq!(text_parts(&business), ["business rejected"]);
    assert_eq!(state.connections.load(Ordering::SeqCst), 1);
    assert_eq!(state.tool_calls.load(Ordering::SeqCst), 5);

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[tokio::test]
async fn invalid_arguments_fail_locally_without_an_mcp_request() {
    let (session, state, server) = in_process_session(ServerScenario::Standard).await;
    let runtime = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("discovery")
        .runtime();
    let sentinel = "SECRET_INVALID_MCP_ARGUMENT";
    let error = runtime
        .execute(&call(
            "invalid-id",
            "echo",
            json!({"text": 7, "secret": sentinel}),
        ))
        .await
        .expect_err("cached schema rejects arguments");
    assert_eq!(error.kind(), ToolRuntimeErrorKind::InvalidArguments);
    assert_eq!(state.tool_calls.load(Ordering::SeqCst), 0);
    assert!(!format!("{error}").contains(sentinel));
    assert!(!format!("{error:?}").contains(sentinel));

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[tokio::test]
async fn unsupported_and_protocol_failures_are_redacted_and_source_preserving() {
    let (session, _, server) = in_process_session(ServerScenario::Standard).await;
    let runtime = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("discovery")
        .runtime();

    for (tool_name, sentinel) in [
        ("unsupported_image", "SECRET_IMAGE_DATA"),
        ("unsupported_audio", "SECRET_AUDIO_DATA"),
        ("unsupported_resource", "SECRET_RESOURCE_TEXT"),
        ("unsupported_resource_link", "SECRET_RESOURCE_LINK"),
    ] {
        let unsupported = runtime
            .execute(&call("unsupported-id", tool_name, json!({})))
            .await
            .expect_err("non-text content fails closed");
        let adapter = source_of::<McpAdapterError>(&unsupported).expect("adapter source");
        assert_eq!(adapter.kind(), McpAdapterErrorKind::UnsupportedContent);
        assert!(!format!("{unsupported}").contains(sentinel));
        assert!(!format!("{unsupported:?}").contains(sentinel));
    }

    let protocol = runtime
        .execute(&call("protocol-id", "protocol_error", json!({})))
        .await
        .expect_err("JSON-RPC error is infrastructure failure");
    let adapter = source_of::<McpAdapterError>(&protocol).expect("adapter source");
    assert_eq!(adapter.kind(), McpAdapterErrorKind::Protocol);
    assert!(source_of::<rmcp::ServiceError>(&protocol).is_some());
    assert!(!format!("{protocol}").contains("SECRET_REMOTE_PROTOCOL_ERROR"));
    assert!(!format!("{protocol:?}").contains("SECRET_PROTOCOL_PAYLOAD"));
    assert!(!format!("{adapter}").contains("SECRET_REMOTE_PROTOCOL_ERROR"));
    assert!(!format!("{adapter:?}").contains("SECRET_PROTOCOL_PAYLOAD"));

    let malformed = runtime
        .execute(&call("malformed-id", "malformed", json!({})))
        .await
        .expect_err("unexpected result type fails");
    let adapter = source_of::<McpAdapterError>(&malformed).expect("adapter source");
    assert_eq!(adapter.kind(), McpAdapterErrorKind::Protocol);

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[tokio::test]
async fn disconnect_and_shutdown_make_future_calls_fail_with_reachable_sources() {
    let (disconnected, _, disconnected_server) =
        in_process_session(ServerScenario::DisconnectOnCall).await;
    let runtime = disconnected
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("discovery before disconnect")
        .runtime();
    let error = runtime
        .execute(&call("disconnect-id", "echo", json!({"text": "x"})))
        .await
        .expect_err("disconnect fails");
    let adapter = source_of::<McpAdapterError>(&error).expect("adapter source");
    assert_eq!(adapter.kind(), McpAdapterErrorKind::Transport);
    assert!(source_of::<rmcp::ServiceError>(&error).is_some());
    disconnected_server.await.expect("server joins");
    disconnected
        .shutdown()
        .await
        .expect("closed shutdown is safe");

    let (session, _, server) = in_process_session(ServerScenario::Standard).await;
    let runtime = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("discovery")
        .runtime();
    session.shutdown().await.expect("shutdown succeeds");
    let error = runtime
        .execute(&call("closed-id", "echo", json!({"text": "x"})))
        .await
        .expect_err("closed session rejects call");
    let adapter = source_of::<McpAdapterError>(&error).expect("adapter source");
    assert_eq!(adapter.kind(), McpAdapterErrorKind::SessionClosed);
    server.await.expect("server joins");
}

#[tokio::test]
async fn tool_runtime_timeout_drops_call_ownership_without_claiming_remote_rollback() {
    let (session, state, server) = in_process_session(ServerScenario::Standard).await;
    let runtime = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("discovery")
        .runtime();
    let pending_call = call("pending-timeout", "pending", json!({}));
    let execution = runtime.execute_with_options(
        &pending_call,
        ToolExecutionOptions::new().with_timeout(Duration::from_millis(20)),
    );
    let error = execution.await.expect_err("runtime timeout wins");
    assert_eq!(error.kind(), ToolRuntimeErrorKind::TimedOut);
    assert_eq!(state.tool_calls.load(Ordering::SeqCst), 1);
    state.pending_release.add_permits(1);
    tokio::task::yield_now().await;

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[tokio::test]
async fn directly_dropping_an_mcp_batch_future_drops_all_pending_call_ownership() {
    let (session, state, server) = in_process_session(ServerScenario::Standard).await;
    let runtime = session
        .discover(
            McpDiscoveryConfig::new()
                .with_behavior_override("pending", ToolBehavior::read_only())
                .expect("valid override"),
        )
        .await
        .expect("discovery")
        .runtime();
    let mut batch = Box::pin(runtime.execute_batch(
        vec![
            call("drop-pending-1", "pending", json!({})),
            call("drop-pending-2", "pending", json!({})),
        ],
        ToolBatchConfig::new(2),
    ));

    tokio::select! {
        result = &mut batch => panic!("pending batch unexpectedly completed: {result:?}"),
        () = wait_for_count(&state, 2) => {}
    }
    drop(batch);
    assert_eq!(state.tool_calls.load(Ordering::SeqCst), 2);
    state.pending_release.add_permits(2);
    for _ in 0..100 {
        if state.active_calls.load(Ordering::SeqCst) == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(state.active_calls.load(Ordering::SeqCst), 0);

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[tokio::test]
async fn default_non_idempotent_batch_is_serial_but_safe_override_can_overlap() {
    let (session, state, server) = in_process_session(ServerScenario::Standard).await;
    let default_runtime = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("default discovery")
        .runtime();
    let default_batch = tokio::spawn(async move {
        default_runtime
            .execute_batch(
                vec![
                    call("default-pending-1", "pending", json!({})),
                    call("default-pending-2", "pending", json!({})),
                ],
                ToolBatchConfig::new(2),
            )
            .await
    });
    wait_for_count(&state, 1).await;
    assert_eq!(state.tool_calls.load(Ordering::SeqCst), 1);
    state.pending_release.add_permits(1);
    wait_for_count(&state, 2).await;
    assert_eq!(state.max_active_calls.load(Ordering::SeqCst), 1);
    state.pending_release.add_permits(1);
    let report = default_batch
        .await
        .expect("batch task joins")
        .expect("serial batch completes");
    assert_eq!(report.len(), 2);

    state.tool_calls.store(0, Ordering::SeqCst);
    state.max_active_calls.store(0, Ordering::SeqCst);
    let parallel_runtime = session
        .discover(
            McpDiscoveryConfig::new()
                .with_behavior_override("pending", ToolBehavior::read_only())
                .expect("valid override"),
        )
        .await
        .expect("parallel discovery")
        .runtime();
    let parallel_batch = tokio::spawn(async move {
        parallel_runtime
            .execute_batch(
                vec![
                    call("parallel-pending-1", "pending", json!({})),
                    call("parallel-pending-2", "pending", json!({})),
                    call("parallel-pending-3", "pending", json!({})),
                ],
                ToolBatchConfig::new(2),
            )
            .await
    });
    wait_for_count(&state, 2).await;
    assert_eq!(state.max_active_calls.load(Ordering::SeqCst), 2);
    state.pending_release.add_permits(1);
    wait_for_count(&state, 3).await;
    assert_eq!(state.max_active_calls.load(Ordering::SeqCst), 2);
    state.pending_release.add_permits(2);
    let report = parallel_batch
        .await
        .expect("batch task joins")
        .expect("parallel batch completes");
    assert_eq!(report.len(), 3);

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[tokio::test]
async fn mcp_fail_fast_stops_scheduling_and_drains_started_pending_call_in_input_order() {
    let (session, state, server) = in_process_session(ServerScenario::Standard).await;
    let config = McpDiscoveryConfig::new()
        .with_behavior_override("pending", ToolBehavior::read_only())
        .expect("pending override")
        .with_behavior_override("unsupported_image", ToolBehavior::read_only())
        .expect("unsupported override")
        .with_behavior_override("echo", ToolBehavior::read_only())
        .expect("echo override");
    let runtime = session.discover(config).await.expect("discovery").runtime();
    let batch = tokio::spawn(async move {
        runtime
            .execute_batch(
                vec![
                    call("started-pending", "pending", json!({})),
                    call("trigger-failure", "unsupported_image", json!({})),
                    call("never-started", "echo", json!({"text": "must not run"})),
                ],
                ToolBatchConfig::new(2).with_failure_policy(ToolBatchFailurePolicy::FailFast),
            )
            .await
    });
    wait_for_count(&state, 2).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !batch.is_finished(),
        "fail-fast returned before draining pending MCP call"
    );
    state.pending_release.add_permits(1);
    let report = batch
        .await
        .expect("batch task joins")
        .expect("drained report");
    assert_eq!(
        text_parts(report.results()[0].as_ref().expect("pending succeeds")),
        ["released"]
    );
    assert_eq!(
        report.results()[1]
            .as_ref()
            .expect_err("unsupported content fails")
            .kind(),
        ToolRuntimeErrorKind::ExecutionFailed
    );
    assert_eq!(
        report.results()[2]
            .as_ref()
            .expect_err("third call never starts")
            .kind(),
        ToolRuntimeErrorKind::NotStartedDueToFailFast
    );
    assert_eq!(state.tool_calls.load(Ordering::SeqCst), 2);

    session.shutdown().await.expect("shutdown succeeds");
    server.await.expect("server joins");
}

#[test]
fn config_and_public_errors_redact_command_environment_and_payload_sentinels() {
    let config = McpServerConfig::new(
        McpServerId::new("safe-server").expect("valid id"),
        "SECRET_EXECUTABLE_PATH",
    )
    .expect("valid config")
    .with_arg("SECRET_ARGUMENT")
    .with_environment("SECRET_KEY", "SECRET_ENV_VALUE")
    .expect("valid environment");
    let debug = format!("{config:?}");
    assert!(!debug.contains("SECRET_EXECUTABLE_PATH"));
    assert!(!debug.contains("SECRET_ARGUMENT"));
    assert!(!debug.contains("SECRET_ENV_VALUE"));
    assert!(!debug.contains("SECRET_KEY"));
}

#[test]
fn zero_discovery_limits_are_invalid_while_zero_shutdown_grace_is_immediate() {
    let page_error = McpDiscoveryConfig::new()
        .with_max_pages(0)
        .expect_err("zero page limit is invalid");
    assert_eq!(page_error, McpConfigError::ZeroDiscoveryPageLimit);
    assert_eq!(
        McpAdapterError::from(page_error).kind(),
        McpAdapterErrorKind::InvalidConfig
    );
    let tool_error = McpDiscoveryConfig::new()
        .with_max_tools(0)
        .expect_err("zero tool limit is invalid");
    assert_eq!(tool_error, McpConfigError::ZeroDiscoveryToolLimit);
    assert_eq!(
        McpAdapterError::from(tool_error).kind(),
        McpAdapterErrorKind::InvalidConfig
    );

    let minimum = McpDiscoveryConfig::new()
        .with_max_pages(1)
        .expect("one page is valid")
        .with_max_tools(1)
        .expect("one tool is valid");
    assert_eq!(minimum.max_pages(), 1);
    assert_eq!(minimum.max_tools(), 1);

    let zero_grace = McpServerConfig::new(
        McpServerId::new("zero-grace").expect("valid id"),
        "SECRET_ZERO_GRACE_EXECUTABLE",
    )
    .expect("valid config")
    .with_shutdown_grace(Duration::ZERO)
    .expect("zero means immediate forced termination");
    let debug = format!("{zero_grace:?}");
    assert!(debug.contains("0ns"));
    assert!(!debug.contains("SECRET_ZERO_GRACE_EXECUTABLE"));
}

#[cfg(unix)]
#[tokio::test]
async fn child_stdio_session_reuses_process_shutdown_reaps_and_drop_leaves_no_child() {
    let executable = env!("CARGO_BIN_EXE_group-agent-mcp-test-server");
    let session = McpClientSession::connect_stdio(
        McpServerConfig::new(McpServerId::new("child").expect("valid id"), executable)
            .expect("valid child config"),
    )
    .await
    .expect("child initializes");
    let pid = session.child_process_id().expect("child pid");
    let set = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("child discovery");
    let result = set
        .runtime()
        .execute(&call("child-call", "echo", json!({"text": "child"})))
        .await
        .expect("child call");
    assert_eq!(text_parts(&result), ["child"]);
    let reported_pid = set
        .runtime()
        .execute(&call("child-pid-call", "child_pid", json!({})))
        .await
        .expect("child reports its PID");
    assert_eq!(text_parts(&reported_pid), [pid.to_string()]);
    session.shutdown().await.expect("child shutdown");
    assert!(!process_exists(pid));

    let abnormal = McpClientSession::connect_stdio(
        McpServerConfig::new(
            McpServerId::new("abnormal-child").expect("valid id"),
            executable,
        )
        .expect("valid child config")
        .with_arg("--disconnect-on-call"),
    )
    .await
    .expect("abnormal child initializes");
    let abnormal_pid = abnormal.child_process_id().expect("child pid");
    let abnormal_set = abnormal
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("child discovery");
    let error = abnormal_set
        .runtime()
        .execute(&call(
            "abnormal-child-call",
            "echo",
            json!({"text": "never returned"}),
        ))
        .await
        .expect_err("abnormal child exit fails the in-flight call");
    let adapter = source_of::<McpAdapterError>(&error).expect("adapter source");
    assert_eq!(adapter.kind(), McpAdapterErrorKind::Transport);
    abnormal.shutdown().await.expect("closed child shutdown");
    assert_process_exits(abnormal_pid);

    let dropped = McpClientSession::connect_stdio(
        McpServerConfig::new(
            McpServerId::new("dropped-child").expect("valid id"),
            executable,
        )
        .expect("valid child config"),
    )
    .await
    .expect("second child initializes");
    let dropped_pid = dropped.child_process_id().expect("child pid");
    let dropped_set = dropped
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("child discovery");
    drop(dropped_set);
    drop(dropped);
    for _ in 0..100 {
        if !process_exists(dropped_pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("dropping all session owners left the child process running");
}

#[cfg(unix)]
#[tokio::test]
async fn stubborn_child_shutdown_is_bounded_reaped_idempotent_and_shared_by_callers() {
    let executable = env!("CARGO_BIN_EXE_group-agent-mcp-test-server");
    let config = McpServerConfig::new(
        McpServerId::new("stubborn-shutdown").expect("valid id"),
        executable,
    )
    .expect("valid child config")
    .with_arg("--stubborn")
    .with_shutdown_grace(Duration::from_millis(30))
    .expect("positive grace");
    let session = McpClientSession::connect_stdio(config)
        .await
        .expect("stubborn child initializes");
    let pid = session.child_process_id().expect("child pid");
    let runtime = session
        .discover(McpDiscoveryConfig::new())
        .await
        .expect("child discovery")
        .runtime();

    let other = session.clone();
    let (first, concurrent) = tokio::join!(session.shutdown(), other.shutdown());
    first.expect("first shutdown succeeds");
    concurrent.expect("concurrent shutdown observes the same success");
    assert!(
        session.is_closed(),
        "CLOSED is published before shutdown waiters return"
    );
    session
        .shutdown()
        .await
        .expect("repeated shutdown is idempotent");
    assert_process_exits(pid);

    let error = runtime
        .execute(&call(
            "after-shutdown",
            "echo",
            json!({"text": "must not execute"}),
        ))
        .await
        .expect_err("shutdown rejects new calls");
    let adapter = source_of::<McpAdapterError>(&error).expect("adapter source");
    assert_eq!(adapter.kind(), McpAdapterErrorKind::SessionClosed);
}

#[cfg(unix)]
#[tokio::test]
async fn cancelling_first_shutdown_waiter_does_not_cancel_shared_child_cleanup() {
    let shutdown_marker = marker_path("cancelled-shutdown");
    let session = connect_stubborn_stdio_with_markers(
        "cancelled-shutdown",
        None,
        Some(&shutdown_marker),
        Duration::from_millis(500),
    )
    .await;
    let pid = session.child_process_id().expect("child pid");
    let first_session = session.clone();
    let first = tokio::spawn(async move { first_session.shutdown().await });
    wait_for_marker(&shutdown_marker).await;
    assert!(process_exists(pid), "child remains in its shutdown grace");

    first.abort();
    first.await.expect_err("first shutdown waiter is cancelled");
    let second_session = session.clone();
    let (second, concurrent) = tokio::join!(session.shutdown(), second_session.shutdown());
    second.expect("second caller observes completed cleanup");
    concurrent.expect("concurrent caller observes the same completion");
    assert!(
        session.is_closed(),
        "CLOSED is published before replacement waiters return"
    );
    assert_process_exits(pid);
    let marker = std::fs::read_to_string(&shutdown_marker).expect("marker is readable");
    assert_eq!(marker.lines().count(), 1, "rmcp close ran exactly once");
    remove_marker(&shutdown_marker);
}

#[cfg(unix)]
#[tokio::test]
async fn zero_shutdown_grace_checks_exit_then_immediately_kills_and_reaps() {
    let session =
        connect_stubborn_stdio_with_markers("zero-grace-child", None, None, Duration::ZERO).await;
    let pid = session.child_process_id().expect("child pid");
    session
        .shutdown()
        .await
        .expect("zero-grace shutdown succeeds");
    assert_process_exits(pid);
}

#[cfg(unix)]
#[test]
fn session_drop_after_runtime_teardown_force_terminates_and_reaps_stubborn_child() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime builds");
    let (session, pid) = runtime.block_on(async {
        let session = connect_stubborn_stdio("drop-after-runtime").await;
        let pid = session.child_process_id().expect("child pid");
        session
            .discover(McpDiscoveryConfig::new())
            .await
            .expect("child discovery");
        (session, pid)
    });
    drop(runtime);
    drop(session);
    assert_process_exits(pid);
}

#[cfg(unix)]
#[test]
fn runtime_teardown_with_last_session_owner_and_pending_call_leaves_no_child() {
    let pending_marker = marker_path("pending-runtime-teardown");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime builds");
    let pid = runtime.block_on(async {
        let session = connect_stubborn_stdio_with_markers(
            "pending-runtime-teardown",
            Some(&pending_marker),
            None,
            Duration::from_millis(30),
        )
        .await;
        let pid = session.child_process_id().expect("child pid");
        let runtime = session
            .discover(McpDiscoveryConfig::new())
            .await
            .expect("child discovery")
            .runtime();
        tokio::spawn(async move {
            let _ = runtime
                .execute(&call("teardown-pending", "pending", json!({})))
                .await;
        });
        wait_for_marker(&pending_marker).await;
        pid
    });
    drop(runtime);
    assert_process_exits(pid);
    remove_marker(&pending_marker);
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(unix)]
fn assert_process_exits(pid: u32) {
    for _ in 0..200 {
        if !process_exists(pid) {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("child process {pid} remained alive");
}

#[cfg(unix)]
async fn connect_stubborn_stdio(server_id: &str) -> McpClientSession {
    connect_stubborn_stdio_with_markers(server_id, None, None, Duration::from_millis(30)).await
}

#[cfg(unix)]
async fn connect_stubborn_stdio_with_markers(
    server_id: &str,
    pending_marker: Option<&Path>,
    shutdown_marker: Option<&Path>,
    shutdown_grace: Duration,
) -> McpClientSession {
    let mut config = McpServerConfig::new(
        McpServerId::new(server_id).expect("valid id"),
        env!("CARGO_BIN_EXE_group-agent-mcp-test-server"),
    )
    .expect("valid child config")
    .with_arg("--stubborn")
    .with_shutdown_grace(shutdown_grace)
    .expect("shutdown grace is valid");
    if let Some(marker) = pending_marker {
        config = config
            .with_arg("--pending-marker")
            .with_arg(marker.as_os_str());
    }
    if let Some(marker) = shutdown_marker {
        config = config
            .with_arg("--shutdown-marker")
            .with_arg(marker.as_os_str());
    }
    McpClientSession::connect_stdio(config)
        .await
        .expect("stubborn child initializes")
}

#[cfg(unix)]
fn marker_path(label: &str) -> PathBuf {
    static NEXT_MARKER: AtomicUsize = AtomicUsize::new(0);

    std::env::temp_dir().join(format!(
        "group-agent-mcp-{label}-{}-{}",
        std::process::id(),
        NEXT_MARKER.fetch_add(1, Ordering::Relaxed)
    ))
}

#[cfg(unix)]
async fn wait_for_marker(marker: &Path) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("server marker appears");
}

#[cfg(unix)]
fn remove_marker(marker: &Path) {
    if let Err(source) = std::fs::remove_file(marker) {
        assert_eq!(
            source.kind(),
            std::io::ErrorKind::NotFound,
            "marker cleanup failed"
        );
    }
}
