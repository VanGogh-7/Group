use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::stream;
use group_agent_model::{
    ChatStreamCollector, ChatStreamEvent, Extensions, FinishReason, ModelError, ModelErrorKind,
    ModelId, ResponseId, StreamProtocolError, TokenUsage, TokenUsageError, ToolCallDelta,
    ToolCallId, ToolName, collect_chat_stream,
};
use serde_json::json;

fn id(value: &str) -> ToolCallId {
    ToolCallId::new(value).expect("valid call id")
}

fn name(value: &str) -> ToolName {
    ToolName::new(value).expect("valid tool name")
}

fn extension(key: &str, value: serde_json::Value) -> Extensions {
    Extensions::new().with(key, value).expect("valid extension")
}

fn protocol_source(error: &ModelError) -> &StreamProtocolError {
    error
        .source()
        .and_then(|source| source.downcast_ref())
        .expect("stream protocol source")
}

fn assert_collector_is_permanently_failed(mut collector: ChatStreamCollector) {
    let push_error = collector
        .push(ChatStreamEvent::Finished(FinishReason::Stop))
        .expect_err("failed collector rejects every later event");
    assert!(matches!(
        protocol_source(&push_error),
        StreamProtocolError::CollectorAlreadyFailed
    ));

    let finish_error = collector
        .finish()
        .expect_err("failed collector cannot produce a response");
    assert!(matches!(
        protocol_source(&finish_error),
        StreamProtocolError::CollectorAlreadyFailed
    ));
}

#[tokio::test]
async fn text_deltas_are_concatenated_in_stream_order() {
    let response = collect_chat_stream(stream::iter([
        Ok(ChatStreamEvent::TextDelta("one".to_owned())),
        Ok(ChatStreamEvent::TextDelta("-two".to_owned())),
        Ok(ChatStreamEvent::TextDelta("-three".to_owned())),
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
    ]))
    .await
    .expect("valid stream");

    assert_eq!(response.message().text_content(), "one-two-three");
    assert_eq!(response.model(), None);
}

#[tokio::test]
async fn response_started_round_trips_every_reported_field() {
    let metadata = extension("provider.response", json!({"opaque": 1}));
    let response = collect_chat_stream(stream::iter([
        Ok(ChatStreamEvent::ResponseStarted {
            response_id: Some(ResponseId::new("response-1").expect("valid response id")),
            model: Some(ModelId::new("actual-model").expect("valid model id")),
            extensions: metadata.clone(),
        }),
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
    ]))
    .await
    .expect("valid stream");

    assert_eq!(
        response.response_id().map(ResponseId::as_str),
        Some("response-1")
    );
    assert_eq!(response.model().map(ModelId::as_str), Some("actual-model"));
    assert_eq!(response.extensions(), &metadata);
}

#[test]
fn duplicate_response_started_is_a_protocol_error() {
    let mut collector = ChatStreamCollector::new();
    let started = || ChatStreamEvent::ResponseStarted {
        response_id: None,
        model: None,
        extensions: Extensions::new(),
    };
    collector.push(started()).expect("first start");
    let error = collector.push(started()).expect_err("duplicate start");

    assert!(matches!(
        protocol_source(&error),
        StreamProtocolError::DuplicateResponseStarted
    ));
    assert_collector_is_permanently_failed(collector);
}

#[tokio::test]
async fn two_interleaved_tool_calls_remain_isolated_and_stably_sorted() {
    let response = collect_chat_stream(stream::iter([
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(1).with_name(name("second")),
        )),
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0).with_id(id("call-0")),
        )),
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(1).with_arguments_fragment("{\"n\":"),
        )),
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0).with_name(name("first")),
        )),
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0).with_arguments_fragment("{\"n\":0}"),
        )),
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(1)
                .with_id(id("call-1"))
                .with_arguments_fragment("1}"),
        )),
        Ok(ChatStreamEvent::Finished(FinishReason::ToolCalls)),
    ]))
    .await
    .expect("valid interleaving");

    let calls = response.message().tool_calls();
    assert_eq!(calls[0].id().as_str(), "call-0");
    assert_eq!(calls[0].name().as_str(), "first");
    assert_eq!(calls[0].arguments(), &json!({"n": 0}));
    assert_eq!(calls[1].id().as_str(), "call-1");
    assert_eq!(calls[1].name().as_str(), "second");
    assert_eq!(calls[1].arguments(), &json!({"n": 1}));
}

#[tokio::test]
async fn tool_call_extensions_merge_idempotently_and_round_trip() {
    let continuation = json!({"opaque": "value"});
    let response = collect_chat_stream(stream::iter([
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_id(id("call-0"))
                .with_extensions(extension("continuation", continuation.clone())),
        )),
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_name(name("lookup"))
                .with_arguments_fragment("{}")
                .with_extensions(extension("continuation", continuation)),
        )),
        Ok(ChatStreamEvent::Finished(FinishReason::ToolCalls)),
    ]))
    .await
    .expect("idempotent extensions");

    assert_eq!(
        response.message().tool_calls()[0]
            .extensions()
            .get("continuation"),
        Some(&json!({"opaque": "value"}))
    );
}

#[test]
fn conflicting_tool_call_extensions_are_rejected() {
    let mut collector = ChatStreamCollector::new();
    collector
        .push(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0).with_extensions(extension("key", json!(1))),
        ))
        .expect("first value");
    let error = collector
        .push(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0).with_extensions(extension("key", json!(2))),
        ))
        .expect_err("conflicting value");

    assert!(matches!(
        protocol_source(&error),
        StreamProtocolError::ConflictingToolCallExtension { index: 0, .. }
    ));
    assert_collector_is_permanently_failed(collector);
}

#[test]
fn tool_call_extension_growth_is_bounded_per_index() {
    let mut collector = ChatStreamCollector::new().with_max_tool_call_extensions(1);
    collector
        .push(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0).with_extensions(extension("first", json!(1))),
        ))
        .expect("first key");
    let error = collector
        .push(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0).with_extensions(extension("second", json!(2))),
        ))
        .expect_err("extension bound");

    assert!(matches!(
        protocol_source(&error),
        StreamProtocolError::ExtensionLimitExceeded {
            index: 0,
            maximum: 1
        }
    ));
    assert_collector_is_permanently_failed(collector);
}

#[test]
fn duplicate_id_or_name_is_a_protocol_error() {
    for (first, duplicate, field) in [
        (
            ToolCallDelta::new(0).with_id(id("call-0")),
            ToolCallDelta::new(0).with_id(id("call-0")),
            "id",
        ),
        (
            ToolCallDelta::new(0).with_name(name("lookup")),
            ToolCallDelta::new(0).with_name(name("lookup")),
            "name",
        ),
    ] {
        let mut collector = ChatStreamCollector::new();
        collector
            .push(ChatStreamEvent::ToolCallDelta(first))
            .expect("first field");
        let error = collector
            .push(ChatStreamEvent::ToolCallDelta(duplicate))
            .expect_err("duplicate field");
        assert!(matches!(
            protocol_source(&error),
            StreamProtocolError::DuplicateToolCallField {
                index: 0,
                field: actual
            } if *actual == field
        ));
        assert_collector_is_permanently_failed(collector);
    }
}

#[tokio::test]
async fn invalid_json_arguments_preserve_decode_source_chain() {
    let error = collect_chat_stream(stream::iter([
        Ok(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_id(id("call-0"))
                .with_name(name("lookup"))
                .with_arguments_fragment("{invalid"),
        )),
        Ok(ChatStreamEvent::Finished(FinishReason::ToolCalls)),
    ]))
    .await
    .expect_err("invalid JSON");

    assert!(matches!(error.kind(), ModelErrorKind::Decode));
    let protocol = protocol_source(&error);
    assert!(matches!(
        protocol,
        StreamProtocolError::InvalidToolArguments { index: 0, .. }
    ));
    assert!(protocol.source().is_some());
}

#[test]
fn finished_requires_complete_tool_call_identity() {
    for (delta, missing_id) in [
        (
            ToolCallDelta::new(0)
                .with_name(name("lookup"))
                .with_arguments_fragment("{}"),
            true,
        ),
        (
            ToolCallDelta::new(0)
                .with_id(id("call-0"))
                .with_arguments_fragment("{}"),
            false,
        ),
    ] {
        let mut collector = ChatStreamCollector::new();
        collector
            .push(ChatStreamEvent::ToolCallDelta(delta))
            .expect("partial call");
        let error = collector
            .push(ChatStreamEvent::Finished(FinishReason::ToolCalls))
            .expect_err("finished validates complete identity before commit");
        if missing_id {
            assert!(matches!(
                protocol_source(&error),
                StreamProtocolError::MissingToolCallId { index: 0 }
            ));
        } else {
            assert!(matches!(
                protocol_source(&error),
                StreamProtocolError::MissingToolCallName { index: 0 }
            ));
        }
        assert_collector_is_permanently_failed(collector);
    }
}

#[tokio::test]
async fn transport_eof_without_finished_is_rejected() {
    let error = collect_chat_stream(stream::iter([Ok(ChatStreamEvent::TextDelta(
        "partial".to_owned(),
    ))]))
    .await
    .expect_err("missing finish");

    assert!(matches!(
        protocol_source(&error),
        StreamProtocolError::MissingFinished
    ));
}

#[test]
fn every_event_variant_after_finished_is_rejected() {
    let events = [
        ChatStreamEvent::ResponseStarted {
            response_id: None,
            model: None,
            extensions: Extensions::new(),
        },
        ChatStreamEvent::TextDelta("late".to_owned()),
        ChatStreamEvent::ToolCallDelta(ToolCallDelta::new(0)),
        ChatStreamEvent::Usage(TokenUsage::new()),
        ChatStreamEvent::Finished(FinishReason::Stop),
    ];

    for event in events {
        let expected = match &event {
            ChatStreamEvent::ResponseStarted { .. } => "response_started",
            ChatStreamEvent::TextDelta(_) => "text_delta",
            ChatStreamEvent::ToolCallDelta(_) => "tool_call_delta",
            ChatStreamEvent::Usage(_) => "usage",
            ChatStreamEvent::Finished(_) => "finished",
            _ => unreachable!("test covers every current event variant"),
        };
        let mut collector = ChatStreamCollector::new();
        collector
            .push(ChatStreamEvent::Finished(FinishReason::Stop))
            .expect("first finish");
        let error = collector.push(event).expect_err("post-finish event");
        assert!(matches!(
            protocol_source(&error),
            StreamProtocolError::EventAfterFinished { event } if *event == expected
        ));
    }
}

#[derive(Debug)]
struct RootCause;

impl fmt::Display for RootCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("root cause")
    }
}

impl StdError for RootCause {}

#[tokio::test]
async fn first_stream_item_error_stops_polling_and_preserves_source() {
    let polls = Arc::new(AtomicUsize::new(0));
    let stream_polls = Arc::clone(&polls);
    let input = stream::poll_fn(move |_| {
        let poll = stream_polls.fetch_add(1, Ordering::SeqCst);
        std::task::Poll::Ready(match poll {
            0 => Some(Ok(ChatStreamEvent::TextDelta("partial".to_owned()))),
            1 => Some(Err(ModelError::with_source(
                ModelErrorKind::ProviderUnavailable,
                "provider failed",
                RootCause,
            ))),
            2 => Some(Ok(ChatStreamEvent::Finished(FinishReason::Stop))),
            _ => None,
        })
    });

    let error = collect_chat_stream(input).await.expect_err("item error");

    assert!(matches!(error.kind(), ModelErrorKind::ProviderUnavailable));
    assert!(
        error
            .source()
            .is_some_and(|source| source.is::<RootCause>())
    );
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cumulative_partial_usage_merges_without_clearing_known_fields() {
    let response = collect_chat_stream(stream::iter([
        Ok(ChatStreamEvent::Usage(
            TokenUsage::from_parts(Some(2), None, None).expect("usage"),
        )),
        Ok(ChatStreamEvent::Usage(
            TokenUsage::from_parts(None, Some(3), None).expect("usage"),
        )),
        Ok(ChatStreamEvent::Usage(
            TokenUsage::from_parts(None, None, Some(6)).expect("usage"),
        )),
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
    ]))
    .await
    .expect("valid stream");

    let usage = response.usage().expect("partial usage retained");
    assert_eq!(usage.input_tokens(), Some(2));
    assert_eq!(usage.output_tokens(), Some(3));
    assert_eq!(usage.total_tokens(), Some(6));
}

#[tokio::test]
async fn cumulative_usage_must_not_decrease() {
    let error = collect_chat_stream(stream::iter([
        Ok(ChatStreamEvent::Usage(
            TokenUsage::from_parts(Some(5), None, None).expect("usage"),
        )),
        Ok(ChatStreamEvent::Usage(
            TokenUsage::from_parts(Some(4), None, None).expect("usage"),
        )),
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
    ]))
    .await
    .expect_err("counter decreased");

    assert!(matches!(
        protocol_source(&error),
        StreamProtocolError::InvalidUsage {
            source: TokenUsageError::CounterDecreased {
                field: "input_tokens",
                previous: 5,
                current: 4
            }
        }
    ));
}

#[tokio::test]
async fn cumulative_usage_extensions_merge_and_conflicts_fail() {
    let first = TokenUsage::from_parts(Some(1), None, None)
        .expect("usage")
        .with_extensions(extension("cached", json!(1)));
    let second = TokenUsage::from_parts(None, Some(2), None)
        .expect("usage")
        .with_extensions(extension("cached", json!(1)));
    let response = collect_chat_stream(stream::iter([
        Ok(ChatStreamEvent::Usage(first)),
        Ok(ChatStreamEvent::Usage(second)),
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
    ]))
    .await
    .expect("idempotent usage extension");
    assert_eq!(
        response.usage().expect("usage").extensions().get("cached"),
        Some(&json!(1))
    );

    let error = collect_chat_stream(stream::iter([
        Ok(ChatStreamEvent::Usage(
            TokenUsage::new().with_extensions(extension("cached", json!(1))),
        )),
        Ok(ChatStreamEvent::Usage(
            TokenUsage::new().with_extensions(extension("cached", json!(2))),
        )),
        Ok(ChatStreamEvent::Finished(FinishReason::Stop)),
    ]))
    .await
    .expect_err("usage extension conflict");
    assert!(matches!(
        protocol_source(&error),
        StreamProtocolError::InvalidUsage {
            source: TokenUsageError::ExtensionConflict(_)
        }
    ));
}

#[test]
fn sparse_tool_index_is_bounded_without_vector_growth() {
    let mut collector = ChatStreamCollector::new().with_max_tool_call_index(8);
    let error = collector
        .push(ChatStreamEvent::ToolCallDelta(ToolCallDelta::new(
            1_000_000,
        )))
        .expect_err("large sparse index");

    assert!(matches!(
        protocol_source(&error),
        StreamProtocolError::ToolCallIndexTooLarge {
            index: 1_000_000,
            maximum: 8
        }
    ));
    assert_collector_is_permanently_failed(collector);
}

#[test]
fn failed_delta_that_would_complete_json_cannot_be_recovered() {
    let mut collector = ChatStreamCollector::new();
    collector
        .push(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_id(id("call-0"))
                .with_name(name("lookup"))
                .with_arguments_fragment("{\"ok\":")
                .with_extensions(extension("continuation", json!(1))),
        ))
        .expect("first fragment");

    let error = collector
        .push(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_arguments_fragment("true}")
                .with_extensions(
                    Extensions::try_from_iter([
                        ("added-before-conflict", json!(2)),
                        ("continuation", json!("conflict")),
                    ])
                    .expect("valid fragment extensions"),
                ),
        ))
        .expect_err("extension conflict rejects the complete delta");

    assert!(matches!(
        protocol_source(&error),
        StreamProtocolError::ConflictingToolCallExtension { index: 0, .. }
    ));
    assert_collector_is_permanently_failed(collector);
}

#[test]
fn text_and_tool_argument_limits_are_checked_before_append() {
    let mut text = ChatStreamCollector::new().with_max_text_bytes(3);
    text.push(ChatStreamEvent::TextDelta("abc".to_owned()))
        .expect("text at limit");
    let error = text
        .push(ChatStreamEvent::TextDelta("d".to_owned()))
        .expect_err("text over limit");
    assert!(matches!(
        protocol_source(&error),
        StreamProtocolError::TextLimitExceeded { maximum: 3 }
    ));
    assert_collector_is_permanently_failed(text);

    let mut arguments = ChatStreamCollector::new().with_max_tool_argument_bytes(2);
    arguments
        .push(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0).with_arguments_fragment("{}"),
        ))
        .expect("arguments at limit");
    let error = arguments
        .push(ChatStreamEvent::ToolCallDelta(
            ToolCallDelta::new(0)
                .with_id(id("call-0"))
                .with_name(name("lookup"))
                .with_arguments_fragment("x"),
        ))
        .expect_err("arguments over limit");
    assert!(matches!(
        protocol_source(&error),
        StreamProtocolError::ToolArgumentsLimitExceeded {
            index: 0,
            maximum: 2
        }
    ));
    assert_collector_is_permanently_failed(arguments);
}

#[test]
fn decreasing_usage_poisons_manual_collector() {
    let mut collector = ChatStreamCollector::new();
    collector
        .push(ChatStreamEvent::Usage(
            TokenUsage::from_parts(Some(5), None, None).expect("usage"),
        ))
        .expect("first usage");
    let error = collector
        .push(ChatStreamEvent::Usage(
            TokenUsage::from_parts(Some(4), Some(2), None).expect("standalone usage"),
        ))
        .expect_err("decreasing usage");

    assert!(matches!(
        protocol_source(&error),
        StreamProtocolError::InvalidUsage {
            source: TokenUsageError::CounterDecreased {
                field: "input_tokens",
                previous: 5,
                current: 4
            }
        }
    ));
    assert_collector_is_permanently_failed(collector);
}
