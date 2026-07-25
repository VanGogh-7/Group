use group_agent_model::{
    AssistantMessage, ChatResponse, Extensions, FinishReason, ModelId, ResponseId, TokenUsage,
    TokenUsageError,
};
use serde_json::json;

fn model_id() -> ModelId {
    ModelId::new("mock-v1").expect("valid model id")
}

#[test]
fn response_level_usage_and_partial_counters_are_distinct() {
    let absent = ChatResponse::new(AssistantMessage::text("hello"), FinishReason::Stop);
    let unknown = TokenUsage::new();
    let reported = ChatResponse::new(AssistantMessage::text("hello"), FinishReason::Stop)
        .with_usage(unknown.clone());

    assert_eq!(absent.usage(), None);
    assert_eq!(reported.usage(), Some(&unknown));
    assert_eq!(unknown.input_tokens(), None);
    assert_eq!(unknown.output_tokens(), None);
    assert_eq!(unknown.total_tokens(), None);
}

#[test]
fn every_partial_usage_shape_is_lossless() {
    let cases = [
        (None, None, None),
        (Some(10), None, None),
        (None, Some(5), None),
        (None, None, Some(20)),
        (Some(10), Some(5), None),
        (Some(10), Some(5), Some(18)),
    ];

    for (input, output, total) in cases {
        let usage = TokenUsage::from_parts(input, output, total).expect("consistent usage");
        assert_eq!(usage.input_tokens(), input);
        assert_eq!(usage.output_tokens(), output);
        assert_eq!(usage.total_tokens(), total);
    }
}

#[test]
fn computed_and_explicit_totals_follow_documented_rules() {
    let computed =
        TokenUsage::from_parts(Some(10), Some(5), None).expect("computed total is valid");
    assert_eq!(computed.checked_computed_total(), Ok(Some(15)));
    assert_eq!(computed.effective_total(), Ok(Some(15)));

    let explicit =
        TokenUsage::from_parts(Some(10), Some(5), Some(18)).expect("extra tokens are valid");
    assert_eq!(explicit.checked_computed_total(), Ok(Some(15)));
    assert_eq!(explicit.effective_total(), Ok(Some(18)));

    assert!(matches!(
        TokenUsage::from_parts(Some(10), Some(5), Some(14)),
        Err(TokenUsageError::InconsistentTotal { .. })
    ));
    assert!(matches!(
        TokenUsage::from_parts(Some(10), None, Some(9)),
        Err(TokenUsageError::InconsistentTotal { .. })
    ));
    assert!(matches!(
        TokenUsage::from_parts(None, Some(5), Some(4)),
        Err(TokenUsageError::InconsistentTotal { .. })
    ));
    assert!(matches!(
        TokenUsage::from_parts(Some(u64::MAX), Some(1), None),
        Err(TokenUsageError::TotalOverflow { .. })
    ));
}

#[test]
fn response_keeps_finish_identity_model_and_extensions() {
    let extensions = Extensions::new()
        .with("provider.field", json!("value"))
        .expect("valid extension");
    let response = ChatResponse::new(
        AssistantMessage::text("hello"),
        FinishReason::Other("provider-stop".to_owned()),
    )
    .with_response_id(ResponseId::new("response-1").expect("valid response id"))
    .with_model(model_id())
    .with_extensions(extensions.clone());

    assert_eq!(
        response.finish_reason(),
        &FinishReason::Other("provider-stop".to_owned())
    );
    assert_eq!(
        response.response_id().map(ResponseId::as_str),
        Some("response-1")
    );
    assert_eq!(response.model(), Some(&model_id()));
    assert_eq!(response.extensions(), &extensions);
}

#[test]
fn response_identity_remains_unknown_when_not_reported() {
    let response = ChatResponse::new(AssistantMessage::text("hello"), FinishReason::Stop);

    assert_eq!(response.response_id(), None);
    assert_eq!(response.model(), None);
}

#[test]
fn usage_merge_is_in_place_atomic_and_none_preserving() {
    let existing_extensions =
        Extensions::try_from_iter([("a", json!({"large": [1, 2, 3]})), ("m", json!("retained"))])
            .expect("valid extensions");
    let mut usage = TokenUsage::from_parts(Some(10), Some(5), Some(20))
        .expect("valid usage")
        .with_extensions(existing_extensions.clone());

    usage
        .merge_snapshot(
            TokenUsage::from_parts(None, Some(8), None)
                .expect("valid snapshot")
                .with_extensions(
                    Extensions::try_from_iter([
                        ("z", json!(3)),
                        ("a", json!({"large": [1, 2, 3]})),
                    ])
                    .expect("valid extensions"),
                ),
        )
        .expect("valid cumulative merge");

    assert_eq!(usage.input_tokens(), Some(10));
    assert_eq!(usage.output_tokens(), Some(8));
    assert_eq!(usage.total_tokens(), Some(20));
    assert_eq!(usage.extensions().get("a"), existing_extensions.get("a"));
    assert_eq!(
        usage.extensions().keys().collect::<Vec<_>>(),
        ["a", "m", "z"]
    );
}

#[test]
fn usage_counter_failure_does_not_change_extensions() {
    let mut usage = TokenUsage::from_parts(Some(10), None, None)
        .expect("valid usage")
        .with_extensions(
            Extensions::new()
                .with("existing", json!(1))
                .expect("valid extensions"),
        );
    let before = usage.clone();
    let snapshot = TokenUsage::from_parts(Some(9), None, None)
        .expect("valid standalone snapshot")
        .with_extensions(
            Extensions::new()
                .with("new", json!(2))
                .expect("valid extensions"),
        );

    assert!(matches!(
        usage.merge_snapshot(snapshot),
        Err(TokenUsageError::CounterDecreased {
            field: "input_tokens",
            previous: 10,
            current: 9
        })
    ));
    assert_eq!(usage, before);
}

#[test]
fn usage_extension_failure_does_not_change_counters_or_insert_earlier_keys() {
    let mut usage = TokenUsage::from_parts(Some(10), None, None)
        .expect("valid usage")
        .with_extensions(
            Extensions::new()
                .with("m", json!(1))
                .expect("valid extensions"),
        );
    let before = usage.clone();
    let snapshot = TokenUsage::from_parts(Some(12), Some(3), None)
        .expect("valid standalone snapshot")
        .with_extensions(
            Extensions::try_from_iter([
                ("a", json!("would-be-new")),
                ("m", json!("conflict")),
                ("z", json!("would-be-new")),
            ])
            .expect("valid extensions"),
        );

    assert!(matches!(
        usage.merge_snapshot(snapshot),
        Err(TokenUsageError::ExtensionConflict(_))
    ));
    assert_eq!(usage, before);
}

#[test]
fn usage_large_existing_extensions_merge_only_new_entries() {
    let existing = Extensions::try_from_iter(
        (0..256).map(|index| (format!("existing.{index:03}"), json!({"value": index}))),
    )
    .expect("valid extensions");
    let retained = existing.get("existing.000").cloned();
    let mut usage = TokenUsage::new().with_extensions(existing);

    usage
        .merge_snapshot(
            TokenUsage::new().with_extensions(
                Extensions::new()
                    .with("new.entry", json!({"value": 256}))
                    .expect("valid extension"),
            ),
        )
        .expect("single new extension merges");

    assert_eq!(usage.extensions().len(), 257);
    assert_eq!(usage.extensions().get("existing.000"), retained.as_ref());
    assert_eq!(usage.extensions().keys().next(), Some("existing.000"));
    assert_eq!(usage.extensions().keys().last(), Some("new.entry"));
}
