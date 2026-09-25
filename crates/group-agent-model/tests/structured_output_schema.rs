#![cfg(feature = "structured-output")]
use group_agent_model::{AssistantMessage, ChatResponse, FinishReason, StructuredOutput};
use serde_json::{Value, json};
use std::error::Error;

fn object(child: Value) -> Value {
    json!({"type":"object","properties":{"answer":child},"required":["answer"],"additionalProperties":false})
}
fn accepts(schema: Value) -> bool {
    StructuredOutput::new("answer", schema).is_ok()
}
#[test]
fn schema_boundaries_are_inclusive() {
    for count in [256, 257] {
        let props: serde_json::Map<String, Value> = (0..count)
            .map(|n| (format!("p{n}"), json!({"type":"string"})))
            .collect();
        let required: Vec<_> = props.keys().cloned().collect();
        assert_eq!(
            accepts(
                json!({"type":"object","properties":props,"required":required,"additionalProperties":false})
            ),
            count == 256
        );
        let values: Vec<_> = (0..count).map(|n| format!("e{n}")).collect();
        assert_eq!(
            accepts(object(json!({"type":"string","enum":values}))),
            count == 256
        );
    }
    for depth in [8, 9] {
        let mut child = json!({"type":"string"});
        for _ in 0..depth - 2 {
            child = json!({"type":"array","items":child});
        }
        assert_eq!(accepts(object(child)), depth == 8);
    }
    for bytes in [16384, 16385] {
        assert_eq!(
            accepts(object(
                json!({"type":"string","description":"x".repeat(bytes - 6)})
            )),
            bytes == 16384
        );
    }
    for size in [65536, 65537] {
        let mut schema = object(json!({"type":"string","description":""}));
        let remaining = size - serde_json::to_vec(&schema).unwrap().len();
        schema["properties"]["answer"]["description"] =
            json!("\0".repeat(remaining / 6) + &"x".repeat(remaining % 6));
        assert_eq!(serde_json::to_vec(&schema).unwrap().len(), size);
        assert_eq!(accepts(schema), size == 65536);
    }
}
#[test]
fn nullable_enum_and_keyword_placement_are_checked() {
    let output = StructuredOutput::new(
        "answer",
        object(json!({"type":["string","null"],"enum":["yes",null]})),
    )
    .unwrap();
    for text in [r#"{"answer":"yes"}"#, r#"{"answer":null}"#] {
        assert!(
            output
                .validate_response(&ChatResponse::new(
                    AssistantMessage::text(text),
                    FinishReason::Stop
                ))
                .is_ok()
        );
    }
    for child in [
        json!({"type":"string","items":{"type":"string"}}),
        json!({"type":"integer","enum":[1]}),
        json!({"type":"string","enum":[null]}),
        json!({"type":"string","enum":["a","a"]}),
    ] {
        assert!(!accepts(object(child)));
    }
    let mut repeated = object(json!({"type":"string"}));
    repeated["properties"]["second"] = json!({"type":"string"});
    repeated["required"] = json!(["answer", "answer"]);
    assert!(!accepts(repeated));
}
#[test]
fn identity_preserves_annotations_and_array_order() {
    let base = object(json!({"type":"string","enum":["a","b"]}));
    let id = StructuredOutput::new("answer", base.clone()).unwrap();
    let mut annotated = base.clone();
    annotated["description"] = json!("annotation");
    let mut reordered = base;
    reordered["properties"]["answer"]["enum"] = json!(["b", "a"]);
    for changed in [annotated, reordered] {
        assert_ne!(
            id.contract_id(),
            StructuredOutput::new("answer", changed)
                .unwrap()
                .contract_id()
        );
    }
}
#[test]
fn concrete_sources_remain_reachable_without_default_payload_display() {
    let output = StructuredOutput::new("answer", object(json!({"type":"integer"}))).unwrap();
    for (text, json_error) in [
        ("SECRET_INVALID", true),
        (r#"{"answer":"SECRET_VALUE"}"#, false),
    ] {
        let error = output
            .validate_response(&ChatResponse::new(
                AssistantMessage::text(text),
                FinishReason::Stop,
            ))
            .unwrap_err();
        assert!(!format!("{error:?} {error}").contains("SECRET"));
        let source = error.source().unwrap();
        if json_error {
            assert!(source.is::<serde_json::Error>());
        } else {
            assert!(source.is::<jsonschema::ValidationError<'static>>());
        }
    }
    let value = output
        .validate_response(&ChatResponse::new(
            AssistantMessage::text(r#"{"answer":1}"#),
            FinishReason::Stop,
        ))
        .unwrap()
        .unwrap();
    let error = value.deserialize::<String>().unwrap_err();
    assert!(error.source().unwrap().is::<serde_json::Error>());
    assert!(!format!("{error:?} {error}").contains("answer"));
}
