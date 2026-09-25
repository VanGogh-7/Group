use super::OutputSchemaError as Error;
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn admit(name: &str, schema: &Value) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Error::InvalidName);
    }
    let mut counts = Counts::default();
    visit(schema, 1, &mut counts)?;
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err(Error::UnsupportedProfile);
    }
    if serde_json::to_vec(schema)
        .map_err(|_| Error::UnsupportedProfile)?
        .len()
        > 65536
    {
        return Err(Error::LimitExceeded);
    }
    Ok(())
}
#[derive(Default)]
struct Counts {
    properties: usize,
    enums: usize,
    bytes: usize,
}
fn visit(schema: &Value, depth: usize, counts: &mut Counts) -> Result<(), Error> {
    if depth > 8 {
        return Err(Error::LimitExceeded);
    }
    let obj = schema.as_object().ok_or(Error::UnsupportedProfile)?;
    let kind = match obj.get("type") {
        Some(Value::String(s)) => s.as_str(),
        Some(Value::Array(types)) if types.len() == 2 => {
            let a = types[0].as_str().ok_or(Error::UnsupportedProfile)?;
            let b = types[1].as_str().ok_or(Error::UnsupportedProfile)?;
            match (a, b) {
                ("null", t) | (t, "null") if t != "null" => t,
                _ => return Err(Error::UnsupportedProfile),
            }
        }
        _ => return Err(Error::UnsupportedProfile),
    };
    for key in obj.keys() {
        let allowed = match key.as_str() {
            "type" | "description" => true,
            "properties" | "required" | "additionalProperties" => kind == "object",
            "items" => kind == "array",
            "enum" => kind == "string",
            _ => false,
        };
        if !allowed {
            return Err(Error::UnsupportedProfile);
        }
    }
    if let Some(description) = obj.get("description") {
        counts.bytes += description.as_str().ok_or(Error::UnsupportedProfile)?.len();
    }
    match kind {
        "object" => {
            let properties = obj
                .get("properties")
                .and_then(Value::as_object)
                .ok_or(Error::UnsupportedProfile)?;
            let required = obj
                .get("required")
                .and_then(Value::as_array)
                .ok_or(Error::UnsupportedProfile)?;
            if obj.get("additionalProperties") != Some(&Value::Bool(false))
                || required.len() != properties.len()
            {
                return Err(Error::UnsupportedProfile);
            }
            let mut names = BTreeSet::new();
            for name in required {
                let name = name.as_str().ok_or(Error::UnsupportedProfile)?;
                if !properties.contains_key(name) || !names.insert(name) {
                    return Err(Error::UnsupportedProfile);
                }
            }
            counts.properties += properties.len();
            if counts.properties > 256 {
                return Err(Error::LimitExceeded);
            }
            for (name, child) in properties {
                counts.bytes += name.len();
                if counts.bytes > 16384 {
                    return Err(Error::LimitExceeded);
                }
                visit(child, depth + 1, counts)?;
            }
        }
        "array" => visit(
            obj.get("items").ok_or(Error::UnsupportedProfile)?,
            depth + 1,
            counts,
        )?,
        "string" => {
            if let Some(values) = obj.get("enum") {
                let values = values.as_array().ok_or(Error::UnsupportedProfile)?;
                if values.is_empty() {
                    return Err(Error::UnsupportedProfile);
                }
                counts.enums += values.len();
                if counts.enums > 256 {
                    return Err(Error::LimitExceeded);
                }
                let mut seen = BTreeSet::new();
                for value in values {
                    let entry = if value.is_null() && obj["type"].is_array() {
                        None
                    } else {
                        Some(value.as_str().ok_or(Error::UnsupportedProfile)?)
                    };
                    if !seen.insert(entry) {
                        return Err(Error::UnsupportedProfile);
                    }
                    counts.bytes += entry.map_or(0, str::len);
                }
            }
        }
        "number" | "integer" | "boolean" => {}
        _ => return Err(Error::UnsupportedProfile),
    }
    if counts.bytes > 16384 {
        return Err(Error::LimitExceeded);
    }
    Ok(())
}

// Explicit key sorting remains stable even if a consumer enables preserve_order.
pub(super) fn canonical(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: std::collections::BTreeMap<_, _> = map.iter().collect();
            Value::Object(
                sorted
                    .into_iter()
                    .map(|(k, v)| (k.clone(), canonical(v)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
        _ => value.clone(),
    }
}
