mod error;
mod schema;
pub(crate) mod stream;
mod validate;

use crate::{ChatResponse, FinishReason};
pub use error::{OutputDecodeError, OutputSchemaError, StructuredOutputError};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{fmt, sync::Arc};

pub(crate) const MAX_TEXT: usize = 1024 * 1024;

/// Immutable compiled contract for the bounded `group-json-output/1` profile.
#[derive(Clone)]
pub struct StructuredOutput(Arc<Contract>);
struct Contract {
    name: String,
    schema: Value,
    id: String,
    validator: jsonschema::Validator,
}
impl StructuredOutput {
    /// Compiles an immutable contract using Group's closed schema profile.
    ///
    /// Rejects unsupported keywords and schemas exceeding the documented bounds.
    /// Contracts are cheap to clone; validation reuses the compiled schema.
    pub fn new(name: impl Into<String>, schema: Value) -> Result<Self, OutputSchemaError> {
        let name = name.into();
        schema::admit(&name, &schema)?;
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .build(&schema)
            .map_err(|e| OutputSchemaError::Compilation(Box::new(e.to_owned())))?;
        let canonical =
            schema::canonical(&serde_json::json!(["group-json-output/1", name, schema]));
        let encoded = serde_json::to_vec(&canonical).expect("admitted JSON serializes");
        let id = format!("{:x}", Sha256::digest(encoded));
        Ok(Self(Arc::new(Contract {
            name,
            schema,
            id,
            validator,
        })))
    }
    /// Returns the provider-facing contract name.
    pub fn name(&self) -> &str {
        &self.0.name
    }
    /// Returns the exact admitted schema, without provider rewriting.
    pub fn schema(&self) -> &Value {
        &self.0.schema
    }
    /// Returns the canonical profile/name/schema SHA-256 identity.
    pub fn contract_id(&self) -> &str {
        &self.0.id
    }

    /// Checks final output; a valid intermediate ToolCalls response returns None.
    pub fn validate_response(
        &self,
        response: &ChatResponse,
    ) -> Result<Option<ValidatedJsonOutput>, StructuredOutputError> {
        let mut bytes = 0usize;
        for part in response.message().content() {
            bytes = bytes
                .checked_add(
                    part.as_text()
                        .ok_or(StructuredOutputError::Incomplete)?
                        .len(),
                )
                .filter(|n| *n <= MAX_TEXT)
                .ok_or(StructuredOutputError::LimitExceeded)?;
        }
        if !response.message().tool_calls().is_empty() {
            if !matches!(response.finish_reason(), FinishReason::ToolCalls) {
                return Err(StructuredOutputError::InvalidToolRound);
            }
            let mut ids = std::collections::BTreeSet::new();
            if response
                .message()
                .tool_calls()
                .iter()
                .any(|c| !ids.insert(c.id()))
            {
                return Err(StructuredOutputError::InvalidToolRound);
            }
            return Ok(None);
        }
        if !matches!(response.finish_reason(), FinishReason::Stop) {
            return Err(StructuredOutputError::Incomplete);
        }
        let text = response.message().text_content();
        let value = validate::parse(&text).map_err(StructuredOutputError::InvalidJson)?;
        self.0
            .validator
            .validate(&value)
            .map_err(|e| StructuredOutputError::SchemaMismatch(Box::new(e.to_owned())))?;
        Ok(Some(ValidatedJsonOutput(value)))
    }
}
impl PartialEq for StructuredOutput {
    fn eq(&self, other: &Self) -> bool {
        self.0.name == other.0.name && self.0.schema == other.0.schema
    }
}
impl Eq for StructuredOutput {}
impl fmt::Debug for StructuredOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StructuredOutput")
            .field("name_bytes", &self.0.name.len())
            .finish_non_exhaustive()
    }
}
/// JSON that passed one output contract. Does not establish factual correctness.
#[derive(Clone, PartialEq)]
pub struct ValidatedJsonOutput(Value);
impl ValidatedJsonOutput {
    /// Borrows the validated JSON result.
    pub fn value(&self) -> &Value {
        &self.0
    }
    /// Converts locally to a Rust type; failure performs no model or Tool calls.
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, OutputDecodeError> {
        T::deserialize(&self.0).map_err(OutputDecodeError)
    }
}
impl fmt::Debug for ValidatedJsonOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ValidatedJsonOutput { .. }")
    }
}
