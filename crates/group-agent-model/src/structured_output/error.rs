use std::{error::Error, fmt};

/// Admission failure for the bounded Group schema profile.
#[non_exhaustive]
pub enum OutputSchemaError {
    InvalidName,
    UnsupportedProfile,
    LimitExceeded,
    Compilation(Box<jsonschema::ValidationError<'static>>),
}

/// Failure to produce a complete, schema-valid output.
#[non_exhaustive]
pub enum StructuredOutputError {
    Refused,
    Incomplete,
    InvalidToolRound,
    LimitExceeded,
    InvalidJson(serde_json::Error),
    SchemaMismatch(Box<jsonschema::ValidationError<'static>>),
}

/// A valid JSON result could not be converted to the application's Rust type.
pub struct OutputDecodeError(pub(crate) serde_json::Error);

macro_rules! safe_format {
    ($ty:ty, $message:literal) => {
        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str($message)
            }
        }
        impl fmt::Debug for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(self, f)
            }
        }
    };
}
safe_format!(OutputSchemaError, "structured output schema rejected");
safe_format!(StructuredOutputError, "structured output validation failed");
safe_format!(OutputDecodeError, "structured output Rust decoding failed");
impl Error for OutputSchemaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Compilation(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}
impl Error for StructuredOutputError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidJson(e) => Some(e),
            Self::SchemaMismatch(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}
impl Error for OutputDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
impl StructuredOutputError {
    /// Wraps this failure without exposing the response or schema in formatting.
    pub fn into_model_error(self) -> crate::ModelError {
        crate::ModelError::with_source(
            crate::ModelErrorKind::OutputValidation,
            "structured output validation failed",
            self,
        )
    }
}
