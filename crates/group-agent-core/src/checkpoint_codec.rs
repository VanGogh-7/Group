use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use thiserror::Error;

use crate::{CheckpointFormatVersion, InterruptPayload};

type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

/// Stable schema and encoding identity attached to one encoded checkpoint value.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CodecDescriptor {
    schema: Arc<str>,
    schema_version: u32,
    encoding: Arc<str>,
}

impl CodecDescriptor {
    /// Creates a descriptor from payload schema, schema version, and encoding.
    #[must_use]
    pub fn new(
        schema: impl Into<Arc<str>>,
        schema_version: u32,
        encoding: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            schema: schema.into(),
            schema_version,
            encoding: encoding.into(),
        }
    }

    /// Returns the stable schema identifier.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the payload schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Returns the stable codec or wire-encoding identifier.
    #[must_use]
    pub fn encoding(&self) -> &str {
        &self.encoding
    }
}

impl fmt::Display for CodecDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}@{} encoded-as {}",
            self.schema, self.schema_version, self.encoding
        )
    }
}

/// Storage-neutral bytes with explicit schema and encoding identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedValue {
    descriptor: CodecDescriptor,
    bytes: Arc<[u8]>,
}

impl EncodedValue {
    /// Creates an encoded value from stable schema/encoding metadata and bytes.
    #[must_use]
    pub fn new(descriptor: CodecDescriptor, bytes: impl Into<Arc<[u8]>>) -> Self {
        Self {
            descriptor,
            bytes: bytes.into(),
        }
    }

    /// Returns the schema and encoding metadata.
    #[must_use]
    pub const fn descriptor(&self) -> &CodecDescriptor {
        &self.descriptor
    }

    /// Returns the encoded bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// A user codec failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CheckpointCodecError {
    /// A process-local interrupt payload has no durable representation.
    #[error("interrupt payload type `{actual_type}` has no durable encoding")]
    UnsupportedInterruptPayload { actual_type: Arc<str> },
    /// A codec operation failed.
    #[error("{message}")]
    Failed {
        message: String,
        #[source]
        source: Option<BoxedError>,
    },
}

impl CheckpointCodecError {
    /// Creates a message-only codec error.
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
            source: None,
        }
    }

    /// Creates a codec error while preserving its source.
    #[must_use]
    pub fn with_source<E>(message: impl Into<String>, source: E) -> Self
    where
        E: Into<BoxedError>,
    {
        Self::Failed {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Creates the explicit local-only interrupt failure.
    #[must_use]
    pub fn unsupported_interrupt(payload: &InterruptPayload) -> Self {
        Self::UnsupportedInterruptPayload {
            actual_type: Arc::from(payload.type_name()),
        }
    }
}

/// Encodes and decodes one Snapshot type plus supported interrupt payloads.
///
/// Implementations define their own byte format and need not use Serde.
/// Methods are synchronous and are always called outside store locks. A codec
/// must produce deterministic, canonical bytes for one logical value because
/// record idempotency compares encoded content. Every descriptor emitted by one
/// codec must use the same encoding identity as its Snapshot descriptor.
pub trait CheckpointCodec<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    /// Returns the Snapshot schema and encoding accepted by this codec.
    fn snapshot_descriptor(&self) -> CodecDescriptor;

    /// Encodes a Snapshot into stable bytes.
    fn encode_snapshot(&self, snapshot: &T) -> Result<Vec<u8>, CheckpointCodecError>;

    /// Decodes Snapshot bytes after descriptor validation.
    fn decode_snapshot(&self, bytes: &[u8]) -> Result<T, CheckpointCodecError>;

    /// Encodes one durable interrupt payload.
    ///
    /// The default explicitly rejects process-local payloads.
    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        Err(CheckpointCodecError::unsupported_interrupt(payload))
    }

    /// Decodes one durable interrupt payload.
    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        Err(CheckpointCodecError::message(format!(
            "interrupt schema `{}` is unsupported",
            value.descriptor()
        )))
    }
}

/// Encoding failed before a checkpoint entered storage.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CheckpointEncodingError {
    /// Snapshot encoding failed.
    #[error("checkpoint snapshot encoding failed: {source}")]
    Snapshot {
        #[source]
        source: CheckpointCodecError,
    },
    /// Interrupt payload encoding failed.
    #[error("checkpoint interrupt payload encoding failed: {source}")]
    Interrupt {
        #[source]
        source: CheckpointCodecError,
    },
}

/// A stored record could not be reconstructed as a typed checkpoint.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CheckpointReconstructionError {
    /// The checkpoint record format is not supported.
    #[error("checkpoint format version {actual} is unsupported; this runtime supports {supported}")]
    FormatVersion {
        actual: CheckpointFormatVersion,
        supported: CheckpointFormatVersion,
    },
    /// The encoded Snapshot uses another codec or wire encoding.
    #[error("snapshot encoding `{actual}` does not match codec encoding `{expected}`")]
    SnapshotEncoding {
        expected: Arc<str>,
        actual: Arc<str>,
    },
    /// The encoded Snapshot uses another payload schema or schema version.
    #[error("snapshot schema `{actual}` does not match codec schema `{expected}`")]
    SnapshotSchema {
        expected: CodecDescriptor,
        actual: CodecDescriptor,
    },
    /// The encoded interrupt payload uses another codec or wire encoding.
    #[error("interrupt encoding `{actual}` does not match codec encoding `{expected}`")]
    InterruptEncoding {
        expected: Arc<str>,
        actual: Arc<str>,
    },
    /// A durable counter cannot be represented by this Runtime's `usize`.
    #[error("checkpoint {field} value {value} exceeds this Runtime's usize range")]
    CounterOutOfRange { field: &'static str, value: u64 },
    /// Snapshot decoding failed.
    #[error("checkpoint snapshot decoding failed: {source}")]
    Snapshot {
        #[source]
        source: CheckpointCodecError,
    },
    /// Interrupt payload decoding failed.
    #[error("checkpoint interrupt payload decoding failed: {source}")]
    Interrupt {
        #[source]
        source: CheckpointCodecError,
    },
}
