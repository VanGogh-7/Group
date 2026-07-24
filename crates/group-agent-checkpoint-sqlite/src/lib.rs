//! SQLx-backed SQLite persistence for Group checkpoint records.
//!
//! The application still owns checkpoint encoding through
//! [`group_agent_core::CheckpointCodec`]. This crate stores and reconstructs
//! only the storage-neutral [`group_agent_core::CheckpointRecord`] model.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    CheckpointFormatVersion, CheckpointId, CheckpointRecord, CheckpointRecordError,
    CheckpointRecordInterrupt, CheckpointRecordParts, CheckpointStore, CheckpointWriteError,
    CheckpointerError, CodecDescriptor, EncodedValue, GraphPath, GraphVersion, InterruptId,
    NodePath, RunId, ThreadId,
};
use serde::{Deserialize, Serialize};
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use thiserror::Error;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const SELECT_RECORD_COLUMNS: &str = "\
    sequence, checkpoint_id, thread_id, run_id, parent_id, graph_version, \
    format_version, superstep_be, step_be, snapshot_schema, \
    snapshot_schema_version, snapshot_encoding, snapshot_bytes, frontier_json, \
    completed, interrupt_id, interrupt_node_path_json, interrupt_schema, \
    interrupt_schema_version, interrupt_encoding, interrupt_bytes";

/// SQLx SQLite failures exposed by the durable store adapter.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SqliteCheckpointError {
    /// Opening or using SQLite failed.
    #[error("SQLite checkpoint {operation} failed")]
    Database {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    /// Applying embedded migrations failed.
    #[error("SQLite checkpoint migration failed")]
    Migration {
        #[source]
        source: MigrateError,
    },
    /// A record could not be encoded into the adapter's stable storage layout.
    #[error("SQLite checkpoint record encoding failed")]
    Encode {
        #[source]
        source: SqliteRecordError,
    },
    /// Stored fields could not be reconstructed as a checkpoint record.
    #[error("SQLite contains an invalid checkpoint record")]
    CorruptRecord {
        #[source]
        source: SqliteRecordError,
    },
}

/// Why a SQLite row is not a lossless Group checkpoint record.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SqliteRecordError {
    /// SQLx could not decode a column using the required storage type.
    #[error("checkpoint column `{field}` has an invalid SQLite type")]
    Column {
        field: &'static str,
        #[source]
        source: sqlx::Error,
    },
    /// A UUID column was not the stable 16-byte representation.
    #[error("checkpoint column `{field}` must contain exactly 16 UUID bytes, found {length}")]
    InvalidUuidLength { field: &'static str, length: usize },
    /// A fixed-width counter was not its stable 8-byte big-endian form.
    #[error("checkpoint column `{field}` must contain exactly 8 bytes, found {length}")]
    InvalidCounterLength { field: &'static str, length: usize },
    /// A SQLite integer did not fit the public unsigned field.
    #[error("checkpoint column `{field}` contains out-of-range integer {value}")]
    IntegerOutOfRange { field: &'static str, value: i64 },
    /// A stored boolean was not encoded as zero or one.
    #[error("checkpoint column `{field}` contains invalid boolean {value}")]
    InvalidBoolean { field: &'static str, value: i64 },
    /// Structured path JSON was malformed.
    #[error("checkpoint column `{field}` contains invalid structured path JSON")]
    PathJson {
        field: &'static str,
        #[source]
        source: serde_json::Error,
    },
    /// A stored node path did not contain a leaf segment.
    #[error("checkpoint column `{field}` contains an empty node path")]
    EmptyNodePath { field: &'static str },
    /// Interrupt columns were only partially populated.
    #[error("checkpoint interrupt columns must be either all NULL or all populated")]
    PartialInterrupt,
    /// Reconstructed fields violate the frozen durable Record contract.
    #[error("checkpoint record fields are structurally incompatible")]
    Record {
        #[source]
        source: CheckpointRecordError,
    },
}

/// Production SQLite implementation of Group's storage-neutral checkpoint port.
#[derive(Clone, Debug)]
pub struct SqliteCheckpointStore {
    pool: SqlitePool,
}

impl SqliteCheckpointStore {
    /// Opens a pooled SQLite database.
    ///
    /// File databases are created when absent. Connections use foreign-key
    /// enforcement, WAL journaling, and a five-second busy timeout. Call
    /// [`Self::migrate`] before using the store.
    pub async fn connect(database_url: impl AsRef<str>) -> Result<Self, SqliteCheckpointError> {
        let options = SqliteConnectOptions::from_str(database_url.as_ref())
            .map_err(|source| database_error("connection configuration", source))?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(|source| database_error("connection", source))?;
        Ok(Self { pool })
    }

    /// Wraps an application-managed SQLx SQLite pool.
    ///
    /// The application is responsible for connection options such as foreign
    /// keys, WAL, and busy timeout when constructing the pool.
    #[must_use]
    pub const fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Applies this crate's embedded migrations.
    pub async fn migrate(&self) -> Result<(), SqliteCheckpointError> {
        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|source| SqliteCheckpointError::Migration { source })
    }

    async fn begin_save(&self) -> Result<Transaction<'static, Sqlite>, SqliteCheckpointError> {
        self.pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|source| database_error("save transaction begin", source))
    }
}

#[async_trait]
impl CheckpointStore for SqliteCheckpointStore {
    async fn save(
        &self,
        record: CheckpointRecord,
    ) -> Result<Arc<CheckpointRecord>, CheckpointWriteError> {
        let encoded = EncodedRecord::try_from_record(&record)
            .map_err(|source| store_write_error(SqliteCheckpointError::Encode { source }))?;
        let mut transaction = self.begin_save().await.map_err(store_write_error)?;

        if let Some(existing) = fetch_record_by_id(&mut transaction, record.id())
            .await
            .map_err(store_write_error)?
        {
            rollback(transaction).await.map_err(store_write_error)?;
            return if existing == record {
                Ok(Arc::new(existing))
            } else {
                Err(CheckpointWriteError::IdempotencyConflict {
                    checkpoint_id: record.id(),
                })
            };
        }

        let actual_parent = fetch_head(&mut transaction, record.thread_id())
            .await
            .map_err(store_write_error)?;
        if actual_parent != record.parent_id() {
            rollback(transaction).await.map_err(store_write_error)?;
            return Err(CheckpointWriteError::Conflict {
                expected_parent: record.parent_id(),
                actual_parent,
            });
        }

        insert_record(&mut transaction, &encoded)
            .await
            .map_err(store_write_error)?;
        update_head(&mut transaction, &record)
            .await
            .map_err(store_write_error)?;
        transaction.commit().await.map_err(|source| {
            store_write_error(database_error("save transaction commit", source))
        })?;
        Ok(Arc::new(record))
    }

    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<CheckpointRecord>>, CheckpointerError> {
        let sql = format!(
            "SELECT {SELECT_RECORD_COLUMNS} FROM group_checkpoint_records \
             WHERE checkpoint_id = (\
                 SELECT checkpoint_id FROM group_checkpoint_heads WHERE thread_id = ?\
             )"
        );
        let row = sqlx::query(&sql)
            .bind(thread_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|source| store_read_error(database_error("latest query", source)))?;
        decode_optional(row).map_err(store_read_error)
    }

    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<CheckpointRecord>>, CheckpointerError> {
        let sql = format!(
            "SELECT {SELECT_RECORD_COLUMNS} FROM group_checkpoint_records \
             WHERE thread_id = ? AND checkpoint_id = ?"
        );
        let row = sqlx::query(&sql)
            .bind(thread_id.as_str())
            .bind(checkpoint_id.into_bytes().to_vec())
            .fetch_optional(&self.pool)
            .await
            .map_err(|source| store_read_error(database_error("get query", source)))?;
        decode_optional(row).map_err(store_read_error)
    }

    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<CheckpointRecord>>, CheckpointerError> {
        let sql = format!(
            "SELECT {SELECT_RECORD_COLUMNS} FROM group_checkpoint_records \
             WHERE thread_id = ? ORDER BY sequence ASC"
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|source| store_read_error(database_error("history query", source)))?;
        rows.into_iter()
            .map(|row| decode_row(&row).map(Arc::new).map_err(store_read_error))
            .collect()
    }
}

#[derive(Serialize)]
struct StoredPaths<'a>(Vec<Vec<&'a str>>);

#[derive(Deserialize)]
struct LoadedPaths(Vec<Vec<String>>);

struct EncodedRecord {
    checkpoint_id: Vec<u8>,
    thread_id: String,
    run_id: Vec<u8>,
    parent_id: Option<Vec<u8>>,
    graph_version: Option<String>,
    format_version: i64,
    superstep_be: Vec<u8>,
    step_be: Vec<u8>,
    snapshot_schema: String,
    snapshot_schema_version: i64,
    snapshot_encoding: String,
    snapshot_bytes: Vec<u8>,
    frontier_json: String,
    completed: i64,
    interrupt_id: Option<Vec<u8>>,
    interrupt_node_path_json: Option<String>,
    interrupt_schema: Option<String>,
    interrupt_schema_version: Option<i64>,
    interrupt_encoding: Option<String>,
    interrupt_bytes: Option<Vec<u8>>,
}

impl EncodedRecord {
    fn try_from_record(record: &CheckpointRecord) -> Result<Self, SqliteRecordError> {
        let snapshot = record.snapshot();
        let snapshot_descriptor = snapshot.descriptor();
        let frontier_json = encode_paths(record.next_frontier(), "frontier_json")?;
        let (
            interrupt_id,
            interrupt_node_path_json,
            interrupt_schema,
            interrupt_schema_version,
            interrupt_encoding,
            interrupt_bytes,
        ) = if let Some(interrupt) = record.interrupt() {
            let descriptor = interrupt.payload().descriptor();
            (
                Some(interrupt.id().into_bytes().to_vec()),
                Some(encode_paths(
                    std::slice::from_ref(interrupt.node_path()),
                    "interrupt_node_path_json",
                )?),
                Some(descriptor.schema().to_owned()),
                Some(i64::from(descriptor.schema_version())),
                Some(descriptor.encoding().to_owned()),
                Some(interrupt.payload().bytes().to_vec()),
            )
        } else {
            (None, None, None, None, None, None)
        };

        Ok(Self {
            checkpoint_id: record.id().into_bytes().to_vec(),
            thread_id: record.thread_id().as_str().to_owned(),
            run_id: record.run_id().into_bytes().to_vec(),
            parent_id: record.parent_id().map(|id| id.into_bytes().to_vec()),
            graph_version: record
                .graph_version()
                .map(|version| version.as_str().to_owned()),
            format_version: i64::from(record.format_version().get()),
            superstep_be: record.superstep().to_be_bytes().to_vec(),
            step_be: record.step().to_be_bytes().to_vec(),
            snapshot_schema: snapshot_descriptor.schema().to_owned(),
            snapshot_schema_version: i64::from(snapshot_descriptor.schema_version()),
            snapshot_encoding: snapshot_descriptor.encoding().to_owned(),
            snapshot_bytes: snapshot.bytes().to_vec(),
            frontier_json,
            completed: i64::from(record.completed()),
            interrupt_id,
            interrupt_node_path_json,
            interrupt_schema,
            interrupt_schema_version,
            interrupt_encoding,
            interrupt_bytes,
        })
    }
}

async fn fetch_record_by_id(
    transaction: &mut Transaction<'_, Sqlite>,
    checkpoint_id: CheckpointId,
) -> Result<Option<CheckpointRecord>, SqliteCheckpointError> {
    let sql = format!(
        "SELECT {SELECT_RECORD_COLUMNS} FROM group_checkpoint_records \
         WHERE checkpoint_id = ?"
    );
    let row = sqlx::query(&sql)
        .bind(checkpoint_id.into_bytes().to_vec())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| database_error("idempotency query", source))?;
    row.map(|row| decode_row(&row)).transpose()
}

async fn fetch_head(
    transaction: &mut Transaction<'_, Sqlite>,
    thread_id: &ThreadId,
) -> Result<Option<CheckpointId>, SqliteCheckpointError> {
    let row = sqlx::query("SELECT checkpoint_id FROM group_checkpoint_heads WHERE thread_id = ?")
        .bind(thread_id.as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| database_error("thread head query", source))?;
    row.map(|row| {
        let bytes = column::<Vec<u8>>(&row, "checkpoint_id")?;
        decode_uuid("checkpoint_id", bytes).map(CheckpointId::from_bytes)
    })
    .transpose()
    .map_err(|source| SqliteCheckpointError::CorruptRecord { source })
}

async fn insert_record(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &EncodedRecord,
) -> Result<(), SqliteCheckpointError> {
    sqlx::query(
        "INSERT INTO group_checkpoint_records (\
            checkpoint_id, thread_id, run_id, parent_id, graph_version, format_version, \
            superstep_be, step_be, snapshot_schema, snapshot_schema_version, \
            snapshot_encoding, snapshot_bytes, frontier_json, completed, interrupt_id, \
            interrupt_node_path_json, interrupt_schema, interrupt_schema_version, \
            interrupt_encoding, interrupt_bytes\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&record.checkpoint_id)
    .bind(&record.thread_id)
    .bind(&record.run_id)
    .bind(&record.parent_id)
    .bind(&record.graph_version)
    .bind(record.format_version)
    .bind(&record.superstep_be)
    .bind(&record.step_be)
    .bind(&record.snapshot_schema)
    .bind(record.snapshot_schema_version)
    .bind(&record.snapshot_encoding)
    .bind(&record.snapshot_bytes)
    .bind(&record.frontier_json)
    .bind(record.completed)
    .bind(&record.interrupt_id)
    .bind(&record.interrupt_node_path_json)
    .bind(&record.interrupt_schema)
    .bind(record.interrupt_schema_version)
    .bind(&record.interrupt_encoding)
    .bind(&record.interrupt_bytes)
    .execute(&mut **transaction)
    .await
    .map_err(|source| database_error("record insertion", source))?;
    Ok(())
}

async fn update_head(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &CheckpointRecord,
) -> Result<(), SqliteCheckpointError> {
    sqlx::query(
        "INSERT INTO group_checkpoint_heads (thread_id, checkpoint_id) VALUES (?, ?) \
         ON CONFLICT(thread_id) DO UPDATE SET checkpoint_id = excluded.checkpoint_id",
    )
    .bind(record.thread_id().as_str())
    .bind(record.id().into_bytes().to_vec())
    .execute(&mut **transaction)
    .await
    .map_err(|source| database_error("thread head update", source))?;
    Ok(())
}

async fn rollback(transaction: Transaction<'_, Sqlite>) -> Result<(), SqliteCheckpointError> {
    transaction
        .rollback()
        .await
        .map_err(|source| database_error("save transaction rollback", source))
}

fn decode_optional(
    row: Option<SqliteRow>,
) -> Result<Option<Arc<CheckpointRecord>>, SqliteCheckpointError> {
    row.map(|row| decode_row(&row).map(Arc::new)).transpose()
}

fn decode_row(row: &SqliteRow) -> Result<CheckpointRecord, SqliteCheckpointError> {
    decode_row_fields(row).map_err(|source| SqliteCheckpointError::CorruptRecord { source })
}

fn decode_row_fields(row: &SqliteRow) -> Result<CheckpointRecord, SqliteRecordError> {
    let checkpoint_id =
        CheckpointId::from_bytes(decode_uuid("checkpoint_id", column(row, "checkpoint_id")?)?);
    let run_id = RunId::from_bytes(decode_uuid("run_id", column(row, "run_id")?)?);
    let parent_id = column::<Option<Vec<u8>>>(row, "parent_id")?
        .map(|bytes| decode_uuid("parent_id", bytes).map(CheckpointId::from_bytes))
        .transpose()?;
    let format_version = decode_u32("format_version", column(row, "format_version")?)?;
    let superstep = decode_u64("superstep_be", column(row, "superstep_be")?)?;
    let step = decode_u64("step_be", column(row, "step_be")?)?;
    let completed_value = column::<i64>(row, "completed")?;
    let completed = match completed_value {
        0 => false,
        1 => true,
        value => {
            return Err(SqliteRecordError::InvalidBoolean {
                field: "completed",
                value,
            });
        }
    };
    let snapshot_schema_version = decode_u32(
        "snapshot_schema_version",
        column(row, "snapshot_schema_version")?,
    )?;
    let snapshot = EncodedValue::new(
        CodecDescriptor::new(
            column::<String>(row, "snapshot_schema")?,
            snapshot_schema_version,
            column::<String>(row, "snapshot_encoding")?,
        ),
        column::<Vec<u8>>(row, "snapshot_bytes")?,
    );
    let next_frontier = decode_paths(&column::<String>(row, "frontier_json")?, "frontier_json")?;
    let interrupt = decode_interrupt(row)?;

    CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: CheckpointFormatVersion::new(format_version),
        checkpoint_id,
        thread_id: ThreadId::new(column::<String>(row, "thread_id")?),
        run_id,
        parent_id,
        graph_version: column::<Option<String>>(row, "graph_version")?.map(GraphVersion::new),
        superstep,
        step,
        snapshot,
        next_frontier,
        completed,
        interrupt,
    })
    .map_err(|source| SqliteRecordError::Record { source })
}

fn decode_interrupt(
    row: &SqliteRow,
) -> Result<Option<CheckpointRecordInterrupt>, SqliteRecordError> {
    let id = column::<Option<Vec<u8>>>(row, "interrupt_id")?;
    let path = column::<Option<String>>(row, "interrupt_node_path_json")?;
    let schema = column::<Option<String>>(row, "interrupt_schema")?;
    let schema_version = column::<Option<i64>>(row, "interrupt_schema_version")?;
    let encoding = column::<Option<String>>(row, "interrupt_encoding")?;
    let bytes = column::<Option<Vec<u8>>>(row, "interrupt_bytes")?;

    match (id, path, schema, schema_version, encoding, bytes) {
        (None, None, None, None, None, None) => Ok(None),
        (Some(id), Some(path), Some(schema), Some(schema_version), Some(encoding), Some(bytes)) => {
            let mut paths = decode_paths(&path, "interrupt_node_path_json")?;
            if paths.len() != 1 {
                return Err(SqliteRecordError::PartialInterrupt);
            }
            Ok(Some(CheckpointRecordInterrupt::new(
                InterruptId::from_bytes(decode_uuid("interrupt_id", id)?),
                paths.remove(0),
                EncodedValue::new(
                    CodecDescriptor::new(
                        schema,
                        decode_u32("interrupt_schema_version", schema_version)?,
                        encoding,
                    ),
                    bytes,
                ),
            )))
        }
        _ => Err(SqliteRecordError::PartialInterrupt),
    }
}

fn encode_paths(paths: &[NodePath], field: &'static str) -> Result<String, SqliteRecordError> {
    let paths = StoredPaths(
        paths
            .iter()
            .map(|path| {
                path.segments()
                    .iter()
                    .map(|segment| segment.as_str())
                    .collect()
            })
            .collect(),
    );
    serde_json::to_string(&paths).map_err(|source| SqliteRecordError::PathJson { field, source })
}

fn decode_paths(value: &str, field: &'static str) -> Result<Vec<NodePath>, SqliteRecordError> {
    let LoadedPaths(paths) = serde_json::from_str(value)
        .map_err(|source| SqliteRecordError::PathJson { field, source })?;
    paths
        .into_iter()
        .map(|mut segments| {
            let leaf = segments
                .pop()
                .ok_or(SqliteRecordError::EmptyNodePath { field })?;
            Ok(NodePath::new(&GraphPath::new(segments), leaf))
        })
        .collect()
}

fn column<T>(row: &SqliteRow, field: &'static str) -> Result<T, SqliteRecordError>
where
    for<'value> T: sqlx::Decode<'value, Sqlite> + sqlx::Type<Sqlite>,
{
    row.try_get(field)
        .map_err(|source| SqliteRecordError::Column { field, source })
}

fn decode_uuid(field: &'static str, bytes: Vec<u8>) -> Result<[u8; 16], SqliteRecordError> {
    let length = bytes.len();
    bytes
        .try_into()
        .map_err(|_| SqliteRecordError::InvalidUuidLength { field, length })
}

fn decode_u64(field: &'static str, bytes: Vec<u8>) -> Result<u64, SqliteRecordError> {
    let length = bytes.len();
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| SqliteRecordError::InvalidCounterLength { field, length })?;
    Ok(u64::from_be_bytes(bytes))
}

fn decode_u32(field: &'static str, value: i64) -> Result<u32, SqliteRecordError> {
    u32::try_from(value).map_err(|_| SqliteRecordError::IntegerOutOfRange { field, value })
}

fn database_error(operation: &'static str, source: sqlx::Error) -> SqliteCheckpointError {
    SqliteCheckpointError::Database { operation, source }
}

fn store_write_error(source: SqliteCheckpointError) -> CheckpointWriteError {
    CheckpointWriteError::Failed(CheckpointerError::with_source(
        "SQLite checkpoint storage failed",
        source,
    ))
}

fn store_read_error(source: SqliteCheckpointError) -> CheckpointerError {
    CheckpointerError::with_source("SQLite checkpoint storage failed", source)
}
