use std::error::Error as _;
use std::sync::Arc;
use std::time::Duration;

use group_agent_checkpoint_sqlite::{
    SqliteCheckpointError, SqliteCheckpointStore, SqliteRecordError,
};
use group_agent_core::{
    CheckpointFormatVersion, CheckpointId, CheckpointRecord, CheckpointRecordParts,
    CheckpointStore, CheckpointWriteError, CodecDescriptor, EncodedValue, GraphPath, GraphVersion,
    NodePath, RunId, ThreadId,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;

fn database() -> (TempDir, String) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("checkpoints.sqlite3");
    (directory, format!("sqlite://{}", path.to_string_lossy()))
}

async fn store(database_url: &str) -> SqliteCheckpointStore {
    let store = SqliteCheckpointStore::connect(database_url)
        .await
        .expect("SQLite should connect");
    store.migrate().await.expect("migration should succeed");
    store
}

fn record(
    checkpoint_id: CheckpointId,
    thread: &str,
    parent_id: Option<CheckpointId>,
    step: u64,
    completed: bool,
) -> CheckpointRecord {
    CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: CheckpointFormatVersion::CURRENT,
        checkpoint_id,
        thread_id: ThreadId::from(thread),
        run_id: RunId::new(),
        parent_id,
        graph_version: Some(GraphVersion::from("sqlite-tests-v1")),
        superstep: step,
        step,
        snapshot: EncodedValue::new(
            CodecDescriptor::new("group.tests.sqlite.snapshot", 7, "raw/test-v2"),
            step.to_be_bytes().to_vec(),
        ),
        next_frontier: if completed {
            Vec::new()
        } else {
            vec![
                NodePath::new(&GraphPath::new(["nested"]), "alpha"),
                NodePath::new(&GraphPath::new(["nested"]), "beta"),
            ]
        },
        completed,
        interrupt: None,
    })
    .expect("test record should be valid")
}

#[tokio::test]
async fn save_latest_get_history_and_thread_isolation_round_trip_all_fields() {
    let (_directory, database_url) = database();
    let store = store(&database_url).await;
    let first_id = CheckpointId::new();
    let first = record(first_id, "one", None, u64::MAX, false);
    let second_id = CheckpointId::new();
    let second = record(second_id, "one", Some(first_id), u64::MAX - 1, true);
    let other = record(CheckpointId::new(), "two", None, 3, true);

    store.save(first.clone()).await.expect("first save");
    store.save(second.clone()).await.expect("second save");
    store.save(other.clone()).await.expect("other thread save");

    assert_eq!(
        store
            .latest(&ThreadId::from("one"))
            .await
            .expect("latest")
            .as_deref(),
        Some(&second)
    );
    assert_eq!(
        store
            .get(&ThreadId::from("one"), first_id)
            .await
            .expect("get")
            .as_deref(),
        Some(&first)
    );
    assert!(
        store
            .get(&ThreadId::from("two"), first_id)
            .await
            .expect("cross-thread get")
            .is_none()
    );
    assert_eq!(
        store
            .history(&ThreadId::from("one"))
            .await
            .expect("history")
            .iter()
            .map(|entry| entry.id())
            .collect::<Vec<_>>(),
        [first_id, second_id]
    );
    assert_eq!(
        store
            .history(&ThreadId::from("two"))
            .await
            .expect("other history")
            .iter()
            .map(|entry| entry.id())
            .collect::<Vec<_>>(),
        [other.id()]
    );
    assert_eq!(first.superstep(), u64::MAX);
    assert_eq!(
        first.snapshot().descriptor(),
        &CodecDescriptor::new("group.tests.sqlite.snapshot", 7, "raw/test-v2")
    );
}

#[tokio::test]
async fn migrations_are_embedded_and_repeatable() {
    let (_directory, database_url) = database();
    let store = SqliteCheckpointStore::connect(&database_url)
        .await
        .expect("connect");
    store.migrate().await.expect("first migration");
    store.migrate().await.expect("repeated migration");
    assert!(
        store
            .latest(&ThreadId::from("empty"))
            .await
            .expect("query")
            .is_none()
    );
}

#[tokio::test]
async fn durable_u64_counters_are_lossless_and_lexicographically_sortable() {
    let (_directory, database_url) = database();
    let options: SqliteConnectOptions = database_url.parse().expect("valid database URL");
    let pool = SqlitePoolOptions::new()
        .connect_with(options.create_if_missing(true))
        .await
        .expect("pool");
    let store = SqliteCheckpointStore::from_pool(pool.clone());
    store.migrate().await.expect("migrate");
    store
        .save(record(CheckpointId::new(), "small", None, 1, true))
        .await
        .expect("small");
    store
        .save(record(CheckpointId::new(), "large", None, u64::MAX, true))
        .await
        .expect("large");

    let encoded: Vec<String> = sqlx::query_scalar(
        "SELECT hex(step_be) FROM group_checkpoint_records ORDER BY step_be ASC",
    )
    .fetch_all(&pool)
    .await
    .expect("encoded counters");
    assert_eq!(encoded, ["0000000000000001", "FFFFFFFFFFFFFFFF"]);
}

#[tokio::test]
async fn idempotency_precedes_parent_cas_and_compares_stable_record_content() {
    let (_directory, database_url) = database();
    let store = store(&database_url).await;
    let first = record(CheckpointId::new(), "idempotent", None, 1, false);
    let second = record(CheckpointId::new(), "idempotent", Some(first.id()), 2, true);
    store.save(first.clone()).await.expect("first");
    store.save(second).await.expect("advance latest");

    assert_eq!(
        store
            .save(first.clone())
            .await
            .expect("old exact replay")
            .as_ref(),
        &first
    );

    let conflicting = CheckpointRecord::try_from_parts(CheckpointRecordParts {
        run_id: RunId::new(),
        ..parts_from(&first)
    })
    .expect("conflicting record remains structurally valid");
    assert!(matches!(
        store.save(conflicting).await,
        Err(CheckpointWriteError::IdempotencyConflict { checkpoint_id })
            if checkpoint_id == first.id()
    ));
}

#[tokio::test]
async fn concurrent_writers_from_one_parent_cannot_form_a_fork() {
    let (_directory, database_url) = database();
    let store = Arc::new(store(&database_url).await);
    let parent = record(CheckpointId::new(), "cas", None, 1, false);
    store.save(parent.clone()).await.expect("parent");
    let left = record(CheckpointId::new(), "cas", Some(parent.id()), 2, true);
    let right = record(CheckpointId::new(), "cas", Some(parent.id()), 2, true);

    let left_store = Arc::clone(&store);
    let left_record = left.clone();
    let right_store = Arc::clone(&store);
    let right_record = right.clone();
    let (left_result, right_result) = tokio::join!(
        async move { left_store.save(left_record).await },
        async move { right_store.save(right_record).await },
    );

    let successes = usize::from(left_result.is_ok()) + usize::from(right_result.is_ok());
    assert_eq!(successes, 1);
    let failure = if left_result.is_err() {
        left_result.expect_err("left should fail")
    } else {
        right_result.expect_err("right should fail")
    };
    assert!(matches!(
        failure,
        CheckpointWriteError::Conflict {
            expected_parent: Some(expected),
            actual_parent: Some(actual),
        } if expected == parent.id() && (actual == left.id() || actual == right.id())
    ));
    assert_eq!(
        store
            .history(&ThreadId::from("cas"))
            .await
            .expect("history")
            .len(),
        2
    );
}

#[tokio::test]
async fn sqlite_busy_is_a_source_preserving_storage_failure_not_a_lineage_conflict() {
    let (_directory, database_url) = database();
    let options: SqliteConnectOptions = database_url.parse().expect("valid database URL");
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(
            options
                .create_if_missing(true)
                .busy_timeout(Duration::from_millis(1)),
        )
        .await
        .expect("pool");
    let store = SqliteCheckpointStore::from_pool(pool.clone());
    store.migrate().await.expect("migrate");
    let blocker = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("writer lock");

    let error = store
        .save(record(CheckpointId::new(), "busy", None, 1, true))
        .await
        .expect_err("second writer should observe SQLite busy");
    let CheckpointWriteError::Failed(error) = error else {
        panic!("SQLite busy must not be mapped to a lineage conflict");
    };
    assert!(matches!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<SqliteCheckpointError>()),
        Some(SqliteCheckpointError::Database {
            operation: "save transaction begin",
            ..
        })
    ));
    blocker.rollback().await.expect("release writer lock");
}

#[derive(Clone, Copy, Debug)]
enum ExpectedCorruption {
    UuidLength(usize),
    CounterLength(&'static str, usize),
    StorageClass(&'static str),
    InvalidBoolean(i64),
    PartialInterrupt,
    PathJson,
    EmptyNodePath,
}

#[tokio::test]
async fn malformed_database_fields_return_structured_errors_without_panicking() {
    let cases = [
        (
            "15-byte UUID",
            "UPDATE group_checkpoint_records SET run_id = zeroblob(15)",
            ExpectedCorruption::UuidLength(15),
        ),
        (
            "17-byte UUID",
            "UPDATE group_checkpoint_records SET run_id = zeroblob(17)",
            ExpectedCorruption::UuidLength(17),
        ),
        (
            "short step",
            "UPDATE group_checkpoint_records SET step_be = zeroblob(7)",
            ExpectedCorruption::CounterLength("step_be", 7),
        ),
        (
            "long superstep",
            "UPDATE group_checkpoint_records SET superstep_be = zeroblob(9)",
            ExpectedCorruption::CounterLength("superstep_be", 9),
        ),
        (
            "wrong storage class",
            "UPDATE group_checkpoint_records SET snapshot_bytes = 42",
            ExpectedCorruption::StorageClass("snapshot_bytes"),
        ),
        (
            "invalid completed",
            "UPDATE group_checkpoint_records SET completed = 2",
            ExpectedCorruption::InvalidBoolean(2),
        ),
        (
            "partial interrupt",
            "UPDATE group_checkpoint_records SET interrupt_id = zeroblob(16)",
            ExpectedCorruption::PartialInterrupt,
        ),
        (
            "invalid path JSON",
            "UPDATE group_checkpoint_records SET frontier_json = '{not JSON'",
            ExpectedCorruption::PathJson,
        ),
        (
            "empty node path",
            "UPDATE group_checkpoint_records SET frontier_json = '[[]]'",
            ExpectedCorruption::EmptyNodePath,
        ),
    ];

    for (name, mutation, expected) in cases {
        let (_directory, database_url) = database();
        let options: SqliteConnectOptions = database_url.parse().expect("valid database URL");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options.create_if_missing(true))
            .await
            .expect("pool");
        let store = SqliteCheckpointStore::from_pool(pool.clone());
        store.migrate().await.expect("migrate");
        let saved = record(CheckpointId::new(), "corrupt", None, 1, false);
        store.save(saved).await.expect("save");
        sqlx::query("PRAGMA ignore_check_constraints = ON")
            .execute(&pool)
            .await
            .expect("disable checks for corruption injection");
        sqlx::query(mutation)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{name} mutation failed: {error}"));

        let error = store
            .latest(&ThreadId::from("corrupt"))
            .await
            .expect_err("corrupt record must return an error");
        assert_corruption(name, &error, expected);
    }
}

fn assert_corruption(
    name: &str,
    error: &group_agent_core::CheckpointerError,
    expected: ExpectedCorruption,
) {
    let adapter = error
        .source()
        .and_then(|source| source.downcast_ref::<SqliteCheckpointError>())
        .unwrap_or_else(|| panic!("{name} lost the adapter source"));
    let SqliteCheckpointError::CorruptRecord { source } = adapter else {
        panic!("{name} returned non-corruption adapter error: {adapter}");
    };
    let matched = match expected {
        ExpectedCorruption::UuidLength(length) => matches!(
            source,
            SqliteRecordError::InvalidUuidLength {
                field: "run_id",
                length: actual,
            } if *actual == length
        ),
        ExpectedCorruption::CounterLength(field, length) => matches!(
            source,
            SqliteRecordError::InvalidCounterLength {
                field: actual_field,
                length: actual_length,
            } if *actual_field == field && *actual_length == length
        ),
        ExpectedCorruption::StorageClass(field) => matches!(
            source,
            SqliteRecordError::Column {
                field: actual_field,
                ..
            } if *actual_field == field
        ),
        ExpectedCorruption::InvalidBoolean(value) => matches!(
            source,
            SqliteRecordError::InvalidBoolean {
                field: "completed",
                value: actual,
            } if *actual == value
        ),
        ExpectedCorruption::PartialInterrupt => {
            matches!(source, SqliteRecordError::PartialInterrupt)
        }
        ExpectedCorruption::PathJson => matches!(
            source,
            SqliteRecordError::PathJson {
                field: "frontier_json",
                ..
            }
        ),
        ExpectedCorruption::EmptyNodePath => matches!(
            source,
            SqliteRecordError::EmptyNodePath {
                field: "frontier_json"
            }
        ),
    };
    assert!(matched, "{name} returned unexpected error: {source}");
    if matches!(
        source,
        SqliteRecordError::Column { .. } | SqliteRecordError::PathJson { .. }
    ) {
        assert!(source.source().is_some(), "{name} lost its root source");
    }
}

#[tokio::test]
async fn head_update_failure_rolls_back_the_inserted_record_and_head() {
    let (_directory, database_url) = database();
    let options: SqliteConnectOptions = database_url.parse().expect("valid database URL");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options.create_if_missing(true))
        .await
        .expect("pool");
    let store = SqliteCheckpointStore::from_pool(pool.clone());
    store.migrate().await.expect("migrate");
    sqlx::query(
        "CREATE TRIGGER reject_checkpoint_head \
         BEFORE INSERT ON group_checkpoint_heads \
         BEGIN SELECT RAISE(ABORT, 'injected head failure'); END",
    )
    .execute(&pool)
    .await
    .expect("failure trigger");

    assert!(matches!(
        store
            .save(record(CheckpointId::new(), "rollback", None, 1, true))
            .await,
        Err(CheckpointWriteError::Failed(_))
    ));
    let record_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM group_checkpoint_records WHERE thread_id = 'rollback'",
    )
    .fetch_one(&pool)
    .await
    .expect("record count");
    let head_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM group_checkpoint_heads WHERE thread_id = 'rollback'",
    )
    .fetch_one(&pool)
    .await
    .expect("head count");
    assert_eq!((record_count, head_count), (0, 0));
}

fn parts_from(record: &CheckpointRecord) -> CheckpointRecordParts {
    CheckpointRecordParts {
        format_version: record.format_version(),
        checkpoint_id: record.id(),
        thread_id: record.thread_id().clone(),
        run_id: record.run_id(),
        parent_id: record.parent_id(),
        graph_version: record.graph_version().cloned(),
        superstep: record.superstep(),
        step: record.step(),
        snapshot: record.snapshot().clone(),
        next_frontier: record.next_frontier().to_vec(),
        completed: record.completed(),
        interrupt: record.interrupt().cloned(),
    }
}
