use std::borrow::Cow;
use std::error::Error as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use group_agent_checkpoint_sqlite::{
    SqliteCheckpointError, SqliteCheckpointStore, SqliteRecordError,
};
use group_agent_core::{
    BranchId, CheckpointFormatVersion, CheckpointId, CheckpointRecord, CheckpointRecordParts,
    CheckpointStore, CheckpointWriteError, CodecDescriptor, EncodedValue, GraphPath, GraphVersion,
    NodePath, RunId, ThreadId,
};
use sqlx::migrate::Migrator;
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
async fn ownership_migration_upgrades_an_existing_branch_database() {
    let (_directory, database_url) = database();
    let options: SqliteConnectOptions = database_url.parse().expect("valid database URL");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options.create_if_missing(true).foreign_keys(true))
        .await
        .expect("pool");
    let migrations = Migrator::new(Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"))
        .await
        .expect("load migrations");
    let legacy = Migrator {
        migrations: Cow::Owned(migrations.iter().take(2).cloned().collect()),
        ..Migrator::DEFAULT
    };
    legacy.run(&pool).await.expect("legacy migrations");
    let store = SqliteCheckpointStore::from_pool(pool.clone());
    let source = record(CheckpointId::new(), "upgrade", None, 1, false);
    store.save(source.clone()).await.expect("legacy source");
    let branch_id = BranchId::new();
    store
        .create_branch(&ThreadId::from("upgrade"), branch_id, source.id())
        .await
        .expect("legacy branch");
    let child = record(CheckpointId::new(), "upgrade", Some(source.id()), 2, true);
    store
        .save(child.clone())
        .await
        .expect("legacy child record");
    sqlx::query(
        "INSERT INTO group_checkpoint_branch_records (checkpoint_id, branch_id) VALUES (?, ?)",
    )
    .bind(child.id().into_bytes().to_vec())
    .bind(branch_id.into_bytes().to_vec())
    .execute(&pool)
    .await
    .expect("legacy membership");
    sqlx::query("UPDATE group_checkpoint_branches SET head_checkpoint_id = ? WHERE branch_id = ?")
        .bind(child.id().into_bytes().to_vec())
        .bind(branch_id.into_bytes().to_vec())
        .execute(&pool)
        .await
        .expect("legacy branch head");
    sqlx::query("UPDATE group_checkpoint_heads SET checkpoint_id = ? WHERE thread_id = ?")
        .bind(source.id().into_bytes().to_vec())
        .bind("upgrade")
        .execute(&pool)
        .await
        .expect("restore legacy default head");

    store.migrate().await.expect("ownership migration");
    store.migrate().await.expect("repeat completed migration");
    assert_eq!(
        store
            .branch_history(&ThreadId::from("upgrade"), branch_id)
            .await
            .expect("upgraded history")
            .iter()
            .map(|entry| entry.id())
            .collect::<Vec<_>>(),
        [source.id(), child.id()]
    );
    assert_eq!(
        store
            .branch_head(&ThreadId::from("upgrade"), branch_id)
            .await
            .expect("upgraded head")
            .expect("branch")
            .id(),
        child.id()
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
async fn concurrent_writers_on_one_branch_head_use_an_independent_cas() {
    let (_directory, database_url) = database();
    let store = Arc::new(store(&database_url).await);
    let source = record(CheckpointId::new(), "branch-cas", None, 1, false);
    store.save(source.clone()).await.expect("source");
    let branch_id = BranchId::new();
    store
        .create_branch(&ThreadId::from("branch-cas"), branch_id, source.id())
        .await
        .expect("branch");
    let left = record(
        CheckpointId::new(),
        "branch-cas",
        Some(source.id()),
        2,
        true,
    );
    let right = record(
        CheckpointId::new(),
        "branch-cas",
        Some(source.id()),
        2,
        true,
    );

    let left_store = Arc::clone(&store);
    let left_record = left.clone();
    let right_store = Arc::clone(&store);
    let right_record = right.clone();
    let (left_result, right_result) = tokio::join!(
        async move { left_store.save_branch(branch_id, left_record).await },
        async move { right_store.save_branch(branch_id, right_record).await },
    );
    assert_eq!(
        usize::from(left_result.is_ok()) + usize::from(right_result.is_ok()),
        1
    );
    assert!(matches!(
        left_result.err().or_else(|| right_result.err()),
        Some(CheckpointWriteError::Conflict {
            expected_parent: Some(expected),
            actual_parent: Some(actual),
        }) if expected == source.id() && (actual == left.id() || actual == right.id())
    ));
    assert_eq!(
        store
            .history(&ThreadId::from("branch-cas"))
            .await
            .expect("default history")
            .len(),
        1
    );
    let branch = store
        .branch_history(&ThreadId::from("branch-cas"), branch_id)
        .await
        .expect("branch history");
    assert_eq!(branch.len(), 2);
    assert_eq!(branch[0].id(), source.id());
    assert_eq!(branch[1].parent_id(), Some(source.id()));
}

#[tokio::test]
async fn sqlite_branch_creation_and_idempotency_contract_is_deterministic() {
    let (_directory, database_url) = database();
    let store = Arc::new(store(&database_url).await);
    let source = record(CheckpointId::new(), "branch-contract", None, 1, false);
    store.save(source.clone()).await.expect("source");
    let left = BranchId::new();
    let right = BranchId::new();

    let left_store = Arc::clone(&store);
    let right_store = Arc::clone(&store);
    let thread = ThreadId::from("branch-contract");
    let (first, second) = tokio::join!(
        left_store.create_branch(&thread, left, source.id()),
        right_store.create_branch(&thread, left, source.id()),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(matches!(
        first.err().or_else(|| second.err()),
        Some(CheckpointWriteError::BranchAlreadyExists { branch_id }) if branch_id == left
    ));
    assert!(matches!(
        store.create_branch(&thread, left, source.id()).await,
        Err(CheckpointWriteError::BranchAlreadyExists { branch_id }) if branch_id == left
    ));
    store
        .create_branch(&thread, right, source.id())
        .await
        .expect("independent right branch");

    let left_record = record(
        CheckpointId::new(),
        "branch-contract",
        Some(source.id()),
        2,
        true,
    );
    let original = store
        .save_branch(left, left_record.clone())
        .await
        .expect("left save");
    let replay = store
        .save_branch(left, left_record.clone())
        .await
        .expect("same-id same-content replay");
    assert_eq!(original.as_ref(), replay.as_ref());

    let changed = CheckpointRecord::try_from_parts(CheckpointRecordParts {
        run_id: RunId::new(),
        ..parts_from(&left_record)
    })
    .expect("changed record");
    assert!(matches!(
        store.save_branch(left, changed).await,
        Err(CheckpointWriteError::IdempotencyConflict { checkpoint_id })
            if checkpoint_id == left_record.id()
    ));
    assert!(matches!(
        store.save_branch(right, left_record.clone()).await,
        Err(CheckpointWriteError::IdempotencyConflict { checkpoint_id })
            if checkpoint_id == left_record.id()
    ));

    let right_record = record(
        CheckpointId::new(),
        "branch-contract",
        Some(source.id()),
        3,
        true,
    );
    store
        .save_branch(right, right_record.clone())
        .await
        .expect("right save");
    assert_eq!(
        store
            .branch_head(&thread, left)
            .await
            .expect("left head")
            .expect("left")
            .id(),
        left_record.id()
    );
    assert_eq!(
        store
            .branch_head(&thread, right)
            .await
            .expect("right head")
            .expect("right")
            .id(),
        right_record.id()
    );
    assert_eq!(
        store.history(&thread).await.expect("default history").len(),
        1
    );
}

#[tokio::test]
async fn sqlite_branch_ownership_rejects_cross_thread_create_query_and_save() {
    let (_directory, database_url) = database();
    let store = store(&database_url).await;
    let owner_source = record(CheckpointId::new(), "owner", None, 1, false);
    let other_source = record(CheckpointId::new(), "other", None, 1, false);
    store
        .save(owner_source.clone())
        .await
        .expect("owner source");
    store
        .save(other_source.clone())
        .await
        .expect("other source");
    let branch_id = BranchId::new();
    store
        .create_branch(&ThreadId::from("owner"), branch_id, owner_source.id())
        .await
        .expect("owner branch");

    assert!(matches!(
        store
            .create_branch(&ThreadId::from("other"), branch_id, other_source.id())
            .await,
        Err(CheckpointWriteError::BranchAlreadyExists { branch_id: actual })
            if actual == branch_id
    ));
    assert!(matches!(
        store
            .create_branch(&ThreadId::from("other"), BranchId::new(), owner_source.id())
            .await,
        Err(CheckpointWriteError::BranchSourceNotFound { .. })
    ));
    assert!(
        store
            .branch_head(&ThreadId::from("other"), branch_id)
            .await
            .expect("wrong-thread head")
            .is_none()
    );
    assert!(
        store
            .branch_history(&ThreadId::from("other"), branch_id)
            .await
            .expect("wrong-thread history")
            .is_empty()
    );
    let cross_thread = record(
        CheckpointId::new(),
        "other",
        Some(owner_source.id()),
        2,
        true,
    );
    assert!(matches!(
        store.save_branch(branch_id, cross_thread).await,
        Err(CheckpointWriteError::BranchNotFound { branch_id: actual })
            if actual == branch_id
    ));
}

#[tokio::test]
async fn composite_foreign_keys_reject_cross_thread_branch_metadata() {
    let (_directory, database_url) = database();
    let options: SqliteConnectOptions = database_url.parse().expect("valid database URL");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options.create_if_missing(true).foreign_keys(true))
        .await
        .expect("pool");
    let store = SqliteCheckpointStore::from_pool(pool.clone());
    store.migrate().await.expect("migrate");
    let owner = record(CheckpointId::new(), "owner", None, 1, false);
    let other = record(CheckpointId::new(), "other", None, 1, false);
    store.save(owner.clone()).await.expect("owner");
    store.save(other.clone()).await.expect("other");

    let invalid_branch = sqlx::query(
        "INSERT INTO group_checkpoint_branches \
         (branch_id, thread_id, source_checkpoint_id, head_checkpoint_id) VALUES (?, ?, ?, ?)",
    )
    .bind(BranchId::new().into_bytes().to_vec())
    .bind("owner")
    .bind(other.id().into_bytes().to_vec())
    .bind(other.id().into_bytes().to_vec())
    .execute(&pool)
    .await;
    assert!(
        invalid_branch.is_err(),
        "cross-thread source/head must fail"
    );

    let branch_id = BranchId::new();
    store
        .create_branch(&ThreadId::from("owner"), branch_id, owner.id())
        .await
        .expect("valid branch");
    let invalid_membership = sqlx::query(
        "INSERT INTO group_checkpoint_branch_records \
         (checkpoint_id, thread_id, branch_id) VALUES (?, ?, ?)",
    )
    .bind(other.id().into_bytes().to_vec())
    .bind("owner")
    .bind(branch_id.into_bytes().to_vec())
    .execute(&pool)
    .await;
    assert!(
        invalid_membership.is_err(),
        "cross-thread membership must fail"
    );
}

#[tokio::test]
async fn corrupted_branch_ownership_returns_a_structured_error() {
    let (_directory, database_url) = database();
    let options: SqliteConnectOptions = database_url.parse().expect("valid database URL");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options.create_if_missing(true).foreign_keys(true))
        .await
        .expect("pool");
    let store = SqliteCheckpointStore::from_pool(pool.clone());
    store.migrate().await.expect("migrate");
    let owner = record(CheckpointId::new(), "owner", None, 1, false);
    let other = record(CheckpointId::new(), "other", None, 1, false);
    store.save(owner.clone()).await.expect("owner");
    store.save(other.clone()).await.expect("other");
    let branch_id = BranchId::new();
    store
        .create_branch(&ThreadId::from("owner"), branch_id, owner.id())
        .await
        .expect("branch");

    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&pool)
        .await
        .expect("disable foreign keys for corruption injection");
    sqlx::query("UPDATE group_checkpoint_branches SET head_checkpoint_id = ? WHERE branch_id = ?")
        .bind(other.id().into_bytes().to_vec())
        .bind(branch_id.into_bytes().to_vec())
        .execute(&pool)
        .await
        .expect("inject cross-thread head");

    let error = store
        .branch_head(&ThreadId::from("owner"), branch_id)
        .await
        .expect_err("corrupt head must fail");
    let adapter = error
        .source()
        .and_then(|source| source.downcast_ref::<SqliteCheckpointError>())
        .expect("adapter source");
    assert!(matches!(
        adapter,
        SqliteCheckpointError::CorruptRecord {
            source: SqliteRecordError::BranchOwnership {
                relation: "head",
                branch_id: actual,
                checkpoint_id,
                ..
            }
        } if *actual == branch_id && *checkpoint_id == other.id()
    ));
}

#[tokio::test]
async fn branch_membership_and_head_failures_roll_back_record_membership_and_head() {
    for (name, trigger) in [
        (
            "membership",
            "CREATE TRIGGER reject_branch_membership \
             BEFORE INSERT ON group_checkpoint_branch_records \
             BEGIN SELECT RAISE(ABORT, 'injected membership failure'); END",
        ),
        (
            "head",
            "CREATE TRIGGER reject_branch_head_update \
             BEFORE UPDATE OF head_checkpoint_id ON group_checkpoint_branches \
             BEGIN SELECT RAISE(ABORT, 'injected branch head failure'); END",
        ),
    ] {
        let (_directory, database_url) = database();
        let options: SqliteConnectOptions = database_url.parse().expect("valid database URL");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options.create_if_missing(true).foreign_keys(true))
            .await
            .expect("pool");
        let store = SqliteCheckpointStore::from_pool(pool.clone());
        store.migrate().await.expect("migrate");
        let source = record(CheckpointId::new(), "rollback-branch", None, 1, false);
        store.save(source.clone()).await.expect("source");
        let branch_id = BranchId::new();
        store
            .create_branch(&ThreadId::from("rollback-branch"), branch_id, source.id())
            .await
            .expect("branch");
        sqlx::query(trigger)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{name} trigger failed: {error}"));
        let child = record(
            CheckpointId::new(),
            "rollback-branch",
            Some(source.id()),
            2,
            true,
        );
        assert!(
            matches!(
                store.save_branch(branch_id, child.clone()).await,
                Err(CheckpointWriteError::Failed(_))
            ),
            "{name} injection should be a storage failure"
        );

        let record_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM group_checkpoint_records WHERE checkpoint_id = ?",
        )
        .bind(child.id().into_bytes().to_vec())
        .fetch_one(&pool)
        .await
        .expect("record count");
        let membership_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM group_checkpoint_branch_records WHERE checkpoint_id = ?",
        )
        .bind(child.id().into_bytes().to_vec())
        .fetch_one(&pool)
        .await
        .expect("membership count");
        let head: Vec<u8> = sqlx::query_scalar(
            "SELECT head_checkpoint_id FROM group_checkpoint_branches WHERE branch_id = ?",
        )
        .bind(branch_id.into_bytes().to_vec())
        .fetch_one(&pool)
        .await
        .expect("branch head");
        assert_eq!(
            (record_count, membership_count, head),
            (0, 0, source.id().into_bytes().to_vec()),
            "{name} failure must roll back all three writes"
        );
    }
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

#[tokio::test]
async fn sqlite_busy_during_branch_save_is_not_a_branch_cas_conflict() {
    let (_directory, database_url) = database();
    let options: SqliteConnectOptions = database_url.parse().expect("valid database URL");
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(
            options
                .create_if_missing(true)
                .foreign_keys(true)
                .busy_timeout(Duration::from_millis(1)),
        )
        .await
        .expect("pool");
    let store = SqliteCheckpointStore::from_pool(pool.clone());
    store.migrate().await.expect("migrate");
    let source = record(CheckpointId::new(), "branch-busy", None, 1, false);
    store.save(source.clone()).await.expect("source");
    let branch_id = BranchId::new();
    store
        .create_branch(&ThreadId::from("branch-busy"), branch_id, source.id())
        .await
        .expect("branch");
    let blocker = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("writer lock");

    let error = store
        .save_branch(
            branch_id,
            record(
                CheckpointId::new(),
                "branch-busy",
                Some(source.id()),
                2,
                true,
            ),
        )
        .await
        .expect_err("branch writer should observe SQLite busy");
    assert!(matches!(error, CheckpointWriteError::Failed(_)));
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
