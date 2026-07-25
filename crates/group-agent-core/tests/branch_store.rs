use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use group_agent_core::{
    BranchId, CheckpointFormatVersion, CheckpointId, CheckpointRecord, CheckpointRecordParts,
    CheckpointStore, CheckpointWriteError, CodecDescriptor, EncodedValue, GraphVersion,
    InMemoryCheckpointStore, RunId, ThreadId,
};
use tokio::sync::{Barrier, Notify};

fn record(
    checkpoint_id: CheckpointId,
    thread: &str,
    parent_id: Option<CheckpointId>,
    step: u64,
) -> CheckpointRecord {
    CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: CheckpointFormatVersion::CURRENT,
        checkpoint_id,
        thread_id: ThreadId::from(thread),
        run_id: RunId::new(),
        parent_id,
        graph_version: Some(GraphVersion::from("branch-store-v1")),
        superstep: step,
        step,
        snapshot: EncodedValue::new(
            CodecDescriptor::new("group.tests.branch-store", 1, "raw"),
            step.to_be_bytes().to_vec(),
        ),
        next_frontier: Vec::new(),
        completed: true,
        interrupt: None,
    })
    .expect("test record should be valid")
}

fn with_run_id(record: &CheckpointRecord, run_id: RunId) -> CheckpointRecord {
    CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: record.format_version(),
        checkpoint_id: record.id(),
        thread_id: record.thread_id().clone(),
        run_id,
        parent_id: record.parent_id(),
        graph_version: record.graph_version().cloned(),
        superstep: record.superstep(),
        step: record.step(),
        snapshot: record.snapshot().clone(),
        next_frontier: record.next_frontier().to_vec(),
        completed: record.completed(),
        interrupt: record.interrupt().cloned(),
    })
    .expect("changed run id remains structurally valid")
}

#[tokio::test]
async fn two_branches_from_one_source_advance_independently() {
    let store = InMemoryCheckpointStore::new();
    let source = record(CheckpointId::new(), "thread", None, 1);
    store.save(source.clone()).await.expect("source");
    let left = BranchId::new();
    let right = BranchId::new();
    store
        .create_branch(&ThreadId::from("thread"), left, source.id())
        .await
        .expect("left branch");
    store
        .create_branch(&ThreadId::from("thread"), right, source.id())
        .await
        .expect("right branch");

    let left_record = record(CheckpointId::new(), "thread", Some(source.id()), 2);
    let right_record = record(CheckpointId::new(), "thread", Some(source.id()), 20);
    store
        .save_branch(left, left_record.clone())
        .await
        .expect("left save");
    store
        .save_branch(right, right_record.clone())
        .await
        .expect("right save");

    assert_eq!(
        store
            .branch_head(&ThreadId::from("thread"), left)
            .await
            .expect("left head")
            .expect("left branch")
            .id(),
        left_record.id()
    );
    assert_eq!(
        store
            .branch_head(&ThreadId::from("thread"), right)
            .await
            .expect("right head")
            .expect("right branch")
            .id(),
        right_record.id()
    );
    assert_eq!(
        store
            .history(&ThreadId::from("thread"))
            .await
            .expect("default history")
            .iter()
            .map(|entry| entry.id())
            .collect::<Vec<_>>(),
        [source.id()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_and_repeated_branch_creation_never_becomes_idempotent_success() {
    let store = Arc::new(InMemoryCheckpointStore::new());
    let source = record(CheckpointId::new(), "thread", None, 1);
    store.save(source.clone()).await.expect("source");
    let branch_id = BranchId::new();

    let barrier = Arc::new(Barrier::new(3));
    let ready = Arc::new(AtomicUsize::new(0));
    let ready_notify = Arc::new(Notify::new());
    let thread = ThreadId::from("thread");
    let source_id = source.id();
    let create = |store: Arc<InMemoryCheckpointStore>| {
        let barrier = Arc::clone(&barrier);
        let ready = Arc::clone(&ready);
        let ready_notify = Arc::clone(&ready_notify);
        let thread = thread.clone();
        tokio::spawn(async move {
            if ready.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                ready_notify.notify_one();
            }
            barrier.wait().await;
            store.create_branch(&thread, branch_id, source_id).await
        })
    };
    let left = create(Arc::clone(&store));
    let right = create(Arc::clone(&store));
    while ready.load(Ordering::SeqCst) != 2 {
        ready_notify.notified().await;
    }
    barrier.wait().await;
    let (left, right) = (
        left.await.expect("left create task"),
        right.await.expect("right create task"),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert!(matches!(
        left.err().or_else(|| right.err()),
        Some(CheckpointWriteError::BranchAlreadyExists { branch_id: actual })
            if actual == branch_id
    ));
    assert!(matches!(
        store
            .create_branch(&thread, branch_id, source.id())
            .await,
        Err(CheckpointWriteError::BranchAlreadyExists { branch_id: actual })
            if actual == branch_id
    ));
}

#[tokio::test]
async fn branch_id_has_one_owner_and_cross_thread_operations_fail_closed() {
    let store = InMemoryCheckpointStore::new();
    let source = record(CheckpointId::new(), "owner", None, 1);
    let same_thread_source = record(CheckpointId::new(), "owner", Some(source.id()), 2);
    let other_source = record(CheckpointId::new(), "other", None, 1);
    store.save(source.clone()).await.expect("owner source");
    store
        .save(same_thread_source.clone())
        .await
        .expect("same-thread source");
    store
        .save(other_source.clone())
        .await
        .expect("other source");
    let branch_id = BranchId::new();
    store
        .create_branch(&ThreadId::from("owner"), branch_id, source.id())
        .await
        .expect("branch");

    assert!(matches!(
        store
            .create_branch(&ThreadId::from("other"), branch_id, other_source.id())
            .await,
        Err(CheckpointWriteError::BranchAlreadyExists { branch_id: actual })
            if actual == branch_id
    ));
    assert!(matches!(
        store
            .create_branch(
                &ThreadId::from("owner"),
                branch_id,
                same_thread_source.id(),
            )
            .await,
        Err(CheckpointWriteError::BranchAlreadyExists { branch_id: actual })
            if actual == branch_id
    ));
    assert!(matches!(
        store
            .create_branch(&ThreadId::from("other"), BranchId::new(), source.id())
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
    let cross_thread = record(CheckpointId::new(), "other", Some(source.id()), 2);
    assert!(matches!(
        store.save_branch(branch_id, cross_thread).await,
        Err(CheckpointWriteError::BranchNotFound { branch_id: actual })
            if actual == branch_id
    ));
}

#[tokio::test]
async fn branch_idempotency_matrix_is_scoped_to_exact_content_and_branch() {
    let store = InMemoryCheckpointStore::new();
    let source = record(CheckpointId::new(), "thread", None, 1);
    store.save(source.clone()).await.expect("source");
    let left = BranchId::new();
    let right = BranchId::new();
    store
        .create_branch(&ThreadId::from("thread"), left, source.id())
        .await
        .expect("left");
    store
        .create_branch(&ThreadId::from("thread"), right, source.id())
        .await
        .expect("right");

    let child = record(CheckpointId::new(), "thread", Some(source.id()), 2);
    let first = store
        .save_branch(left, child.clone())
        .await
        .expect("first save");
    let replay = store
        .save_branch(left, child.clone())
        .await
        .expect("same-id same-content replay");
    assert!(Arc::ptr_eq(&first, &replay));

    let changed = with_run_id(&child, RunId::new());
    assert!(matches!(
        store.save_branch(left, changed).await,
        Err(CheckpointWriteError::IdempotencyConflict { checkpoint_id })
            if checkpoint_id == child.id()
    ));
    assert!(matches!(
        store.save_branch(right, child.clone()).await,
        Err(CheckpointWriteError::IdempotencyConflict { checkpoint_id })
            if checkpoint_id == child.id()
    ));

    let right_child = record(CheckpointId::new(), "thread", Some(source.id()), 3);
    store
        .save_branch(right, right_child.clone())
        .await
        .expect("different id on other branch");
    assert_eq!(
        store
            .branch_history(&ThreadId::from("thread"), left)
            .await
            .expect("left history")
            .iter()
            .map(|entry| entry.id())
            .collect::<Vec<_>>(),
        [source.id(), child.id()]
    );
    assert_eq!(
        store
            .branch_history(&ThreadId::from("thread"), right)
            .await
            .expect("right history")
            .iter()
            .map(|entry| entry.id())
            .collect::<Vec<_>>(),
        [source.id(), right_child.id()]
    );
}

#[tokio::test]
async fn branch_cas_conflict_does_not_poison_the_store() {
    let store = InMemoryCheckpointStore::new();
    let source = record(CheckpointId::new(), "thread", None, 1);
    store.save(source.clone()).await.expect("source");
    let branch_id = BranchId::new();
    store
        .create_branch(&ThreadId::from("thread"), branch_id, source.id())
        .await
        .expect("branch");
    let winner = record(CheckpointId::new(), "thread", Some(source.id()), 2);
    store
        .save_branch(branch_id, winner.clone())
        .await
        .expect("winner");
    let stale = record(CheckpointId::new(), "thread", Some(source.id()), 3);
    assert!(matches!(
        store.save_branch(branch_id, stale).await,
        Err(CheckpointWriteError::Conflict {
            expected_parent: Some(expected),
            actual_parent: Some(actual),
        }) if expected == source.id() && actual == winner.id()
    ));

    let successor = record(CheckpointId::new(), "thread", Some(winner.id()), 4);
    store
        .save_branch(branch_id, successor.clone())
        .await
        .expect("store remains reusable");
    assert_eq!(
        store
            .branch_head(&ThreadId::from("thread"), branch_id)
            .await
            .expect("head")
            .expect("branch")
            .id(),
        successor.id()
    );
}
