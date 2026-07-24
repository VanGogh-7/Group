use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::{CheckpointId, CheckpointRecord, CheckpointWriteError, CheckpointerError, ThreadId};

/// Asynchronous storage port for storage-neutral checkpoint records.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Atomically applies idempotency, parent CAS, and insertion.
    async fn save(
        &self,
        record: CheckpointRecord,
    ) -> Result<Arc<CheckpointRecord>, CheckpointWriteError>;

    /// Returns the latest record for a logical thread.
    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<CheckpointRecord>>, CheckpointerError>;

    /// Gets one record only when it belongs to the supplied thread.
    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<CheckpointRecord>>, CheckpointerError>;

    /// Returns a thread's records from oldest to newest.
    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<CheckpointRecord>>, CheckpointerError>;
}

/// Thread-safe in-memory storage adapter for checkpoint records.
pub struct InMemoryCheckpointStore {
    state: Mutex<InMemoryRecordState>,
}

#[derive(Default)]
struct InMemoryRecordState {
    histories: HashMap<ThreadId, Vec<Arc<CheckpointRecord>>>,
    by_id: HashMap<CheckpointId, Arc<CheckpointRecord>>,
}

impl InMemoryCheckpointStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(InMemoryRecordState::default()),
        }
    }

    /// Rebuilds an in-memory store from records ordered oldest to newest.
    ///
    /// The same idempotency and parent CAS checks used by asynchronous writes
    /// are applied during import.
    pub fn try_from_records<I>(records: I) -> Result<Self, CheckpointWriteError>
    where
        I: IntoIterator<Item = CheckpointRecord>,
    {
        let store = Self::new();
        for record in records {
            store.insert(record)?;
        }
        Ok(store)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, InMemoryRecordState>, CheckpointerError> {
        self.state
            .lock()
            .map_err(|_| CheckpointerError::message("in-memory checkpoint lock was poisoned"))
    }

    fn insert(
        &self,
        record: CheckpointRecord,
    ) -> Result<Arc<CheckpointRecord>, CheckpointWriteError> {
        let mut state = self.lock().map_err(CheckpointWriteError::Failed)?;
        if let Some(existing) = state.by_id.get(&record.id()) {
            return if existing.as_ref() == &record {
                Ok(Arc::clone(existing))
            } else {
                Err(CheckpointWriteError::IdempotencyConflict {
                    checkpoint_id: record.id(),
                })
            };
        }

        let actual_parent = state
            .histories
            .get(record.thread_id())
            .and_then(|history| history.last())
            .map(|checkpoint| checkpoint.id());
        if actual_parent != record.parent_id() {
            return Err(CheckpointWriteError::Conflict {
                expected_parent: record.parent_id(),
                actual_parent,
            });
        }

        let thread_id = record.thread_id().clone();
        let record = Arc::new(record);
        state
            .histories
            .entry(thread_id)
            .or_default()
            .push(Arc::clone(&record));
        state.by_id.insert(record.id(), Arc::clone(&record));
        Ok(record)
    }
}

impl Default for InMemoryCheckpointStore {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InMemoryCheckpointStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryCheckpointStore")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn save(
        &self,
        record: CheckpointRecord,
    ) -> Result<Arc<CheckpointRecord>, CheckpointWriteError> {
        self.insert(record)
    }

    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<CheckpointRecord>>, CheckpointerError> {
        Ok(self
            .lock()?
            .histories
            .get(thread_id)
            .and_then(|history| history.last())
            .cloned())
    }

    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<CheckpointRecord>>, CheckpointerError> {
        Ok(self
            .lock()?
            .by_id
            .get(&checkpoint_id)
            .filter(|checkpoint| checkpoint.thread_id() == thread_id)
            .cloned())
    }

    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<CheckpointRecord>>, CheckpointerError> {
        Ok(self
            .lock()?
            .histories
            .get(thread_id)
            .cloned()
            .unwrap_or_default())
    }
}
