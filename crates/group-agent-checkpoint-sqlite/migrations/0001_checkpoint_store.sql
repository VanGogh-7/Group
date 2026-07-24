CREATE TABLE group_checkpoint_records (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint_id BLOB NOT NULL UNIQUE CHECK (length(checkpoint_id) = 16),
    thread_id TEXT NOT NULL,
    run_id BLOB NOT NULL CHECK (length(run_id) = 16),
    parent_id BLOB CHECK (parent_id IS NULL OR length(parent_id) = 16),
    graph_version TEXT,
    format_version INTEGER NOT NULL CHECK (format_version BETWEEN 0 AND 4294967295),
    superstep_be BLOB NOT NULL CHECK (length(superstep_be) = 8),
    step_be BLOB NOT NULL CHECK (length(step_be) = 8),
    snapshot_schema TEXT NOT NULL,
    snapshot_schema_version INTEGER NOT NULL
        CHECK (snapshot_schema_version BETWEEN 0 AND 4294967295),
    snapshot_encoding TEXT NOT NULL,
    snapshot_bytes BLOB NOT NULL,
    frontier_json TEXT NOT NULL,
    completed INTEGER NOT NULL CHECK (completed IN (0, 1)),
    interrupt_id BLOB CHECK (interrupt_id IS NULL OR length(interrupt_id) = 16),
    interrupt_node_path_json TEXT,
    interrupt_schema TEXT,
    interrupt_schema_version INTEGER
        CHECK (
            interrupt_schema_version IS NULL
            OR interrupt_schema_version BETWEEN 0 AND 4294967295
        ),
    interrupt_encoding TEXT,
    interrupt_bytes BLOB,
    CHECK (
        (
            interrupt_id IS NULL
            AND interrupt_node_path_json IS NULL
            AND interrupt_schema IS NULL
            AND interrupt_schema_version IS NULL
            AND interrupt_encoding IS NULL
            AND interrupt_bytes IS NULL
        )
        OR
        (
            interrupt_id IS NOT NULL
            AND interrupt_node_path_json IS NOT NULL
            AND interrupt_schema IS NOT NULL
            AND interrupt_schema_version IS NOT NULL
            AND interrupt_encoding IS NOT NULL
            AND interrupt_bytes IS NOT NULL
        )
    )
);

CREATE INDEX group_checkpoint_records_thread_sequence
    ON group_checkpoint_records (thread_id, sequence);

CREATE TABLE group_checkpoint_heads (
    thread_id TEXT PRIMARY KEY,
    checkpoint_id BLOB NOT NULL UNIQUE,
    FOREIGN KEY (checkpoint_id)
        REFERENCES group_checkpoint_records (checkpoint_id)
        ON DELETE RESTRICT
);
