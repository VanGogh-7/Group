CREATE UNIQUE INDEX group_checkpoint_records_thread_checkpoint
    ON group_checkpoint_records (thread_id, checkpoint_id);

CREATE TABLE group_checkpoint_branches_v2 (
    branch_id BLOB PRIMARY KEY CHECK (length(branch_id) = 16),
    thread_id TEXT NOT NULL,
    source_checkpoint_id BLOB NOT NULL,
    head_checkpoint_id BLOB NOT NULL,
    UNIQUE (thread_id, branch_id),
    FOREIGN KEY (thread_id, source_checkpoint_id)
        REFERENCES group_checkpoint_records (thread_id, checkpoint_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (thread_id, head_checkpoint_id)
        REFERENCES group_checkpoint_records (thread_id, checkpoint_id)
        ON DELETE RESTRICT
);

INSERT INTO group_checkpoint_branches_v2 (
    branch_id,
    thread_id,
    source_checkpoint_id,
    head_checkpoint_id
)
SELECT
    branch_id,
    thread_id,
    source_checkpoint_id,
    head_checkpoint_id
FROM group_checkpoint_branches;

CREATE TABLE group_checkpoint_branch_records_v2 (
    checkpoint_id BLOB PRIMARY KEY CHECK (length(checkpoint_id) = 16),
    thread_id TEXT NOT NULL,
    branch_id BLOB NOT NULL CHECK (length(branch_id) = 16),
    FOREIGN KEY (thread_id, checkpoint_id)
        REFERENCES group_checkpoint_records (thread_id, checkpoint_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (thread_id, branch_id)
        REFERENCES group_checkpoint_branches_v2 (thread_id, branch_id)
        ON DELETE RESTRICT
);

INSERT INTO group_checkpoint_branch_records_v2 (
    checkpoint_id,
    thread_id,
    branch_id
)
SELECT
    branch_records.checkpoint_id,
    branches.thread_id,
    branch_records.branch_id
FROM group_checkpoint_branch_records AS branch_records
JOIN group_checkpoint_branches AS branches
    ON branches.branch_id = branch_records.branch_id;

DROP TABLE group_checkpoint_branch_records;
DROP TABLE group_checkpoint_branches;

ALTER TABLE group_checkpoint_branches_v2
    RENAME TO group_checkpoint_branches;
ALTER TABLE group_checkpoint_branch_records_v2
    RENAME TO group_checkpoint_branch_records;

CREATE INDEX group_checkpoint_branches_thread
    ON group_checkpoint_branches (thread_id, branch_id);

CREATE INDEX group_checkpoint_branch_records_branch
    ON group_checkpoint_branch_records (thread_id, branch_id, checkpoint_id);
