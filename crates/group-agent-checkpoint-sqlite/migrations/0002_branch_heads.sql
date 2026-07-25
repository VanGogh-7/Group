CREATE TABLE group_checkpoint_branches (
    branch_id BLOB PRIMARY KEY CHECK (length(branch_id) = 16),
    thread_id TEXT NOT NULL,
    source_checkpoint_id BLOB NOT NULL,
    head_checkpoint_id BLOB NOT NULL,
    FOREIGN KEY (source_checkpoint_id)
        REFERENCES group_checkpoint_records (checkpoint_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (head_checkpoint_id)
        REFERENCES group_checkpoint_records (checkpoint_id)
        ON DELETE RESTRICT
);

CREATE INDEX group_checkpoint_branches_thread
    ON group_checkpoint_branches (thread_id, branch_id);

CREATE TABLE group_checkpoint_branch_records (
    checkpoint_id BLOB PRIMARY KEY,
    branch_id BLOB NOT NULL,
    FOREIGN KEY (checkpoint_id)
        REFERENCES group_checkpoint_records (checkpoint_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (branch_id)
        REFERENCES group_checkpoint_branches (branch_id)
        ON DELETE RESTRICT
);

CREATE INDEX group_checkpoint_branch_records_branch
    ON group_checkpoint_branch_records (branch_id, checkpoint_id);
