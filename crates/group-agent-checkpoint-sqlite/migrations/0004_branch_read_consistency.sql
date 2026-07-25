CREATE INDEX group_checkpoint_branch_records_branch_thread_checkpoint
    ON group_checkpoint_branch_records (branch_id, thread_id, checkpoint_id);

CREATE TRIGGER group_checkpoint_branch_insert_requires_source_head
BEFORE INSERT ON group_checkpoint_branches
WHEN NEW.head_checkpoint_id != NEW.source_checkpoint_id
BEGIN
    SELECT RAISE(ABORT, 'a new branch head must equal its source checkpoint');
END;

CREATE TRIGGER group_checkpoint_branch_head_requires_membership
BEFORE UPDATE OF head_checkpoint_id ON group_checkpoint_branches
WHEN NEW.head_checkpoint_id != NEW.source_checkpoint_id
 AND NOT EXISTS (
     SELECT 1
     FROM group_checkpoint_branch_records AS membership
     WHERE membership.branch_id = NEW.branch_id
       AND membership.thread_id = NEW.thread_id
       AND membership.checkpoint_id = NEW.head_checkpoint_id
 )
BEGIN
    SELECT RAISE(ABORT, 'a non-source branch head must belong to the branch');
END;

CREATE TRIGGER group_checkpoint_branch_membership_requires_parent
BEFORE INSERT ON group_checkpoint_branch_records
WHEN NOT EXISTS (
    SELECT 1
    FROM group_checkpoint_branches AS branch
    JOIN group_checkpoint_records AS record
      ON record.thread_id = NEW.thread_id
     AND record.checkpoint_id = NEW.checkpoint_id
    WHERE branch.branch_id = NEW.branch_id
      AND branch.thread_id = NEW.thread_id
      AND record.parent_id IS branch.head_checkpoint_id
)
BEGIN
    SELECT RAISE(ABORT, 'a branch member must continue the current branch head');
END;
