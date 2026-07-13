-- Partial indexes for per-cycle statements whose touched set is a small frontier but whose only
-- plan was a full pass over a ~6M-row table (see results/baseline.log). Same pattern as
-- sched_chunk_metadata_marked: predicate matches the statement's WHERE, rows leave the index once
-- stamped, so a quiet cycle is a near-empty index scan.

-- apply_deltas_and_swap section 4 stamps applied_at_worker_assignment_id onto chunks new to the
-- ideal. The stamp is first-touch (never reset), so the unapplied set is the new-chunk frontier
-- plus the never-placed/rejected residue -- without this index the UPDATE joins the full future
-- ideal (~6M rows) against metadata every cycle to change a few thousand rows.
CREATE INDEX IF NOT EXISTS sched_chunk_metadata_unapplied
    ON sched_chunk_metadata (chunk_pk)
    WHERE applied_at_worker_assignment_id IS NULL;

-- activate_confirmed_drains flips every pending stale mapping at/under the confirmation watermark
-- each visibility cycle; keying the watermark column lets the <= bound prune. Rows leave when
-- their portal drop is stamped.
CREATE INDEX IF NOT EXISTS sched_stale_mappings_pending_drain
    ON sched_stale_mappings (superseded_at_worker_assignment_id)
    WHERE dropped_at_portal_assignment_id IS NULL;

-- expire_drained_stale_mappings deletes drained mappings whose portal drop aged out m_ticks ago;
-- this bounds that DELETE to the drained rolling window instead of the whole holdover table.
CREATE INDEX IF NOT EXISTS sched_stale_mappings_drained
    ON sched_stale_mappings (dropped_at_portal_assignment_id)
    WHERE dropped_at_portal_assignment_id IS NOT NULL;
