-- Drop the chunks FK from the ideal twin, making both twins structurally identical (bare PK), as
-- the future twin's "deliberately do not reference chunks" comment intends. The swap renames made
-- the FK ping-pong between the twins, so COPY paid ~6M per-row FK checks every other cycle
-- (results/baseline.log: write_future_ideal alternating ~20s/~83s) for a guarantee held only half
-- the time.
--
-- Integrity is not weakened: a pk new to the ideal always differs from the live ideal, so the same
-- cycle transaction inserts it into sched_worker_assignment_diffs, whose chunks FK validates it —
-- every pk entering the ideal is checked once, at churn size, when it first appears. And chunks
-- rows are never deleted (sched_chunk_metadata's FK pins them), so the parent side never fires.
--
-- By catalog, not name: the swap's table renames carry constraint names along, so after an odd
-- number of swaps the FK lives on the table named sched_future_ideal_chunk_workers.
DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT conrelid::regclass AS tbl, conname
        FROM pg_constraint
        WHERE contype = 'f'
          AND conrelid IN ('sched_ideal_chunk_workers'::regclass,
                           'sched_future_ideal_chunk_workers'::regclass)
    LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.tbl, r.conname);
    END LOOP;
END $$;
