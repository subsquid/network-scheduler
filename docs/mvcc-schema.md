# MVCC storage — schema reference

Table definitions live in `migrations/0001_sched_tables.sql`. The protocol these tables serve is
described in [mvcc-storage.md](mvcc-storage.md).

**Time is logical ticks, not wall-clock.** Every `created_at` / `marked_for_removal` /
`inactive_since` / `confirmed_at` is a `BIGINT` tick counter supplied by the caller, and every
drain/GC predicate is integer arithmetic (`created_at <= $now - $m_ticks`), not
`now() - INTERVAL`. The in-memory backend uses the same `u64` tick model. "M ticks" throughout is
a logical grace window, not minutes.

**Single scheduler.** Only one scheduler instance may operate at a time, enforced by a Postgres
advisory lock acquired at startup:

```sql
SELECT pg_try_advisory_lock(hashtext('network-scheduler:' || current_database()));
```

`true` = acquired; `false` = another scheduler holds it (the backend returns
`StorageError::AlreadyRunning`). Released automatically when the connection closes.

## Shared tables

Shared across the ingester, backfill process, and the scheduler. Chunk data is immutable after
insertion.

### datasets

Dataset registry; created by the ingester before any of its chunks are inserted.

```sql
CREATE TABLE datasets (
    id   SMALLSERIAL PRIMARY KEY,
    name TEXT        NOT NULL UNIQUE
);
```

### schemas

A dataset's data schema: its tables, each table's fields, and per-table default fields, stored as
jsonb and read/written whole. Scoped to a dataset (`dataset_id`); identical schemas within a dataset
share one row, deduped by `UNIQUE (dataset_id, hash)` over a **canonical** hash (field order doesn't
matter). A dataset's **current read schema** is the row with `superseded_at IS NULL` — at most one
per dataset (the partial unique index); `set_dataset_schema` stamps the old current's `superseded_at`
and activates the new one, so past schema *contents* are retained (activation history is not:
reverting to an earlier schema reactivates its row in place, erasing when it was superseded). A
chunk references the schema it was **written** under (`chunks.schema_id`).

```sql
CREATE TABLE schemas (
    id            SERIAL      PRIMARY KEY,
    dataset_id    SMALLINT    NOT NULL REFERENCES datasets(id),
    hash          BYTEA       NOT NULL,  -- SHA-256 of the canonical schema json
    schema        JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    superseded_at TIMESTAMPTZ            -- NULL = current read schema
);
```

### chunks

Chunk catalog. The ingester inserts new chunks here; the backfill process also inserts replacement
chunks here atomically as part of `register_correction` (see [chunk_corrections](#chunk_corrections)
below). `registered_at` is the only wall-clock column in the schema — set by the ingester at
insertion time, outside the scheduler's logical-tick model.

```sql
CREATE TABLE chunks (
    chunk_pk             BIGSERIAL   PRIMARY KEY,
    dataset_id           SMALLINT    NOT NULL REFERENCES datasets(id),
    chunk_id             TEXT        NOT NULL,
    size                 INT         NOT NULL,
    schema_id            INTEGER     NOT NULL,  -- schema written under; stamped at insert
    tables_present       BIT VARYING,            -- which of the schema's tables the chunk carries
    first_block          BIGINT      NOT NULL,
    last_block_delta     INT         NOT NULL,  -- last_block - first_block; the span fits in INT
    last_block_hash      TEXT,
    last_block_timestamp BIGINT,
    registered_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

A chunk's file set is **not** stored. Every chunk carries a schema pin: the insert stamps the
dataset's current schema unless the writer supplies one, so `set_dataset_schema` affects only
chunks inserted afterwards. The published/portal reads return the pin plus `tables_present`;
turning those into the actual `<table>.parquet` list (`schema_files`, fed by the trait's
`load_schemas`) happens at assignment construction, not in storage (wire format is a follow-up).
The hot scheduling-cycle read doesn't select either column: it decodes into the slim `AlgoChunk`
view, so the algorithm never sees schema metadata.

`tables_present` is an optional bitmap over the pinned schema's tables in sorted-name order (bit
*i*, left-to-right, = table *i*): 1 = `<table>.parquet` is present, 0 = legitimately absent (a
worker's 404 for it is expected, not data loss). NULL = unknown → all tables present. Ingestion
populating it is a follow-up.

The block range is stored as `(first_block, last_block_delta)` rather than two `BIGINT`s because a
chunk's span fits in `INT`. It is a scheduling input: the weight strategy maps each chunk's
`first_block` to a replication weight, and the range is emitted into the published assignment. The
published/portal reads decode `chunk_pk, dataset_id (via name), chunk_id, size, first_block,
last_block_delta` plus `schema_id` and `tables_present` into `WorkerAssignmentChunk` (`ChunkRow`
in `postgres/rows.rs`); the cycle read decodes the same columns minus the schema pair into
`AlgoChunk` (`AlgoChunkRow`). `last_block_hash`, `last_block_timestamp`, and `registered_at`
are not read by the scheduler.

### chunk_corrections

Written by the backfill process via the scheduler's `register_correction` API, which atomically
inserts the replacement chunk into `chunks` and the correction record here. The scheduler reads
pending rows during the visibility cycle and stamps `applied_at_portal_assignment_id` when the
correction fires. See [mvcc-storage.md](mvcc-storage.md), "Corrections".

```sql
CREATE TABLE chunk_corrections (
    old_chunk_pk                    BIGINT   PRIMARY KEY REFERENCES chunks(chunk_pk),
    new_chunk_pk                    BIGINT   NOT NULL    REFERENCES chunks(chunk_pk),
    dataset_id                      SMALLINT NOT NULL    REFERENCES datasets(id),
    created_at                      BIGINT   NOT NULL,
    applied_at_portal_assignment_id BIGINT   REFERENCES sched_portal_assignments(id),  -- NULL while pending
    CHECK (old_chunk_pk <> new_chunk_pk)
);

-- The replacement must share the old chunk's dataset. Enforced in the database (not application
-- code) because chunks and corrections are written by clients other than the scheduler.
CREATE TRIGGER chunk_corrections_same_dataset
    BEFORE INSERT OR UPDATE OF old_chunk_pk, new_chunk_pk, dataset_id ON chunk_corrections
    FOR EACH ROW EXECUTE FUNCTION chunk_corrections_same_dataset();

-- Serves the structural-readiness lookup: "is this chunk the new_chunk_pk of a pending
-- correction?" (i.e. still being produced by an earlier chain link).
CREATE INDEX chunk_corrections_pending_by_new_chunk
    ON chunk_corrections (new_chunk_pk)
    WHERE applied_at_portal_assignment_id IS NULL;
```

- `old_chunk_pk` (PK) — each chunk superseded at most once.
- `new_chunk_pk` — the replacement. `CHECK (old_chunk_pk <> new_chunk_pk)` forbids self-correction.
- `dataset_id` — denormalized from the chunk. The `chunk_corrections_same_dataset` trigger rejects
  any row whose old or new chunk belongs to a different dataset.
- `created_at` — registration tick; **audit metadata only**. Readiness is structural (a correction
  fires once its replacement is confirmed and no pending correction still has to produce its old
  chunk), so `created_at` no longer orders anything.
- `applied_at_portal_assignment_id` — NULL while **pending**; stamped with the applying portal
  assignment once **completed**. Completed rows are retained as an audit trail, never deleted, so
  "pending" everywhere means `applied_at_portal_assignment_id IS NULL`. The partial index keeps
  retained rows off the hot path; completed rows past a retention horizon could be archived
  independently if the log ever needs bounding.

No schema trigger checks that the old and new chunk cover the same block range; `register_correction`
enforces the 1-to-1 same-range swap at the application layer, rejecting a range-changing replacement
with `CorrectionRejected` (see [nonoverlap-promotion-gate.md](nonoverlap-promotion-gate.md)).

## Scheduler tables

All `sched_*` tables are written exclusively by the scheduler. No external process reads or writes
them directly.

**Two id sequences.** Worker and portal assignments are independent streams, so each has its own
sequence and every referencing column targets exactly one of them. This makes the distinction
structural and visible in the schema; ids are never compared across kinds in code.

```sql
-- One row per scheduling cycle.
CREATE TABLE sched_worker_assignments (
    id                BIGSERIAL PRIMARY KEY,
    created_at        BIGINT    NOT NULL,
    scheduler_version TEXT
);

-- One row per visibility cycle. created_at anchors the M-tick drain window.
CREATE TABLE sched_portal_assignments (
    id         BIGSERIAL PRIMARY KEY,
    created_at BIGINT    NOT NULL
);
```

### sched_chunk_metadata

Per-chunk lifecycle state, separated from immutable chunk metadata to avoid write amplification.

```sql
CREATE TABLE sched_chunk_metadata (
    chunk_pk                        BIGINT PRIMARY KEY REFERENCES chunks(chunk_pk),
    applied_at_worker_assignment_id BIGINT REFERENCES sched_worker_assignments(id),
    applied_at_portal_assignment_id BIGINT REFERENCES sched_portal_assignments(id),
    marked_for_removal              BIGINT,  -- tick when marked; NULL = not marked
    rejected                        BOOLEAN NOT NULL DEFAULT FALSE,  -- rejected at registration; terminal
    dropped_at_portal_assignment_id BIGINT REFERENCES sched_portal_assignments(id),
    dropped_from_worker_assignment_at BIGINT  -- tick when tombstoned; NULL = not tombstoned
);

CREATE INDEX sched_chunk_metadata_portal_drop
    ON sched_chunk_metadata (dropped_at_portal_assignment_id)
    WHERE dropped_at_portal_assignment_id IS NOT NULL
      AND dropped_from_worker_assignment_at IS NULL;
```

**Lifecycle invariant.** Once a chunk has a `sched_chunk_metadata` row it exists for as long as
the chunk stays in `chunks`. After a chunk is dropped from the worker assignment its row remains
as a tombstone (`dropped_from_worker_assignment_at` set) and the chunk is excluded from future
cycles.

`marked_for_removal` is set for two reasons: a confirmed correction's old chunk (driven by
`chunk_corrections`; see [mvcc-storage.md](mvcc-storage.md)), or an outright removal mark. Either
way the drop goes through the drop-marked step.

`rejected` is set when registration refuses a chunk for overlapping a live chunk in its dataset (a
bool, not a tick — registration has no clock). It is a **terminal off-ramp**, not part of the
visibility chain: the chunk is never scheduled, promoted, or reconsidered, and is excluded from
every read that gathers schedulable or live chunks (see
[nonoverlap-promotion-gate.md](nonoverlap-promotion-gate.md)).

### Removal at two granularities, one timing rule

`dropped_at_portal_assignment_id` appears in two tables; in both it anchors the same M-tick drain
(the `created_at` of the referenced portal assignment — Invariant 4). Only the scope differs:

- `sched_chunk_metadata` (keyed by `chunk_pk`) — the **whole chunk** leaves portal visibility
  (gate A).
- `sched_stale_mappings` (keyed by `(chunk_pk, worker_id)`) — a single **(chunk, worker) pair**
  leaves the confirmed routing while the chunk stays visible (gate B).

### sched_workers

Worker registry. The scheduler periodically syncs the active set from ClickHouse: new workers are
inserted, returning workers reactivated (`inactive_since` → NULL), absent workers marked stale
(`inactive_since` set), and workers stale past a GC horizon are deleted. When a worker is marked
departed (absent from the active set) its `sched_stale_mappings` rows are deleted too (no point
draining for a worker that no longer serves) — done on departure-detection in `update_worker_set`,
ahead of the eventual GC of the worker row.

```sql
CREATE TABLE sched_workers (
    id             BIGSERIAL PRIMARY KEY,
    peer_id        TEXT      NOT NULL UNIQUE,
    version        TEXT,            -- semver, e.g. '2.8.0'; NULL = unknown
    inactive_since BIGINT           -- NULL = currently active
);
```

### sched_worker_confirmations

The highest worker assignment confirmed by workers. The confirmation **watermark** is
`MAX(assignment_id)` over this table. The X% quorum that decides *which* id to insert is computed
by the caller, not here (see [mvcc-storage.md](mvcc-storage.md), Invariant 3).

```sql
CREATE TABLE sched_worker_confirmations (
    assignment_id BIGINT PRIMARY KEY REFERENCES sched_worker_assignments(id),
    confirmed_at  BIGINT NOT NULL
);
```

### sched_ideal_chunk_workers / sched_confirmed_chunk_workers

The two routing tables behind the two-gate model. `sched_ideal_chunk_workers` is what the
scheduler currently *wants*; the scheduler serves it to workers as the download target.
`sched_confirmed_chunk_workers` is the confirmed routing the scheduler serves to portals, lagging
the ideal until the confirmation watermark advances. Workers and portals do not read these tables
directly — both consume the scheduler's published assignment. Same shape:

```sql
CREATE TABLE sched_ideal_chunk_workers (
    chunk_pk   BIGINT   PRIMARY KEY REFERENCES chunks(chunk_pk),
    worker_ids BIGINT[] NOT NULL
);

CREATE TABLE sched_confirmed_chunk_workers (
    chunk_pk   BIGINT   PRIMARY KEY REFERENCES chunks(chunk_pk),
    worker_ids BIGINT[] NOT NULL
);
```

The published worker assignment is `sched_ideal_chunk_workers ∪ sched_stale_mappings` — it does
**not** read the confirmed routing (see [mvcc-storage.md](mvcc-storage.md), "Deferred removal").

Sizing: ~7M chunks × ~350 bytes ≈ 2.5 GB each.

### sched_worker_assignment_diffs

Per-cycle routing deltas awaiting replay into `sched_confirmed_chunk_workers`. Each scheduling
cycle records the chunks whose ideal routing changed (vs the previous cycle) and their new
routing; an empty `worker_ids` array means "remove this chunk from confirmed routing". When a
worker assignment is confirmed, the diffs in `(prev_watermark, new_watermark]` are replayed in
order and deleted. Bounded by confirmation lag — empty in steady state.

```sql
CREATE TABLE sched_worker_assignment_diffs (
    worker_assignment_id BIGINT   NOT NULL REFERENCES sched_worker_assignments(id),
    chunk_pk             BIGINT   NOT NULL REFERENCES chunks(chunk_pk),
    worker_ids           BIGINT[] NOT NULL,  -- empty = remove from confirmed routing
    PRIMARY KEY (worker_assignment_id, chunk_pk)
);
```

### sched_stale_mappings

`(chunk, worker)` pairs the ideal removed but the worker must keep serving — while the removal is
unconfirmed and through the M-tick drain after it leaves the portal assignment (gate B). Removals
only; newly *added* pairs ride in `sched_ideal_chunk_workers`.

```sql
CREATE TABLE sched_stale_mappings (
    chunk_pk                           BIGINT NOT NULL REFERENCES chunks(chunk_pk),
    worker_id                          BIGINT NOT NULL REFERENCES sched_workers(id),
    superseded_at_worker_assignment_id BIGINT NOT NULL REFERENCES sched_worker_assignments(id),
    dropped_at_portal_assignment_id    BIGINT REFERENCES sched_portal_assignments(id),  -- NULL while pending
    PRIMARY KEY (chunk_pk, worker_id)
);

CREATE INDEX sched_stale_mappings_worker_id ON sched_stale_mappings (worker_id);
```

- `superseded_at_worker_assignment_id` — first worker assignment whose ideal no longer routes the
  chunk to this worker. Activation gate: the removal is confirmed once this is `<=` the watermark.
- `dropped_at_portal_assignment_id` — NULL while pending; stamped when the removal reaches the
  portal, anchoring the M-tick drain (same rule as `sched_chunk_metadata`).

Lifecycle (pending → draining → dropped) and per-cycle logic: [mvcc-storage.md](mvcc-storage.md),
"Deferred removal". Transient — empty on stable cycles.

## Cycle queries

The two cycles below run their writes inside a single transaction each. **Placement is not done
in SQL** — the cycle loads candidate chunks and the current placement, runs the in-Rust
`SchedulingAlgorithm` to compute the ideal, then writes the result back. (Query bodies are in
`postgres/scheduling_cycle.rs` and `postgres/visibility.rs`.)

### Scheduler registration

Give every chunk that lacks a `sched_chunk_metadata` row one — but as the **non-overlap admission
gate**, not a blind insert: a chunk that overlaps a live chunk in its dataset gets a row with
`rejected = TRUE` instead of a default row (a correction's same-range replacement is exempt). So the
single insert above splits into admitted (default row) and rejected (`rejected = TRUE`) writes; the
overlap resolver and both backends' queries are described in
[nonoverlap-promotion-gate.md](nonoverlap-promotion-gate.md).

```sql
-- admitted: a default row for each non-overlapping new chunk
INSERT INTO sched_chunk_metadata (chunk_pk) SELECT ... RETURNING chunk_pk;
-- rejected: a terminal row for each new chunk that overlaps a live one
INSERT INTO sched_chunk_metadata (chunk_pk, rejected) SELECT ..., TRUE;
```

### Scheduling cycle (builds the worker assignment)

Split across two transactions: a **Phase A** committed up front so the clock-driven GC survives a
**Phase B** shortage rollback (otherwise stale never drains under a sustained shortage).

Phase A (GC):

1. **Tombstone expired chunks** — whole-chunk removals whose portal-drop is ≥ M ticks old, stamping
   the drop tick:
   ```sql
   UPDATE sched_chunk_metadata
   SET dropped_from_worker_assignment_at = $now
   WHERE dropped_at_portal_assignment_id IS NOT NULL
     AND dropped_from_worker_assignment_at IS NULL
     AND dropped_at_portal_assignment_id IN (
         SELECT id FROM sched_portal_assignments WHERE created_at <= $now - $m_ticks);
   ```
2. **Expire drained stale mappings** — the per-pair equivalent (`DELETE FROM sched_stale_mappings
   WHERE dropped_at_portal_assignment_id` references a portal assignment ≥ M ticks old).

Phase B (placement reconcile):

3. **Load inputs** — active chunks (`dropped_from_worker_assignment_at IS NULL`, joined to
   `datasets` for the name), the worker set, and the current placement
   (`sched_ideal_chunk_workers ∪ sched_stale_mappings`).
4. **Compute the ideal in Rust** via the `SchedulingAlgorithm`.
5. **Open** a new `sched_worker_assignments` row (`created_at = now`).
6. **Mint pending stale mappings** for each `(chunk, worker)` pair the new ideal drops (skipping
   chunks being removed at the chunk level and workers GC'd from `sched_workers`):
   `INSERT INTO sched_stale_mappings (...) ON CONFLICT (chunk_pk, worker_id) DO NOTHING`.
7. **Record routing diffs** into `sched_worker_assignment_diffs` (changed chunks + removals as
   empty arrays).
8. **Commit the new ideal** — upsert present pairs, delete absent chunks from
   `sched_ideal_chunk_workers`.
9. **Resolve flip-flops** — delete stale rows for pairs the new ideal re-added.
10. **Stamp entered chunks** — `applied_at_worker_assignment_id = $new_wa_id` for newly placed
    chunks (`WHERE applied_at_worker_assignment_id IS NULL`).

The published assignment is then read post-commit as `ideal ∪ stale` over non-tombstoned chunks.

### Visibility cycle (builds the portal assignment)

All in one transaction so the swap is atomic (Invariant 5):

1. **Apply ready corrections** (before promote/drop). Resolve the ready set with an
   order-independent fixpoint (a correction fires once its replacement is confirmed and its old
   chunk is no longer the `new_chunk_pk` of a pending correction), so chains collapse in one pass;
   for each fired correction set `marked_for_removal` on the old chunk and stamp the correction's
   `applied_at_portal_assignment_id`.
2. **Promote eligible chunks** — confirmed, not yet promoted, not dropped, not marked, and not a
   pending correction's `new_chunk_pk`:
   ```sql
   UPDATE sched_chunk_metadata
   SET applied_at_portal_assignment_id = $new_pa_id
   WHERE applied_at_worker_assignment_id IS NOT NULL
     AND applied_at_worker_assignment_id <= $confirmed_watermark
     AND applied_at_portal_assignment_id IS NULL
     AND dropped_at_portal_assignment_id IS NULL
     AND marked_for_removal IS NULL
     AND chunk_pk NOT IN (
         SELECT new_chunk_pk FROM chunk_corrections WHERE applied_at_portal_assignment_id IS NULL);
   ```
3. **Drop marked chunks** — `dropped_at_portal_assignment_id = $new_pa_id WHERE marked_for_removal
   IS NOT NULL AND dropped_at_portal_assignment_id IS NULL`.
4. **Activate confirmed drains** — stamp pending stale mappings whose
   `superseded_at_worker_assignment_id <= $confirmed_watermark` with `dropped_at_portal_assignment_id`.
5. **Return** portal-visible chunks (`applied_at_portal_assignment_id IS NOT NULL AND
   dropped_at_portal_assignment_id IS NULL`) with their confirmed routing.

### Confirming a worker assignment

When the caller reports a new confirmation watermark, the diffs in `(prev, new]` are replayed into
`sched_confirmed_chunk_workers` in `(worker_assignment_id, chunk_pk)` order (empty array =
delete) and then dropped from `sched_worker_assignment_diffs`.

## Both backends

The in-memory backend (`src/scheduler_storage/in_memory/`) models the same tables as Rust
collections with matching field names (`SchedulerChunkMetadata`, `StaleMapping`,
`ChunkCorrection`, the `sched_*` maps) and the same `u64` tick model, so the two backends agree on
behavior. The protocol is property-tested against both.
