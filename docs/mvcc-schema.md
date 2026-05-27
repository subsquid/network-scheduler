# MVCC Chunks -- Postgres Schema

## Shared tables

### chunks

Chunk metadata. Immutable after insertion.

```sql
CREATE TABLE chunks (
    chunk_pk             BIGSERIAL PRIMARY KEY,
    dataset_id           SMALLINT NOT NULL REFERENCES datasets(id),
    chunk_id             TEXT    NOT NULL,
    size                 INT     NOT NULL,
    files                TEXT[]  NOT NULL,
    last_block_hash      TEXT,
    last_block_timestamp BIGINT,
    registered_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (dataset_id, chunk_id)
);
```

### datasets

Dataset registry. Avoids repeating the dataset string in every chunk row.

```sql
CREATE TABLE datasets (
    id      SMALLSERIAL PRIMARY KEY,
    name    TEXT        NOT NULL UNIQUE
);
```

## Scheduler tables

Only a single scheduler instance may operate at any given time.
Enforced via a Postgres advisory lock acquired at startup:

```sql
SELECT pg_try_advisory_lock(hashtext('network-scheduler'));
```

Returns `true` if the lock is acquired, `false` if another scheduler
is already running. The lock is released automatically when the
connection closes.

### sched_chunk_metadata

Scheduler lifecycle state for each chunk. Separated from chunk metadata
to avoid write amplification — lifecycle updates are frequent while
metadata is immutable.

```sql
CREATE TABLE sched_chunk_metadata (
    chunk_pk                        BIGINT  PRIMARY KEY REFERENCES chunks(chunk_pk),
    applied_at_worker_assignment_id           BIGINT  REFERENCES sched_assignments(id),
    applied_at_portal_assignment_id          BIGINT  REFERENCES sched_assignments(id),
    marked_for_removal              TIMESTAMPTZ,
    dropped_at_portal_assignment_id          BIGINT  REFERENCES sched_assignments(id),
    dropped_at_worker_assignment_id           BIGINT  REFERENCES sched_assignments(id)
);
```

A chunk can be marked for removal (`marked_for_removal` is set) when:

- **Backfill**: the replacement chunk has `applied_at_worker_assignment_id` set
  and that assignment has been confirmed by workers
  (`applied_at_worker_assignment_id <= $latest_worker_assignment_id`).
  The original chunk must be marked before the portal assignment cycle so that
  the drop and promote happen in the same update.
- **Other reasons**: TBD.

### sched_assignments

Each scheduling cycle produces one assignment version. Tracks promotion status
(whether 99% of workers have confirmed applying it).

```sql
CREATE TABLE sched_assignments (
    id              BIGSERIAL   PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    scheduler_version TEXT,
    replication_by_weight JSONB  -- low cardinality, e.g. {"1": 5, "6": 15}
);
```

### sched_workers

Worker registry. The scheduler periodically fetches the active workers list
from ClickHouse. New workers are inserted; workers absent from the active list
are marked stale. Workers stale for longer than 1 month are garbage-collected.
When a worker becomes stale, its rows in `sched_stale_mappings` are deleted
(no point draining data for a worker that is no longer serving requests).

```sql
CREATE TABLE sched_workers (
    id          BIGSERIAL   PRIMARY KEY,
    peer_id     TEXT        NOT NULL UNIQUE,
    version     TEXT,                    -- semver, e.g. '2.8.0'; NULL = unknown
    inactive_since TIMESTAMPTZ          -- NULL = currently active
);
```

Per-cycle logic (inside `update_worker_set`):

1. Workers in `active_workers` but not in `sched_workers` — insert with
   `inactive_since = NULL`.
2. Workers in `sched_workers` with `inactive_since IS NOT NULL` but present in
   `active_workers` — set `inactive_since = NULL`.
3. Workers in `sched_workers` with `inactive_since IS NULL` but absent from
   `active_workers` — set `inactive_since = now()`. Delete their rows from
   `sched_stale_mappings`.
4. Workers where `inactive_since < now() - INTERVAL '1 month'` — delete from
   `sched_workers`.

### sched_worker_confirmations

Tracks the highest assignment version known to be confirmed by workers.

```sql
CREATE TABLE sched_worker_confirmations (
    assignment_id       BIGINT  PRIMARY KEY REFERENCES sched_assignments(id),
    confirmed_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### sched_replication_reservations

Records the intent to free capacity by reducing replication factors
when new chunk ingestion requires more space than is available at the
current replication level. Not used for standalone replication changes.
The `reserved_bytes` specifies how much capacity needs to be freed.
The scheduling algorithm determines which weights to reduce and by how much.

```sql
CREATE TABLE sched_replication_reservations (
    id                   BIGSERIAL   PRIMARY KEY,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    reserved_bytes       BIGINT      NOT NULL,
    portal_assignment_id BIGINT      REFERENCES sched_assignments(id),
    worker_assignment_id BIGINT      REFERENCES sched_assignments(id),
    fulfilled_at         TIMESTAMPTZ
);
```

`portal_assignment_id` is set when the portal assignment applies the reduced replication.
`worker_assignment_id` is set when workers adopt the new replication and the
reserved capacity is used for new chunks.
`fulfilled_at` is set after M minutes have passed since the portal assignment,
indicating the reservation can be consumed.

### sched_chunk_workers

Ideal chunk-to-worker mapping as computed by the scheduling algorithm. Each cycle,
the scheduler diffs the new ideal against the previous one and writes only the
changed rows. This table does not include grace-period holdovers — the published
worker assignment is the union of `sched_chunk_workers` and unexpired entries in
`sched_stale_mappings`.

```sql
CREATE TABLE sched_chunk_workers (
    chunk_pk    BIGINT   PRIMARY KEY REFERENCES chunks(chunk_pk),
    worker_ids  BIGINT[] NOT NULL
);
```

Sizing: 7M chunks × ~350 bytes per row ≈ 2.5 GB.

### sched_stale_mappings

Grace-period holdovers for chunk-to-worker mappings changed by reshuffling. When
the scheduling algorithm moves a chunk from one worker to another (due to worker
join/leave, PeerId rotation, version upgrade, etc.), a record is inserted here to
track when the removal was detected. The published worker assignment is the union
of `sched_chunk_workers` (ideal) and unexpired entries in this table, so the old
worker continues serving the chunk during the M-minute drain window. After M
minutes the stale record is deleted and the old worker drops off the published
assignment naturally.

```sql
CREATE TABLE sched_stale_mappings (
    chunk_pk    BIGINT      NOT NULL,
    worker_id   BIGINT      NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chunk_pk, worker_id)
);
```

Per-cycle logic:

1. Compute the ideal assignment in memory.
2. Diff the new ideal against `sched_chunk_workers` (previous ideal). For any
   worker that lost a chunk, insert into `sched_stale_mappings` (if not already
   present).
3. If the algorithm reassigned the chunk back to the same worker (flip-flop),
   delete the stale record.
4. If `detected_at + M` has passed, delete the stale record. The old worker
   drops off the published assignment automatically since it no longer appears
   in either table.
5. Write only the changed rows to `sched_chunk_workers` (diff-and-patch).
6. When a chunk is dropped entirely via the lifecycle mechanism
   (`dropped_at_worker_assignment_id` is set in `sched_chunk_metadata`),
   delete any corresponding `sched_stale_mappings` rows and the chunk's row
   from `sched_chunk_workers`.

## Variables

- `$latest_worker_assignment_id` — the latest assignment version confirmed by workers
- `$latest_portal_assignment_id` — the latest assignment version published to portals
- `$new_worker_assignment_id` — the new worker assignment version being created in this cycle
- `$new_portal_assignment_id` — the new portal assignment version being created in this cycle

## Queries

### Add a new chunk

Performed by the ingester (or by the scheduler for now):

```sql
INSERT INTO chunks (dataset_id, chunk_id, size, files, last_block_hash, last_block_timestamp)
VALUES ($1, $2, $3, $4, $5, $6);
```

### Scheduler registration

The scheduler finds all chunks that do not have a `sched_chunk_metadata`
row yet and creates one for each:

```sql
INSERT INTO sched_chunk_metadata (chunk_pk)
SELECT chunk_pk FROM chunks c
WHERE NOT EXISTS (
    SELECT 1 FROM sched_chunk_metadata s WHERE s.chunk_pk = c.chunk_pk
);
```

### Assignment generation

#### Build the worker assignment

Drop chunks that have been removed from the portal assignment
and M minutes have passed:

```sql
UPDATE sched_chunk_metadata
SET dropped_at_worker_assignment_id = $new_worker_assignment_id
WHERE dropped_at_portal_assignment_id IS NOT NULL
  AND dropped_at_worker_assignment_id IS NULL
  AND dropped_at_portal_assignment_id IN (
      SELECT id FROM sched_assignments
      WHERE created_at < now() - INTERVAL 'M MINUTES'
  );
```

Fetch unassigned chunks ordered by registration time. The scheduler
iterates over the results, accumulating sizes until the available
capacity budget is reached. The budget accounts for chunks dropped
earlier in this cycle, since workers delete chunks they no longer
see in the assignment before downloading new ones:

```
available = total_worker_capacity
          - size_of_current_worker_chunks
          + size_of_chunks_dropped_this_cycle
```

```sql
SELECT c.dataset_id, c.chunk_id, c.size
FROM chunks c
JOIN sched_chunk_metadata s ON s.chunk_pk = c.chunk_pk
WHERE s.applied_at_worker_assignment_id IS NULL
  AND s.marked_for_removal IS NULL
ORDER BY c.registered_at;
```

Mark the selected subset as assigned to workers:

```sql
UPDATE sched_chunk_metadata
SET applied_at_worker_assignment_id = $new_worker_assignment_id
WHERE chunk_pk IN ($selected_chunk_pks);
```

Return all chunks in the worker assignment:

```sql
SELECT c.dataset_id, c.chunk_id, c.size, c.files, c.last_block_hash, c.last_block_timestamp
FROM chunks c
JOIN sched_chunk_metadata s ON s.chunk_pk = c.chunk_pk
WHERE s.dropped_at_worker_assignment_id IS NULL;
```

#### Build the portal assignment

The drop and promote must execute atomically (single transaction or
CTE (Common Table Expression)) to avoid gaps where neither the original
nor the replacement chunk is in the portal assignment.

Mark newly eligible chunks as promoted to the portal:

```sql
UPDATE sched_chunk_metadata
SET applied_at_portal_assignment_id = $new_portal_assignment_id
WHERE applied_at_worker_assignment_id IS NOT NULL
  AND applied_at_worker_assignment_id <= $latest_worker_assignment_id
  AND applied_at_portal_assignment_id IS NULL
  AND dropped_at_portal_assignment_id IS NULL;
```

Drop chunks marked for removal from the portal:

```sql
UPDATE sched_chunk_metadata
SET dropped_at_portal_assignment_id = $new_portal_assignment_id
WHERE marked_for_removal IS NOT NULL
  AND dropped_at_portal_assignment_id IS NULL;
```

Return all chunks in the portal assignment:

```sql
SELECT c.dataset_id, c.chunk_id, c.size, c.files, c.last_block_hash, c.last_block_timestamp
FROM chunks c
JOIN sched_chunk_metadata s ON s.chunk_pk = c.chunk_pk
WHERE s.applied_at_portal_assignment_id IS NOT NULL
  AND s.dropped_at_portal_assignment_id IS NULL;
```
