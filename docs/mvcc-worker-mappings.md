# MVCC Worker Mappings: Deferred Removal

## Problem

The MVCC two-phase mechanism (worker assignment first, portal assignment after confirmation)
protects chunk lifecycle events: additions, backfills, and replication changes. However, it
does not protect **chunk-to-worker mapping changes caused by reshuffling**.

When the worker set changes (join, leave, PeerId rotation, version upgrade), the scheduling
algorithm recomputes assignments from scratch. Some chunks move from one worker to another.
The losing worker sees the new assignment, doesn't find the chunk, and deletes it. If portals
still hold a stale assignment that routes queries to that worker, those queries fail.

This is the same class of problem the M-minute window solves for chunk removal, but applied
to mapping changes that bypass the explicit lifecycle machinery.

## Design

### Schema

Two tables work together: an ideal mapping table that stores the scheduling algorithm's
output, and a stale mappings table that tracks grace-period holdovers. The published worker
assignment is the union of both tables.

```sql
-- Ideal chunk-to-worker mapping as computed by the scheduling algorithm.
-- Updated each cycle (diff-and-patch: only changed rows are written).
-- The published worker assignment is the union of this table and
-- unexpired sched_stale_mappings entries.
CREATE TABLE sched_chunk_workers (
    chunk_pk    BIGINT   PRIMARY KEY REFERENCES chunks(chunk_pk),
    worker_ids  BIGINT[] NOT NULL
);

-- Deferred removals. A record means: this worker used to hold this chunk,
-- the algorithm no longer assigns it there, but the worker must keep it
-- until detected_at + M. It appears in the published assignment (via union
-- with sched_chunk_workers) until the record expires.
CREATE TABLE sched_stale_mappings (
    chunk_pk    BIGINT      NOT NULL,
    worker_id   BIGINT      NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chunk_pk, worker_id)
);
```

**Sizing estimate (worst case):** 7M chunks, replication factor up to 30. The
`sched_chunk_workers` table holds 7M rows at ~350 bytes each, totaling ~2.5 GB. The
`sched_stale_mappings` table is transient -- it only contains in-flight transitions and
is empty on stable cycles.

### Per-cycle logic

Each scheduler cycle executes the following steps:

1. **Compute the ideal assignment** using the scheduling algorithm (hash ring, replication
   factors, current worker set). This runs entirely in memory.

2. **Diff against the previous ideal** (`sched_chunk_workers`). For each chunk where a
   worker **lost** its assignment:
   - Insert a record into `sched_stale_mappings` if one does not already exist.

3. **Resolve flip-flops.** For any record in `sched_stale_mappings` where the algorithm
   now assigns the chunk back to that worker:
   - Delete the stale record. The removal resolved itself.

4. **Expire stale mappings.** For any record where `detected_at + M` has passed:
   - Delete the stale record.
   The old worker drops off the published assignment automatically.

5. **Update `sched_chunk_workers`.** Write only the changed rows (diff-and-patch) so
   the table reflects the new ideal assignment.

6. **Publish.** The worker assignment is built from the union of `sched_chunk_workers`
   (ideal) and unexpired `sched_stale_mappings` (grace-period holdovers).

The portal assignment uses `sched_chunk_workers` directly (without stale mappings), so
portals stop routing to the draining workers immediately. Workers keep serving those
chunks for M minutes to cover portals that haven't fetched the new portal assignment yet.

### Capacity accounting

Workers holding stale mappings consume real capacity. The scheduling algorithm's capacity
budget must subtract the space occupied by stale mappings:

```
available_capacity(worker) = worker_capacity
                           - size_of_ideal_chunks(worker)
                           - size_of_stale_chunks(worker)
```

At normal churn rates (single worker join/leave), per-worker stale data is well under 1%
of capacity, comfortably within the saturation headroom. Under large-scale disruptions, stale
data may exceed headroom. See "Availability tradeoffs" below.

## Availability tradeoffs

The system faces a fundamental consistency-vs-availability tradeoff that no schema design
can eliminate.

### Portal lag

Portals fetch assignments periodically. Between fetches, a portal operates on a stale
snapshot. The M-minute window ensures workers keep data long enough for portals to catch up.
This works when:

- M is greater than the portal's maximum fetch interval plus processing time.
- The scheduler cycle interval is shorter than M (so multiple cycles fit within the window).

With M at 5-10 minutes and the scheduler running every 5 minutes, 1-2 cycles of overlap
is the common case.

### Slow worker confirmation

Chunks are promoted to the portal assignment only after workers confirm applying the worker
assignment (99% threshold). If a significant proportion of workers are slow to confirm
(misconfiguration, network issues, overload), the portal assignment stagnates:

- **Worker assignment moves forward** -- new chunks are added each cycle.
- **Portal assignment stalls** -- waiting for confirmations that aren't arriving.
- The gap between the two grows over time.

There are two choices:

- **Favor consistency:** Keep the 99% threshold strict. The portal assignment falls behind
  but never points to workers without data. New chunks become invisible to queries until
  workers catch up. Availability degrades.
- **Favor availability:** Lower the threshold or promote after a timeout. The portal
  assignment stays current but may route queries to workers that haven't downloaded the
  data yet. Some queries return not-found errors.

The 99% threshold is the tuning knob between these two. The right setting depends on the
network's tolerance for stale-but-correct vs fresh-but-occasionally-wrong results.

### Large-scale disruptions

When a large fraction of the fleet changes simultaneously (mass departure, bulk PeerId
rotation, reliability flip affecting 10%+ of workers), the volume of stale mappings may
exceed the saturation headroom. In this regime:

- Workers cannot hold both ideal and stale chunks within their capacity.
- The scheduler must either drop stale mappings early (accepting query failures on stale
  portals) or delay new assignments (stalling ingestion).
- This is an operational emergency regardless of the MVCC design. The appropriate response
  is monitoring and alerting, not automated resolution.

Straggler workers that persistently fail to confirm should be detected and eventually treated
as offline (removed from both assignments), preventing them from blocking portal promotion
indefinitely.
