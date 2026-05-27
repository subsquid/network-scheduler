# Multi-Version Concurrency Control for Chunks

## Motivation

Today we do not have a mechanism in place to replace chunks in a consistent way without affecting the availability. And while the problem might be less pressing under the low load with a large replication factor, that becomes more relevant both from consistency and availability point of view when corrections or backfills of the data are expected. The same gap also blocks any backfill or correction workflow from being expressed safely, making this proposal a prerequisite for that work.

The solution that this proposal is suggesting is to use multi-version chunk control, which will allow multiple versions of chunks at the same time, providing the read snapshot capabilities.

## Overview

Each scheduling cycle the scheduler produces two assignments:

- **Portal assignment** — a point-in-time view of the data lake across all datasets. Multiple portals may see different portal assignments at any given time, but they are expected to converge to the latest one. There is a set of N last portal assignments that workers are expected to be able to serve.
- **Worker assignment** — a superset of all active portal assignments. It contains every chunk that workers must hold in order to cover requests from portals operating on any currently active portal assignment.

## Invariants

### 1. Single scheduler

Only one scheduler instance may calculate assignments at any given time. Concurrent schedulers would produce conflicting assignments and corrupt the chunk lifecycle state.

### 2. Two-phase assignment publishing

The MVCC mechanism ensures that portals never route queries to workers that don't actually hold the data, and that workers never delete data that portals might still route to.

### 3. Portal assignment promotion threshold

Chunks from the worker assignment are promoted to the portal assignment once X proportion of workers (configuration parameter, agreed on) have reported applying the worker assignment that includes them.

The remaining workers are treated as stragglers -- the portal assignment is published without waiting for them. At any given time we may expect the number of workers that may not run on the supported version, or have difficulties applying assignments for different reasons.

### 4. Chunk removal safety window

When a chunk is superseded by another chunk (backfill or compaction; compaction currently out of scope), it must not be removed from the worker assignment until at least **M minutes** after it was removed from the portal assignment. The worker keeps serving the chunk during the grace period for any stale portals. The scheduler must track when each chunk was removed from the portal assignment to know when it is safe to remove from the worker assignment.

### 5. No overlapping chunks in the portal assignment

When a chunk is replaced by a backfill, the portal assignment must never contain both the original and the replacement chunk covering the same block range. The original chunk must be dropped from the portal assignment in the same update that promotes the replacement chunk, to avoid gaps in coverage.

Workers may temporarily hold multiple chunks covering the same block range during the transition. This overlap in the worker assignment is expected.

For now we do not support multiple-chunk backfill in the atomic way, only a single pair of chunk replacement (old and new) will be supported in the first draft.

## Chunk lifecycle

```mermaid
flowchart LR
    discovered -->|scheduled| assigned[assigned-to-workers]
    assigned -->|X% applied| confirmed
    confirmed -->|promoted| portal[portal-visible]
    portal -->|superseded| retiring[marked-for-removal]
    retiring -->|+M min| removed
```

## Flows

### Adding a new chunk

1. New chunk is discovered and inserted into the database.
2. Worker assignment cycle: the chunk is included in the worker assignment. Workers download it.
3. Workers confirm applying the assignment (X% threshold).
4. Portal assignment cycle: the chunk is promoted to the portal assignment. Portals start routing queries to it.

### Removing a chunk (backfill)

1. The replacement chunk goes through the addition flow above. It is included in the worker assignment and confirmed by workers.
2. The original chunk is marked for removal (`marked_for_removal` is set). This happens after the replacement is confirmed but before the next portal assignment cycle.
3. Portal assignment cycle: the original chunk is dropped and the replacement chunk is promoted in the same update. The portal assignment never contains both chunks covering the same block range (invariant 5).
4. Wait M minutes for all portals to fetch the updated portal assignment (invariant 4).
5. Worker assignment cycle: the original chunk is dropped from the worker assignment. The worker deletes it.

### Chunk migration (worker to worker)

When a chunk moves from worker A to worker B:

1. Add chunk to worker B in the **worker assignment**.
2. Worker B downloads the chunk and confirms.
3. Add chunk-to-worker-B to the **portal assignment** (portals start routing to B).
4. Remove chunk-to-worker-A from the **portal assignment** (portals stop routing to A).
5. Wait M minutes (invariant 4).
6. Remove chunk from worker A in the **worker assignment** (A deletes it).

Steps 3 and 4 happen in the same portal assignment update.

### Replication factor decrease

When the replication factor decreases (e.g., R=3 to R=2):

1. The portal assignment is recomputed with the new replication factor. Portals see fewer chunk-to-worker mappings.
2. Workers continue serving at the old replication factor during the M-minute grace period (invariant 4).
3. After M minutes, the worker assignment is recomputed with the new replication factor. Workers drop excess replicas.

### Replication factor increase

When the replication factor increases (e.g., R=2 to R=3):

1. The new replicas are added to the worker assignment immediately. Workers download the additional copies.
2. Workers confirm applying the assignment (X% threshold).
3. The new replicas appear in the portal assignment. Portals start routing to the additional workers.

### Adding chunks with replication factor decrease

When new chunks would cause the replication factor to change, the scheduler must not block chunk ingestion while the replication change propagates. Instead, it proceeds in parallel. A reservation is created to track the capacity being freed by the in-flight replication decrease, preventing the scheduler from double-committing that capacity across cycles.

Example: 100 existing chunks at R=3, 10 new chunks to add, adding all 10 would cause R to drop to R=2.

**Cycle 1:**
1. Scheduler computes the target assignment (110 chunks). Detects R would change from 3 to 2.
2. Scheduler recalculates: finds that 4 of the 10 new chunks can be added without exceeding capacity.
3. Worker assignment: 100 existing chunks at R=3, plus 4 new chunks at R=2. Workers download the 4 new chunks. The new chunks are added at R=2 directly — adding them at R=3 would create excess replicas that must be dropped in the next cycle.
4. Portal assignment: 100 existing chunks recomputed at R=2. A reservation is recorded for the replication change.
5. Workers confirm the assignment (X% threshold).

**Cycle 2 (after M minutes):**
1. The reservation is fulfilled. The 4 confirmed new chunks are promoted to the portal at R=2.
2. Worker assignment: existing chunks are recomputed at R=2. Workers drop excess replicas from the replication decrease. Capacity is freed.
3. The remaining 6 chunks are evaluated. Some or all can now be added at R=2 into the freed capacity.
4. Repeat until all chunks are ingested.

At every cycle, all pending chunks make progress: some are added immediately, while the replication change propagates in parallel.

If zero new chunks fit without changing the replication factor (capacity is fully saturated), the scheduler must wait for the replication decrease reservation to complete before any new chunks can be added.

## Open questions

- What is the expected value of M? (affects invariant 4 and all flows with grace periods)
- Should the X% promotion threshold be configurable? (invariant 3)
- When a straggler worker (the minority that never applies) persists, at what point is it treated as offline and removed from both assignments? (invariant 3)
