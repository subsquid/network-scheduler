# Chunk Reshuffling Analysis

## Overview

The scheduler assigns chunks to workers using **consistent-hashing-with-bounded-loads**. Assignments are recalculated from scratch each run, but consistent hashing keeps most assignments stable unless inputs change. This document describes when and why chunks move between workers, and the operational cost of each trigger.

## Algorithm Summary

1. Workers are partitioned into **reliable** (Online) and **unreliable** (Offline, Stale, UnsupportedVersion).
2. Replication factors are computed per chunk weight: `R = round(weight × K)`, bounded between `min_replication` and `num_workers`, where K is chosen so that total replicated data fills available capacity.
3. Chunks are distributed via **6000 hash rings** using bounded-load consistent hashing. Each of R replicas is hashed to a ring independently.
4. When `ignore_reliability` is false, two passes run: Pass 1 assigns chunks to reliable workers only; Pass 2 assigns to all workers. Reliable workers keep their Pass 1 assignments; unreliable workers get their Pass 2 assignments.

## Conditions That Cause Chunk Reshuffling

All examples below use the same test parameters for consistency:
- **2,000 workers**, each with **600 GB** capacity
- **500,000 chunks** (~47.6 TB total data), each chunk is **100 MB**
- **Saturation:** 0.95 (total effective capacity: 2,000 × 600 GB × 0.95 = **1,140 TB**)

### 1. Worker Joins the Network

*Test: `scenario_worker_joins` in `src/tests/scenarios.rs`*

**Trigger:** A new worker appears in the worker list.

A fraction of existing workers give up data to the newcomer. No data moves between existing workers.

| Metric | Single weight (R=23) | Multi-weight [1,6,24] |
|--------|---------------------|----------------------|
| Existing workers that lost chunks | 78% | 77% |
| Data removed per existing worker | avg <0.1%, max 0.3% | avg <0.1%, max 0.3% |
| New worker received | ~550 GB | ~548 GB |

Each existing worker loses approximately **1/N** of its data (N = number of workers before join).

### 2. Worker Leaves the Network

*Test: `scenario_worker_leaves` in `src/tests/scenarios.rs`*

**Trigger:** A worker stops pinging and is marked Offline (after `worker_inactive_timeout_sec`, set to 600 seconds in production), or is removed from the worker list entirely.

Mirror image of worker join. Remaining workers only gain chunks — none lose any.

| Metric | Single weight (R=23) | Multi-weight [1,6,24] |
|--------|---------------------|----------------------|
| Remaining workers that gained chunks | 78% | 79% |
| Data added per remaining worker | avg <0.1%, max 0.4% | avg <0.1%, max 0.4% |
| S3 download by remaining workers | ~545 GB | ~560 GB |

**Mass departure (min_replication=22, single weight):**

When many workers leave, total capacity drops and replication factors degrade. Below a threshold, scheduling fails entirely with `NotEnoughCapacity`.

| Workers remaining | Replication | S3 download vs baseline | Status |
|-------------------|------------|------------------------|--------|
| 2,000 | R=23 | 0 TB | Baseline |
| 1,970 | R=22 | 15.7 TB (33%) | Hit min_replication floor |
| 1,900 | R=22 | 52.5 TB (110%) | OK |
| 1,885 | R=22 | 60.4 TB (127%) | Last viable config |
| **1,884** | — | — | **FAILED: NotEnoughCapacity** |

**This is a hard cliff** — 1,885 works, 1,884 fails completely with no assignment for the entire network. Setting `min_replication` close to the natural replication factor leaves very little slack (5.75% worker loss tolerance here).

### 3. Worker Changes Reliability Status

*Test: `scenario_reliability_flip` in `src/tests/scheduling.rs`*

**Trigger:** A worker transitions between Online (reliable) and Offline/Stale/UnsupportedVersion (unreliable). In production, `worker_inactive_timeout_sec` is set to 600 (10 minutes) — a worker that stops pinging for 10 minutes is marked Offline.

**With `ignore_reliability: true` (current production setting), reliability status changes cause zero reshuffling and zero S3 downloads.** The scheduler runs a single pass over all workers regardless of status — Online/Offline is ignored, the worker set doesn't change, replication factors stay the same, and assignments are identical at every step.

**The examples below use `ignore_reliability: false` (two-pass scheduling)** to show the worst-case impact if reliability-aware scheduling were enabled.

**What happens during the full cycle (Online → Offline → Online) with `ignore_reliability: false`?**

When workers go offline, they become unreliable. The scheduler's first pass (which assigns chunks to reliable workers) no longer sees them — from Pass 1's perspective, those workers don't exist. The remaining reliable workers must cover the entire dataset among themselves, which means they download the offline workers' share. This happens even though the offline workers still receive a separate assignment via Pass 2 — reliable workers' assignments are computed independently and don't account for unreliable workers.

Additionally, fewer reliable workers means less total reliable capacity (`worker_capacity × num_reliable_workers × saturation`). If the capacity drop is large enough, replication factors decrease (e.g., weight-6 drops from R=40 to R=39), triggering a further reshuffle of all chunks at that weight. When the workers return, replication factors are restored and that reshuffle reverses.

Each step causes S3 downloads by the **stable workers** (those that stayed online the whole time). The **returning workers download nothing** — since the algorithm is deterministic and nothing else changed, State C equals State A, and the returning workers already have their State A data locally.

**Example — 1 worker goes Online → Offline → Online:**

Replication factors: State A {1: 6, 6: 39} → State B {1: 6, 6: 39} → State C {1: 6, 6: 39}. No change.

| Step | Stable workers S3 download | Returning workers S3 download |
|------|---------------------------|-------------------------------|
| Step 1: Online → Offline | ~546 GB (1.1% of dataset) | 0 GB |
| Step 2: Offline → Online | 0 GB | 0 GB |
| **Total** | **~546 GB (1.1%)** | **0 GB** |

Replication factors stay the same throughout — only consistent-hashing redistribution. Step 1: stable workers download ~546 GB to absorb the 1 offline worker's share. Step 2: when the worker returns, State C equals State A, so stable workers already have their correct data — nothing to download.

**Example — 10 workers go Online → Offline → Online:**

Replication factors: State A {1: 6, 6: 39} → State B {1: 6, 6: 39} → State C {1: 6, 6: 39}. No change.

| Step | Stable workers S3 download | Returning workers S3 download |
|------|---------------------------|-------------------------------|
| Step 1: Online → Offline | ~5.4 TB (11.2% of dataset) | 0 GB |
| Step 2: Offline → Online | 0 GB | 0 GB |
| **Total** | **~5.4 TB (11.2%)** | **0 GB** |

Same as the 1-worker case — replication factors unchanged, so Step 2 is free. The cost scales linearly: 10 workers = ~10x the 1-worker cost.

**Example — 50 workers go Online → Offline → Online:**

Replication factors: State A {1: 6, 6: 40} → State B {1: 6, 6: 39} → State C {1: 6, 6: 40}. Weight-6 drops by 1 during offline.

| Step | Stable workers S3 download | Returning workers S3 download |
|------|---------------------------|-------------------------------|
| Step 1: Online → Offline | ~27.4 TB (56.2% of dataset) | 0 GB |
| Step 2: Offline → Online | ~23.8 TB (48.7%) | 0 GB |
| **Total** | **~51.2 TB (104.9%)** | **0 GB** |

**Why does Step 2 cost ~23.8 TB when returning workers download nothing?** With 50 workers offline, the total reliable capacity drops enough to push weight-6's replication from 40 to 39. Stable workers in State B hold assignments computed for 1,950 reliable workers with R=39. When the 50 workers return (Step 2), the scheduler recomputes for 2,000 reliable workers with R=40. Stable workers must: (1) give back chunks to the returning workers, and (2) download the restored 40th replica that didn't exist in State B. The replication factor restoration is the dominant cost — it's the same mechanism as "new chunks added" (section 4).

**Example — 200 workers go Online → Offline → Online:**

Replication factors: State A {1: 6, 6: 40} → State B {1: 6, 6: 36} → State C {1: 6, 6: 40}. Weight-6 drops by 4 during offline.

| Step | Stable workers S3 download | Returning workers S3 download |
|------|---------------------------|-------------------------------|
| Step 1: Online → Offline | ~102 TB (209% of dataset) | 0 GB |
| Step 2: Offline → Online | ~87.9 TB (180%) | 0 GB |
| **Total** | **~190 TB (389%)** | **0 GB** |

With 200 workers offline (10% of the fleet), the replication factor drops significantly (40→36 = 4 fewer replicas). Both steps are expensive because each replication factor change redistributes all chunks at that weight across the network. The total S3 download is nearly 4x the raw dataset size.

**Key findings:**

- **Returning workers never download from S3.** The algorithm is deterministic — State C equals State A exactly. Since the workers still have their State A data locally, they already have everything they need.
- **Stable workers download in both steps** when replication factors change. Step 1: they absorb offline workers' chunks and adjust to lower replication. Step 2: they adjust back to the original higher replication and give chunks back to returning workers. Both adjustments require downloading new data.
- **When replication factors don't change** (1 and 10 worker examples), Step 2 is free — stable workers' State A and State C assignments are identical.
- **The full cycle is a no-op for assignments** — State A and State C are identical. But the S3 download cost for stable workers is real and unavoidable.
- **Impact scales linearly.** 200 out of 2,000 workers (10%) going down and coming back costs ~190 TB (389% of dataset) of total S3 downloads by stable workers.

**However, if anything changed during the offline period** (new chunks ingested, other workers joined/left, config changed), the returning workers will get a different assignment and may need to download data they don't have locally.

### 4. New Chunks Are Added

*Test: `scenario_new_chunks_added` in `src/tests/scheduling.rs`*

**Trigger:** New data chunks are discovered in S3 storage during `load_new_chunks()`.

**What happens:** If the added chunks don't change the replication factors, existing chunks stay exactly where they are — zero reshuffling. But in practice, adding chunks increases total data size while capacity stays fixed, which forces replication factors down. When replication factors change, **every worker at the affected weights is hit**.

**Single-weight results (500K chunks → initial R=23):**

| Growth | New chunks | R after | Existing chunks that changed owners | S3 download |
|--------|-----------|---------|------------------------------------|-------------|
| 0.5% | 2,500 (~244 GB) | 23 (unchanged) | 0% | ~5.6 TB (11.5% of dataset) |
| 2% | 10,000 (~977 GB) | **22** | **100%** | ~21.5 TB (44%) |
| 50% | 250,000 (~24.4 TB) | **15** | **100%** | ~366 TB (750%) |

**The cliff:** Between 0.5% and 2% growth, the replication factor crosses a rounding threshold (23→22), and suddenly **100% of existing chunks are marked as having changed owners**. The exact threshold in this configuration is between 1% and 2%.

**Multi-weight results (500K chunks, weights [1,6,24], initial R={1:2, 6:13, 24:54}):**

| Growth | New chunks | R after | Existing chunks changed | S3 download |
|--------|-----------|---------|------------------------|-------------|
| 0.5% | 2,500 (~244 GB) | {1:2, 6:13, **24:53**} | 33.4% (weight-24 only) | ~5.5 TB (11.3%) |
| 5% | 25,000 (~2.44 TB) | {1:2, **6:12**, **24:51**} | 66.6% (weights 6, 24) | ~53.1 TB (109%) |
| 50% | 250,000 (~24.4 TB) | {1:2, **6:8**, **24:35**} | 66.7% (weights 6, 24) | ~366 TB (750%) |

With multiple weights, only the weights whose replication factor crosses a threshold are reshuffled. Weight-1 (R=2 is already at the min_replication floor) is never affected. Weight-24 is most sensitive because its high replication factor means small changes in the multiplier K cross thresholds easily.

**S3 downloads:** Only new chunks are fetched (scaled by the post-change replication factor). Existing chunks are not re-downloaded when their replication drops — owners just discard the extra replicas.

**Ownership routing:** When a replication factor changes, a substantial proportion of chunks at that weight have their routing updated. This affects query routing but not S3 traffic.

### 5. Chunk Weight Configuration Changes

**Trigger:** The `datasets` config is modified — e.g., changing weight values or segment boundaries.

**Effect:** Chunks may receive different weights, which changes their replication factors. A chunk whose replication factor increases from R to R+1 gains an additional virtual replica that must be placed on a new worker. A chunk whose replication decreases loses a replica, freeing capacity.

**Scope:** Potentially broad. Weight changes affect all chunks in the modified segment, which can cascade through replication factor calculations for all weights.

### 6. Replication Factor Changes

**Trigger:** Any change to inputs of `calc_replication_factors()`:
- Total capacity changes (workers added/removed, `worker_storage_bytes` changed, `saturation` changed)
- Chunk size distribution changes (new chunks added, chunks removed)
- `min_replication` config changes

**Effect:** When replication factors change, the number of virtual replicas per chunk changes. Each virtual replica is independently hashed, so:
- **Increased replication:** New replicas are placed on workers via the hash rings. Existing replicas stay put.
- **Decreased replication:** Some replicas are removed. The remaining replicas' positions don't change.

**Important nuance:** Because replication factors are computed globally (to fill total capacity), adding workers can increase replication factors (more capacity → higher multiplier K → more replicas). This means adding a worker can cause more reshuffling than just the ~1/N from consistent hashing.

### 7. Worker Capacity or Saturation Changes

*Test: `scenario_capacity_changes` in `src/tests/scheduling.rs`*

**Trigger:** `worker_storage_bytes` or `saturation` config parameters change.

**What happens:** Both parameters feed into `total_capacity = worker_capacity × num_workers × saturation`, which determines replication factors. Increasing capacity or saturation raises replication factors (more replicas to create = S3 downloads). Decreasing them lowers replication factors (replicas are dropped = no downloads needed, workers just discard data).

**Key asymmetry:** Increasing capacity/saturation is expensive (workers must download new replicas). Decreasing is free (workers just discard excess replicas).

**Worker capacity changes (saturation fixed at 0.95, multi-weight [1,6,24]):**

Baseline: capacity=600 GB, replication={1: 2, 6: 13, 24: 54}.

| Change | Replication after | Workers that gained new chunks | Data fetched from S3 by all workers |
|--------|------------------|-------------------------------|-------------------------------------|
| 600→550 GB (-8.3%) | {1: 2, 6: 12, 24: 49} | 0/2,000 (0%) | 0 TB (0% of dataset) |
| 600→500 GB (-16.7%) | {1: 2, 6: 11, 24: 45} | 8/2,000 (<1%) | ~0.8 GB (~0%) |
| 600→650 GB (+8.3%) | {1: 2, 6: 14, 24: 58} | 2,000/2,000 (100%) | ~79.2 TB (166%) |
| 600→700 GB (+16.7%) | {1: 2, 6: 15, 24: 63} | 2,000/2,000 (100%) | ~174.2 TB (365%) |

Decreasing capacity (550 GB, 500 GB): replication drops, workers discard replicas — **essentially zero S3 downloads** (the 0.8 GB in the 500 GB case is rounding noise from capacity overflow redistribution). Increasing capacity (650 GB, 700 GB): replication rises, every worker must download new replicas — **166–365% of the dataset**.

**Saturation changes (capacity fixed at 600 GB, multi-weight [1,6,24]):**

Baseline: saturation=0.95, replication={1: 2, 6: 13, 24: 54}.

| Change | Replication after | Workers that gained new chunks | Data fetched from S3 by all workers |
|--------|------------------|-------------------------------|-------------------------------------|
| 0.95→0.90 (-5.3%) | {1: 2, 6: 12, 24: 51} | 0/2,000 (0%) | 0 TB (0% of dataset) |
| 0.95→0.85 (-10.5%) | {1: 2, 6: 12, 24: 48} | 0/2,000 (0%) | 0 TB (0%) |
| 0.95→0.99 (+4.2%) | {1: 2, 6: 14, 24: 56} | 2,000/2,000 (100%) | ~48.2 TB (101%) |

Same pattern: lowering saturation is free, raising it costs ~103% of the dataset because replication factors increase.

**Note on saturation trade-off:** Lower saturation provides more headroom for stability (fewer cascading overflows when workers join/leave), but wastes storage. Higher saturation uses storage more efficiently but makes replication factors more sensitive to small capacity changes.

### 8. `ignore_reliability` Flag Toggle

**Trigger:** The `ignore_reliability` config flag is changed.

**Effect:**
- **Enabled (true):** Only one scheduling pass runs, treating all workers as reliable. Unreliable workers get the same treatment as reliable ones.
- **Disabled (false):** Two-pass scheduling activates. Reliable and unreliable workers get different assignments.

Toggling this flag causes a **full reshuffle** because the worker sets and scheduling logic change fundamentally.

## What Does NOT Cause Reshuffling

- **Restarting the scheduler** with the same inputs: Assignments are deterministic. Same chunks + same workers + same config = same output.
- **Worker order in the input list:** The algorithm hashes worker IDs, so input order is irrelevant.
- **Time passing** (without any of the above changes): The scheduler produces identical output when re-run with identical inputs.

## Stability Properties

| Scenario | Affected chunks | Unaffected chunks |
|----------|----------------|-------------------|
| 1 worker joins (N total, replication R) | ~R/N of each existing worker's data | ~(N-R)/N per worker |
| 1 worker leaves (N remaining) | None removed from remaining workers; they absorb departed chunks | All existing assignments stable |
| New chunks added (same replication factor) | None of the existing | All existing stable |
| New chunks change replication factor | Potentially massive reshuffle (dominant factor) | None guaranteed |
| Replication factor +1 for weight W | Chunks with weight W get 1 new replica | Existing replicas unchanged |
| Config change (weights/capacity) | Potentially all | None guaranteed |

**Variable definitions:**

- **N** — total number of workers participating in this scheduling pass. In two-pass mode, this is the number of reliable workers for Pass 1, or all workers for Pass 2.
- **R** — replication factor for a given chunk weight. Computed by `calc_replication_factors()` as `max(min_replication, round(weight * K))`, where K is a multiplier chosen so that total replicated data fills the available capacity. Each chunk gets R independent virtual replicas, each hashed to a different ring.
- **W** — chunk weight, an integer assigned per dataset segment in the config (e.g., weight 1 for old data, weight 12 for recent data). Higher weight means more replicas, so recent/important data is stored on more workers.

## Capacity Overflow Behavior

The `assign_chunks()` function enforces strict capacity bounds. When a worker reaches capacity, chunks "spill" to the next worker on the ring. This means:

- Near-full workers are more likely to cause cascading reassignments.
- The `saturation` parameter (typically 0.90-0.99) provides headroom to absorb small changes without overflow.
- Lower saturation = more stability but less efficient use of storage.
- Higher saturation = tighter packing but more sensitive to changes.
