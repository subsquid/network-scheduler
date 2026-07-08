# Capacity-Aware (Feasibility-Preserving) Scheduling

## Problem

The legacy scheduling algorithm is **placement-blind**: it computes the ideal from scratch via
consistent hashing as if every worker were empty, with no knowledge of stale mappings or what each
worker physically holds. Stale bookkeeping lives entirely in the storage layer, which publishes
`ideal ∪ stale`. So the algorithm can place an ideal that, once unioned with the stale copies
still draining on disk, is **feasible on paper but physically over capacity**:

1. When a chunk moves `A → B`, the new ideal puts the copy on `B`, but the old copy stays on `A` draining for M ticks. Seeing `A` as if empty, the algorithm is free to put *other* chunks there too, so `A` physically holds its draining `stale` copies plus whatever `ideal` it was handed.
2. The replication factor `R` is likewise derived from total capacity with no allowance for the bytes those stale copies occupy, so `R` can assume room the drains are still using.

The failure is silent: the placement step only panics when a replica fits *nowhere*; with the
draining bytes invisible to it, it believes the space is free and places happily, so `ideal ∪
stale` quietly exceeds the disk.

This is not a rare edge case. Because the scheduler can be **restarted on a different algorithm version, run with changed parameters, see workers leave and rejoin, or watch workers cross a version boundary during a rolling upgrade** (which shifts the *eligible* worker set for version-restricted chunks), a near-total retarget is a *normal* event, not an emergency. The design must keep every published assignment physically feasible by construction, regardless of how large the target shift is.

## Core principle

Make feasibility a **structural invariant**, not a tuned hope, by giving the algorithm honest capacity and letting placement under-deliver when it must:

1. **Charge the full current footprint.** Each worker's available space is `worker_capacity − bytes(everything it physically holds, ideal + draining)`, not `worker_capacity − prior_stale`.
2. **Credit already-held copies as free.** A copy the worker already has costs **0** (no download); only genuinely-new copies consume residual space.
3. **Place what fits, skip the rest — never spill, never panic.** When a new replica's home is full, leave it **unplaced** this cycle. The chunk is published with fewer than `R` replicas, and the missing replica lands a later cycle as drains free space.

The intent is remembered for free: the target is recomputed each cycle, so the unplaced replica is retried automatically — no persistent "pending" list is required. This relies on the placement order being deterministic and priority-stable (see *Determinism and ordering*); otherwise the retry would jitter and existing placements would churn.

### Why this accounts for reshuffling without measuring it

A reshuffle-away creates **zero new bytes** on the source worker — the chunk was already on disk and already counted in the footprint; it merely changes label from *ideal* to *stale*. The only new bytes are on the **destination**, and those are checked at placement. So once the algorithm (a) charges the full footprint and (b) credits held copies as free, the staleness it creates is automatically accounted for. No separate "how much stale am I generating" metering is needed.

## Invariants

- **No overcommit.** For every worker, `bytes(held ∪ newly-placed) ≤ worker_capacity` at all times.
- **Held copies are never evicted by capacity pressure.** Because held copies cost 0, only *new* downloads can be skipped. Consequently a currently-held `(chunk, worker)` pair is dropped from the ideal **only** by a genuine reshuffle decision — never because the disk was full. This is what keeps "absent from the new ideal ⇒ intentional drop" unambiguous and safe, and it preserves the portal-routing safety guarantee (portals route by *confirmed* routing; old homes drain over M ticks).
- **Under-placement is transient and self-healing.** Publishing fewer than `R` replicas is an expected operating state during any migration window, not a failure.
- **Convergence when inputs settle.** When capacity has slack, drains free space and the steady-state ideal (which fits by construction, see below) is reached within O(M) once inputs stop changing. Under the replica-sufficiency drain gate (see *Minimum replication guarantee*) some drains may be deferred, so forward progress is guaranteed *only when slack exists*; under genuine shortage the system degrades to backpressure, not deadlock — the retained copies keep serving throughout. There is no circular wait and no silent breach.
- **Backpressure, not breach.** If the dataset cannot fit even at `min_replication`, no new copies are admitted and the scheduler signals backpressure (add capacity). It never drops below the floor and never overcommits.

## Minimum replication guarantee

The floor applies to **portal-visible chunks**: every visible chunk should be held by **≥ `min_replication` eligible workers that have actually confirmed holding it** — confirmed ideal holders plus not-yet-expired stale holders (stale copies are confirmed by definition), counting only workers that satisfy the chunk's `minimum_worker_version`. Freshly-assigned-but-unconfirmed placements do **not** count: a worker told to download a copy that hasn't downloaded it yet provides no durability. "Replication" here is *durability/redundancy* — how many eligible workers physically hold a servable copy. It is distinct from *availability* (how many holders the confirmed routing actually points portals at); both matter and both are upheld below. Three rules together enforce the floor; no single one suffices.

Two things the floor does **not** mean. First, it never causes demotion: a visible chunk is **never removed from the portal for being under-replicated** — the only response to under-replication is re-replication, never hiding the chunk. Second, it is not absolute against *external* events: a worker holding a copy can **depart**, transiently breaching the floor until re-replication restores it (the chunk stays visible and degraded meanwhile). What the rules below ensure is that the scheduler's *own* drains and promotions never take a visible chunk below the floor (see *What this does and does not solve*).

> **Implementation status.** Only **rule 1** is implemented today. Rules 2 and 3 are **design-only — not built yet** (see [README.md](README.md)). Until they land, the floor described here is *not* fully enforced: an under-replicated new chunk can still be promoted, and a reshuffle's old copy drains on the M-tick timer whether or not its replacement has confirmed. The storage layer currently has no notion of a per-chunk confirmed-copy count, which is what both gates need.

1. **Held copies are never evicted by capacity pressure** (see Invariants). Under-placement skips only *new* downloads, so a visible chunk's holder count never *falls* because a disk filled up — only a deliberate reshuffle reduces it, and then gradually.

2. **Drains are gated on replica sufficiency, not just time.** *(Not implemented yet — drains currently expire on the M-tick timer alone, with no replica-count check.)* Before a reshuffle stale copy `(chunk, W)` is expired and deleted, the chunk must still have **≥ `min_replication` eligible *confirmed* copies elsewhere** (confirmed ideal holders + other non-expiring stale; unconfirmed placements do not count — a replacement counts only once its worker confirms the download, mirroring the add-then-confirm-then-drop migration flow). If it would not, the copy is **retained past the nominal M** until replacements actually land. So when a chunk's new homes haven't filled yet, the old homes are held — not dropped on a fixed timer. This gate is *only* for reshuffle drains; a whole-chunk removal/backfill legitimately takes the chunk to zero and is exempt — it is identified by a chunk-level `dropped_at_portal_assignment_id`.

3. **Promotion is gated on replica sufficiency.** *(Not implemented yet — promotion is currently gated only on the chunk's worker assignment being confirmed, not on a confirmed-copy count.)* A chunk becomes portal-visible only once **≥ `min_replication`** eligible copies are placed and confirmed. An under-replicated *new* chunk is therefore simply invisible (still ramping up), never visible-but-unsafe — it cannot create a single-point-of-failure that portals route to.

Together: rule 3 ensures nothing was below the floor when it became visible; rules 1–2 ensure nothing visible *drops* below it afterward. The cost is that rule 2 can hold stale copies longer than M under pressure, consuming capacity. If that consumption blocks the very placements needed to release them, the system stalls in a **converging, fully-served** state and surfaces backpressure (add capacity) — it never breaches the floor and never loses data.

**Liveness depends on capacity slack.** The drain gate plus no-overcommit mean a reshuffle progresses only when there is free space to land a *confirmed* replacement **before** the old copy is released. Saturation headroom (running the fleet below 100%) supplies that slack: replacements land in the headroom → their chunks reach the floor → the old copies drain → more headroom frees → the cascade converges. With *zero* slack (every worker exactly full), a pure permutation reshuffle can **freeze**: no copy can move without first breaching a floor or overcommitting, both forbidden. That frozen state is fully served and loses nothing, but it cannot self-resolve — it needs added capacity or lower saturation. So `saturation` is not merely R-headroom; it is the **liveness margin** for convergence, and must be > 0 for the floor-gated drains to make progress.

## Determinism and ordering

Two requirements on the order in which replicas are placed:

- **Deterministic (within a version).** Identical inputs must yield identical placements. Any non-determinism (unstable parallel collection, hash-map iteration order) causes gratuitous churn — a chunk placed one cycle and skipped the next — and must be removed. This is what lets "recompute the target each cycle" substitute for a persistent pending list. Across algorithm *versions* the placement may legitimately differ; that is a real input change, absorbed as a retarget.

- **Essentials-first.** This orders the *new placements only* — copies a worker already holds are placed free regardless of round and are never re-downloaded. Order the new placements in rounds: the 1st replica of every chunk, then the 2nd, … up to `min_replication`, **before** filling any chunk toward its full `R`, with a stable tiebreak (e.g. by chunk pk). Under contention this starves *extra* replicas before *essential* ones, which makes the floor above achievable quickly rather than dependent on hash luck.

Beyond the essentials-first rounds, the order among the remaining (above-floor) placements is a free tuning knob — heal the most-under-replicated chunks first, or prioritize new-chunk visibility. It affects only how fast different chunks reach full `R`, never correctness or convergence, and can be tuned last.

Note the division of labour with the floor: essentials-first ordering makes the floor *achievable quickly*, but it does **not** by itself *guarantee* the floor — that guarantee is the storage-side drain and promotion gates (rules 2–3). The two are complementary: ordering minimises how long a chunk sits under-replicated; the gates ensure nothing visible is ever below it.

## Two capacity views

The replication factor and the placement use **different** capacity views, deliberately:

| Layer | Capacity view | Rationale |
|-------|---------------|-----------|
| **Replication factor `R`** (global, theoretical) | **Full physical capacity** | `R` drops when *data volume grows* (permanent) — not when transient stale consumes space. Deriving `R` from capacity-minus-stale would make it **oscillate** (down while draining, up after), and `R` changes are the most expensive reshuffle there is. |
| **Placement** (per-worker, concrete) | **`capacity − current footprint`** | Physical reality right now. This is what actually refuses an over-capacity placement. |

The global `R` is the *ceiling* (how many replicas we want); placement is what is *achievable this cycle* (how many fit). The gap between them is exactly the under-placement that drains away over the next few cycles. We do not try to make the theoretical `R` predict per-worker placement — the global average never matches the per-worker truth, so we let placement under-deliver and retry.

Because `R` is derived from full capacity, the steady-state ideal (no drains) always fits — the replication factor is computed to fill capacity and floored at `min_replication`. So the target is always eventually reachable, *unless* total capacity itself is insufficient (e.g. workers removed), which is a genuine shortfall handled by backpressure.

For **version-restricted** chunks, placement lands copies only on version-eligible workers, and the floor is enforced over that eligible pool — `NotEnoughCapacity` is returned if too few eligible workers exist. The *target* `R`, however, is derived from the whole fleet, not the eligible subset — so a version-restricted chunk can be handed a target larger than its eligible pool can hold. See the gap note in [README.md](README.md).

## Responsibility split

The mechanism spans two layers with a narrow contract between them. Keeping the split clean is what makes each piece testable in isolation.

**Scheduling algorithm** — a pure function of its inputs; owns *placement feasibility*, not lifecycle:

- Charges the full per-worker footprint (seeds each worker's allocated bytes from everything it already holds — ideal + stale — not zero).
- Credits already-held copies as free (a copy the worker already has costs 0; only genuinely-new copies consume residual capacity).
- Derives `R` from full physical capacity without subtracting stale.
- Places in essentials-first, deterministic order.
- Skips when a new replica's home is full — never spills, never panics.
- **Output contract:** *up to* `R` replicas per chunk, as capacity allows — partial placement is a valid result, not an error.

**Storage layer** — owns *lifecycle, the floor, and visibility*:

- Builds the algorithm's current-placement input as `ideal ∪ stale`, carrying each worker's version for eligibility.
- Diffs new vs. old ideal to identify genuine drops (pair previously ideal, now absent) from never-placed retries (pair never placed, absent again). The credit-held-free rule keeps these two cases unambiguous.
- **Rule 2 (drain gate)** *(design-only — not built; drains expire on the M-tick timer today):* does not expire a reshuffle stale copy unless the chunk retains ≥ `min_replication` eligible *confirmed* copies elsewhere.
- **Rule 3 (promotion gate)** *(design-only — not built; promotion gates on worker-assignment confirmation today):* does not promote a chunk to portal visibility until ≥ `min_replication` eligible confirmed copies exist.
- Tracks confirmed routing — the source of truth for "confirmed copies" the gates count.
- Emits the observability counters and backpressure signal.

**Contract between them:** the algorithm hands back a possibly-partial ideal; storage never assumes it is complete at `R`. `min_replication` appears on both sides for different purposes — the algorithm uses it to *prioritise* placement (essentials-first); storage uses it to *gate* drains and promotion. Neither layer needs the other's internals.

## What this does and does not solve

**Solves:** the transient overlap where this cycle's own reshuffle/drains would push a worker over capacity. Every published assignment is physically feasible.

**Does not solve:** a genuine capacity *shortfall*. If workers are removed or the dataset grows past what the fleet can hold at `min_replication`, no scheduling trick helps — that surfaces as `NotEnoughCapacity` / backpressure. Convergence rescues you from transient overlap only, never from a real shortage.

**Does not prevent a holder-loss breach.** The drain gate controls *drains*, not *departures*. If the workers physically holding a chunk's only confirmed copies **depart** (stale mappings for departed workers are purged, not drained), the chunk drops below the floor until re-replication catches up — bounded only by available capacity and download time. A simultaneous departure of more than `R − min_replication` of a chunk's holders is an availability event no scheduler can mask; it can only re-replicate as fast as capacity allows.

## Design alternatives considered

- **A — charge footprint + spill (bounded-load).** When a home is full, spill the replica to the next ring worker. Always feasible, but a large retarget (restart on new version, capacity change) turns into a storm of temporary placements that re-churn next cycle. Spill is right for small deltas, pathological for large ones. **Rejected** — the constraint that big retargets are normal makes its worst case common.
- **B — charge footprint, fill free space, skip don't spill.** **Chosen.** Self-paces large retargets (new homes full of drains ⇒ little moves per cycle ⇒ the rest lands as drains clear), always feasible, never evicts a safe copy. For small deltas it behaves identically to A.
- **C — explicit churn cap + reserved headroom.** Redundant once B self-paces; a forced retarget can only be throttled at the *action* side, which is exactly B's skip. Kept in reserve only as an optional S3-bandwidth throttle.

A storage-level admission filter *above* the algorithm was also rejected: it cannot lower the replication factor, which is the strongest and most global lever for making room. Deferring placements while `R` stays high makes new data propagate too slowly. Feasibility must be enforced *inside* the algorithm's capacity math so `R` responds.

## Observability and limits

Under-placement and drain-retention are normal states, so they must be *visible*, not silent:

- **Surface** per cycle: chunks below their target `R` (under-replicated), chunks at/below `min_replication` (at the floor), stale copies retained past the nominal M (rule 2 active), and backpressure events (`NotEnoughCapacity` / nothing admitted).
- **Stale-mapping memory** grows with retention. The original bound was "confirmation lag"; rule 2 loosens it to "convergence time," which is unbounded under sustained shortage. This is benign for correctness (copies keep serving) but must be monitored and alerted — a growing retained-stale set means *add capacity*.
- **Promotion stall** (the X% confirmation threshold not being met) is a separate availability lever from the floor; a chunk can be at full `min_replication` durably yet not promoted because confirmations lag. Tracked independently.

