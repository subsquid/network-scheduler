# Capacity-Aware (Feasibility-Preserving) Scheduling

## Problem

The old production scheduler (`src/scheduling.rs`, analysed in
[chunk-reshuffling.md](chunk-reshuffling.md)) is **placement-blind**: each cycle it recomputes the
ideal from scratch via consistent hashing as if every worker were empty, with no knowledge of what
each worker physically holds. That is cheap to reason about, but it has three costs this redesign
targets:

- **Wholesale reshuffling** — a replication-factor cliff or a worker join/leave can relocate a large fraction of all chunks in a single cycle.
- **Availability gaps** — a moved-away copy is dropped from its old worker at once, so a reshuffle that relocates *all* of a chunk's replicas simultaneously leaves no worker serving it until the new holders finish downloading.
- **Panic on overflow** — a replica that fits nowhere aborts the whole cycle instead of degrading gracefully.

This design keeps consistent hashing but adds **deferred removal** — a moved-away copy is retained
as a `stale` mapping that keeps serving while it drains over M ticks — plus **capacity-aware
placement**. Deferred removal closes the availability gap, but it raises a new feasibility
requirement: a worker now physically holds `ideal ∪ stale`, so placement can no longer be
placement-blind. Were it left placement-blind, the fresh ideal it hands a worker would fit that
worker's capacity *in its own math*, while the stale copies still draining on that worker's disk —
which the math never counted — push actual usage past the disk. **Feasible on paper, physically
over capacity:**

1. When a chunk moves `A → B`, the new ideal puts the copy on `B`, but the old copy stays on `A` draining for M ticks. Computed as if `A` were empty, the ideal is free to put *other* chunks there too, so `A` physically holds its draining `stale` copies plus whatever `ideal` it was handed.
2. The replication factor `R` is likewise derived from total capacity with no allowance for the bytes those stale copies occupy, so `R` can assume room the drains are still using.

Placement must therefore charge the full physical footprint (next section), so the space a draining
copy still occupies is never handed out a second time.

This is not a rare edge case. Because the scheduler can be **restarted on a different algorithm version, run with changed parameters, see workers leave and rejoin, or watch workers cross a version boundary during a rolling upgrade** (which shifts the *eligible* worker set for version-restricted chunks), a near-total retarget is a *normal* event, not an emergency. The design must keep every published assignment physically feasible by construction, regardless of how large the target shift is.

## Core principle

Make feasibility a **structural invariant**, not a tuned hope, by giving the algorithm honest capacity and letting placement under-deliver when it must:

1. **Charge the full current footprint.** Each worker's available space is `worker_capacity − bytes(everything it physically holds: ideal + draining)` — seeded from what the worker actually has on disk, not the full `worker_capacity` a placement-blind pass assumes by treating every worker as empty.
2. **Credit already-held copies as free.** A copy the worker already has costs **0** (no download); only genuinely-new copies consume residual space.
3. **Place what fits — bounded-load spill within the ring, never overcommit, never panic.** Each replica walks its hash ring from its home; when the home worker is full it continues clockwise to the next worker and lands on the first eligible worker with room (consistent-hashing-with-bounded-loads, exactly as the legacy scheduler spills). A replica is left **unplaced** only when *no* eligible worker on the ring has room. The chunk is then published with fewer than `R` replicas, and the missing replica lands a later cycle as drains free space. Two floor exceptions to "publish what fits" (see *Minimum replication guarantee*): a *new* chunk that cannot reach `min_replication` is excluded entirely this cycle (published with **0** replicas, retried whole next cycle) rather than published with a partial floor; and held copies are added back to keep a chunk at its floor.

The intent is remembered for free: the target is recomputed each cycle, so an unplaced replica (ring saturated) is retried automatically — no persistent "pending" list is required. This relies on the placement order being deterministic and priority-stable (see *Determinism and ordering*); otherwise the retry would jitter and existing placements would churn.

### Why this accounts for reshuffling without measuring it

A reshuffle-away creates **zero new bytes** on the source worker — the chunk was already on disk and already counted in the footprint; it merely changes label from *ideal* to *stale*. The only new bytes are on the **destination**, and those are checked at placement. So once the algorithm (a) charges the full footprint and (b) credits held copies as free, the staleness it creates is automatically accounted for. No separate "how much stale am I generating" metering is needed.

## Invariants

- **No overcommit.** For every worker, `bytes(held ∪ newly-placed) ≤ worker_capacity` at all times.
- **Held *floor* copies are never evicted by capacity pressure.** A held copy costs 0 at the placement step, and the floor add-back re-publishes held copies up to `min_replication`, so a held copy needed to keep a chunk at its floor is always retained — dropped from the ideal **only** by a genuine reshuffle decision, never because a disk filled. This is what keeps "absent from the new ideal ⇒ intentional drop" unambiguous and safe for the copies that carry durability, and it preserves the portal-routing safety guarantee (portals route by *confirmed* routing; old homes drain over M ticks). **Above-floor *bonus* copies are not protected this way:** reconcile caps re-growth at the stage-1 ideal count, and suppresses *all* growth for a cycle in which any new chunk was excluded for missing its floor, so held bonus copies beyond the ideal count are deliberately released (to drain and free room for the contended floor). A bonus held pair can therefore leave the published placement either by a reshuffle or by shedding excess under capacity pressure — only the floor copies carry the eviction guarantee unconditionally.
- **Under-placement is transient and self-healing.** Publishing fewer than `R` replicas is an expected operating state during any migration window, not a failure.
- **Convergence when inputs settle.** When capacity has slack, drains free space and the steady-state ideal (which fits by construction, see below) is reached within O(M) once inputs stop changing. Under the replica-sufficiency drain gate (see *Minimum replication guarantee*) some drains may be deferred, so forward progress is guaranteed *only when slack exists*; under genuine shortage the system degrades to backpressure, not deadlock — the retained copies keep serving throughout. There is no circular wait and no silent breach.
- **Backpressure, not breach.** If the dataset cannot fit even at `min_replication` within the saturation budget (`saturation × worker_capacity × n_workers`), no new copies are admitted and the scheduler signals backpressure via `NotEnoughCapacity` (add capacity). It never drops below the floor and never overcommits.

## Minimum replication guarantee

The floor applies to **portal-visible chunks**: every visible chunk should be held by **≥ `min_replication` eligible workers that have actually confirmed holding it** — confirmed ideal holders plus not-yet-expired stale holders (stale copies are confirmed by definition), counting only workers that satisfy the chunk's `minimum_worker_version`. Freshly-assigned-but-unconfirmed placements do **not** count: a worker told to download a copy that hasn't downloaded it yet provides no durability. "Replication" here is *durability/redundancy* — how many eligible workers physically hold a servable copy. It is distinct from *availability* (how many holders the confirmed routing actually points portals at); both matter and both are upheld below. Three rules together enforce the floor; no single one suffices.

Two things the floor does **not** mean. First, it never causes demotion: a visible chunk is **never removed from the portal for being under-replicated** — the only response to under-replication is re-replication, never hiding the chunk. Second, it is not absolute against *external* events: a worker holding a copy can **depart**, transiently breaching the floor until re-replication restores it (the chunk stays visible and degraded meanwhile). What the rules below ensure is that the scheduler's *own* drains and promotions never take a visible chunk below the floor (see *What this does and does not solve*).

> **Implementation status.** Only **rule 1** is implemented today. Rules 2 and 3 are **design-only — not built yet** (see [README.md](README.md)). Until they land, the floor described here is *not* fully enforced: an under-replicated new chunk can still be promoted, and a reshuffle's old copy drains on the M-tick timer whether or not its replacement has confirmed. The storage layer currently has no notion of a per-chunk confirmed-copy count, which is what both gates need.

1. **Held floor copies are never evicted by capacity pressure** (see Invariants). A visible chunk's *confirmed floor* never drops below `min_replication` because a disk filled up — the floor add-back protects it. (Above-floor *bonus* copies can still be shed under capacity pressure — when the stage-1 ideal shrinks, or when Phase B growth is suppressed because a new chunk was excluded — so the total holder count can fall; the floor cannot.) Only a deliberate reshuffle reduces the floor, and then gradually.

2. **Drains are gated on replica sufficiency, not just time.** *(Not implemented yet — drains currently expire on the M-tick timer alone, with no replica-count check.)* Before a reshuffle stale copy `(chunk, W)` is expired and deleted, the chunk must still have **≥ `min_replication` eligible *confirmed* copies elsewhere** (confirmed ideal holders + other non-expiring stale; unconfirmed placements do not count — a replacement counts only once its worker confirms the download, mirroring the add-then-confirm-then-drop migration flow). If it would not, the copy is **retained past the nominal M** until replacements actually land. So when a chunk's new homes haven't filled yet, the old homes are held — not dropped on a fixed timer. This gate is *only* for reshuffle drains; a whole-chunk removal/backfill legitimately takes the chunk to zero and is exempt — it is identified by a chunk-level `dropped_at_portal_assignment_id`.

3. **Promotion is gated on replica sufficiency.** *(Not implemented yet — promotion is currently gated only on the chunk's worker assignment being confirmed, not on a confirmed-copy count.)* A chunk becomes portal-visible only once **≥ `min_replication`** eligible copies are placed and confirmed. An under-replicated *new* chunk is therefore simply invisible (still ramping up), never visible-but-unsafe — it cannot create a single-point-of-failure that portals route to.

Together: rule 3 ensures nothing was below the floor when it became visible; rules 1–2 ensure nothing visible *drops* below it afterward. The cost is that rule 2 can hold stale copies longer than M under pressure, consuming capacity. If that consumption blocks the very placements needed to release them, the system stalls in a **converging, fully-served** state and surfaces backpressure (add capacity) — it never breaches the floor and never loses data.

**Liveness depends on capacity slack.** The drain gate plus no-overcommit mean a reshuffle progresses only when there is free space to land a *confirmed* replacement **before** the old copy is released. Saturation headroom (running the fleet below 100%) supplies that slack: replacements land in the headroom → their chunks reach the floor → the old copies drain → more headroom frees → the cascade converges. With *zero* slack (every worker exactly full), a pure permutation reshuffle can **freeze**: no copy can move without first breaching a floor or overcommitting, both forbidden. That frozen state is fully served and loses nothing, but it cannot self-resolve — it needs added capacity or lower saturation. So `saturation` is not merely R-headroom; it is the **liveness margin** for convergence, and must be > 0 for the floor-gated drains to make progress.

## Determinism and ordering

Two requirements on the order in which replicas are placed:

- **Deterministic (within a version).** Identical inputs must yield identical placements. Any non-determinism (unstable parallel collection, hash-map iteration order) causes gratuitous churn — a chunk placed one cycle and skipped the next — and must be removed. This is what lets "recompute the target each cycle" substitute for a persistent pending list. Across algorithm *versions* the placement may legitimately differ; that is a real input change, absorbed as a retarget.

- **Essentials-first.** Order placements so every chunk's floor is attempted before any bonus (above-floor) copy; copies a worker already holds are placed free and never re-downloaded. **Established** chunks (those with held copies) go tag-outer — the 1st replica of every chunk, then the 2nd, … up to `min_replication` — with a stable tiebreak (version-restricted chunks first, then the caller's input order). **New** chunks (no held copies) are instead placed **chunk-atomically**: each is filled to its full floor (or rolled back and excluded) before the next new chunk, so two contending new chunks don't each grab a partial floor and *both* roll back to zero, leaving placeable capacity unused. Only after the floors does bonus growth begin — toward each chunk's stage-1 ideal count, not `R`, and only if no new chunk was excluded (see *Two capacity views*). Under contention this starves *extra* replicas before *essential* ones, making the floor achievable quickly rather than dependent on hash luck.

Beyond the essentials-first rounds, the order among the remaining (above-floor) placements is a free tuning knob — heal the most-under-replicated chunks first, or prioritize new-chunk visibility. It affects only how fast different chunks reach full `R`, never correctness or convergence, and can be tuned last.

Note the division of labour with the floor. The algorithm already enforces a *placement-level* floor: essentials-first ordering makes it achievable quickly, and beyond ordering it hard-gates on `min_replication` — a new chunk reaches its full floor atomically or is excluded from the cycle (never published under-replicated), and held chunks retain ≥ `min_replication` copies via floor add-back. What the algorithm cannot guarantee alone is the *confirmed*-copy floor, since it emits unconfirmed placements — that dimension is the storage-side drain and promotion gates (rules 2–3). The two are complementary: the algorithm's placement floor plus essentials-first ordering minimise how long a chunk sits under-replicated; the storage gates ensure nothing *visible* is ever below the floor in *confirmed* copies.

## Two capacity views

The replication factor and the placement use **different** capacity views, deliberately:

| Layer | Capacity view | Rationale |
|-------|---------------|-----------|
| **Replication factor `R`** (global, theoretical) | **Saturated fleet budget** (`worker_capacity × n_workers × saturation`), *stale-blind* | `R` drops when *data volume grows* (permanent) — not when transient stale consumes space. Deriving `R` from capacity-minus-stale would make it **oscillate** (down while draining, up after), and `R` changes are the most expensive reshuffle there is. The `saturation < 1` factor deliberately leaves the headroom that is the convergence/liveness margin. |
| **Placement** (per-worker, concrete) | **`worker_capacity − current footprint`** | Physical reality right now, charged against *full* per-worker capacity (not the saturated budget). This is what actually refuses an over-capacity placement. |

`R` is computed **per weight class**: the saturated budget buys one shared multiplier `K`, and a chunk of weight `w` gets `clamp(round(w × K), min_replication, n_workers)` — heavier chunks earn a larger cap. "`R`" throughout this doc means the chunk's weight-class factor.

The global `R` is the *ceiling* (how many replicas we want); placement is what is *achievable this cycle* (how many fit). Two gaps sit under `R`: stage-1 ideal generation may fit fewer than `R` copies even on the (hypothetically empty) fleet, and stage-2 reconcile grows each chunk only up to that stage-1 ideal count (never re-taking shed bonus copies for free, which would pin them on disk). Both are under-placement that drains away over the next few cycles. On top of that, reconcile suppresses *all* above-floor growth for any cycle in which some *new* chunk was excluded for missing its floor — so a worker with free capacity can still receive no bonus copy that cycle, deliberately, to let bonuses drain and free room for the starved floor. (The trigger is new-chunk exclusion only: an *established* chunk left under its floor because its eligible ring is full does **not** suppress growth — held bonuses regrow free that cycle even though its floor is short.) We do not try to make the theoretical `R` predict per-worker placement — the global average never matches the per-worker truth, so we let placement under-deliver and retry.

Because `R` is derived from the *saturated* budget (`worker_capacity × n_workers × saturation`), the steady-state ideal (no drains) always fits inside that budget with headroom to spare, and is floored at `min_replication`. So the target is always eventually reachable, *unless* the dataset exceeds even the saturated budget at `min_replication` (e.g. workers removed), which is a genuine shortfall handled by backpressure.

For **version-restricted** chunks, placement lands copies only on version-eligible workers, and the floor is enforced over that eligible pool — `NotEnoughCapacity` is returned if too few eligible workers exist. The *target* `R`, however, is derived from the whole fleet, not the eligible subset — so a version-restricted chunk can be handed a target larger than its eligible pool can hold. See the gap note in [README.md](README.md).

## Responsibility split

The mechanism spans two layers with a narrow contract between them. Keeping the split clean is what makes each piece testable in isolation.

**Scheduling algorithm** — a pure function of its inputs; owns *placement feasibility*, not lifecycle:

- Charges the full per-worker footprint (seeds each worker's allocated bytes from everything it already holds — ideal + stale — not zero).
- Credits already-held copies as free (a copy the worker already has costs 0; only genuinely-new copies consume residual capacity).
- Derives `R` from the saturated fleet budget (`worker_capacity × n_workers × saturation`) without subtracting stale.
- Places in essentials-first, deterministic order (established chunks tag-outer; new chunks atomically to their full floor).
- Walks the ring past full workers (bounded-load spill), landing each replica on the first eligible worker with room; leaves a replica unplaced only when the whole ring is full — never panics.
- Enforces a **placement-level floor**: a new chunk reaches `min_replication` atomically or is excluded from the cycle; held chunks retain ≥ `min_replication` copies via floor add-back.
- **Output contract:** *up to* `R` replicas per chunk, as capacity allows — partial placement is a valid result, not an error. Exception at the floor: a **new** chunk (no held copies) is atomic — published with either ≥ `min_replication` replicas or **0** (rolled back and excluded this cycle), never a nonzero count below the floor. Established chunks may publish at any count from 1 up to `R` — and can transiently publish *above* the growth cap when the floor add-back retains held copies disjoint from the newly placed ones (worst case: the stage-1 ideal count plus `min_replication` held copies).

**Storage layer** — owns *lifecycle, the floor, and visibility*:

- Builds the algorithm's current-placement input as `ideal ∪ stale`, carrying each worker's version for eligibility.
- Diffs new vs. old ideal to identify genuine drops (pair previously ideal, now absent) from never-placed retries (pair never placed, absent again). The credit-held-free rule keeps these two cases unambiguous.
- **Rule 2 (drain gate)** *(design-only — not built; drains expire on the M-tick timer today):* does not expire a reshuffle stale copy unless the chunk retains ≥ `min_replication` eligible *confirmed* copies elsewhere.
- **Rule 3 (promotion gate)** *(design-only — not built; promotion gates on worker-assignment confirmation today):* does not promote a chunk to portal visibility until ≥ `min_replication` eligible confirmed copies exist.
- Tracks confirmed routing — the source of truth for "confirmed copies" the gates count.
- Emits the observability counters and backpressure signal.

**Contract between them:** the algorithm hands back a possibly-partial ideal; storage never assumes it is complete at `R`. `min_replication` is a **gate on both sides**. The algorithm enforces a *placement-level* floor: it uses `min_replication` both to prioritise placement (essentials-first) *and* as a hard gate — a new chunk reaches its full floor atomically or is excluded, and held chunks retain ≥ `min_replication` copies via floor add-back. Storage rules 2–3 add the *confirmed-copy* dimension on top (gating drains and promotion on confirmed holders), which the algorithm's unconfirmed placements cannot supply. Neither layer needs the other's internals.

## What this does and does not solve

**Solves:** the transient overlap where this cycle's own reshuffle/drains would push a worker over capacity. Every published assignment is physically feasible.

**Does not solve:** a genuine capacity *shortfall*. If workers are removed or the dataset grows past what the fleet can hold at `min_replication` within its saturation budget (`saturation × worker_capacity × n_workers`), no scheduling trick helps — that surfaces as `NotEnoughCapacity` / backpressure. (The `NotEnoughCapacity` guard trips against the saturation-scaled budget, so with `saturation < 1` it fires before the fleet is physically full at the floor.) Convergence rescues you from transient overlap only, never from a real shortage.

**Does not prevent a holder-loss breach.** The drain gate controls *drains*, not *departures*. If the workers physically holding a chunk's only confirmed copies **depart** (stale mappings for departed workers are purged, not drained), the chunk drops below the floor until re-replication catches up — bounded only by available capacity and download time. A simultaneous departure of more than `R − min_replication` of a chunk's holders is an availability event no scheduler can mask; it can only re-replicate as fast as capacity allows.

## Design alternatives considered

- **A — charge footprint + spill (bounded-load).** When a home is full, spill the replica to the next ring worker. Always feasible; never evicts a safe copy. Its cost is that a large retarget (restart on new version, capacity change) can produce a burst of temporary placements that re-churn as drains clear. **Implemented** — this is what `Reconcile` / `walk_ring` do today (the legacy scheduler spills the same way). Footprint charging + credit-held-free keep it feasible; skip-only-on-full-ring + the atomic floor keep it from panicking or publishing under-replicated.
- **B — charge footprint, fill free space, skip don't spill.** When a replica's home is full, leave it unplaced rather than spilling. Would self-pace large retargets (new homes full of drains ⇒ little moves per cycle ⇒ the rest lands as drains clear) and avoid A's re-churn. **Considered, not built** — the current code walks the whole ring and so does *not* skip at a full home; realizing B would mean restricting placement to (or near) the ideal home instead of the full ring walk. Left as a possible future change if A's re-churn on big retargets proves costly.
- **C — explicit churn cap + reserved headroom.** A forced retarget can only be throttled at the *action* side. Not built; kept in reserve as an optional S3-bandwidth throttle.

A storage-level admission filter *above* the algorithm was also rejected: it cannot lower the replication factor, which is the strongest and most global lever for making room. Deferring placements while `R` stays high makes new data propagate too slowly. Feasibility must be enforced *inside* the algorithm's capacity math so `R` responds.

## Observability and limits

Under-placement and drain-retention are normal states, so they must be *visible*, not silent:

- **Surface** per cycle: chunks below their target `R` (under-replicated), chunks at/below `min_replication` (at the floor), stale copies retained past the nominal M (rule 2 active), and backpressure events (`NotEnoughCapacity` / nothing admitted).
- **Stale-mapping memory** grows with retention. The original bound was "confirmation lag"; rule 2 loosens it to "convergence time," which is unbounded under sustained shortage. This is benign for correctness (copies keep serving) but must be monitored and alerted — a growing retained-stale set means *add capacity*.
- **Promotion stall** (the X% confirmation threshold not being met) is a separate availability lever from the floor; a chunk can be at full `min_replication` durably yet not promoted because confirmations lag. Tracked independently.

