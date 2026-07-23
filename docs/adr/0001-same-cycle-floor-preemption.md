# ADR 0001 — Same-cycle floor preemption of above-`min_replication` copies

**Status:** Accepted.
Extends [capacity-aware-scheduling.md](../capacity-aware-scheduling.md). It does not replace deferred
removal; it adds a same-cycle fast path for the one case that design leaves stuck. Paired with
[ADR 0002](0002-schema-bundle-covers-visible-chunks.md), which handles the residual case this
mechanism cannot reach. Both ship in the same PR.

## Terms

- **Worker** — a machine that stores chunk copies. **Portal** — the router that sends queries to
  workers.
- **Floor** — a chunk's `min_replication` mandatory copies. **Bonus** — extra copies above the
  floor, earned by query weight. Floors are hard requirements; bonus replication is not.
- **Committed ideal** — the placement the previous cycle committed. It is the durability baseline:
  the retention oracle (`step_safety`) measures against exactly this set.
- **Drain (stale copy)** — a copy the plan moved off a worker. It keeps serving queries for a grace
  window of M ticks, then the worker deletes it.
- **Confirmed routing** — the placement portals actually route by. It advances only when a quorum of
  workers confirm they applied an assignment; the highest confirmed assignment is the
  **watermark**. Workers that lag behind the quorum are **stragglers**.
- **Portal-visible** — published to portals; readers can query the chunk. **Holderless** — zero
  copies on any worker.

## Context

The reconcile frees space only *gradually*: a moved-away copy drains over M ticks, and when a new
chunk's floor doesn't fit, bonus copies are released to drain while further bonus growth is paused.
Every lever works over *future* cycles — nothing frees a byte in the cycle that needs it.
`capacity-aware-scheduling.md` states the consequence plainly: **liveness depends on capacity
slack** — with no free space, a full fleet can freeze, "fully served but unable to self-resolve."

The freeze is real, not theoretical. In the traced failure
(`departure_leaves_a_visible_chunk_holderless`), a portal-visible chunk **C** loses its only copy
when a worker departs. The fleet is momentarily 100% full, so C cannot get a replacement copy — even
though a full worker's worth of that fill is draining copies and bonus copies of *other* chunks.
Crucially, this is **not a shortage**: measured from an empty fleet, everything fits, so the cycle
commits successfully — with C at zero copies. C then falls out of the worker assignment and out of
the frozen schema bundle while portals still route queries to it: a visible chunk nobody can serve
or even resolve.

Root cause: every space-freeing lever is deferred. There is no *same-cycle* reclamation, so at low
or zero slack a starved floor never lands.

## Decision

Add **same-cycle floor preemption** to the reconcile. When a mandatory floor copy cannot be placed
for lack of room, the scheduler deletes — in the same cycle — copies that sit *above some other
chunk's floor*, then places the starved copy in the freed space.

> **Worker protocol requirement:** a worker applies an assignment's *deletions before its
> downloads*. This is what makes the swap safe on a full disk — the momentary over-commit exists
> only in the scheduler's bookkeeping, never on the disk. A worker that downloaded first would hit
> disk-full exactly when preemption fires, never finish applying, and deadlock the swap.

### Durability is the hard invariant; routing consistency is best-effort

The governing constraint: **confirmation lag is unbounded.** Portals refresh their view within M
ticks, but the confirmed routing they refresh *from* only advances when the quorum is met — and
below a 100% quorum, a persistent set of stragglers can stall the watermark indefinitely. If
eviction refused to touch any copy the routing still points at, a stalled watermark could pin disk
space forever and freeze the scheduler's response to new data and worker churn.

So the priorities are: **durability** (a chunk's committed floor) is a hard invariant; **routing
consistency** (queries landing on workers that hold the data) is best-effort, repaired by
re-replication. The donor set follows from one line:

> **The committed floor is the only hard floor. Everything above it is reclaimable.**

A copy of chunk `X` on worker `w` may be reclaimed to floor another chunk **only if**:

1. **It is above `X`'s committed durability floor.** After removing it (and any copies of `X`
   already evicted this cycle), `X` still keeps at least `min(min_replication, |committed[X]|)` of
   its **committed** copies. Counting committed copies — not drains or in-flight downloads — is what
   stops eviction from deleting a copy the donor's floor still needs. A chunk with no committed
   copies (holderless, or being removed) has floor zero and is freely reclaimable — *unless* it is
   portal-visible, in which case its **last held copy** is never taken: deleting it would create the
   very holderless-visible state preemption exists to prevent. Everything above the committed
   floor — bonus copies and leftover drains — is fair game.

2. **Its deletion is possession-safe.** A committed row only says a copy is *planned*, not that it
   was ever *downloaded* — a replacement can lag without bound. The confirmed routing is the one
   possession proof the algorithm has (it only addresses workers that confirmed holding), so
   deleting a **routed** copy additionally requires a surviving committed copy that is itself
   routed. Without this rule, eviction could delete the donor's only real copy while an
   on-paper-only survivor keeps every check green. (Scope and quorum caveats: *Quorum and the
   possession signal*, below.)

Routing never blocks an eviction; it only *orders* the victims. Copies the routing has already moved
off are taken first; a still-routed copy is sacrificed only when nothing else frees enough room.
Deleting a still-routed copy causes a transient, bounded read miss on the donor (counted by
`portal_consistency_misses`); the donor stays served by its committed floor — degraded, never lost.
Preemption fires for floors only, never to grow a bonus.

### Priority tiers, and the one place routing outranks a floor

From hardest to softest:

1. **Committed durability floor** — never evicted, for any reason. Every chunk's floor outranks any
   chunk's extra copies and any chunk's routing consistency. A heavy chunk replicated at 5 with a
   floor of 2 may be cut to 2 to seat a starved chunk, even on copies a lagging portal still routes
   to — but never below 2.
2. **A visible chunk's routed read** — protected best-effort: still-routed copies are last-resort
   victims, taken only for a starved floor, never for bonus growth.
3. **A not-yet-visible chunk's floor** — outranked by tier 2. Seating a chunk *nobody can read yet*
   must not cost a live chunk's readers their copy, so for a not-yet-visible starved chunk every
   still-routed candidate is **vetoed**. It reclaims only unrouted space (leftover drains, unrouted
   bonus, being-removed copies); if that isn't enough, it defers to a later cycle via the existing
   atomic-floor exclusion. That deferral is intended backpressure, not a failure.
4. **Bonus replication** — freely reclaimable for any starved floor.

The tier-1 guard applies in every case and is never relaxed. The tier-2/tier-3 distinction lives in
`place_by_eviction` (`src/multistep_scheduler/mod.rs`), keyed on whether the *seated* chunk is
portal-visible.

### Worked examples (floor = 2, chunk C starved, fleet full)

- **Bonus copies** — X has 5 committed copies by weight → up to 3 may go; X keeps its floor of 2. ✅
- **Leftover drain** — Y moved from w3 to w1; the old copy on w3 is a drain, no longer committed →
  reclaim it; Y keeps its committed floor. ✅
- **Being removed / holderless** — Z has no committed copies → floor zero, freely reclaimable. ✅
- **At the floor** — V has exactly 2 committed copies → untouchable. ❌
- **Still-routed but above floor** — U has 3 committed copies, all still routed → the above-floor
  one may be reclaimed (routing is soft; unrouted victims preferred first). U keeps its floor of 2;
  the read miss is transient. ✅
- **Nobody above floor** — every chunk at exactly its floor, disks full → nothing reclaimable. This
  is a genuine shortage, reported as `NotEnoughCapacity`. ❌

## Consequences

**Positive**

- Restores the strong guarantee: whenever the floors fit from an empty fleet, every floor **lands
  this cycle, even at zero slack** — the excess above the floors is exactly the reclaimable pool, so
  the room always exists. This lifts the "liveness depends on slack" freeze for the floor case.
- Fixes the holderless-visible breach at its cause (C is re-placed the same cycle) and retires the
  `churn_raise_min_replication_underreplicates_new_chunk` FIXME (same mechanism).

**Negative / accepted tradeoffs**

- Deleting a still-routed copy causes a transient read miss on the donor until the routing catches
  up. Unrouted victims are preferred, but routing is never a veto (see above); the donor stays
  served by its committed floor. This is the routing-lag cost `portal_consistency_misses` counts,
  not a new failure class.
- Donors lose surplus copies sooner than the M-tick drain would take them. Accepted: a starved floor
  (durability at risk) outranks another chunk's surplus.

**Neutral / out of scope**

- A genuine shortage (fleet full of floor copies, nothing above any floor) is unchanged: the cycle
  refuses with `NotEnoughCapacity`, as before.
- A residual holderless window remains in one case: when every blocking byte is a **sole-serving
  drain** — the drain is the only real copy of its chunk because the replacement is still
  downloading (mid-reshuffle). Nothing can be safely deleted, so the starved chunk waits. The wait
  is self-healing and short: as soon as any replacement finishes downloading, its drain becomes
  reclaimable — and the starved chunk has first claim on the space, because floors place in
  copy-rounds (every chunk's first copy before any chunk's second), putting a zero-copy chunk at the
  head of the queue. Meanwhile the chunk stays *resolvable* thanks to
  [ADR 0002](0002-schema-bundle-covers-visible-chunks.md). A true shortage commits nothing, so the
  bundle stays frozen with the chunk — no breach there either.

## Invariant / oracle change

The old coverage invariant said: everything the portals route to must be listed in the worker
assignment (`ideal ∪ stale ⊇ routing`). Under this decision that is **too strict** — a deliberate
reclaim deletes a still-routed pair, and with unbounded confirmation lag the oracle would fire on
accepted behavior. It is corrected to the real safety property:

> **No routed chunk is *globally uncovered*: each keeps at least one active listed holder
> *somewhere* (or is being removed).**

The holders need not be among the routed workers. A stale route to an evicted copy, while the chunk
keeps a listed holder elsewhere, is accepted bounded routing lag (the same envelope
`portal_consistency_misses` counts). The oracle fires only when a chunk has **no** listed holder at
all — then a query cannot be answered anywhere. This *corrects* the oracle rather than weakening it:
it already exempts departed workers on the same reasoning ("transient routing to a departed worker
is a documented availability event, not a coverage bug"), and a reclaim is the same category.

The same correction applies to the strict full-quorum `portal_consistency` oracle (within M ticks, a
routed chunk must point at a worker that holds it): a miss is excused while the chunk keeps a listed
holder anywhere, and fires only on a globally uncovered chunk. Retention (`step_safety`) is
untouched — durability stays hard.

### How the relaxation stays honest (hardening)

The relaxed oracles originally had the same blind spot as the eviction guard: both reasoned about
assignment *listings*, not about which copies physically exist on disk. So the one failure the guard
could still cause — deleting the only downloaded copy in favor of a never-downloaded survivor — was
invisible to exactly the oracle that had been relaxed. Four measures close that loop:

- **Possession-confirmed eviction** (guard rule 2): preemption can no longer create the
  zero-physical-copy state **for routed or portal-visible chunks**. A donor that is neither —
  confirmed but not yet published to portals, so its routing view is empty under both backends'
  visibility filter — is still protected only by its committed floor, which an undownloaded survivor
  satisfies on paper. That pre-promotion window has no readers, and closing it is exactly what the
  required applied-report follow-up (below) does.
- **Edge-triggered physical retention** (`assert_physical_retention`): a routed chunk that had at
  least one physical copy must not drop to zero in a single step, unless that step was a worker
  departure (the data leaves with the worker) or happened under a recorded shortage. A *state*-based
  physical check cannot be fatal — a rejoined worker is legitimately listed while its disk is still
  empty — but the *edge* has an attributable cause: outside departures, only a worker applying an
  assignment that dropped its copy deletes data. Fatal at full quorum; recorded as a statistic below
  it (there, quorum-keyed drain expiry can legitimately outrun a lagging replacement). "Listed but
  zero physical copies" is also recorded as a statistic at every quorum.
- **Misses are counted at every quorum**: `portal_consistency_misses` now runs on the strict path
  too, so "the envelope `portal_consistency_misses` counts" is true in the default configuration;
  and the shortage stand-down records what it masks instead of going silent.
- **A checked bound on "bounded" routing lag**: at every drained, shortage-free fixed point the
  confirmed routing must match the worker assignment **pair by pair** — the strict pre-preemption
  check, reinstated exactly where all deliberate lag has provably resolved. A stale pair that
  survives quiescence is a stuck routing update, and fails the run.

The flagship regressions also pin the mechanism end to end
(`assert_all_routed_chunks_have_a_listed_holder`): the departure-emptied chunk is routed only at the
departed worker, which the coverage oracle deliberately exempts — so without this probe, deleting
the preemption code entirely would leave every replay green.

**Quorum and the possession signal.** The production quorum target is around 80%, and may be relaxed
further under fleet-wide lag; the 100%-quorum test configurations exist to *prove the algorithm*
under bounded confirmation, not as an operational target. At 100%, "routed" implies "downloaded", so
the routed-survivor rule is exact there. Below 100%, the watermark can advance on *other* workers'
confirmations, so routing only suggests possession — still strictly safer than no rule. The
quorum-independent possession proof is the **per-worker applied report**: each worker reports every
assignment id it has fully applied (the same signal `confirm_worker_assignment` already records for
the watermark). **Follow-up, required before production:** derive a per-pair `possessed` view from
those reports (`chunk ∈ slice(worker, last_applied(worker))`; unknown means not possessed) and use
it in the eviction guard in place of the routing proxy, in both backends.

## Precondition: the departure residual is kept out of the walk, not hidden

One residual the property sweep surfaces is **not** a scheduler bug and must not be papered over by
an oracle: a chunk's last physically-held copy departs with a worker while the fleet is saturated,
and the fresh re-download that would restore it is starved by competing load for cycles. That is
unavoidable unavailability — the bytes remain recoverable from dataset storage, but no scheduler
decision could have kept a serving copy. Rather than teaching an oracle to accept it (which would
also hide real bugs), the churn generator's `WorkerLeft` precondition (`is_removal_recoverable`) is
tightened: a departure is rejected when it would take a portal-visible chunk's last physically-held
copy. A held copy is durable — it is kept for free every cycle; a chunk reduced to a pending
re-download is not. The walk still generates real departures (a chunk with two held copies survives
losing one); it only refuses the removals that would destroy the last real copy.

## Alternatives considered

- **Reclaim only drains the routing has already moved off** (the first implementation). Rejected:
  strictly weaker — if the blocking space is still-routed drains, the floor never lands — with no
  safety gain over the coverage rule.
- **Keep waiting for drains and growth suppression** (status quo). Rejected: frees space only over M
  ticks and freezes at zero slack; the holderless window persists.
- **Reclaim leftover drains but never bonus copies.** Rejected: that treats bonus replication as if
  it were a floor. Bonus is soft by definition; refusing to shed it forfeits reclaimable space.
- **Bundle-scoping alone** (keep the chunk resolvable while holderless, without preemption).
  Rejected as a standalone fix — it treats the symptom and leaves the chunk at zero copies. It is
  complementary, and is adopted alongside this ADR as
  [ADR 0002](0002-schema-bundle-covers-visible-chunks.md) for the residual preemption cannot reach.

## Related

- [capacity-aware-scheduling.md](../capacity-aware-scheduling.md) — floor vs bonus, two capacity
  views, liveness-vs-slack (the limit this lifts).
- [schema-bundle-lifecycle.md](../schema-bundle-lifecycle.md) — the invariant that surfaced the bug.
- Tests: `departure_leaves_a_visible_chunk_holderless`, `churn_raise_min_replication_underreplicates_new_chunk`.
