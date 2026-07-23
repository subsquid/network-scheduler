# ADR 0001 — Same-cycle floor preemption of above-`min_replication` copies

**Status:** Accepted.
Extends [capacity-aware-scheduling.md](../capacity-aware-scheduling.md); it does not replace deferred
removal — it adds a same-cycle fast path for the one case that design leaves frozen. Paired with
[ADR 0002](0002-schema-bundle-covers-visible-chunks.md) (schema-bundle consistency for the residual
this cannot reach); both are delivered together in the same PR.

## Context

The multistep reconcile is capacity-aware with deferred removal: a moved-away copy is retained as a
`stale` (draining) mapping that keeps serving and frees over M ticks, and when a new-chunk floor is
unmet, above-ideal **bonus** copies are released to drain and bonus growth is suppressed. Both levers
reclaim space only over *subsequent* cycles. `capacity-aware-scheduling.md` states the resulting
limit plainly: **liveness depends on capacity slack** — at zero slack a full fleet can freeze, "fully
served but unable to self-resolve."

That freeze is not only theoretical. Traced failure (`departure_leaves_a_visible_chunk_holderless`):
a portal-visible chunk **C** whose sole copy left with a departing worker cannot get a fresh floor
copy on a momentarily 100%-full fleet — even though a full worker's worth of that fill is draining or
weight-bonus copies of *other* chunks. Stage 1 succeeds (the floor set fits from empty), so this is
**not a shortage**: the cycle commits `Ok` with C at 0 copies. C then falls out of `ideal ∪ stale`,
out of the worker assignment, and out of the frozen schema bundle — while still portal-visible,
breaking schema-bundle consistency and leaving a visible chunk holderless. Measured at the failing
cycle: 4 workers at 100%, ~10 MiB draining, C needs 1 MiB, `is_infeasible = false`, C committed at 0.

Root cause: every lever that would free the needed byte is **deferred** (drain-on-timer, growth
suppression). There is no *same-cycle* reclamation, so at low or zero slack a starved floor never
lands.

## Decision

Add **same-cycle floor preemption** to the reconcile. When placing a mandatory floor copy
(`tag < min_replication`) fails for lack of room, reclaim space by **deleting, this cycle**, copies
that sit **above some other chunk's `min_replication` floor**, then place the floor copy. It is a
same-cycle swap: the donor worker deletes before it downloads (momentary over-commit accepted).

> **Worker protocol requirement:** a worker applies an assignment's *deletions before its
> downloads*. This is what makes the swap disk-safe at 100% fill — the "momentary over-commit" is
> book-keeping only, never physical. A worker that downloaded first would hit disk-full exactly when
> preemption fires, never complete the apply, and deadlock the swap.

### Durability is the hard invariant; routing consistency is best-effort

The maintainer's constraint: **worker-assignment confirmation lag is unbounded.** The portal's own
staleness is bounded — it re-reads the confirmed routing within M ticks (a few scheduling cycles) —
but the *confirmed routing itself* only advances when the fetch quorum is met. Below a 100% quorum a
persistent straggler set stalls the confirmation watermark indefinitely (e.g. we wait for 98% but only
90% catch up regularly), so the portal keeps routing from stale placement for as long as confirmation
is stuck. Eviction must therefore **not** hard-protect routing: a hard routing veto would let a stalled
watermark pin disk and freeze the scheduler's reaction to new ingestion and worker join/leave. So
durability (a chunk's committed floor) is the hard invariant; keeping routing consistent is best-effort,
bounded by re-replication.

The donor set is governed by one line:

> **The committed floor is the only hard floor. Everything above it is reclaimable.**

A copy of chunk `X` on worker `w` may be reclaimed to floor another chunk **iff**:

1. **It is above `X`'s committed durability floor** — after removing it (and any copies of `X` already
   evicted this cycle), `X` still keeps ≥ `min(min_replication, |committed[X]|)` of its **committed**
   copies. `committed[X]` is the pre-cycle committed ideal — *exactly* the set `step_safety` retention
   measures against — so counting survivors here is what keeps eviction from hard-deleting a copy the
   donor's durability floor still needs. This is the durability-hard guard (it fixes the retention
   breach where a re-promoted drain was miscounted as a floor copy and a committed copy hard-deleted).
   `|committed[X]| = 0` (holderless or being removed) ⇒ floor 0 ⇒ freely reclaimable — *unless* `X`
   is portal-visible, in which case its **last held copy** is never taken (see the possession
   amendment below): deleting it would manufacture the holderless-visible state preemption exists to
   close. Everything above
   the committed floor — weight-earned **bonus** copies and leftover **draining** copies — is fair
   game; weight/bonus replication is soft, floors are hard.

2. **Its deletion is possession-safe.** A committed row alone doesn't prove the copy was ever
   downloaded — a committed-but-never-fetched replacement can lag unboundedly. The confirmed routing
   is the algorithm's one possession proof (it only addresses workers that confirmed holding), so
   deleting a **routed** copy additionally requires a surviving committed copy that is itself routed.
   Counting an unfetched survivor would let eviction hard-delete the donor's only physical copy while
   every oracle sees a "covered" chunk.

Routing is a **soft** input, never a veto: `confirmed_routing[X]` only *orders* eviction victims —
copies the routing has already moved off are evicted before still-routed ones, so a still-routed copy
is sacrificed only when a starved floor cannot otherwise land. Deleting a still-routed copy causes a
**transient, bounded** read miss on the donor (already counted by `portal_consistency_misses`); the
donor stays served by its committed floor, so the chunk keeps a covered holder — degraded, never lost.

Never reclaimed: a committed copy any chunk needs to *stay at* its committed floor. Preemption fires
for floors only — never to grow a bonus.

Priority, stated explicitly: **every chunk's committed durability floor outranks any chunk's
above-floor (weight/bonus) replication and any chunk's routing consistency.** A heavy chunk replicated
at 5 with `min_replication = 2` may be dropped to 2 to floor a starved chunk, even on copies a lagging
portal still routes to.

### Priority tiers, and the one place routing outranks a new chunk's floor

Ordering the levers from hardest to softest:

1. **Committed durability floor** (hardest) — never evicted, for any reason.
2. **An existing (portal-visible) chunk's routed read** — protected best-effort. A still-routed
   above-floor copy is reclaimed only as a last resort (not-routed victims first), and only for a
   *starved floor*, never a bonus. Sacrificing it is bounded routing-lag, not loss.
3. **A new (not-yet-visible) chunk's floor** — outranked by tier 2. Seating a chunk *nobody reads
   yet* must not evict a copy a live chunk's readers still hit. So when the starved chunk `C` is
   **not** portal-visible, eviction is differentiated:
   - `C` **portal-visible** (existing degraded chunk): tier-2 rule — routing is soft ordering; a
     still-routed above-floor copy may be evicted as a last resort.
   - `C` **not portal-visible** (new chunk): a candidate `(X, w)` with `w ∈ routed[X]` is **vetoed**.
     A new chunk reclaims only *non-routed* space (leftover drains, unrouted bonus, being-removed
     copies). If that isn't enough it **defers** via the existing atomic-floor exclusion
     (backpressure) and lands once space frees — intended, not a failure.
4. **Above-floor (bonus/weight) replication** (softest) — freely reclaimable for any starved floor.

The durability guard (tier 1) applies in every case and is never relaxed. The differentiation lives in
`place_by_eviction` (`src/multistep_scheduler/mod.rs`): the seated chunk's `is_portal_visible` gates
whether a still-routed donor copy is a soft-ordered victim (visible) or a hard veto (new).

### Worked examples (`min_replication = 2`, C below its floor, fleet full)

- **Bonus copies** — X has 5 committed copies (by weight) → drop up to 3, down to its floor of 2. ✅
- **Leftover drain** — Y moved w3→w1; w3's old copy is no longer committed (a drain) → drop Y on w3
  (Y keeps its committed floor). ✅
- **Being deleted / holderless** — Z's committed floor is 0 → drop any of Z's copies freely. ✅
- **At the floor** — V has exactly 2 committed copies → off-limits (would breach V's committed
  floor). ❌
- **Still-routed but above floor** — U has 3 committed copies, all still addressed by the confirmed
  routing (which lags) →
  the above-floor one is *reclaimable* (routing is soft), preferring a not-routed copy first if one
  exists. Durability floor of 2 stays; the transient read miss is bounded. ✅
- **Nobody above floor** — everyone at exactly 2 committed, disk full → nothing reclaimable; genuine
  shortage, reported as `NotEnoughCapacity`. ❌

## Consequences

**Positive**

- Restores the strong guarantee: when Stage 1 is feasible (floors fit from empty), a floor **always
  lands, even at zero capacity slack** — lifting the "liveness depends on slack" freeze for the floor
  case. The excess above every floor is exactly the reclaimable pool, so the room always exists.
- Fixes the holderless-visible schema-bundle breach at its cause (C is placed the same cycle), and
  retires the `churn_raise_min_replication_underreplicates_new_chunk` FIXME (same mechanism).

**Negative / accepted tradeoffs**

- Deleting an above-floor copy the routing still addresses causes a **transient read miss** on the
  donor until confirmation sheds that route. Eviction *prefers* not-yet-routed victims, so a
  still-routed copy is taken only when a starved floor cannot otherwise land — but routing is soft, so
  it is never a veto (confirmation lag is unbounded below a full quorum; a hard veto would let a stalled
  watermark freeze the
  scheduler). The donor stays served by its committed floor — degraded, never lost. This is the
  documented X% routing-lag cost already counted by `portal_consistency_misses`, not a new failure
  class.
- It sheds donor redundancy sooner than the M-tick drain would. Accepted: a starved floor (durability
  at risk) outranks another chunk's surplus.

**Neutral / out of scope**

- Does **not** solve a genuine shortage (fleet full of floor copies, nothing above floor to reclaim):
  still `NotEnoughCapacity` backpressure, unchanged.
- A residual **holderless** (0-copy) window remains only when the fill is genuinely un-reclaimable —
  a fleet full of **sole-serving** drains (mid-reshuffle, replacements still downloading). Availability
  there cannot be restored this cycle. While holderless, the chunk keeps first claim on recovery:
  floors place copy-round-outer (tag-outer), so its *first* copy contends in round 0 — ahead of every
  chunk's second copy and all new chunks — and it lands the moment any byte becomes reclaimable. Its *consistency* consequence — a visible chunk momentarily
  absent from the schema bundle — is closed by [ADR 0002](0002-schema-bundle-covers-visible-chunks.md)
  (the bundle covers visible chunks, so the chunk stays resolvable: degraded, but consistent). A true
  *shortage* commits nothing, so the bundle stays frozen with the chunk — no breach there.

## Invariant / oracle change

The published-coverage invariant `ideal ∪ stale ⊇ routing` (sim oracle `published_coverage`) is **too
strict** under this decision — a deliberate reclaim deletes a still-routed pair, and confirmation lag
is unbounded below a full quorum, so demanding routing consistency at all would make the oracle fire on
accepted lag. Correct
it to the real safety property:

> **No routed chunk is *globally uncovered* — each retains ≥ 1 covered active holder *anywhere* (or is
> being removed).**

The routed workers need not be among the covered holders. A stale route to an evicted copy, while the
chunk still has a covered holder elsewhere, is accepted bounded routing-lag (the same envelope
`portal_consistency_misses` counts). The oracle fires only when the chunk has **no** covered holder at
all — genuinely unanswerable, real data loss, not lag. This is *correcting* the oracle to its true
property ("unanswerable" = globally uncovered), not weakening it: precedent already exists in the same
oracle, which exempts departed workers — *"transient routing to a departed worker is a documented
availability event, not a coverage bug."* A reclaim, and bounded routing-lag, are the same category.

The same correction applies to the strict 100%-quorum `portal_consistency` oracle (which requires that
within M ticks a routed chunk points to a worker that *holds* it). The new-chunk veto keeps preemption
from stranding a visible chunk, but the visible-chunk path still evicts a still-routed copy — so within
M a routed worker can momentarily hold nothing while the durable copy lives elsewhere. `portal_consistency`
is therefore excused for a chunk that **keeps a covered holder anywhere** (durability intact) and fires
only on a **globally uncovered** chunk — identical in spirit to `published_coverage` and the existing
departed-worker exemption. `step_safety`/retention is untouched — durability is still hard.

### How the relaxation stays honest (hardening)

The relaxed oracles cut at the same joint as the eviction guard — both reasoned about assignment
*listings*, not physical possession — so the one failure the guard could still create (deleting the
only fetched copy for a never-fetched survivor) was invisible to the one oracle that was weakened.
Four measures keep the envelope checked rather than asserted:

- **Possession-confirmed eviction** (the guard rules above): the scheduler can no longer produce the
  zero-physical-copy state by preemption.
- **Physical retention, edge-triggered** (`assert_physical_retention`): a routed chunk that had ≥ 1
  physical copy must not reach zero physical copies in a single transition unless that transition was
  a departure (data leaves with the worker) or a recorded shortage (drain expiry under a stalled
  scrub). State-based physical checks can't be fatal — "listed holder with an empty disk" is
  legitimate in departure→rejoin windows — but the *edge* is attributable: outside departures, only a
  worker applying an assignment that dropped its copy deletes data, and dropping a routed chunk's
  last downloaded copy for an un-downloaded replacement is the possession bug the guard forbids.
  Fatal at a full quorum; classified below it (quorum-keyed drain expiry can legitimately outrun a
  laggard replacement). Paper-only coverage (listed, zero physical) is additionally classified in the
  statistics at every quorum.
- **Misses are counted at every quorum**: `portal_consistency_misses` runs on the strict path too, so
  "the envelope `portal_consistency_misses` counts" is true in the default configuration, and the
  shortage stand-down classifies what it masks (`strand check: uncovered routed chunk masked by
  shortage stand-down`) instead of going silent.
- **The bound on "bounded" routing-lag**: at every drained, shortage-free fixed point
  (`CheckConverged`) the confirmed routing must match the worker assignment **pair-by-pair** — the
  strict pre-preemption check, reinstated exactly where deliberate lag has provably resolved. A stale
  pair that survives quiescence is a stuck routing update, and fires.

The flagship regressions additionally probe the mechanism end-to-end
(`assert_all_routed_chunks_have_a_listed_holder`): the departure-emptied chunk is routed only at the
departed worker, which the strand oracle deliberately exempts — without the probe, deleting the
preemption path entirely would leave every replay green.

**Quorum and the possession signal.** The production quorum target is ~80% (and may be relaxed
further under fleet-wide lag); the 100%-quorum configs exist to *prove the algorithm* under bounded
confirmation, not as an operational target. At 100% "routed" implies "fetched", so the
routed-survivor rule is exact there; below 100% the watermark can advance on other workers'
confirmations, so routing is only a heuristic possession signal — still strictly safer than none.
The quorum-independent possession proof is the **per-worker applied report** (a worker reports each
assignment id it has fully applied — the same signal `confirm_worker_assignment` already records for
the watermark). Follow-up, required before production: derive a per-pair `possessed` view from those
reports (`chunk ∈ slice(worker, last_applied(worker))`, unknown ⇒ not possessed) and use it in the
eviction guard in place of the routing proxy, in both backends.

## Precondition: the departure residual is kept out of the walk, not hidden

A separate residual the sweep surfaces is **not** a scheduler bug and must not be masked by an oracle:
a chunk's **last held copy departs** (`WorkerLeft`) while the fleet is saturated, so the fresh
re-download that would restore it is starved by competing load over the next cycles — unbounded
unavailability no scheduler can prevent (the bytes stay recoverable from dataset storage; what's lost
is the fleet's serving copy). Relaxing an oracle to accept it would hide that window. Instead the churn model's
`WorkerLeft` precondition (`is_removal_recoverable`) is tightened: on top of the from-scratch floor-fit
check, a departure is rejected when it would take a portal-visible chunk's **last physically-held
copy** (no surviving `holds_chunk`). A held copy is durable (kept for free each cycle); a chunk reduced
to a re-download is not. This keeps the walk feasible and *still generates real departures* — a chunk
with ≥ 2 held copies survives losing one — it only refuses the removals that would destroy data.

## Alternatives considered

- **Restrict reclaim to drains the confirmed routing has already shed** (the first implementation).
  Rejected: strictly weaker guarantee — if the blocking space is still-routed drains, the floor never
  lands — for no safety gain over the strand check, and it threads confirmed routing through the
  algorithm needlessly.
- **Wait for drains / growth suppression only** (status quo). Rejected for this case: frees space
  only over M ticks and freezes at zero slack; the holderless window persists.
- **Reclaim only leftover drains, not bonus copies.** Rejected: treats weight-derived bonus as if it
  were a floor. Bonus replication is soft; refusing to shed it forfeits reclaimable space.
- **Bundle-scoping alone** (keep a visible chunk's schema while holderless, without preemption).
  Rejected *as a standalone fix* — it treats the symptom, not the cause: C stays at 0 copies. It is
  *complementary*, not an alternative, and is adopted alongside this ADR as
  [ADR 0002](0002-schema-bundle-covers-visible-chunks.md) to guarantee consistency in the residual
  preemption cannot reach.

## Related

- [capacity-aware-scheduling.md](../capacity-aware-scheduling.md) — floor vs bonus, two capacity
  views, liveness-vs-slack (the limit this lifts).
- [schema-bundle-lifecycle.md](../schema-bundle-lifecycle.md) — the invariant that surfaced the bug.
- Tests: `departure_leaves_a_visible_chunk_holderless`, `churn_raise_min_replication_underreplicates_new_chunk`.
