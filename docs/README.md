# Multistep scheduler — design docs

These docs are the **design draft for the multistep scheduler**, a work-in-progress replacement
for the production placement algorithm. The design docs (below) describe how the subsystem *should*
work. **This page is the single source of truth for what is actually built and what is not.**

## Where it lives

| Concern | Code |
|---|---|
| Placement algorithm (capacity-aware reconcile) | `src/multistep_scheduler/mod.rs` |
| MVCC storage protocol | `src/scheduler_storage/` |
| In-memory backend | `src/scheduler_storage/in_memory/` |
| Postgres backend | `src/scheduler_storage/postgres/` + `migrations/0001_sched_tables.sql` |
| Simulation harness (drives both backends) | `src/multistep_scheduler/sim/` |

## Reading order

1. **[chunk-reshuffling.md](chunk-reshuffling.md)** — background. How the *current* (legacy)
   scheduler reshuffles chunks and why that motivates the MVCC + capacity-aware work.
2. **[mvcc-storage.md](mvcc-storage.md)** — the core MVCC storage protocol: the two-gate
   visibility model, chunk lifecycle, invariants, and the corrections and deferred-removal
   sub-protocols.
3. **[mvcc-schema.md](mvcc-schema.md)** — Postgres table and column reference; kept in sync with
   `migrations/0001_sched_tables.sql`.
4. **[capacity-aware-scheduling.md](capacity-aware-scheduling.md)** — the placement algorithm:
   how `Reconcile` keeps every published assignment physically feasible.
5. **[nonoverlap-promotion-gate.md](nonoverlap-promotion-gate.md)** — non-overlap enforcement
   (registration-time rejection + the promotion-time backstop) that keeps a dataset's chunk ranges
   non-overlapping.

## Status & limitations

**The whole subsystem is a work in progress: gated behind the `mvcc-chunks` cargo feature, driven
only by the simulation harness and tests, and *not* wired into the production `controller.rs`.**
Production still runs the legacy scheduler in `src/scheduling.rs` (analysed in
[chunk-reshuffling.md](chunk-reshuffling.md)).

### What is built

✅ implemented and tested · 🧪 implemented but only in the sim/test layer · 📐 design-only, not yet
built.

| Mechanism | Status | Notes |
|---|---|---|
| Two-gate visibility (chunk-level + per-pair routing) | ✅ | Both backends. |
| Two-phase publish (worker assignment confirmed → portal promotion) | ✅ | Gated on the confirmation watermark. |
| Chunk corrections (1-to-1 same-range swaps) | ✅ | Both backends; property-tested. |
| Deferred removal / stale mappings (`ideal ∪ stale`) | ✅ | Both backends. |
| Capacity-aware placement (footprint charge, credit-held-free, skip-don't-spill, essentials-first) | ✅ | `Reconcile` in `multistep_scheduler/mod.rs`. |
| X% promotion threshold (quorum) | 🧪 | Computed by the sim's worker fleet; the storage core only consumes a precomputed confirmation watermark. No production quorum logic. |
| Replica-sufficiency **drain gate** (capacity-aware "rule 2") | 📐 | Drains expire on the M-tick timer only. |
| Replica-sufficiency **promotion gate** (capacity-aware "rule 3") | 📐 | Promotion is gated on assignment confirmation, not on a confirmed-copy count. |
| Production wiring (`controller.rs`) | 📐 | Subsystem is sim-only behind `mvcc-chunks`. |
| Backpressure / under-replication observability counters | 📐 | `NotEnoughCapacity` error exists; the per-cycle metrics surface does not. |
| Non-overlap enforcement (no overlapping chunk ranges per dataset) | ✅ | Registration-time rejection + promotion backstop, both backends. See [nonoverlap-promotion-gate.md](nonoverlap-promotion-gate.md). |

### Known limitations and open questions

- **`M` (drain window) and `X%` (quorum) values are unset** — both are parameterised per call; no
  production values have been chosen.
- **Persistent stragglers beyond the quorum tolerance.** The `X%` quorum already ignores a
  straggling minority — with `X < 100` the confirmation watermark advances without the laggards.
  The only gap is when the *persistently* unconfirming fraction exceeds `100 − X%`: those workers
  still count in the quorum denominator (the active-roster size), so the watermark can stall, and
  nothing evicts a worker for *not confirming* (GC is by roster membership, not confirmation
  behaviour) to shrink the denominator and let promotion resume.
- **Chunk metadata GC.** A `sched_chunk_metadata` row lives as long as its chunk row; a dropped
  chunk becomes a permanent tombstone, permanently excluded from future cycles. Removing rows from
  `chunks` and cleaning up their metadata is not supported.
- **Range-changing corrections are out of scope.** Non-overlap among a dataset's chunks is enforced
  (registration-time rejection of overlapping new chunks + the promotion-time backstop — see the
  ✅ row above). Corrections stay **1:1 same-range**; a range-changing/re-partitioning correction is
  rejected at `register_correction` (`CorrectionRejected`) — the replacement is never created — and a
  chunk rejected at registration is terminal (no self-heal). N-to-M re-partitioning is out of scope.
  See [nonoverlap-promotion-gate.md](nonoverlap-promotion-gate.md).
- **Load balance and churn are not measured by the property tests.** Capacity-aware placement
  uses essentials-first (tag-outer) ordering ([capacity-aware-scheduling.md](capacity-aware-scheduling.md)),
  which changes the per-chunk insertion order that consistent-hashing-with-bounded-loads relies on
  for its near-uniform-load guarantee. The PBT suite checks *safety* (no-overcommit, floors,
  routing consistency) but **not** load uniformity or how much data moves on a worker join/leave —
  so a regression in balance or churn would pass silently. Needs a measurement test, calibrated
  against the independent-hashing baseline rather than an absolute threshold.
- **Eligibility beyond version.** The placement floor treats *version* as the only serve-eligibility
  constraint (production runs `ignore_reliability = true`). If offline/stale workers should not count
  toward the floor, the "eligible live copies" definition must also exclude them, which couples to
  worker GC timing.
- **Version-restricted replication factor is global, not eligible-scoped.** `replication_cap`
  derives a chunk's target `R` from the *whole fleet's* capacity and worker count, while placement
  only lands copies on version-eligible workers. So a version-restricted chunk (e.g. pinned to a
  `minimum_worker_version` that few workers run) can be handed a target `R` larger than its eligible
  pool can ever hold, leaving it under-replicated against its own target. Deriving `R` from the
  eligible subset is not yet implemented.
- **Confirmed routing is not scrubbed when a worker departs (suspected; not yet reproduced in a
  test).** Confirmation is the only writer of `sched_confirmed_chunk_workers`; worker GC purges
  stale mappings but not confirmed routing. So an already-visible chunk can keep routing portals to
  a worker that has left and lost its data. *Example:* a saturated chunk's only copy sits on worker
  `W`; `W` departs and is GC-evicted, yet the chunk stays visible and reads for it still route to
  `W` — those queries fail. "Suspected" because the structural gap is clear from the code, but the
  query-failure impact was only seen once via throwaway instrumentation — no committed test pins it,
  and it isn't confirmed that a later cycle doesn't re-route first. Needs a routing-plane test to
  confirm, then a scrub-on-departure fix. (Couples to the sim oracle's `held_before_by_pk`, which
  currently relies on the un-scrubbed behaviour.)
