# ADR 0002 — The schema bundle covers every routable chunk, not just held ones

**Status:** Accepted.
Complements [ADR 0001](0001-same-cycle-floor-preemption.md); both are delivered together in the same
PR. Where ADR 0001 protects **availability** (a starved chunk gets a copy), this protects
**consistency** (a visible chunk stays resolvable even when it has none).

## Context

Each cycle freezes a [schema bundle](../schema-bundle-lifecycle.md) with the worker assignment. The
`schema_bundle_consistency` invariant requires: every chunk named by the worker assignment **or** the
portal assignment must have its `schema_id` in that bundle, or a worker/portal can't derive the
chunk's file set.

Two facts collide:

- **Portal visibility is monotonic.** Once a chunk is promoted to portal-visible it stays visible
  until it is explicitly dropped or tombstoned — *never* because its holders vanished. Losing all
  holders is **degraded availability**, not a de-visibility event; the portal keeps naming the chunk.
- **The bundle is scoped to *held* chunks.** It is built from the worker assignment's chunk set,
  which is `ideal ∪ stale` — chunks with a live or draining copy.

So a portal-visible chunk that reaches **zero holders** (its last copy departed and vanished, and
ADR 0001 preemption couldn't re-place it in the un-reclaimable sole-serving-drain residual) falls out
of `ideal ∪ stale`, and its schema is dropped from the bundle — while the portal still names it.
`schema_bundle_consistency` breaches: the portal routes to a chunk the bundle can't describe.
Reproduced by `departure_leaves_a_visible_chunk_holderless` (`is_infeasible=false`, `ideal={}`,
`stale={}`, still routed).

ADR 0001 makes this state *rare*, but consistency must not be **contingent** on availability always
succeeding — the sole-serving-drain residual still reaches it.

## Decision

Build the schema bundle to cover every chunk in its **routable lifetime**:
`entered_worker_assignment ∧ ¬tombstoned` — has ever been placed (`applied_at_worker_assignment_id IS
NOT NULL`) and not yet finally removed (`dropped_from_worker_assignment_at IS NULL`). Schema payload is
looked up from the chunk's stored metadata, which survives holder loss.

This is deliberately broader than `ideal ∪ stale ∪ portal-visible`. Two ways the portal routes a chunk
that a narrower snapshot misses:

- **Promotion lags placement.** A chunk is promoted to portal-visible off its *first* confirmed entry,
  so a chunk a later assignment dropped from the ideal still promotes off that earlier entry and keeps
  serving its draining copies for M ticks — the portal names it while it is no longer in `ideal ∪ stale`.
- **The snapshot is stale under shortage.** The bundle is frozen in the schedule phase, one step before
  that promotion; a sustained shortage keeps it frozen, so it never catches up. `entered ∧ ¬tombstoned`
  is a lifetime window, not a per-cycle snapshot, so it holds for as long as any reader can be routed to
  the chunk regardless of promotion timing or a stalled watermark.

Schema **resolvability** is thereby a function of the **routable lifetime**, not of current holders:
availability (how many copies exist) and resolvability (can the file set be derived) are decoupled — a
chunk may be degraded to zero copies yet remain fully resolvable.

Both backends build the bundle the same way (parity).

## Consequences

**Positive**

- `schema_bundle_consistency` holds **unconditionally** — independent of whether a visible chunk has
  holders this cycle, and independent of whether ADR 0001 preemption found room. No portal ever names
  a chunk the bundle can't describe.
- Cleanly separates the two failure modes: ADR 0001 restores *copies*; this keeps the chunk
  *describable* while copies are being restored.

**Neutral**

- The bundle can carry a schema for a chunk with no current holders. That is correct: a portal
  resolves the file set and observes zero/low availability, rather than failing to resolve at all.
- In the common case a routable chunk is already in `ideal ∪ stale`, so the covered set is unchanged;
  only the rare in-flight chunk (dropped from the ideal but still routable, not yet tombstoned) is
  added. Bundle-size impact is negligible.

## Alternatives considered

- **Rely on ADR 0001 alone.** Rejected: makes consistency contingent on there always being
  reclaimable space; the sole-serving-drain residual would still breach.
- **Drop the chunk from portal visibility when it goes holderless** (de-visibility on holder loss).
  Rejected: violates visibility monotonicity. A transient availability dip would flap the chunk out of
  and back into portals, and portals would stop serving a chunk whose data returns moments later.
- **Weaken the oracle — exempt holderless chunks from `schema_bundle_consistency`.** Rejected: hides
  the real requirement (portals must resolve everything they route). The fix is to make the bundle
  cover what is visible, not to stop checking.

## Related

- [ADR 0001](0001-same-cycle-floor-preemption.md) — the availability half of the same bug.
- [schema-bundle-lifecycle.md](../schema-bundle-lifecycle.md) — bundle construction and freezing.
- [capacity-aware-scheduling.md](../capacity-aware-scheduling.md) — visibility is never revoked for
  under-replication ("the only response to under-replication is re-replication, never hiding the chunk").
- Tests: `departure_leaves_a_visible_chunk_holderless` (holderless-visible), and
  `shortage_schema_bundle_misses_in_flight_chunk` (in-flight chunk routed after the current assignment
  dropped it, under a frozen-shortage bundle).
