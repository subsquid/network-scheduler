# ADR 0002 — The schema bundle covers every routable chunk, not just held ones

**Status:** Accepted.
Complements [ADR 0001](0001-same-cycle-floor-preemption.md); both ship in the same PR. Where
ADR 0001 protects **availability** (a starved chunk gets a copy back), this protects
**consistency** (a chunk portals can route to always stays *describable*, even while it has no
copies at all).

## Terms

- **Schema bundle** — the catalog frozen alongside each worker assignment. It maps every chunk's
  `schema_id` to the schema a worker or portal needs to derive that chunk's file set. A chunk whose
  schema is missing from the bundle cannot be read *or even resolved*, no matter how many copies
  exist.
- **Tombstoned** — finally removed: the chunk has left the worker assignment for good and will never
  be routed again.
- **Visibility is monotonic** — once a chunk is published to portals it stays published until it is
  explicitly dropped or tombstoned. Losing all copies degrades its availability; it never
  unpublishes the chunk.

## Context

The `schema_bundle_consistency` invariant requires that every chunk named by the worker assignment
**or** the portal assignment has its schema in the bundle frozen with that assignment. Two facts
collide:

- **Visibility is monotonic** (above): portals keep naming a chunk even when its copies vanish.
- **The bundle was scoped to held chunks**: it was built from the worker assignment's chunk set —
  chunks with at least a live or draining copy.

So a portal-visible chunk that reaches zero holders — its last copy departed, and ADR 0001's
preemption could not re-place it in the sole-serving-drain residual — falls out of the worker
assignment, and with it out of the bundle, while portals still name it. The invariant breaches: a
portal routes to a chunk whose files it can no longer even describe. Reproduced by
`departure_leaves_a_visible_chunk_holderless` (a successful, non-shortage cycle; the chunk keeps
routing with an empty holder set).

ADR 0001 makes this state rare, but consistency must not *depend* on availability always being
restorable — the residual still reaches it.

## Decision

Build the schema bundle to cover every chunk in its **routable lifetime**: every chunk that has ever
entered a worker assignment and is not yet tombstoned
(`applied_at_worker_assignment_id IS NOT NULL AND dropped_from_worker_assignment_at IS NULL`).
The schema payload comes from the chunk's stored metadata, which survives the loss of every copy.

This window is deliberately broader than "currently held or visible", because portals can route to a
chunk that any narrower snapshot misses:

- **Promotion lags placement.** A chunk becomes portal-visible based on its *first* confirmed
  placement. A chunk that a *later* assignment dropped can still be promoted off that earlier entry
  and keep serving its draining copies for M ticks — so the portal names it while the current
  assignment no longer lists it.
- **The snapshot goes stale under shortage.** The bundle is frozen during scheduling, one step
  before promotion; a sustained shortage keeps it frozen, so a per-cycle snapshot would never catch
  up. A lifetime window holds no matter how long the freeze lasts or how far the watermark stalls.

The effect: **resolvability follows the routable lifetime, not the current holders.** How many
copies exist (availability) and whether the file set can be derived (resolvability) are decoupled —
a chunk may be degraded to zero copies yet remain fully resolvable.

Both backends build the bundle the same way (parity-tested).

## Consequences

**Positive**

- `schema_bundle_consistency` holds **unconditionally** — regardless of whether a visible chunk has
  holders this cycle, and regardless of whether ADR 0001 found room. No portal ever names a chunk
  the bundle cannot describe.
- The two failure modes separate cleanly: ADR 0001 restores *copies*; this keeps the chunk
  *describable* while the copies are being restored.

**Neutral**

- The bundle can carry a schema for a chunk with no current holders. That is the point: a portal
  resolves the file set and observes low availability, instead of failing to resolve at all.
- In the common case a routable chunk is already in the worker assignment, so the covered set is
  unchanged. Only the rare in-flight chunk (dropped from the current assignment but still routable,
  not yet tombstoned) is added — the bundle-size impact is negligible.

## Alternatives considered

- **Rely on ADR 0001 alone.** Rejected: it makes consistency depend on reclaimable space always
  existing, and the sole-serving-drain residual would still breach.
- **Unpublish a chunk when it goes holderless.** Rejected: it breaks visibility monotonicity. A
  transient availability dip would flap the chunk out of and back into portals, and portals would
  stop serving a chunk whose data returns moments later.
- **Exempt holderless chunks from the oracle.** Rejected: that hides the real requirement — portals
  must be able to resolve everything they route. The fix is to make the bundle cover what is
  routable, not to stop checking.

## Related

- [ADR 0001](0001-same-cycle-floor-preemption.md) — the availability half of the same bug.
- [schema-bundle-lifecycle.md](../schema-bundle-lifecycle.md) — bundle construction and freezing.
- [capacity-aware-scheduling.md](../capacity-aware-scheduling.md) — visibility is never revoked for
  under-replication ("the only response to under-replication is re-replication, never hiding the chunk").
- Tests: `departure_leaves_a_visible_chunk_holderless` (holderless-visible), and
  `shortage_schema_bundle_misses_in_flight_chunk` (in-flight chunk routed after the current
  assignment dropped it, under a frozen-shortage bundle).
