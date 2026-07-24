# ADR 0003 — Chunk-range overlap is decided at insert, under a per-dataset lock

**Status:** Accepted. Ships with the metadata-service stack (#126).

## Terms

- **Overlap invariant** — at most one live chunk claims any block in a dataset. The whole MVCC
  design rests on it: routing, corrections, and the resume point (`head`) all assume ranges
  partition cleanly.
- **Admission gate** — the scheduler's asynchronous sweep (`register_new_chunks`): it examines
  chunks with no scheduling metadata yet, admits the non-overlapping ones, and marks the rest
  `rejected` (terminal).
- **Pending** — committed to `chunks` but not yet seen by the admission gate; invisible to any
  probe that only consults scheduling metadata.
- **Ingester** — the client of the metadata service's insert API. It treats a successful insert as
  durable and resumes discovery *past* the inserted range.

## Context

Before this stack, the insert API's overlap probe consulted only **live** chunks and the actual
overlap decision belonged to the admission gate, later. Two concurrent writers at READ COMMITTED
cannot see each other's uncommitted rows, so both probes pass, both inserts return success — and
the gate silently rejects one afterwards. The losing ingester has already resumed past the range:
the data is never re-sent, and the dataset has a **permanent gap that nothing reports**. The same
loss occurs without any race when an insert overlaps a *pending* chunk, since the live-only probe
cannot see it.

The contract the API needs is stronger than "probably accepted": a returned insert must mean
**durably accepted** — it will not be un-accepted by a later sweep.

## Decision

The insert path decides overlap itself, synchronously, and the answer it returns is final:

1. **Per-dataset advisory lock.** `pg_advisory_xact_lock` on a dedicated key class, taken *last* —
   after resolution, validation, and lowering — so it spans only probe → insert → commit.
   Transaction-scoped: every abort path (error, panic, cancellation, connection death) releases it
   with the rollback; there is no unlock call to forget and nothing can leak into the pool. The
   only entry point is `with_dataset_lock`, which consumes the transaction and commits on `Ok`, so
   probe-without-lock and a section that outlives its commit are unrepresentable, not conventions.
2. **The probe is authoritative.** It drives from `chunks` itself (LEFT JOIN scheduling metadata),
   so committed **pending** chunks count as claimants — under the lock, "not visible yet" can no
   longer mean "not claiming". A candidate's own committed row is exempt, which makes a re-sent
   chunk a reported duplicate (id-only idempotency, decided by `ON CONFLICT` under the same lock)
   rather than a conflict with itself.
3. **Bounded waiting.** `SET LOCAL lock_timeout` (10s) converts a wedged holder into SQLSTATE
   `55P03`, surfaced as a retryable 503 (`dataset_busy`) instead of a queue of stuck connections.
4. **The admission gate is demoted to a backstop** on this path: for API-inserted chunks its
   overlap-reject half should never fire.

Scope: only the metadata-service insert path. The scheduler's discovery insert stays lock-free and
keeps the gate as its sole arbiter (see Consequences).

## Consequences

**Positive**

- A 2xx insert is durable acceptance; a conflicting insert gets `409 range_overlap` while the
  ingester is still there to handle it. The silent-gap failure mode is gone for API writers.
- Client retries are safe: a re-send racing its own original reports a duplicate, never a
  self-conflict, because duplicates and overlaps are decided under the same lock.

**Neutral**

- Same-dataset API writers serialise for the length of probe → insert → commit (milliseconds; all
  per-chunk work happens before the lock). Different datasets are unaffected; waiters are bounded
  by the 10s timeout.
- The gate still runs unchanged — required for discovery-inserted chunks, and its reject half
  doubles as a check that the synchronous path holds.

**Negative / open**

- The scheduler's discovery insert does not take the lock, so "durably accepted" currently holds
  only against other API writers; a discovery-inserted overlap is still settled by the gate. The
  planned follow-up is dual-write participation: `with_dataset_lock` per *batch*, so a cold-start
  bulk load never holds a dataset's lock longer than one batch's probe → insert → commit.

## Alternatives considered

- **Keep asynchronous arbitration** (status quo). Rejected: no report channel can fix it — by the
  time the gate decides, the ingester has moved on, and "re-check later" contradicts what a resume
  point is for.
- **A database exclusion constraint on the range** (`EXCLUDE USING gist … &&`). Rejected: liveness
  lives in the scheduling metadata, not in `chunks` — rejected rows keep their ranges forever and
  corrections legitimately insert a same-range replacement, so a table-level constraint would
  forbid states the model requires.
- **SERIALIZABLE isolation instead of a lock.** Rejected: it turns the race into `40001` retry
  loops on every writer under load, and still needs the probe made pending-inclusive to close the
  no-race case; the targeted lock gives one bounded wait on exactly the contended resource.
- **A session-scoped lock with explicit unlock.** Rejected: a future dropped at an `await` (the
  HTTP timeout does this) would skip the unlock and wedge the dataset via the pooled connection.
  The transaction-scoped lock makes release unconditional.

## Related

- `crates/scheduler-metadata/src/pg/lock.rs` — the lock, `with_dataset_lock`, and the dual-write
  note; `pg/nonoverlap.rs` — the authoritative probe.
- [mvcc-storage.md](../mvcc-storage.md), [nonoverlap-promotion-gate.md](../nonoverlap-promotion-gate.md).
- Tests: `overlapping_insert_blocks_on_dataset_lock_then_loses_probe` (writer B provably parks on
  the lock, then loses the probe), `insert_chunks_aborts_on_pending_overlap` (pending chunks
  claim), `concurrent_same_key_writers_opposite_order_stay_consistent` (raced re-sends stay
  duplicates), `marked_chunk_frees_its_range_for_registration` (liveness boundary, both backends).
