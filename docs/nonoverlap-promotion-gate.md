# Non-overlap enforcement

A dataset's chunks must partition the block space — a portal routes each block height to exactly one
chunk, so two chunks with overlapping `[first_block, last_block]` in one dataset is a routing bug.
This guarantees a published **portal assignment** never contains two overlapping ranges within one
dataset. Implemented in both backends behind the `mvcc-chunks` feature.

## Two enforcement layers

1. **Registration (primary).** `register_new_chunks` refuses a *new* chunk whose range overlaps a
   live chunk in its dataset (or another new chunk in the same batch). The loser gets a **terminal**
   `sched_chunk_metadata.rejected` row — never scheduled, replicated, or re-evaluated, and never
   revived (no self-heal; only a fresh registration fills the freed range). This stops a doomed chunk
   *before* it costs a worker download.
2. **Promotion (backstop).** The visibility-cycle promote pass promotes a chunk only if its range
   doesn't overlap the chunks that stay visible this cycle. With registration doing its job it should
   never fire, but it is the only place that proves the *published* assignment is non-overlapping no
   matter how a chunk reached promotion — a held-back chunk (`promotion_held_back`) is an alarm.

Both backends apply the same rule and the cross-backend simulation checks they decide identically;
each implements it natively — not as a `chunks`-table trigger (shared table, not race-free).

## The rule

Inclusive-range overlap, deterministic resolution: among overlapping candidates the lower
`(first_block, chunk_pk)` wins, the rest lose. Gaps are allowed — only overlap is forbidden. Overlap
iff `a.start() <= b.end() && b.start() <= a.end()` — `[10,20]` & `[20,30]` overlap (share block 20),
`[0,10]` & `[11,20]` don't (`ranges_overlap`; Postgres mirrors the arithmetic).

The comparison set a candidate must not overlap differs by layer:

- **Registration** — the dataset's *live* chunks: admitted, not `rejected` / `marked_for_removal` /
  dropped. Winners get a default metadata row; losers a `rejected = TRUE` one.
- **Promotion** — chunks that *stay* visible this cycle: `applied_at_portal NOT NULL AND
  dropped_at_portal NULL AND marked_for_removal NULL`. Excluding `marked_for_removal` is what lets a
  same-range correction replacement promote past the old chunk it supersedes (the old chunk is marked
  earlier in the same cycle).

## Algorithm

Accept the candidates that overlap neither the `existing` set nor each other, per dataset, in
`(first_block, chunk_pk)` order — the accepted set grows as it goes, so two overlapping candidates in
one batch can't both win.

- **Postgres** does this without loading `existing`: one indexed `EXISTS` per candidate, riding the
  `chunks_dataset_range_gist` GiST index (dataset, then range-overlap `&&`) so it touches only
  chunks that actually intersect the candidate; a short Rust pass then settles overlaps within the
  batch.
- **In-memory** (test oracle): scan the map for `existing` and run the brute-force
  `select_non_overlapping` (O(n²), obviously correct).

A correction's same-range replacement is **exempt** from the registration check — it overlaps only
the old chunk it supersedes, by design. Corrections are strictly 1:1 same-range:
`register_correction` rejects a range-changing replacement, so one is never created; see
[mvcc-storage.md](mvcc-storage.md) for the swap protocol.

## Correctness

| Scenario | Outcome |
|---|---|
| Two overlapping new chunks | lower `(first_block, chunk_pk)` admitted; the other **rejected at registration** (terminal, never replicated) |
| Rejected duplicate, winner later removed | does **not** self-heal; the freed range needs a fresh registration |
| Same-range correction `A→B` | `B` promotes, `A` drops atomically (`A` marked → out of `B`'s comparison set; `B` exempt at registration) |
| Chain `A→B→C` (same range) | collapses in one cycle, only `C` visible |
| Draining chunk (M-tick window) | not in the comparison set |

## Observability

`registration_rejected` and `promotion_held_back` gauges, per dataset (the latter should never fire);
rejections and held-backs are logged. A held-back chunk doesn't block the confirmation watermark or
other chunks' promotion.

## Scope

Run on both backends via `backend_cases!`, with the predicate/resolver unit tests in
`in_memory::nonoverlap`. Out of scope: N-to-M re-partitioning corrections, GC of rejected/tombstoned
rows, gap enforcement, and self-heal of rejected chunks.
