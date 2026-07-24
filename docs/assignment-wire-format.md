# Assignment wire format — worker vs. portal

What the split worker/portal assignments (NET-682/683/684) should actually carry on the wire.
Builds on [mvcc-storage.md](mvcc-storage.md), [mvcc-schema.md](mvcc-schema.md), and
[schema-bundle-lifecycle.md](schema-bundle-lifecycle.md).

## Why this is its own doc

`NetworkState` already publishes two independent pointers — `worker_assignment` and
`portal_assignment` (`sqd-network/crates/assignments/src/common.rs`) — and `sqd-portal` already
prefers `portal_assignment` with a fallback to the legacy blob (PR #125). But the *content* behind
both pointers is still identical: `Controller::serialize_assignments` (`src/controller.rs`) encodes
the same legacy flatbuffer three times and says so directly — *"This first MVCC step only splits
assignment discovery/publication. NET-683/NET-684 will make portal and worker contents diverge."*
This doc is that divergence: what each side needs, what already exists to build it from, and what's
still missing.

## Method

Field-by-field, traced against the actual consumers rather than assumed from the schema:

- **worker-rs** (`src/storage/datasets_index.rs`, `src/controller/*.rs`) — what a worker's own code
  path reads out of an `Assignment` to serve queries.
- **sqd-portal** (`src/network/storage.rs`) — what a portal's own code path reads to route queries
  and report dataset state.
- Cross-referenced against `migrations/0001_sched_tables.sql` and the scheduler's own domain types
  (`WorkerAssignment`, `PortalAssignment`, `WorkerAssignmentChunk`, `AssignmentWorker`,
  `SchemaBundle` — all in `src/scheduler_storage/`).

## Current state

### Legacy wire format (`sqd-network/crates/assignments/schema/assignment.fbs`)

One shared `Assignment { datasets, workers }`, used identically by both consumers today:

| Table | Fields |
|---|---|
| `Dataset` | `id`, `chunks: [Chunk]`, `last_block` |
| `Chunk` | `first_block` (key), `id`, `dataset_id`, `size`, `last_block_hash`, `last_block_timestamp`, `dataset_base_url`, `base_url`, `files: [FileUrl]`, `worker_indexes: [uint16]` |
| `FileUrl` | `filename`, `url` |
| `WorkerAssignment` | `worker_id` (`WorkerId{peer_id:[ubyte;38]}`), `status`, `encrypted_headers` (`EncryptedHeaders{identity,nonce,ciphertext}`), `chunks` (**deprecated**, superseded by `worker_indexes`) |

### Who actually reads what

| Field | Worker | Portal |
|---|---|---|
| `chunk.id`, `dataset_id` | ✅ identity, dedup keying | ✅ `find_chunk`, `DataChunk` parsing |
| `chunk.dataset_base_url`, `base_url`, `files` | ✅ builds download URLs (`datasets_index.rs:32-54`) | ❌ never read |
| `chunk.size` | ❌ not read | ❌ not read |
| `chunk.first_block` / block range | ❌ not read directly | ✅ `block_range()`, `first_block()` (derived from first chunk) |
| `chunk.last_block_hash`, `last_block_timestamp` | ❌ not read | ✅ `dataset.last_block_hash()` (derived: last chunk) → `head()`/`BlockRef` |
| `chunk.worker_indexes` | n/a (finds itself via `get_worker`, then `iter_chunks_with_ref`) | ✅ `find_workers()`, `get_dataset_state()` |
| `dataset.last_block` | ❌ | ✅ metrics, `head()` |
| `worker.status` | ✅ own status | ✅ filters Unsupported/Deprecated peers when routing |
| `worker.encrypted_headers` | ✅ decrypted with own key → HTTP auth headers | ❌ never touched |
| `worker.peer_id` (others') | n/a — only looks up itself | ✅ iterates all workers for routing |

Clean split: worker only cares about its own record plus download plumbing (files/URLs/headers) for
chunks it holds; portal only cares about routing (block ranges, worker indexes, other workers'
status/peer_id) and never touches file URLs or headers. `chunk.size` is read by neither path today.

### What the new (Postgres/MVCC) domain types already carry

`WorkerAssignment` and `PortalAssignment` (`src/scheduler_storage/mod.rs`) exist as internal Rust
types but nothing serializes them to a wire format yet. They are **structurally identical** except
one field:

```
WorkerAssignmentChunk { dataset, id, size, blocks, schema_id, tables_present }
AssignmentWorker      { peer_id, status }

WorkerAssignment { id, chunk_workers, chunks, workers, replication_by_weight }
PortalAssignment { id, chunk_workers, chunks, workers }   // no replication_by_weight
```

`PortalAssignment.chunk_workers` is already sourced from confirmed routing
(`sched_confirmed_chunk_workers`), not the raw ideal — the one piece of MVCC-specific correctness
that's already right by construction.

**Schema bundle (PR #52, `src/scheduler_storage/schema_bundle.rs`) materially changes the file-set
story — and the wire format should lean on it, not route around it.** `run_scheduling_cycle` now
returns `(WorkerAssignment, SchemaBundle)` as a pair — a content-addressed (`BundleId`, SHA-256 over
sorted `schema_id`s) snapshot of exactly the schemas the assignment's chunks reference.
`SchemaBundle::chunk_files(chunk)` resolves a chunk's `<table>.parquet` file list from its pinned
`schema_id` + `tables_present` bitmap — production-quality and tested, not the dead code it was
earlier in this investigation. But the intent behind `schema_id`/`tables_present` was precisely to
avoid ever putting a resolved file list on the wire: the worker reads the schema for its chunk,
determines which tables are present, and downloads accordingly — `chunk_files()` is what the
*worker* runs, not a scheduler-side precomputation to embed. So the worker-assignment should carry
`schema_id` (and `tables_present`) per chunk, not a `files` field.

**The bundle is published as a separate artifact, accessible to both worker and portal** — settled,
not an open question. Per [schema-bundle-lifecycle.md](schema-bundle-lifecycle.md), it's generated
and frozen with the assignment, and its consistency is checked in simulation; delivery of that
artifact alongside/adjacent to the assignment is the still-open follow-up this doc scopes.

The same lifecycle doc also flags **"the portal validating incoming queries against the current
schema"** as planned-but-not-wired. Portal-assignment needs a schema reference too — not
`tables_present` (a per-chunk file-presence fact the portal has no use for), but a `schema_id`
pointing at the dataset's current column/table definitions in the published bundle, for query
validation. This is a correction to the earlier assumption in this investigation that portal never
needs schema information at all.

`DatasetSchema` (`src/types/dataset_schema.rs`) today only carries `tables → { fields,
default_fields }` — table and column *names* only, no types, no nullable/hidden flags. That's
consistent with Vasilii's write_schema/read_schema proposal (`#metadata-initiative`, 2026-07-16),
which explicitly scopes the metadata service down to "table names, column names, types,
nullability" — types and nullability aren't modeled yet either, on top of the hidden/nullable
extensions that proposal adds.

## Gaps

1. **File/URL derivation is half-solved.** Filename derivation is real and worker-side by design
   (`SchemaBundle::chunk_files`, meant to run on the worker once it has the schema, not to be
   precomputed by the scheduler) — but nothing yet delivers the bundle to the worker, or puts
   `schema_id`/`tables_present` on the wire at all. And **URL construction has no equivalent in the
   new path at all** — `dataset_base_url`/`base_url` (built from `config.storage_domain` +
   `chunk.bucket()`) exists only in the legacy `types/assignment.rs::encode_fb`, and that part
   *does* stay scheduler-side regardless of the file-list decision.
2. **Worker version-status is unreachable from Postgres.** `sched_workers.version` is fetched into
   `WorkerRow` and then ignored — `worker_status_from_row` (`postgres/rows.rs:152-158`) only
   branches on `inactive_since`. `DeprecatedVersion`/`UnsupportedVersion` (both consumed by
   sqd-portal for routing, and by the worker for self-reporting) are structurally unreachable today.
3. **`encrypted_headers` has no home.** No column anywhere, and `AssignmentWorker` is just
   `{peer_id, status}`. Needs to be sealed fresh at serialization time (as legacy does, from a
   config-held secret + the worker's peer id) — not something to persist.
4. **`last_block_hash`/`last_block_timestamp` are stored but explicitly unread.** The columns
   survive on `chunks`; nothing in the scheduler reads them. Portal's `head()`/`BlockRef` needs them.
5. **"Stale" changed meaning silently.** Legacy: reported `stored_bytes` below a threshold. New:
   `inactive_since IS NOT NULL` — a liveness signal, not a storage-volume one. `sched_workers` has no
   `stored_bytes` column at all, so the old semantics aren't reconstructable even in principle.
6. **No production caller for the confirmation watermark.** `confirm_worker_assignment` is only
   invoked by tests/sim. worker-rs PR #61 fixed the *input* side (prompt `last_applied_assignment_id`
   heartbeats), but nothing on the scheduler side reads that column back out of ClickHouse, computes
   the X% quorum, and calls `confirm_worker_assignment`. Portal-assignment publication is gated on
   this existing, so it blocks the whole portal path regardless of wire format.

## Recommendation

**Diverge the two wire formats — do not keep sharing one chunk/worker projection.** The Postgres
source rows can stay shared; the serialized shape should not, since the two consumers' needs don't
overlap except on identity fields.

### Worker-assignment

| Field | Status |
|---|---|
| `id`, `dataset_id` | existing |
| `worker_indexes` | existing (`chunk_workers`) |
| `dataset_base_url`, `base_url` | **new** — port URL construction out of legacy `encode_fb` |
| `schema_id`, `tables_present` | existing (`WorkerAssignmentChunk`) — needs to actually reach the wire; the worker resolves files itself via the published bundle's `chunk_files()`, not a precomputed `files` field |
| worker `peer_id` | existing |
| worker `status` (version-aware) | **fix** — make `worker_status_from_row` compare `version` against configured thresholds |
| worker `encrypted_headers` | **new** — port the existing sealing logic; no schema change |
| bundle reference (`BundleId`) | **new** — points at the separately-published schema artifact (see above) |

Explicitly excludes `last_block_hash`/`last_block_timestamp` — no worker consumer.

### Portal-assignment

| Field | Status |
|---|---|
| `id`, `dataset_id`, block range | existing |
| `worker_indexes` from **confirmed** routing | existing, already correct |
| `last_block_hash`, `last_block_timestamp` | **new** — columns already exist, just add to the portal chunk projection/query |
| worker `peer_id`, version-aware `status` | existing + same status fix as worker-assignment |
| current `schema_id` (reference into the published bundle, for query validation) | **new** — a dataset-level reference, not per-chunk, and not the schema content itself |

Explicitly excludes `dataset_base_url`, `base_url`, `files`, `encrypted_headers` — no portal
consumer, and excluding them shrinks the portal blob meaningfully at scale.

### Cross-cutting, not fields

- **`chunk.size`** — unread by either consumer today. Candidate for dropping, but confirm with
  worker-rs/sqd-portal maintainers before removing a currently-shipped field; this is a
  three-repo rollout, not a local change.
- **`replication_by_weight`** — looks purely diagnostic (`Assignment::log_stats`), already exposed
  via metrics/logging. Recommend keeping it off both wire formats.
- **"Stale" redefinition** — needs an explicit decision (reinstate a storage-based signal, or
  formally redefine and communicate the change), since both consumers branch on this status.
- **Confirmation watermark** — not a wire-format field, but portal-assignment publication has no
  production path without it (gap 6). A prerequisite, not a nice-to-have; picking production `M`/`X%`
  values is scheduler-lifecycle work, not part of this doc's scope — see `docs/README.md`'s known
  limitations.
