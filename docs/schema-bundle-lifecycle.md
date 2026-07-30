# Schema bundle lifecycle

How the schema bundle is built, frozen, and retired. Builds on the model in
[mvcc-storage.md](mvcc-storage.md) and [mvcc-schema.md](mvcc-schema.md).

## Why it exists

Today a single assignment serves both workers and portals, and a dataset's schema is **hardcoded in
the portal and the workers** — one schema per dataset *kind*, not per individual dataset. Changing it
means editing and redeploying them. And because the schema lives in their code rather than travelling
with the data, nothing records which schema a chunk was written under: there is no audit trail
linking a schema to the chunks written against it.

The multistep scheduler splits that single assignment into separate **worker** and **portal**
assignments, and adds a **schema bundle** — the dataset schemas carried as data alongside the
assignment. A schema can then change **without a code change**, and — because every chunk pins the
schema it was written under — the schema-to-chunk link becomes explicit and auditable. The schema is
tied to the data, not to the code that happens to be running.

## Today vs planned

Right now the bundle is infrastructure only. It is generated, frozen together with the worker
assignment, and its consistency is checked in simulation. **The portal and workers do not consume it
yet** — their schema handling is still hardcoded.

Planned, but not yet wired:

- the portal validating incoming queries against the current schema,
- the portal telling a worker which schema to read a given chunk under,
- the bundle being delivered to workers alongside the assignment.

The lifecycle below is what will keep that safe once wired: a schema reaches a worker before any
portal can name it, and it outlives every portal that might still name it.

## The bundle

The bundle maps each schema to its definition, and carries a **content fingerprint** so a client can
detect when the set of schemas changes.

Its defining property: it is generated from **committed chunk rows alone** — the routable window
(ADR 0002) plus the persisted write-schema set of the last successful assignment
(`sched_worker_assignment_schemas`, committed in the same transaction as the assignment it
describes). It is therefore independent of whether the round's placement succeeded, and identical
after a process restart: the same generator serves success, shortage, and visibility-driven
refreshes.

## Lifecycle

**A schema enters the bundle when its first chunk is placed.** Promotion happens at the chunk level:
a chunk — and with it, its schema — becomes visible to portals only once it is confirmed. Because the
schema was already in the bundle from the moment the chunk was placed, it is present before any
portal could name that chunk. Once wired, this guarantees a worker always has the schema before a
portal asks for it. There is no separate schema-level promotion.

A schema that is merely active for a dataset with no placed chunk yet is **not** in the bundle — and
that is correct, because no chunk references it, so nothing needs it.

**A schema stays available through a grace window (M) after its chunk stops being served.** When a
chunk is dropped from the portal assignment, it and its schema are retained for M longer before the
chunk is retired from the worker assignment. A portal that is a little behind still routes only to
not-yet-retired chunks, so it still resolves. Crucially this grace window is the **same clock** as
the chunk's own retirement — there is no separate schema clock that could fall out of sync.

**A capacity shortage freezes the assignment but not the bundle.** When the scheduler cannot place
a full assignment, the last good assignment stays frozen — but the bundle keeps regenerating every
round. Its write section cannot shrink below what the frozen assignment names (the persisted set is
only replaced by the next successful cycle, atomically with the assignment that supersedes it), and
its read section always carries the current read schemas — so a read schema promoted mid-shortage
reaches portals immediately instead of waiting out the streak.

In simulation, a consistency check confirms that every chunk the current worker and portal
assignments name resolves under the round's bundle, and a strict oracle confirms every read schema
a portal is told to use resolves in the bundle published with it.

## Limits

- **Delivery to workers is a follow-up.** Getting the bundle to workers alongside the assignment — so
  a worker never serves a chunk without its schema — is not yet built.
