//! Storage primary-key newtypes shared by every backend.
//!
//! Each wraps the surrogate its column stores (Postgres `SERIAL`/`BIGSERIAL`, in-memory counter);
//! the newtypes keep chunk and worker keys from being swapped while staying plain integers on the
//! wire.

/// Primary key of a chunk row (`chunks.chunk_pk`, a 64-bit `BIGSERIAL`: chunk rows are never
/// deleted, so the sequence only climbs).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, sqlx::Type)]
#[sqlx(transparent)]
pub struct ChunkPk(pub i64);

/// Primary key of a worker row (`sched_workers.id`, a 32-bit `SERIAL`). Narrow on purpose: every
/// routing row carries one of these per replica in its `worker_ids` array.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, sqlx::Type)]
#[sqlx(transparent)]
pub struct WorkerPk(pub i32);

/// Primary key of a dataset row.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, sqlx::Type)]
#[sqlx(transparent)]
pub struct DatasetId(pub i16);

/// Primary key of a schema row (`schemas.id`, a 32-bit `SERIAL`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, sqlx::Type)]
#[sqlx(transparent)]
pub struct SchemaId(pub i32);

impl std::fmt::Display for ChunkPk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for WorkerPk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for DatasetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for SchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
