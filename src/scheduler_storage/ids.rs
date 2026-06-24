//! Storage primary-key newtypes shared by every backend.
//!
//! Both wrap an `i64` surrogate (Postgres `SERIAL`, in-memory counter); the newtypes keep chunk
//! and worker keys from being swapped while staying plain integers on the wire.

/// Primary key of a chunk row.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, sqlx::Type)]
#[sqlx(transparent)]
pub struct ChunkPk(pub i64);

/// Primary key of a worker row.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, sqlx::Type)]
#[sqlx(transparent)]
pub struct WorkerPk(pub i64);

/// Primary key of a dataset row.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, sqlx::Type)]
#[sqlx(transparent)]
pub struct DatasetId(pub i16);

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
