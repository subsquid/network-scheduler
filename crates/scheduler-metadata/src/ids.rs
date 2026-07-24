//! Storage primary-key newtypes shared by every backend.
//!
//! Each wraps its column's integer; distinct types stop keys being swapped while staying plain
//! integers on the wire.

macro_rules! id_newtype {
    ($(#[$m:meta])* $name:ident($t:ty)) => {
        $(#[$m])*
        #[derive(sqlx::Type, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
        #[sqlx(transparent)]
        pub struct $name(pub $t);

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

id_newtype!(
    /// Primary key of a chunk row (`chunks.chunk_pk`, 64-bit `BIGSERIAL`; rows are never deleted, so it only climbs).
    ChunkPk(i64)
);

id_newtype!(
    /// Primary key of a worker row (`sched_workers.id`, 32-bit `SERIAL`); narrow to keep `worker_ids` arrays compact.
    WorkerPk(i32)
);

id_newtype!(DatasetPk(i16));

id_newtype!(
    /// Primary key of a write-schema row (`schemas.id`, 32-bit `SERIAL`); a chunk pins one.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[serde(transparent)]
    SchemaId(i32)
);

id_newtype!(
    /// Primary key of a read-schema row (`read_schemas.id`, 32-bit `SERIAL`); own id space disjoint from [`SchemaId`], nothing pins to it.
    ReadSchemaId(i32)
);
