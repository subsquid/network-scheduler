//! Shared metadata layer for the MVCC scheduler storage, re-exported by the
//! `network-scheduler` root crate to keep its `scheduler_storage`/`types` surface stable.

pub mod dataset_schema;
pub mod error;
pub mod ids;
pub mod ingest_api;
pub mod metrics;
pub mod new_chunk;
pub mod new_dataset;

pub use dataset_schema::{DatasetSchema, TableSchema};
pub use error::{IngestError, StorageError};
pub use ids::{ChunkPk, DatasetPk, ReadSchemaId, SchemaId, WorkerPk};
pub use ingest_api::{ChunkStatus, Correction, DatasetOutcome, Head, IngestChunk, SchemaInfo};
pub use new_chunk::NewChunk;
pub use new_dataset::NewDataset;

pub mod pg;

pub use pg::{PgIngest, PoolStats};
