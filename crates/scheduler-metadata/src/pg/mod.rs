//! The Postgres ingest layer: the embedded migration set plus the dataset/schema/chunk/correction
//! SQL. Helpers default to `async fn(&mut PgConnection, …)`, usable on bare and pooled
//! connections alike; those whose semantics only exist inside a transaction (the advisory lock,
//! schema write/promote, the correction steps) take `&mut Transaction` instead.

mod migrations;

pub mod correction;
pub mod ingest;
pub mod lock;
pub mod nonoverlap;
pub mod registration;
pub mod rows;
pub mod schema;

pub use ingest::{PgIngest, PoolStats};
pub use migrations::MIGRATOR;
