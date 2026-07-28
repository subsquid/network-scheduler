//! The one migration set for this database. sqlx tracks applied migrations in a single
//! `_sqlx_migrations` table that cannot be renamed, so a second set in a second crate would corrupt
//! this one's history — the scheduler's `sched_*` tables live here too.

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
