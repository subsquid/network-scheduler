//! The scheduler's view of the shared harness in [`pg_testkit`]: a case
//! database, connected as a [`PostgresStorage`] that owns it and drops it when it drops.
//!
//! `PostgresStorage::connect` `block_on`s its own runtime and nested `block_on` panics, so it runs
//! from plain sync context — the harness's own admin SQL is already behind its runtime.

pub use pg_testkit::{CaseDb, PgData};

use crate::scheduler_storage::postgres::{
    DEFAULT_BATCH_SIZE, DEFAULT_CLAIM_LOCK_TIMEOUT, PostgresStorage,
};

/// Backing the test suite uses: tiny per-case DBs in RAM.
#[cfg(test)]
const TEST_PGDATA: PgData = PgData::Tmpfs { size: "2g" };

/// A fresh database cloned from the migrated template, on the default test backing, dropped with
/// the returned storage. `prefix`+`id` must be unique within the process (a duplicate
/// `CREATE DATABASE` panics).
#[cfg(test)]
pub(crate) fn fresh_db(prefix: &str, id: u64) -> PostgresStorage {
    fresh_db_with(TEST_PGDATA, prefix, id)
}

/// URL of a fresh migrated database on the default test backing, without connecting or taking the
/// scheduler session lock — for tests that need several raw connections to one database. The
/// database lives until the returned [`CaseDb`] drops, so hold it for as long as the URL is used.
#[cfg(test)]
pub(crate) fn fresh_db_url(prefix: &str, id: u64) -> (String, CaseDb) {
    pg_testkit::fresh_db_url(TEST_PGDATA, prefix, id)
}

/// [`fresh_db`] with an explicit container backing, for callers whose DB won't fit the test tmpfs —
/// the mainnet-scale reshuffle-sim passes [`PgData::Disk`]. The first call in a process fixes the
/// backing for the shared container; later calls reuse it.
pub fn fresh_db_with(pgdata: PgData, prefix: &str, id: u64) -> PostgresStorage {
    let (url, db) = pg_testkit::fresh_db_url(pgdata, prefix, id);
    let (storage, _epoch) =
        PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
            .expect("connect fresh db");
    storage.owning(db)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler_storage::test_harness::inspect::StorageInspect;

    #[test]
    fn harness_connects_migrates_and_reads_empty() {
        let storage = fresh_db("harness_smoke", 1);
        assert!(storage.get_chunks(|_| true).is_empty());
        assert!(storage.get_workers(|_| true).is_empty());
    }
}
