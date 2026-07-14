//! How many ids the PG worker-set sync spends. Its behaviour is covered by the in-memory suite.

use super::fresh_storage;
use crate::scheduler_storage::postgres::PostgresStorage;
use crate::scheduler_storage::test_harness::inspect::StorageInspect;
use crate::scheduler_storage::test_harness::utils::worker;
use crate::scheduler_storage::{SchedulerStorage, StorageError};
use crate::types::Worker;

/// Last value handed out by `sched_workers.id`'s sequence — i.e. how many ids the syncs consumed.
fn ids_consumed(storage: &mut PostgresStorage) -> i64 {
    storage
        .with_conn(async move |conn| {
            let used: i64 =
                sqlx::query_scalar("SELECT COALESCE(last_value, 0) FROM sched_workers_id_seq")
                    .fetch_one(&mut *conn)
                    .await
                    .unwrap();
            Ok::<_, StorageError>(used)
        })
        .unwrap()
}

/// `sched_workers.id` is a 32-bit SERIAL, so ids must be spent per distinct worker, not per sync. An
/// `ON CONFLICT DO UPDATE` upsert breaks this invisibly — the rows it writes are correct, only the
/// sequence runs away — so nothing else in the suite would catch it.
#[test]
fn resyncing_an_unchanged_worker_set_consumes_no_ids() {
    let mut storage = fresh_storage("worker_seq");
    let active: Vec<Worker> = (1..=8).map(|seed| worker(seed, Some("2.8.0"))).collect();

    storage
        .update_worker_set(&active, 1, 100)
        .expect("first sync");
    let after_first = ids_consumed(&mut storage);
    assert_eq!(after_first, 8, "one id per worker on first registration");

    for tick in 2..=10 {
        storage
            .update_worker_set(&active, tick, 100)
            .expect("re-sync");
    }
    assert_eq!(
        ids_consumed(&mut storage),
        after_first,
        "re-syncing the same active set must not consume ids",
    );
    assert_eq!(storage.get_workers(|_| true).len(), 8, "no duplicate rows");
}
