//! Cross-connection leadership behaviour: the advisory-lock admission check, the epoch fence every
//! write carries, and the worker-GC row-lock race with a concurrent reactivation.

use std::sync::atomic::Ordering;
use std::time::Duration;

use sqlx::Connection as _;

use super::TEST_ID;
use crate::scheduler_storage::postgres::{
    DEFAULT_BATCH_SIZE, DEFAULT_CLAIM_LOCK_TIMEOUT, PostgresStorage, SessionMemory,
};
use crate::scheduler_storage::test_harness::pg_harness::fresh_db_url;
use crate::scheduler_storage::test_harness::utils::worker;
use crate::scheduler_storage::{SchedulerStorage, StorageError};

/// The advisory lock is admission control only: a second leader is refused while the first's
/// connection lives, regardless of the epoch.
#[test]
fn a_second_leader_is_refused_while_the_first_connection_lives() {
    let (url, _db) = fresh_db_url("locks", TEST_ID.fetch_add(1, Ordering::Relaxed));
    let (leader, _) =
        PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
            .expect("leader connects");
    assert!(
        matches!(
            PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE),
            Err(StorageError::AlreadyRunning)
        ),
        "a second leader must be refused while the first lives"
    );

    drop(leader);
    PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
        .expect("replacement leader once the connection is gone");
}

/// A follower carries its leader's epoch, so it stops writing the moment a replacement leader claims
/// the next one — no dependency on the old leader's connections disappearing. Its reads are
/// unfenced and keep working.
#[test]
fn follower_with_a_stale_epoch_is_fenced_out_but_still_reads() {
    let (url, _db) = fresh_db_url("locks", TEST_ID.fetch_add(1, Ordering::Relaxed));
    let (leader, epoch) =
        PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
            .expect("leader connects");
    let mut follower = PostgresStorage::connect_follower(
        &url,
        SessionMemory::ServerDefault,
        epoch,
        DEFAULT_BATCH_SIZE,
    )
    .expect("follower connects with the leader's epoch");
    follower
        .update_worker_set(&[worker(1, None)], 100)
        .expect("the follower writes under the current epoch");

    // Dropping the leader frees the singleton lock; the replacement claims the next epoch.
    drop(leader);
    let (_replacement, next) =
        PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
            .expect("replacement leader connects");
    assert_ne!(next, epoch, "the claim must mint a fresh epoch");

    assert!(
        matches!(
            follower.update_worker_set(&[worker(2, None)], 200),
            Err(StorageError::FencedOut)
        ),
        "the demoted instance's follower must not commit"
    );
    assert_eq!(
        follower
            .datasets_with_last_chunk()
            .expect("reads stay unfenced")
            .len(),
        0
    );
}

/// Pins the `FOR SHARE` in `begin_fenced`: while a fenced transaction is in flight, a leadership
/// claim *parks* on the leadership row — observed via `pg_stat_activity`, not timing — and only
/// succeeds once that transaction commits. Without it a new leader could believe it is exclusive
/// while the old one's write is still uncommitted.
#[test]
fn an_in_flight_fenced_transaction_blocks_a_leadership_claim() {
    let (url, _db) = fresh_db_url("locks", TEST_ID.fetch_add(1, Ordering::Relaxed));
    let (mut storage, epoch) =
        PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
            .expect("leader connects");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build test runtime");
    rt.block_on(async {
        // A fenced write held open on its own thread: the fence read is taken, the row lock is held,
        // and the commit waits for `release`.
        let (proceed, release) = std::sync::mpsc::channel::<()>();
        let write = std::thread::spawn(move || {
            storage.with_conn(async move |conn| {
                let mut tx =
                    super::super::begin_fenced(conn, epoch, super::super::Isolation::ServerDefault)
                        .await?;
                sqlx::query("UPDATE sched_workers SET version = NULL")
                    .execute(&mut *tx)
                    .await
                    .expect("write inside the fenced tx");
                release.recv().expect("wait for the observer");
                tx.commit().await.expect("commit the fenced tx");
                Ok::<_, StorageError>(())
            })
        });

        // The competing claim, raw SQL: a second `connect` would fast-fail on the advisory lock long
        // before reaching the claim.
        let claim = {
            let url = url.clone();
            tokio::spawn(async move {
                let mut conn = sqlx::postgres::PgConnection::connect(&url)
                    .await
                    .expect("claiming connection");
                sqlx::query_scalar::<_, i64>(
                    "UPDATE sched_leadership SET epoch = epoch + 1, leader_pid = pg_backend_pid() \
                     WHERE only_row RETURNING epoch",
                )
                .fetch_one(&mut conn)
                .await
                .expect("claim leadership")
            })
        };

        // Provably parked on the leadership row's lock (each test runs in its own database, so the
        // filter can't pick up a neighbour's wait).
        let mut observer = sqlx::postgres::PgConnection::connect(&url)
            .await
            .expect("observer connection");
        let mut polls = 0u32;
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event_type = 'Lock'",
            )
            .fetch_one(&mut observer)
            .await
            .expect("poll lock waits");
            if waiting > 0 {
                break;
            }
            assert!(
                !claim.is_finished(),
                "the claim finished without parking on the leadership row"
            );
            polls += 1;
            assert!(polls < 100, "the claim never parked on the leadership row");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        proceed.send(()).expect("release the fenced write");
        write.join().expect("write thread").expect("fenced write");
        let claimed = claim.await.expect("claim task");
        assert_eq!(
            claimed,
            epoch.0 + 1,
            "the claim lands the next epoch, once the fenced write let it through"
        );
    });
}

/// The 40001 the fence can raise: under REPEATABLE READ the fence read parks on an uncommitted
/// claim's row lock, and once that claim commits Postgres refuses the now-outdated snapshot. That is
/// the same event as a plain epoch mismatch, so it must surface as `FencedOut`, not `Serialization`.
#[test]
fn a_repeatable_read_fence_check_maps_40001_to_fenced_out() {
    let (url, _db) = fresh_db_url("locks", TEST_ID.fetch_add(1, Ordering::Relaxed));
    let (mut storage, epoch) =
        PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
            .expect("leader connects");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build test runtime");
    rt.block_on(async {
        // An uncommitted claim holding the leadership row.
        let mut claimer = sqlx::postgres::PgConnection::connect(&url)
            .await
            .expect("claiming connection");
        let mut claim_tx = claimer.begin().await.expect("begin claim");
        sqlx::query(
            "UPDATE sched_leadership SET epoch = epoch + 1, leader_pid = pg_backend_pid() \
             WHERE only_row",
        )
        .execute(&mut *claim_tx)
        .await
        .expect("claim leadership (uncommitted)");

        // A REPEATABLE READ fenced transaction on its own thread: its fence read must park.
        let cycle = std::thread::spawn(move || {
            storage.with_conn(async move |conn| {
                super::super::begin_fenced(conn, epoch, super::super::Isolation::RepeatableRead)
                    .await
                    .map(|_| ())
            })
        });

        let mut observer = sqlx::postgres::PgConnection::connect(&url)
            .await
            .expect("observer connection");
        let mut polls = 0u32;
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event_type = 'Lock'",
            )
            .fetch_one(&mut observer)
            .await
            .expect("poll lock waits");
            if waiting > 0 {
                break;
            }
            assert!(
                !cycle.is_finished(),
                "the fence read finished without parking on the claim"
            );
            polls += 1;
            assert!(polls < 100, "the fence read never parked on the claim");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        claim_tx.commit().await.expect("commit the claim");
        assert!(
            matches!(
                cycle.join().expect("cycle thread"),
                Err(StorageError::FencedOut)
            ),
            "40001 on the fence read is a lost-leadership event"
        );
    });
}

/// Pins the designed resolution of the GC-vs-reactivation race (no advisory lock, no 40001):
/// `gc_inactive_workers`'s READ COMMITTED DELETE matches the inactive row version, *parks* on
/// the row lock of an uncommitted reactivating UPDATE — observed via `pg_stat_activity`'s lock
/// wait, not timing — and on commit re-evaluates its predicate against the new row version
/// (EvalPlanQual), skipping the returning worker. Deterministically red if the DELETE stops
/// re-checking: the worker row would vanish despite the committed reactivation.
#[test]
fn worker_gc_spares_a_concurrently_reactivated_worker() {
    let (url, _db) = fresh_db_url("locks", TEST_ID.fetch_add(1, Ordering::Relaxed));
    let (mut storage, _) =
        PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
            .expect("connect");
    storage
        .update_worker_set(&[worker(1, None)], 100)
        .expect("register the worker");
    storage
        .update_worker_set(&[], 200)
        .expect("mark it inactive at 200");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build test runtime");
    rt.block_on(async {
        // The reactivation: an open transaction holding the worker's row lock.
        let mut reactivate = sqlx::postgres::PgConnection::connect(&url)
            .await
            .expect("reactivating connection");
        let mut tx = reactivate.begin().await.expect("begin reactivation");
        sqlx::query("UPDATE sched_workers SET inactive_since = NULL")
            .execute(&mut *tx)
            .await
            .expect("reactivate the worker (uncommitted)");

        // GC on its own thread (`PostgresStorage` is sync): its DELETE matches the committed
        // inactive version (200 < 1000 - 60) and must park on the row lock.
        let gc = std::thread::spawn(move || storage.gc_inactive_workers(1000, 60));

        // Provably parked on the row lock (each test runs in its own database, so the filter
        // can't pick up a neighbour's wait).
        let mut observer = sqlx::postgres::PgConnection::connect(&url)
            .await
            .expect("observer connection");
        let mut polls = 0u32;
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event_type = 'Lock'",
            )
            .fetch_one(&mut observer)
            .await
            .expect("poll lock waits");
            if waiting > 0 {
                break;
            }
            assert!(
                !gc.is_finished(),
                "gc finished without parking on the reactivated row's lock"
            );
            polls += 1;
            assert!(polls < 100, "gc never parked on the row lock");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tx.commit().await.expect("commit the reactivation");
        gc.join()
            .expect("gc thread")
            .expect("gc completes once the reactivation commits");

        let (total, active): (i64, i64) = sqlx::query_as(
            "SELECT count(*), count(*) FILTER (WHERE inactive_since IS NULL) \
             FROM sched_workers",
        )
        .fetch_one(&mut observer)
        .await
        .expect("read the worker row back");
        assert_eq!(
            (total, active),
            (1, 1),
            "the DELETE must re-evaluate its predicate and spare the reactivated worker"
        );
    });
}
