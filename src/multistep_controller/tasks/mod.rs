//! The service's three periodic tasks — scheduling, worker-status, chunk-discovery — one module
//! and one OS thread each. A task owns its `PostgresStorage` outright: created on the task's
//! thread and called directly, since the sync `block_on` facade must stay off the async runtime.
//! ClickHouse/S3 futures and cancellable waits are driven on the main runtime through
//! `Handle::block_on`, so the shared clients never leave the runtime they were built on.
//!
//! Failure policy: a failed operation is logged and retried next tick, unless [`retry_or_die`]
//! finds it fatal — leadership lost to a newer epoch, or the Postgres connection gone — in which
//! case the task exits (which shuts the service down for an orchestrator restart) instead of
//! retrying against a database that will refuse it anyway.

mod chunk_discovery;
mod scheduling;
mod worker_status;

pub(super) use chunk_discovery::ChunkDiscoveryTask;
pub(super) use scheduling::SchedulingTask;
pub(super) use worker_status::WorkerStatusTask;

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use crate::scheduler_storage::StorageError;
use crate::scheduler_storage::postgres::PostgresStorage;

/// A storage operation failed: fatal if a newer leader has taken the epoch (the connection is
/// healthy, but every write of ours is refused from now on) or if the Postgres connection is gone —
/// either way the service must exit and restart rather than retry — otherwise logged and retried
/// next tick.
pub(super) fn retry_or_die(
    storage: &mut PostgresStorage,
    ping_timeout: Duration,
    error: impl Into<anyhow::Error>,
    what: &str,
) -> anyhow::Result<()> {
    let error = error.into();
    // Before the liveness probe: being fenced out says nothing about the connection.
    if matches!(
        error.downcast_ref::<StorageError>(),
        Some(StorageError::FencedOut)
    ) {
        return Err(error.context(format!("{what}: fenced out by a newer leader")));
    }
    if let Err(ping) = storage.ping(ping_timeout) {
        return Err(error.context(format!("{what}: Postgres connection lost ({ping})")));
    }
    tracing::error!(
        error = format!("{error:#}"),
        "{what} failed, retrying next tick"
    );
    Ok(())
}

/// Park the task's thread until the next tick (`Continue`) or shutdown (`Break`). The period
/// restarts after each tick, so an overrunning cycle gets a full breather, not an immediate
/// re-run.
///
/// `probe` runs every `probe_every` of the wait (0 disables it). Without it a connection that dies
/// while the task sleeps goes unnoticed until the next tick's first statement fails — twenty
/// minutes for the scheduling task — and the periodic traffic also keeps the connection out of
/// reach of idle timeouts. A failing probe is fatal, like any other lost connection.
fn wait_for_next_tick(
    handle: &Handle,
    token: &CancellationToken,
    period: Duration,
    probe_every: Duration,
    mut probe: impl FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<ControlFlow<()>> {
    let started = Instant::now();
    loop {
        let remaining = period.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Ok(ControlFlow::Continue(()));
        }
        // The last leg is whatever is left; earlier ones are a probe interval each. Probing after
        // the last would be pointless — the tick that follows is itself the liveness check.
        let final_leg = probe_every.is_zero() || remaining <= probe_every;
        let leg = if final_leg { remaining } else { probe_every };
        let shutting_down = handle.block_on(async {
            tokio::select! {
                () = token.cancelled() => true,
                () = tokio::time::sleep(leg) => false,
            }
        });
        if shutting_down {
            return Ok(ControlFlow::Break(()));
        }
        if !final_leg {
            probe()?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::scheduler_storage::SchedulerStorage;
    use crate::scheduler_storage::postgres::{
        DEFAULT_BATCH_SIZE, DEFAULT_CLAIM_LOCK_TIMEOUT, SessionMemory,
    };
    use crate::scheduler_storage::test_harness::pg_harness::fresh_db_url;
    use crate::scheduler_storage::test_harness::utils::{
        StaticSchedulingAlgorithm, chunk, new_dataset, worker,
    };
    use crate::types::{DatasetSchema, Worker};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn retry_or_die_is_fatal_only_when_the_connection_is_gone() {
        use sqlx::Connection as _;

        let (url, _db) = fresh_db_url("tasks", TEST_ID.fetch_add(1, Ordering::Relaxed));
        let (mut storage, _) =
            PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
                .expect("connect");
        retry_or_die(
            &mut storage,
            PING_TIMEOUT,
            anyhow::anyhow!("op failed"),
            "test op",
        )
        .expect("a live connection retries");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test runtime");
        rt.block_on(async {
            let mut admin = sqlx::postgres::PgConnection::connect(&url)
                .await
                .expect("admin connection");
            sqlx::query(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                 WHERE datname = current_database() AND pid <> pg_backend_pid()",
            )
            .execute(&mut admin)
            .await
            .expect("terminate the task's backend");
        });

        let err = storage
            .datasets_with_last_chunk()
            .expect_err("query on a dead connection must fail");
        retry_or_die(&mut storage, PING_TIMEOUT, err, "test op")
            .expect_err("a dead connection is fatal");
    }

    /// Losing leadership is fatal on a perfectly healthy connection: retrying in place would keep
    /// hitting a database that now refuses us, so the task must exit and let the orchestrator
    /// restart it as a fresh candidate.
    #[test]
    fn retry_or_die_is_fatal_when_fenced_out_despite_a_live_connection() {
        let (url, _db) = fresh_db_url("tasks", TEST_ID.fetch_add(1, Ordering::Relaxed));
        let (leader, epoch) =
            PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
                .expect("connect leader");
        let mut follower = PostgresStorage::connect_follower(
            &url,
            SessionMemory::ServerDefault,
            epoch,
            DEFAULT_BATCH_SIZE,
        )
        .expect("connect follower");
        drop(leader);
        let _replacement =
            PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
                .expect("replacement leader claims a new epoch");

        let err = follower
            .update_worker_set(&[worker(1, None)], 100)
            .expect_err("the stale epoch is refused");
        let fatal = retry_or_die(&mut follower, PING_TIMEOUT, err, "update worker set")
            .expect_err("fenced out is fatal");
        assert!(format!("{fatal:#}").contains("fenced out by a newer leader"));

        // The shape the chunk-discovery task passes in: `FencedOut` under a `.context` layer, which
        // the downcast must still see.
        let err = follower
            .update_worker_set(&[worker(1, None)], 100)
            .expect_err("the stale epoch is refused");
        retry_or_die(
            &mut follower,
            PING_TIMEOUT,
            anyhow::Error::new(err).context("register discovered chunks"),
            "update worker set",
        )
        .expect_err("a wrapped FencedOut is just as fatal");

        follower
            .ping(PING_TIMEOUT)
            .expect("the connection was never the problem");
    }

    /// Smoke test for the service's three-connection layout: scheduling cycles (with worker GC,
    /// as in the scheduling task's tick), chunk ingest, and status-only worker-set updates run
    /// concurrently on one database — the lock-free ownership-map layout, where each table has a
    /// single writing connection — and the state stays consistent (every chunk admitted exactly
    /// once, a final cycle succeeds).
    #[test]
    fn cycle_ingest_and_worker_updates_run_concurrently() {
        const ROUNDS: u32 = 5;
        let (url, _db) = fresh_db_url("tasks", TEST_ID.fetch_add(1, Ordering::Relaxed));
        let (mut sched, epoch) =
            PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
                .expect("connect leader");
        let mut ingest = PostgresStorage::connect_follower(
            &url,
            SessionMemory::ServerDefault,
            epoch,
            DEFAULT_BATCH_SIZE,
        )
        .expect("connect ingest");
        let mut workers = PostgresStorage::connect_follower(
            &url,
            SessionMemory::Raised,
            epoch,
            DEFAULT_BATCH_SIZE,
        )
        .expect("connect worker");

        ingest
            .insert_new_datasets(vec![new_dataset("actors", DatasetSchema::default())])
            .expect("create dataset");

        let admitted = std::thread::scope(|scope| {
            let sched_side = scope.spawn(|| {
                let algorithm = StaticSchedulingAlgorithm {
                    mapping: Vec::new(),
                };
                for i in 0..ROUNDS {
                    sched
                        .run_scheduling_cycle(&algorithm, &(), 100 + u64::from(i), 60)
                        .expect("scheduling cycle");
                    sched
                        .gc_inactive_workers(100 + u64::from(i), 1000)
                        .expect("gc inactive workers");
                }
            });
            let ingest_side = scope.spawn(|| {
                let mut admitted = 0usize;
                for i in 0..ROUNDS {
                    ingest
                        .insert_new_chunks(vec![chunk("actors", i, 10)])
                        .expect("insert chunk");
                    admitted += ingest.register_new_chunks().expect("register chunks").len();
                }
                admitted
            });
            let worker_side = scope.spawn(|| {
                for round in 0..ROUNDS {
                    let active: Vec<Worker> = (1..=3).map(|s| worker(s, None)).collect();
                    workers
                        .update_worker_set(&active, 1000 + u64::from(round))
                        .expect("update worker set");
                }
            });
            sched_side.join().expect("sched thread");
            worker_side.join().expect("worker thread");
            ingest_side.join().expect("ingest thread")
        });
        // The non-overlapping chunks were each admitted exactly once, and a cycle over the final
        // state still succeeds.
        assert_eq!(admitted, ROUNDS as usize);
        let algorithm = StaticSchedulingAlgorithm {
            mapping: Vec::new(),
        };
        sched
            .run_scheduling_cycle(&algorithm, &(), 200, 60)
            .expect("final cycle");
    }

    #[test]
    fn wait_for_next_tick_returns_on_deadline_and_on_cancel() {
        // Multi-thread runtime: its workers drive the sleep while this thread is parked in
        // `Handle::block_on`, as in production.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build test runtime");
        let token = CancellationToken::new();
        assert!(
            wait_for_next_tick(
                rt.handle(),
                &token,
                Duration::from_millis(1),
                PROBE_EVERY,
                ok_probe
            )
            .expect("no probe can fail")
            .is_continue(),
            "deadline elapses without shutdown"
        );
        token.cancel();
        assert!(
            wait_for_next_tick(
                rt.handle(),
                &token,
                Duration::from_secs(3600),
                PROBE_EVERY,
                ok_probe
            )
            .expect("no probe can fail")
            .is_break(),
            "cancellation interrupts the wait"
        );
    }

    fn ok_probe() -> anyhow::Result<()> {
        Ok(())
    }

    /// Short enough to keep the wait tests in milliseconds.
    const PROBE_EVERY: Duration = Duration::from_millis(20);

    /// Generous: these tests care about the verdict, not the bound.
    const PING_TIMEOUT: Duration = Duration::from_secs(5);

    /// A wait shorter than the probe interval must not probe at all — the tick that follows is
    /// itself the liveness check, and a probe per tick would double every task's round trips.
    #[test]
    fn a_short_wait_does_not_probe() {
        let rt = multi_thread_rt();
        let probes = AtomicU64::new(0);
        let waited = wait_for_next_tick(
            rt.handle(),
            &CancellationToken::new(),
            Duration::from_millis(1),
            PROBE_EVERY,
            || {
                probes.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        )
        .expect("no probe can fail");
        assert!(waited.is_continue());
        assert_eq!(probes.load(Ordering::Relaxed), 0);
    }

    /// A connection that dies while the task sleeps is fatal there and then, rather than at the
    /// next tick's first statement — which for the scheduling task is a period away.
    #[test]
    fn a_failing_probe_ends_the_wait_fatally() {
        let rt = multi_thread_rt();
        let started = Instant::now();
        let err = wait_for_next_tick(
            rt.handle(),
            &CancellationToken::new(),
            // Far longer than the test may run: only the probe can end this wait.
            Duration::from_secs(3600),
            PROBE_EVERY,
            || anyhow::bail!("connection gone"),
        )
        .expect_err("a failed probe is fatal");
        assert_eq!(err.to_string(), "connection gone");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the probe must end the wait, not the period"
        );
    }

    /// `0` turns probing off — an operational escape hatch, and the value that would otherwise
    /// spin the wait on zero-length naps.
    #[test]
    fn a_zero_interval_disables_probing() {
        let rt = multi_thread_rt();
        let probes = AtomicU64::new(0);
        let waited = wait_for_next_tick(
            rt.handle(),
            &CancellationToken::new(),
            Duration::from_millis(30),
            Duration::ZERO,
            || {
                probes.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        )
        .expect("no probe can fail");
        assert!(waited.is_continue());
        assert_eq!(probes.load(Ordering::Relaxed), 0);
    }

    fn multi_thread_rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build test runtime")
    }
}
