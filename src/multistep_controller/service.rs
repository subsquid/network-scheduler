//! Long-running multistep scheduler service (`RunMode::Service`): three periodic tasks —
//! scheduling, worker-status, chunk-discovery — each running on its own OS thread and owning its
//! own Postgres connection (see `tasks/`). The scheduling task is the leader (advisory lock,
//! migrations, epoch claim) and runs worker GC and the visibility cycle after every scheduling
//! cycle. The tasks interleave freely — no cross-task lock: each table has a single writing
//! connection (the ownership map in `scheduler_storage::postgres`), so chunk registration, the
//! confirmation pass, and the status-only worker-set update all proceed while a long scheduling
//! cycle runs.
//!
//! The worker-status task drives the confirmation gate: quorum over the assignment ids workers
//! echo in their pings advances the confirmation watermark. Assignments are still only computed
//! and logged — publishing is not wired yet (see `docs/README.md`).

use std::sync::{Arc, OnceLock};

use anyhow::Context;
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    sync::{oneshot, watch},
};
use tokio_util::sync::CancellationToken;

use crate::{
    cli,
    clickhouse::ClickhouseClient,
    dataset_data_storage::S3Storage,
    scheduler_storage::{AssignmentId, algorithm::MultistepAlgorithm},
};

use super::ops::{self, Heartbeat, Ops};
use super::tasks::{ChunkDiscoveryTask, SchedulingTask, WorkerStatusTask};

/// Runs the service until SIGTERM (clean exit) or a task-thread failure (error exit — the
/// orchestrator restarts us); ctrl-c exits immediately instead (see [`handle_signals`]).
///
/// # Errors
///
/// Startup failures (ClickHouse, Postgres connect/migrate — including `AlreadyRunning`), or a
/// task thread that died mid-flight (fatal storage error or panic).
pub async fn run(args: &cli::Args, config: cli::Config) -> anyhow::Result<()> {
    let database_url = args
        .database_url
        .clone()
        .context("--database-url is required in service mode")?;
    // Loop threads drive their async work (ClickHouse, S3, timers) on this runtime via
    // `Handle::block_on`; a current-thread runtime has no worker to serve them while `run`
    // itself is parked.
    anyhow::ensure!(
        !matches!(
            Handle::current().runtime_flavor(),
            RuntimeFlavor::CurrentThread
        ),
        "service mode requires the multi-thread tokio runtime"
    );

    // Signals are handled before the startup passes, so even a slow cold-start S3 listing shuts
    // down cleanly.
    let token = CancellationToken::new();
    let cause: Arc<OnceLock<Shutdown>> = Arc::new(OnceLock::new());
    tokio::spawn(handle_signals(token.clone(), cause.clone()));

    // Serving before the startup passes: a slow cold start then reads as not-ready rather than
    // unreachable, and a startup that wedges is visible instead of silent.
    let heartbeats = Heartbeats::new(args);
    let ops = Ops::new(
        crate::metrics::register_metrics(config.network.clone()),
        heartbeats.all(),
    );
    tokio::spawn(ops::serve(args.ops_addr, ops.clone(), token.clone()));

    let threads = tokio::select! {
        // Polled in order: a failing task thread cancels the token as it exits, and its startup
        // error must win over that cancellation.
        biased;
        threads = start_tasks(args, config, database_url, &heartbeats, &token, &cause) => match threads {
            Ok(threads) => threads,
            Err(e) => {
                // Unpark any task already waiting (e.g. the scheduling task at its release).
                // Already-spawned threads are deliberately left unjoined: they exit on the
                // cancelled token, and a replacement leader's epoch claim refuses whatever they
                // still try to write in the meantime.
                token.cancel();
                return Err(e);
            }
        },
        () = token.cancelled() => {
            // Deliberately drops the startup future, detaching any threads it spawned (one
            // may sit in a sync connect, which nothing can interrupt); process exit reaps
            // them and Postgres cleans up their connections server-side. Their join handles go
            // with it, so `cause` — not a join — is what separates a requested stop from a task
            // that died while a later one was still starting. Getting that wrong would exit 0 on
            // a fatal failure, and `Restart=on-failure` would leave the scheduler down.
            return startup_cancelled(cause.get());
        }
    };

    ops.mark_ready();

    // Every task-thread exit — signal-triggered, fatal, or panic — cancels the token.
    token.cancelled().await;

    // Thread exit order is unconstrained: a replacement leader's claim fences every connection of
    // this instance at once, whichever of ours are still open (see `scheduler_storage::postgres`).
    let joined = tokio::task::spawn_blocking(move || threads.map(|t| (t.name, t.handle.join())))
        .await
        .context("join task threads")?;
    let mut fatal: Option<anyhow::Error> = None;
    for (name, joined) in joined {
        let result = joined.unwrap_or_else(|panic| {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            Err(anyhow::anyhow!("{name} task panicked: {msg}"))
        });
        if let Err(e) = result {
            // The first error is returned (`main` prints its chain); later ones are logged here.
            if fatal.is_none() {
                fatal = Some(e);
            } else {
                tracing::error!(error = format!("{e:#}"), "Another task failed");
            }
        }
    }
    match fatal {
        Some(e) => Err(e),
        None => {
            tracing::info!("Service stopped");
            Ok(())
        }
    }
}

/// Connect ClickHouse and bring up the three task threads: the scheduling task first — it is the
/// leader, and its epoch is what the other two connect with — parked until released; then the
/// worker and chunk-discovery tasks, whose startup passes run before they report ready; then the
/// release, so the first scheduling cycle sees workers and chunks. Returns the threads in join
/// order.
async fn start_tasks(
    args: &cli::Args,
    config: cli::Config,
    database_url: cli::Secret,
    heartbeats: &Heartbeats,
    token: &CancellationToken,
    cause: &Arc<OnceLock<Shutdown>>,
) -> anyhow::Result<[TaskThread; 3]> {
    let clickhouse = ClickhouseClient::new(&args.clickhouse)
        .await
        .context("connect to ClickHouse")?;
    let config = Arc::new(config);
    let s3_storage = S3Storage::new(&args.s3.config().await);
    let handle = Handle::current();
    let (latest_tx, latest_rx) = watch::channel(0 as AssignmentId);
    let (start_tx, start_rx) = oneshot::channel();

    let sched = SchedulingTask {
        database_url: database_url.clone(),
        claim_lock_timeout: args.leadership_claim_timeout.into(),
        batch_size: args.batch_size,
        algorithm: MultistepAlgorithm::new(config.datasets.clone()),
        config: config.scheduling.clone(),
        period: args.schedule_interval,
        drain_window_ticks: args.multistep_drain_window.as_secs(),
        worker_gc_ticks: args.multistep_worker_gc.as_secs(),
        latest_published: latest_tx,
        probe_interval: args.connection_probe_interval,
        ping_timeout: args.connection_ping_timeout,
        heartbeat: heartbeats.scheduling.clone(),
        start: start_rx,
        handle: handle.clone(),
        token: token.clone(),
    };
    let (sched, epoch) =
        spawn_task("scheduling", token, cause, move |ready| sched.run(ready)).await?;

    let worker = WorkerStatusTask {
        database_url: database_url.clone(),
        epoch,
        batch_size: args.batch_size,
        clickhouse,
        config: config.clone(),
        period: args.worker_update_interval,
        quorum_pct: args.confirmation_quorum_pct,
        latest_published: latest_rx,
        probe_interval: args.connection_probe_interval,
        ping_timeout: args.connection_ping_timeout,
        heartbeat: heartbeats.worker_status.clone(),
        handle: handle.clone(),
        token: token.clone(),
    };
    let (worker, ()) = spawn_task("worker-status", token, cause, move |ready| {
        worker.run(ready)
    })
    .await?;

    let discovery = ChunkDiscoveryTask {
        database_url,
        epoch,
        batch_size: args.batch_size,
        s3_storage,
        config,
        period: args.chunk_discovery_interval,
        probe_interval: args.connection_probe_interval,
        ping_timeout: args.connection_ping_timeout,
        heartbeat: heartbeats.chunk_discovery.clone(),
        handle,
        token: token.clone(),
    };
    let (discovery, ()) = spawn_task("chunk-discovery", token, cause, move |ready| {
        discovery.run(ready)
    })
    .await?;

    // Both startup passes are in — release the first scheduling cycle.
    let _ = start_tx.send(());
    Ok([worker, discovery, sched])
}

/// One heartbeat per task, built up front so the ops surface can report staleness from the moment
/// it starts serving — before the tasks that own them exist.
struct Heartbeats {
    scheduling: Arc<Heartbeat>,
    worker_status: Arc<Heartbeat>,
    chunk_discovery: Arc<Heartbeat>,
}

impl Heartbeats {
    fn new(args: &cli::Args) -> Self {
        Self {
            scheduling: Heartbeat::new("scheduling", args.schedule_interval),
            worker_status: Heartbeat::new("worker-status", args.worker_update_interval),
            chunk_discovery: Heartbeat::new("chunk-discovery", args.chunk_discovery_interval),
        }
    }

    fn all(&self) -> Vec<Arc<Heartbeat>> {
        vec![
            self.scheduling.clone(),
            self.worker_status.clone(),
            self.chunk_discovery.clone(),
        ]
    }
}

#[derive(Debug)]
struct TaskThread {
    name: &'static str,
    handle: std::thread::JoinHandle<anyhow::Result<()>>,
}

/// What a cancellation observed during startup means for the exit status. A task that died is a
/// failure even though the cancellation looks identical to a requested stop: reporting it as clean
/// would exit 0, and `Restart=on-failure` would leave the scheduler down.
fn startup_cancelled(cause: Option<&Shutdown>) -> anyhow::Result<()> {
    match cause {
        Some(Shutdown::TaskExit(name)) => Err(anyhow::anyhow!("{name} task exited during startup")),
        None | Some(Shutdown::Signal) => {
            tracing::info!("Shutdown requested during startup");
            Ok(())
        }
    }
}

/// Why the service is winding down. Recorded once — the first cause wins, so a task exit that
/// races SIGTERM still reports the failure, since the exit code is what the orchestrator acts on.
#[derive(Debug)]
enum Shutdown {
    /// SIGTERM: a clean, requested stop.
    Signal,
    /// A task thread left: fatal error, panic, or an unexpected clean return.
    TaskExit(&'static str),
}

/// Cancels the token when a task thread leaves, recording the task as the cause. A `Drop` impl
/// rather than a step at the end of the thread body, so a panicking task is attributed too.
struct ExitGuard {
    name: &'static str,
    cause: Arc<OnceLock<Shutdown>>,
    token: CancellationToken,
}

impl Drop for ExitGuard {
    fn drop(&mut self) {
        let _ = self.cause.set(Shutdown::TaskExit(self.name));
        self.token.cancel();
    }
}

/// Spawn task thread `name` and wait for it to report its startup outcome (connect + startup
/// pass) plus its ready payload `T` — the scheduling task's leadership epoch, `()` for the rest.
/// `body` reports startup failures through `ready` — send the `Err`, then return `Ok(())`; its own
/// result covers only post-startup operation. Any later exit — clean, fatal, or panic — cancels
/// `token` and records itself in `cause`; the thread's result is collected by [`run`] after
/// joining, except on the startup-cancellation path, where the handle goes with the dropped future
/// and `cause` is all that survives.
async fn spawn_task<T: Send + 'static>(
    name: &'static str,
    token: &CancellationToken,
    cause: &Arc<OnceLock<Shutdown>>,
    body: impl FnOnce(oneshot::Sender<anyhow::Result<T>>) -> anyhow::Result<()> + Send + 'static,
) -> anyhow::Result<(TaskThread, T)> {
    let (ready_tx, ready_rx) = oneshot::channel();
    let guard = ExitGuard {
        name,
        cause: cause.clone(),
        token: token.clone(),
    };
    let handle = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let _cancel_on_exit = guard;
            let result = body(ready_tx);
            if let Err(e) = &result {
                // Logged here rather than left to `run`: on the startup-cancellation path `run`
                // never sees this result, and the chain would go with the dropped join handle.
                tracing::error!(
                    error = format!("{e:#}"),
                    "{name} task failed, shutting down"
                );
            }
            result
        })
        .with_context(|| format!("spawn {name} thread"))?;
    // On the error paths the thread has already terminated, so the blocking `join` returns at
    // once.
    match ready_rx.await {
        Ok(Ok(ready)) => Ok((TaskThread { name, handle }, ready)),
        Ok(Err(e)) => {
            let _ = handle.join();
            Err(e)
        }
        Err(_) => {
            let _ = handle.join();
            Err(anyhow::anyhow!("{name} task thread died during startup"))
        }
    }
}

/// SIGTERM begins the graceful shutdown (cancel the token, drain, join); a repeat is a no-op —
/// supervisor escalation is SIGKILL, which needs no cooperation. SIGINT (ctrl-c) instead exits
/// on the spot, whatever the state: the interactive escape hatch must not wait out a possibly
/// minutes-long, uncancellable drain, and Postgres cleans up dropped connections server-side.
/// Never returns, so both meanings stay live during a failure-initiated wind-down.
async fn handle_signals(token: CancellationToken, cause: Arc<OnceLock<Shutdown>>) {
    let mut signals = ShutdownSignals::install();
    loop {
        match signals.recv().await {
            ShutdownSignal::Interrupt => {
                tracing::info!("Received ctrl-c, exiting immediately");
                std::process::exit(1);
            }
            ShutdownSignal::Terminate => {
                tracing::info!("Received SIGTERM, shutting down");
                request_shutdown(&token, &cause);
            }
        }
    }
}

/// Begin a requested shutdown. The cause is claimed *before* the cancellation, so the task exits
/// this unparks cannot overwrite it and turn a clean stop into a reported failure.
fn request_shutdown(token: &CancellationToken, cause: &OnceLock<Shutdown>) {
    let _ = cause.set(Shutdown::Signal);
    token.cancel();
}

enum ShutdownSignal {
    /// SIGINT/ctrl-c: exit immediately.
    Interrupt,
    /// SIGTERM: graceful shutdown.
    Terminate,
}

/// Ctrl-c plus SIGTERM, receivable repeatedly.
struct ShutdownSignals {
    sigterm: Option<tokio::signal::unix::Signal>,
}

impl ShutdownSignals {
    fn install() -> Self {
        let sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|e| tracing::error!(error = %e, "Failed to install SIGTERM handler"))
            .ok();
        Self { sigterm }
    }

    async fn recv(&mut self) -> ShutdownSignal {
        let terminate = async {
            match self.sigterm.as_mut() {
                Some(sigterm) => {
                    sigterm.recv().await;
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => ShutdownSignal::Interrupt,
            () = terminate => ShutdownSignal::Terminate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test runtime")
    }

    fn fresh_cause() -> Arc<OnceLock<Shutdown>> {
        Arc::new(OnceLock::new())
    }

    #[test]
    fn startup_failure_is_reported_through_ready() {
        test_rt().block_on(async {
            let token = CancellationToken::new();
            let result = spawn_task::<()>("test-task", &token, &fresh_cause(), |ready| {
                let _ = ready.send(Err(anyhow::anyhow!("no database")));
                Ok(())
            })
            .await;
            let err = result.expect_err("startup error must propagate");
            assert_eq!(err.to_string(), "no database");
        });
    }

    #[test]
    fn thread_death_before_ready_is_a_startup_error() {
        test_rt().block_on(async {
            let token = CancellationToken::new();
            let result =
                spawn_task::<()>("test-task", &token, &fresh_cause(), |_ready| Ok(())).await;
            let err = result.expect_err("a dropped ready channel is a startup failure");
            assert!(err.to_string().contains("died during startup"));
        });
    }

    /// A task exiting after startup — here with a fatal error — cancels the token, winding down
    /// the sibling tasks, and its result is recovered by joining.
    #[test]
    fn task_exit_cancels_the_token() {
        test_rt().block_on(async {
            let token = CancellationToken::new();
            let (thread, ()) = spawn_task("test-task", &token, &fresh_cause(), |ready| {
                let _ = ready.send(Ok(()));
                anyhow::bail!("boom")
            })
            .await
            .expect("startup succeeds");
            tokio::time::timeout(Duration::from_secs(10), token.cancelled())
                .await
                .expect("task exit must cancel the token");
            let err = thread
                .handle
                .join()
                .expect("no panic")
                .expect_err("fatal task result");
            assert_eq!(err.to_string(), "boom");
        });
    }

    /// The verdict `run`'s startup cancel arm reaches. A task exit and a SIGTERM cancel the same
    /// token, so only the recorded cause separates exit 1 from exit 0.
    #[test]
    fn a_startup_cancellation_reports_a_task_exit_but_not_a_signal() {
        startup_cancelled(None).expect("no recorded cause reads as a requested stop");
        startup_cancelled(Some(&Shutdown::Signal)).expect("SIGTERM is a clean exit");
        let err = startup_cancelled(Some(&Shutdown::TaskExit("worker-status")))
            .expect_err("a task that died must not exit 0");
        assert_eq!(err.to_string(), "worker-status task exited during startup");
    }

    /// The startup race that must not read as an operator shutdown: a task dies after reporting
    /// ready while `start_tasks` is still awaiting a later one, so `run`'s cancel arm wins and the
    /// join handle is dropped with the startup future. `cause` is then the only thing standing
    /// between a fatal failure and an exit code of 0.
    #[test]
    fn a_post_ready_task_exit_is_attributed_to_the_task() {
        test_rt().block_on(async {
            let token = CancellationToken::new();
            let cause = fresh_cause();
            let (_thread, ()) = spawn_task("test-task", &token, &cause, |ready| {
                let _ = ready.send(Ok(()));
                anyhow::bail!("boom")
            })
            .await
            .expect("startup succeeds");
            tokio::time::timeout(Duration::from_secs(10), token.cancelled())
                .await
                .expect("task exit must cancel the token");
            assert!(
                matches!(cause.get(), Some(Shutdown::TaskExit("test-task"))),
                "got {:?}",
                cause.get()
            );
        });
    }

    /// The reason attribution lives in a `Drop` impl: a panicking task never reaches the end of
    /// its body, and it must still be told apart from a requested shutdown.
    #[test]
    fn a_panicking_task_is_attributed_too() {
        test_rt().block_on(async {
            let token = CancellationToken::new();
            let cause = fresh_cause();
            let (thread, ()) = spawn_task("test-task", &token, &cause, |ready| {
                let _ = ready.send(Ok(()));
                panic!("kaboom")
            })
            .await
            .expect("startup succeeds");
            tokio::time::timeout(Duration::from_secs(10), token.cancelled())
                .await
                .expect("a panic must cancel the token");
            assert!(
                matches!(cause.get(), Some(Shutdown::TaskExit("test-task"))),
                "got {:?}",
                cause.get()
            );
            assert!(thread.handle.join().is_err(), "the panic is recoverable");
        });
    }

    /// SIGTERM claims the cause before cancelling, so the task exits it unparks cannot turn a
    /// requested stop into a reported failure — the first cause wins.
    #[test]
    fn a_signal_outranks_the_task_exits_it_causes() {
        test_rt().block_on(async {
            let token = CancellationToken::new();
            let cause = fresh_cause();
            let (thread, ()) = spawn_task("test-task", &token, &cause, {
                let token = token.clone();
                move |ready| {
                    let _ = ready.send(Ok(()));
                    // Leaves only once cancelled, as a task parked in its tick wait does.
                    while !token.is_cancelled() {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Ok(())
                }
            })
            .await
            .expect("startup succeeds");

            // The same call SIGTERM makes, so the claim-before-cancel ordering is what is pinned.
            request_shutdown(&token, &cause);

            thread
                .handle
                .join()
                .expect("no panic")
                .expect("a cancelled task exits cleanly");
            assert!(
                matches!(cause.get(), Some(Shutdown::Signal)),
                "got {:?}",
                cause.get()
            );
        });
    }
}
