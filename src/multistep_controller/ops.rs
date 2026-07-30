//! The service's operational surface: `/metrics`, `/health`, `/ready`.
//!
//! Liveness is the heartbeat rule. Each task stamps [`Heartbeat::beat`] when a unit of work
//! completes — not when a tick merely runs — so a task that keeps looping while every attempt
//! fails, or one wedged inside an uncancellable call, both stop advancing their stamp. One number
//! per task then covers every stall cause without instrumenting each dependency, and `/health`
//! turns it into the only recovery the service has for a wedge: a restart.

use std::{
    net::SocketAddr,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};

use anyhow::Context;
use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use prometheus_client::registry::Registry;
use tokio_util::sync::CancellationToken;

use super::now_ticks;

/// How far past its own period a task may go before it counts as stalled. Generous because the
/// action is a process restart: a pass running 3x its interval is wedged, one running 1.5x is a
/// big database. A placeholder like the intervals themselves — the exported timestamps are what
/// alerting should be tuned on.
const STALL_PERIODS: u32 = 3;

/// One task's liveness and freshness, which are deliberately separate signals:
///
/// * `last_pass` — the loop came round again, whether or not its work landed. This is what
///   `/health` reads, because the only thing a restart fixes is a wedged uncancellable call. A
///   ClickHouse or S3 outage is not that: killing the process cannot reach the dependency, and the
///   replacement has to redo startup against the same outage.
/// * `last_success` — work actually landed. Exported as `task_last_success_seconds` and left to
///   alerting, which can page a human instead of restarting a process that is doing its best.
///
/// Both start at process construction, so a task that never succeeds still goes stale on its own
/// clock rather than staying green forever behind an unbounded startup grace.
pub struct Heartbeat {
    task: &'static str,
    period: Duration,
    /// Unix seconds of the last completed pass; process start until the first one.
    last_pass: AtomicU64,
    /// Unix seconds of the last landed unit of work; 0 until the first — never a lie about work
    /// that did not happen, so alerting on `now - task_last_success` stays honest.
    last_success: AtomicU64,
}

impl Heartbeat {
    pub fn new(task: &'static str, period: Duration) -> Arc<Self> {
        let heartbeat = Arc::new(Self {
            task,
            period,
            last_pass: AtomicU64::new(now_ticks()),
            last_success: AtomicU64::new(0),
        });
        // Publish both series up front. `get_or_create` inside the setters alone would leave them
        // absent exactly when a task never succeeds — the case the alert exists for.
        heartbeat.publish(&crate::metrics::TASK_LAST_PASS, heartbeat.last_pass());
        heartbeat.publish(&crate::metrics::TASK_LAST_SUCCESS, 0);
        heartbeat
    }

    /// Record a completed pass: the loop returned instead of wedging. Says nothing about whether
    /// the work landed — that is [`Self::beat`].
    pub fn still_running(&self) {
        let now = now_ticks();
        self.last_pass.store(now, Ordering::Relaxed);
        self.publish(&crate::metrics::TASK_LAST_PASS, now as i64);
    }

    /// Record a landed unit of work. Also a completed pass, so callers never need both.
    pub fn beat(&self) {
        let now = now_ticks();
        self.last_success.store(now, Ordering::Relaxed);
        self.publish(&crate::metrics::TASK_LAST_SUCCESS, now as i64);
        self.still_running();
    }

    fn publish(&self, family: &crate::metrics::TaskGauges, value: i64) {
        family
            .get_or_create(&vec![("task", self.task.to_string())])
            .set(value);
    }

    fn last_pass(&self) -> i64 {
        self.last_pass.load(Ordering::Relaxed) as i64
    }

    /// `Some(age)` once the loop has not come round for [`STALL_PERIODS`] of its period — a wedge,
    /// the one failure a restart resolves. A task looping on failed dependencies is *not* stalled;
    /// its staleness shows up in `task_last_success_seconds` instead.
    fn stalled_for(&self, now: u64) -> Option<Duration> {
        let last = self.last_pass.load(Ordering::Relaxed);
        let budget = self.period * STALL_PERIODS;
        let age = Duration::from_secs(now.saturating_sub(last));
        (age > budget).then_some(age)
    }
}

/// What the endpoints read. `ready` flips once startup finished; it deliberately says nothing about
/// leadership, so a future standby replica can report ready while waiting to take over.
pub struct Ops {
    registry: Registry,
    heartbeats: Vec<Arc<Heartbeat>>,
    ready: AtomicBool,
}

impl Ops {
    pub fn new(registry: Registry, heartbeats: Vec<Arc<Heartbeat>>) -> Arc<Self> {
        Arc::new(Self {
            registry,
            heartbeats,
            ready: AtomicBool::new(false),
        })
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Relaxed);
    }
}

/// Serve until `token` is cancelled. Started before the startup passes, so a slow cold start is
/// visibly not-ready rather than unreachable.
pub async fn serve(
    addr: SocketAddr,
    ops: Arc<Ops>,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!(%addr, "Ops surface listening");
    let router = Router::new()
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(ops);
    axum::serve(listener, router)
        .with_graceful_shutdown(async move { token.cancelled().await })
        .await
        .context("serve the ops surface")
}

async fn metrics(State(ops): State<Arc<Ops>>) -> Response {
    match crate::metrics::encode_metrics(&ops.registry) {
        Ok(body) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            body,
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = format!("{e:#}"), "Failed to encode metrics");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// 503 once any task has stopped completing work for [`STALL_PERIODS`] of its period: the
/// orchestrator restarts us, which is the only way out of a wedged uncancellable call.
async fn health(State(ops): State<Arc<Ops>>) -> Response {
    let now = now_ticks();
    let stalled: Vec<_> = ops
        .heartbeats
        .iter()
        .filter_map(|hb| hb.stalled_for(now).map(|age| (hb.task, age.as_secs())))
        .collect();
    if stalled.is_empty() {
        return StatusCode::OK.into_response();
    }
    tracing::warn!(?stalled, "Task stalled past its budget");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        format!("stalled: {stalled:?}"),
    )
        .into_response()
}

async fn ready(State(ops): State<Arc<Ops>>) -> StatusCode {
    if ops.ready.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PERIOD: Duration = Duration::from_secs(60);

    /// A task still inside its first pass is not stalled — but the grace is bounded by process
    /// start, not open-ended. The old rule ("no success yet" ⇒ never stalled) meant a task that
    /// failed from its very first pass stayed green forever.
    #[test]
    fn a_task_that_never_finished_anything_stalls_on_its_own_clock() {
        let hb = Heartbeat::new("test", PERIOD);
        let started = hb.last_pass.load(Ordering::Relaxed);
        let budget = PERIOD.as_secs() * u64::from(STALL_PERIODS);
        assert_eq!(hb.stalled_for(started + budget), None, "still starting up");
        assert!(
            hb.stalled_for(started + budget + 1).is_some(),
            "a task that never completed a pass must not stay green forever"
        );
    }

    /// Liveness follows passes, not successes: a task retrying a failed dependency is running, and
    /// restarting it would not reach the dependency. Freshness is `task_last_success`'s job.
    #[test]
    fn a_task_looping_without_success_is_not_stalled() {
        let hb = Heartbeat::new("test", PERIOD);
        let budget = PERIOD.as_secs() * u64::from(STALL_PERIODS);
        hb.last_pass.store(1_000, Ordering::Relaxed);
        assert!(hb.stalled_for(1_000 + budget + 1).is_some(), "wedged");
        hb.still_running();
        assert_eq!(hb.stalled_for(now_ticks()), None);
        assert_eq!(
            hb.last_success.load(Ordering::Relaxed),
            0,
            "a pass is not a success"
        );
    }

    #[test]
    fn stall_is_reported_only_past_the_budget() {
        let hb = Heartbeat::new("test", PERIOD);
        hb.last_pass.store(1_000, Ordering::Relaxed);
        let budget = PERIOD.as_secs() * u64::from(STALL_PERIODS);
        assert_eq!(hb.stalled_for(1_000 + budget), None, "at the budget");
        assert_eq!(
            hb.stalled_for(1_000 + budget + 1).map(|d| d.as_secs()),
            Some(budget + 1),
        );
    }

    #[test]
    fn a_beat_clears_a_stall_and_counts_as_a_pass() {
        let hb = Heartbeat::new("test", PERIOD);
        hb.last_pass.store(1_000, Ordering::Relaxed);
        assert!(hb.stalled_for(1_000_000).is_some());
        hb.beat();
        assert_eq!(hb.stalled_for(now_ticks()), None);
        assert_ne!(hb.last_success.load(Ordering::Relaxed), 0);
    }

    /// Both series exist from construction. A `get_or_create` only in the setters would leave
    /// `task_last_success` absent exactly when a task never succeeds — the case it is alerted on.
    #[test]
    fn both_series_are_published_before_any_work_lands() {
        let registry = crate::metrics::register_metrics("test".to_string());
        let _hb = Heartbeat::new("published", PERIOD);
        let encoded = crate::metrics::encode_metrics(&registry).expect("encode");
        assert!(
            encoded.contains(r#"task_last_success_seconds{network="test",task="published"} 0"#),
            "missing zeroed success series:\n{encoded}"
        );
        assert!(
            encoded.contains(r#"task_last_pass_seconds{network="test",task="published"}"#),
            "missing pass series:\n{encoded}"
        );
    }

    /// The surface end to end: it serves before readiness is marked, `/ready` flips, `/metrics`
    /// renders the registry, and `/health` follows the heartbeat rather than the process.
    #[test]
    fn the_served_surface_reports_readiness_metrics_and_stalls() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build test runtime");
        rt.block_on(async {
            let hb = Heartbeat::new("scheduling", PERIOD);
            let ops = Ops::new(
                crate::metrics::register_metrics("test".to_string()),
                vec![hb.clone()],
            );
            let token = CancellationToken::new();
            // Port 0: the OS picks a free one, so concurrent tests never collide.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("local addr");
            drop(listener);
            let server = tokio::spawn(serve(addr, ops.clone(), token.clone()));

            // A raw GET keeps an HTTP client out of the dependency tree for one test.
            let get = async |path: &str| {
                use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
                loop {
                    // The server may not have bound yet.
                    let Ok(mut sock) = tokio::net::TcpStream::connect(addr).await else {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    };
                    sock.write_all(
                        format!("GET {path} HTTP/1.1\r\nHost: ops\r\nConnection: close\r\n\r\n")
                            .as_bytes(),
                    )
                    .await
                    .expect("send request");
                    let mut raw = String::new();
                    sock.read_to_string(&mut raw).await.expect("read response");
                    let (head, body) = raw.split_once("\r\n\r\n").expect("headers end");
                    let status: u16 = head
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                        .expect("status line");
                    return (status, body.to_string());
                }
            };

            assert_eq!(
                get("/ready").await.0,
                503,
                "not ready until startup finishes"
            );
            ops.mark_ready();
            assert_eq!(get("/ready").await.0, 200);

            assert_eq!(get("/health").await.0, 200, "no beat yet is not a stall");
            // Liveness follows passes, not successes: only a loop that stopped coming round is a
            // wedge a restart can fix.
            hb.last_pass.store(
                now_ticks() - PERIOD.as_secs() * u64::from(STALL_PERIODS) - 1,
                Ordering::Relaxed,
            );
            assert_eq!(get("/health").await.0, 503, "a stalled task fails liveness");
            hb.still_running();
            assert_eq!(get("/health").await.0, 200, "a pass clears the stall");
            hb.beat();
            assert_eq!(get("/health").await.0, 200);

            let (status, body) = get("/metrics").await;
            assert_eq!(status, 200);
            assert!(
                body.contains("scheduler_task_last_success_seconds")
                    && body.contains("task=\"scheduling\""),
                "the heartbeat must be exported: {body}"
            );

            token.cancel();
            server
                .await
                .expect("server task")
                .expect("graceful shutdown");
        });
    }
}
