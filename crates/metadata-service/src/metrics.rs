//! Prometheus registry (shared `exec_times` + service counters) and the request-tracking middleware.

use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::{Registry, Unit};
use tracing::Instrument;

use crate::AppState;

/// Label type matches the shared `EXEC_TIMES` family (`Vec<(&'static str, String)>`), so no derive
/// is needed and it is guaranteed to encode.
type Labels = Vec<(&'static str, String)>;

/// Request-latency histogram bounds (seconds), widened from the Prometheus default at both ends: a
/// 1 ms floor for fast point reads, a 30 s ceiling for batch inserts / schema promotions.
const DURATION_BUCKETS: [f64; 14] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

pub struct Metrics {
    pub registry: Registry,
    http_requests: Family<Labels, Counter>,
    http_request_duration: Family<Labels, Histogram>,
    inflight: Gauge,
    error_responses: Family<Labels, Counter>,
    pool_connections: Gauge,
    pool_idle_connections: Gauge,
    pub(crate) datasets_created: Counter,
    pub(crate) write_schemas_registered: Counter,
    pub(crate) read_schemas_promoted: Counter,
    pub(crate) chunks_inserted: Counter,
    pub(crate) chunks_duplicate: Counter,
    pub(crate) corrections_applied: Counter,
}

impl Metrics {
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::default();
        scheduler_metadata::metrics::register(&mut registry); // shared exec_times family

        let http_requests = Family::<Labels, Counter>::default();
        registry.register(
            "http_requests",
            "HTTP requests by method, route template and status",
            http_requests.clone(),
        );
        // Histogram has no Default (buckets are mandatory), so build per-series via a constructor.
        let http_request_duration = Family::<Labels, Histogram>::new_with_constructor(|| {
            Histogram::new(DURATION_BUCKETS.iter().copied())
        });
        registry.register_with_unit(
            "http_request_duration",
            "HTTP request duration by method and route template",
            Unit::Seconds,
            http_request_duration.clone(),
        );
        let inflight = Gauge::default();
        registry.register(
            "http_requests_inflight",
            "In-flight HTTP requests",
            inflight.clone(),
        );
        let error_responses = Family::<Labels, Counter>::default();
        registry.register(
            "error_responses",
            "Error responses by error code",
            error_responses.clone(),
        );

        let pool_connections = Gauge::default();
        registry.register(
            "db_pool_connections",
            "Open connections in the ingest pool (idle + in-use)",
            pool_connections.clone(),
        );
        let pool_idle_connections = Gauge::default();
        registry.register(
            "db_pool_idle_connections",
            "Idle connections in the ingest pool",
            pool_idle_connections.clone(),
        );

        let datasets_created = Counter::default();
        registry.register(
            "datasets_created",
            "Datasets newly created (idempotent no-ops excluded)",
            datasets_created.clone(),
        );
        let write_schemas_registered = Counter::default();
        registry.register(
            "write_schemas_registered",
            "Write schemas registered",
            write_schemas_registered.clone(),
        );
        let read_schemas_promoted = Counter::default();
        registry.register(
            "read_schemas_promoted",
            "Read schemas promoted (admin)",
            read_schemas_promoted.clone(),
        );
        let chunks_inserted = Counter::default();
        registry.register(
            "chunks_inserted",
            "Chunks committed to storage",
            chunks_inserted.clone(),
        );
        let chunks_duplicate = Counter::default();
        registry.register(
            "chunks_duplicate",
            "Chunk ids rejected as duplicates",
            chunks_duplicate.clone(),
        );
        let corrections_applied = Counter::default();
        registry.register(
            "corrections_applied",
            "Chunk corrections applied",
            corrections_applied.clone(),
        );

        Self {
            registry,
            http_requests,
            http_request_duration,
            inflight,
            error_responses,
            pool_connections,
            pool_idle_connections,
            datasets_created,
            write_schemas_registered,
            read_schemas_promoted,
            chunks_inserted,
            chunks_duplicate,
            corrections_applied,
        }
    }

    /// Update the pool gauges from a fresh snapshot; called by the periodic sampler.
    pub(crate) fn set_pool_stats(&self, size: u32, idle: usize) {
        self.pool_connections.set(i64::from(size));
        self.pool_idle_connections
            .set(i64::try_from(idle).unwrap_or(i64::MAX));
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Records inflight and the terminal status for one request, cancel-safe. Built after `MatchedPath`
/// is resolved (so the `route` label is the template). Status defaults to 503 so a request whose
/// future is *dropped* mid-await — a timeout (the outer `TimeoutLayer`) or a client abort — still
/// balances the gauge and is counted; [`RequestGuard::complete`] overwrites it on normal return.
/// `Gauge`/`Family` clones share their inner atomics, so the guard writes the same series.
struct RequestGuard {
    start: Instant,
    inflight: Gauge,
    http_requests: Family<Labels, Counter>,
    duration: Histogram,
    method: String,
    route: String,
    status: u16,
    completed: bool,
}

impl RequestGuard {
    fn new(state: &AppState, method: String, route: String) -> Self {
        // The duration series is method+route only — fully known up front — so the guard holds a
        // cloned (Arc-backed) handle and observes on drop, cancel-safe like the inflight/status.
        let duration = state
            .metrics
            .http_request_duration
            .get_or_create(&vec![("method", method.clone()), ("route", route.clone())])
            .clone();
        let guard = Self {
            start: Instant::now(),
            inflight: state.metrics.inflight.clone(),
            http_requests: state.metrics.http_requests.clone(),
            duration,
            method,
            route,
            status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            completed: false,
        };
        // Increment only once the guard is fully built, so an earlier panic can't leak the gauge
        // (Drop, which decrements, runs only for a constructed value).
        guard.inflight.inc();
        guard
    }

    fn complete(&mut self, status: u16) {
        self.status = status;
        self.completed = true;
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.inflight.dec(); // fires even if the future was cancelled mid-await
        self.duration.observe(self.start.elapsed().as_secs_f64());
        let labels: Labels = vec![
            ("method", self.method.clone()),
            ("route", self.route.clone()),
            ("status", self.status.to_string()),
        ];
        self.http_requests.get_or_create(&labels).inc();
        if !self.completed {
            tracing::warn!(
                method = %self.method,
                route = %self.route,
                status = self.status,
                "request cancelled before completion (timeout or client abort)"
            );
        }
    }
}

/// Bound the `method` label to a fixed set. hyper accepts arbitrary RFC-7230 token methods, and a
/// method mismatch is a 405 (the route still matched, so this middleware runs), so the raw method is
/// attacker-controllable and unbounded — map unknown verbs to `other` to cap cardinality.
fn method_label(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        Method::PATCH => "PATCH",
        Method::HEAD => "HEAD",
        Method::OPTIONS => "OPTIONS",
        Method::CONNECT => "CONNECT",
        Method::TRACE => "TRACE",
        _ => "other",
    }
}

/// Records method + `MatchedPath` template (bounded cardinality — never the raw dataset name) +
/// status via [`RequestGuard`], cancel-safe. Opens a per-request span so handler logs carry
/// method + route.
pub async fn track_metrics(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let method = method_label(req.method()).to_owned();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_owned(), |m| m.as_str().to_owned());

    let span = tracing::info_span!("request", %method, route = %route);

    // `method`/`route` move into the guard; the span above already captured them by Display.
    let mut guard = RequestGuard::new(&state, method, route);
    let response = next.run(req).instrument(span).await;
    guard.complete(response.status().as_u16());
    // Error-code counter: `error.rs` tags failure responses with a bounded static `ErrorCode`;
    // count it here where `AppState` is in hand and the request actually completed.
    if let Some(code) = response.extensions().get::<crate::error::ErrorCode>() {
        state
            .metrics
            .error_responses
            .get_or_create(&vec![("code", code.0.to_owned())])
            .inc();
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(f: &Family<Labels, Counter>, method: &str, route: &str, status: &str) -> u64 {
        f.get_or_create(&vec![
            ("method", method.to_owned()),
            ("route", route.to_owned()),
            ("status", status.to_owned()),
        ])
        .get()
    }

    // Regression for the inflight-leak bug: a request whose future is dropped without `complete`
    // (timeout / client abort) must still decrement inflight and be counted as 503.
    #[test]
    fn guard_drop_is_cancel_safe() {
        let inflight = Gauge::default();
        inflight.inc(); // as RequestGuard::new would
        let http_requests = Family::<Labels, Counter>::default();
        let guard = RequestGuard {
            start: Instant::now(),
            inflight: inflight.clone(),
            http_requests: http_requests.clone(),
            duration: Histogram::new(DURATION_BUCKETS.iter().copied()),
            method: "POST".to_owned(),
            route: "/datasets/{name}/chunks".to_owned(),
            status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            completed: false,
        };
        drop(guard); // the future was cancelled mid-await
        assert_eq!(inflight.get(), 0, "inflight must return to 0 on cancel");
        assert_eq!(
            count(&http_requests, "POST", "/datasets/{name}/chunks", "503"),
            1,
            "a cancelled request counts as one 503"
        );
    }

    // A completed request counts its real status and balances inflight.
    #[test]
    fn guard_complete_counts_real_status() {
        let inflight = Gauge::default();
        inflight.inc();
        let http_requests = Family::<Labels, Counter>::default();
        let mut guard = RequestGuard {
            start: Instant::now(),
            inflight: inflight.clone(),
            http_requests: http_requests.clone(),
            duration: Histogram::new(DURATION_BUCKETS.iter().copied()),
            method: "GET".to_owned(),
            route: "/health".to_owned(),
            status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            completed: false,
        };
        guard.complete(200);
        drop(guard);
        assert_eq!(inflight.get(), 0);
        assert_eq!(count(&http_requests, "GET", "/health", "200"), 1);
        assert_eq!(count(&http_requests, "GET", "/health", "503"), 0);
    }

    // Drive a future shaped like `track_metrics` — guard created, then parked at an await — and DROP
    // it mid-await, reproducing a TimeoutLayer expiry / client abort. Proves the guard is part of the
    // future state so its Drop runs on cancellation: inflight returns to 0, one 503 counted, the
    // duration histogram observed.
    #[test]
    fn real_future_drop_midawait_balances_inflight() {
        use std::future::pending;
        use std::task::{Context, Poll, Waker};

        let inflight = Gauge::default();
        let http_requests = Family::<Labels, Counter>::default();
        let duration_family = Family::<Labels, Histogram>::new_with_constructor(|| {
            Histogram::new(DURATION_BUCKETS.iter().copied())
        });
        let ig = inflight.clone();
        let hr = http_requests.clone();
        let df = duration_family.clone();

        let fut = async move {
            // Mirror RequestGuard::new + track_metrics body: inc, build guard, then await.
            ig.inc();
            let duration = df
                .get_or_create(&vec![
                    ("method", "POST".to_owned()),
                    ("route", "/c".to_owned()),
                ])
                .clone();
            let _guard = RequestGuard {
                start: Instant::now(),
                inflight: ig.clone(),
                http_requests: hr.clone(),
                duration,
                method: "POST".to_owned(),
                route: "/c".to_owned(),
                status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                completed: false,
            };
            pending::<()>().await; // never resolves — stands in for next.run(req).await
        };

        // Box::pin gives an OWNED pinned future, so `drop(fut)` actually drops the state machine
        // (a bare `pin!` would shadow with a `Pin<&mut _>` and drop only the borrow).
        let mut fut = Box::pin(fut);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        // Poll once: past guard creation, now parked at the await.
        assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(inflight.get(), 1, "guard live: inflight incremented");

        // The cancellation: drop the future while it is suspended at the await.
        drop(fut);

        assert_eq!(
            inflight.get(),
            0,
            "dropped-mid-await future returns inflight to 0 (no leak)"
        );
        assert_eq!(
            count(&http_requests, "POST", "/c", "503"),
            1,
            "cancelled request counted exactly once as 503"
        );
        // Histogram observed on cancel: encode the family and read the series `_count`.
        let mut reg = Registry::default();
        reg.register("d", "d", duration_family.clone());
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &reg).unwrap();
        let count_line = out
            .lines()
            .find(|l| l.starts_with("d_count{") && l.contains("method=\"POST\""))
            .expect("histogram count series present");
        assert!(
            count_line.ends_with(" 1"),
            "duration histogram observed exactly once on cancel, got: {count_line}"
        );
    }
}
