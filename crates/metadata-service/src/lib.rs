//! HTTP service over `scheduler_metadata::PgIngest`: bearer-scoped ingestion write path plus
//! `/health` and `/metrics`. `main.rs` stays thin so integration tests mount `build_router`
//! in-process.

pub mod config;

mod auth;
mod dto;
mod error;
mod handlers;
mod metrics;

use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::StatusCode,
    middleware,
    routing::{get, post, put},
};
use scheduler_metadata::PgIngest;
use tower_http::timeout::TimeoutLayer;

pub use auth::{Role, TokenStore};
pub use config::{Args, IngesterConfig, TokensConfig};
pub use error::ServiceError;

use metrics::Metrics;

/// Shared, cheaply-cloned request state. Fields are private; the test harness builds it via
/// [`AppState::new`]. Only one DB pool exists — `PgIngest`'s — and `/health` probes it via
/// [`PgIngest::ping`], so there is no second pool here.
#[derive(Clone)]
pub struct AppState {
    ingest: PgIngest,
    // Swappable so tokens can be reloaded (e.g. on SIGHUP) without a restart, revoking a leaked
    // token in place. Clones share the same store.
    tokens: Arc<RwLock<Arc<TokenStore>>>,
    metrics: Arc<Metrics>,
}

impl AppState {
    #[must_use]
    pub fn new(ingest: PgIngest, tokens: TokenStore) -> Self {
        Self {
            ingest,
            tokens: Arc::new(RwLock::new(Arc::new(tokens))),
            metrics: Arc::new(Metrics::new()),
        }
    }

    /// Resolve a bearer token against the current store.
    pub(crate) fn authenticate(&self, token: &str) -> Option<auth::Principal> {
        let store = self
            .tokens
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        store.authenticate(token)
    }

    /// Swap in a freshly loaded token store (in-place revocation/rotation).
    pub fn replace_tokens(&self, tokens: TokenStore) {
        *self.tokens.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(tokens);
    }

    /// Sample the pool into the gauges every `period` (non-zero — `interval` panics on zero). Reads
    /// atomics only, so it never holds a connection; the sole await is the tick, so `abort()` stops it
    /// cleanly. Spawn from `main`, not `build_router`, so in-process tests get no stray task.
    #[must_use]
    pub fn spawn_pool_sampler(&self, period: Duration) -> tokio::task::JoinHandle<()> {
        let ingest = self.ingest.clone();
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let s = ingest.pool_stats();
                metrics.set_pool_stats(s.size, s.idle);
            }
        })
    }
}

/// Routes + auth + metrics. Each auth layer guards its whole subtree, so a new route under it is
/// authenticated by construction. Path params use axum-0.8 `{…}` syntax.
pub fn build_router(state: AppState, max_body_bytes: usize, request_timeout: Duration) -> Router {
    let dataset_scoped = Router::new()
        .route(
            "/datasets/{name}/write-schemas",
            post(handlers::post_write_schema).get(handlers::get_write_schemas),
        )
        .route(
            "/datasets/{name}/write-schemas/{id}",
            get(handlers::get_write_schema),
        )
        .route(
            "/datasets/{name}/read-schema",
            get(handlers::get_read_schema),
        )
        .route("/datasets/{name}/chunks", post(handlers::post_chunks))
        .route(
            "/datasets/{name}/corrections",
            post(handlers::post_corrections),
        )
        .route("/datasets/{name}/head", get(handlers::get_head))
        .route(
            "/datasets/{name}/chunks/{id}/status",
            get(handlers::get_chunk_status),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_dataset_scope,
        ));

    // Admin-only: dataset creation and read-schema promotion. read-schema PUT is admin because a
    // read schema governs every dataset's decode contract, not one ingester's writes.
    let admin = Router::new()
        .route("/datasets", post(handlers::create_dataset))
        .route(
            "/datasets/{name}/read-schema",
            put(handlers::put_read_schema),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_admin,
        ));

    Router::new()
        .merge(dataset_scoped)
        .merge(admin)
        .route("/health", get(handlers::health))
        .route("/metrics", get(handlers::metrics))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        // route_layer populates MatchedPath before the middleware runs, and skips 404s.
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            metrics::track_metrics,
        ))
        // Outermost, so it bounds the whole request; on expiry it returns 503 and drops the handler
        // future, releasing its pooled connection. 503 (retryable), not 408 (a client-side stall).
        .layer(TimeoutLayer::with_status_code(
            StatusCode::SERVICE_UNAVAILABLE,
            request_timeout,
        ))
        .with_state(state)
}
