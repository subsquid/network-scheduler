//! Thin entrypoint: parse args, build the pool, verify the schema version (or migrate under
//! --migrate), load tokens, serve.

use std::str::FromStr;

use anyhow::Context;
use clap::Parser;
use secrecy::ExposeSecret;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use metadata_service::config::{Args, TokensConfig};
use metadata_service::{AppState, TokenStore, build_router};
use scheduler_metadata::PgIngest;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let pool = PgPoolOptions::new()
        .max_connections(args.max_connections)
        .acquire_timeout(args.acquire_timeout)
        .connect(args.database_url.expose_secret())
        .await
        .context("connect database pool")?;
    let ingest = PgIngest::new(pool);

    if args.migrate {
        tracing::warn!("applying migrations (dev-only --migrate)");
        ingest.migrate().await.context("migrate")?;
    }
    ingest
        .check_schema_version()
        .await
        .context("database schema mismatch; run migrations (or start with --migrate for dev)")?;

    let tokens = TokenStore::from_config(TokensConfig::load(&args.config)?)?;

    // Host + dbname for the startup log, parsed with the pool's own parser — never the DSN/password.
    let (db_host, db_name) = PgConnectOptions::from_str(args.database_url.expose_secret())
        .map(|o| {
            (
                o.get_host().to_owned(),
                o.get_database().unwrap_or("").to_owned(),
            )
        })
        .unwrap_or_else(|_| ("<unparsed>".to_owned(), String::new()));
    tracing::info!(
        bind = %args.bind,
        db_host = %db_host,
        db_name = %db_name,
        pool_max_connections = args.max_connections,
        acquire_timeout = ?args.acquire_timeout,
        max_body_size = %args.max_body_size,
        request_timeout = ?args.request_timeout,
        tokens = tokens.len(),
        migrate = args.migrate,
        "metadata-service configured"
    );

    let state = AppState::new(ingest, tokens);

    #[cfg(unix)]
    spawn_token_reloader(state.clone(), args.config.clone());

    // Sample the DB pool into gauges every 5s; stopped after graceful shutdown below.
    let pool_sampler = state.spawn_pool_sampler(std::time::Duration::from_secs(5));

    let listener = tokio::net::TcpListener::bind(args.bind)
        .await
        .with_context(|| format!("bind {}", args.bind))?;
    tracing::info!(bind = %args.bind, "metadata-service listening");
    let served = axum::serve(
        listener,
        build_router(
            state,
            args.max_body_size.as_u64() as usize,
            args.request_timeout,
        ),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await;
    pool_sampler.abort(); // stop the sampler on any exit path, not just the happy one
    served.context("serve")?;
    Ok(())
}

/// Reload the token store from disk on SIGHUP, so a leaked token can be revoked (or a new one added)
/// without a restart. A failed reload keeps the current tokens rather than dropping all auth.
#[cfg(unix)]
fn spawn_token_reloader(state: AppState, config_path: std::path::PathBuf) {
    use tokio::signal::unix::{SignalKind, signal};
    tokio::spawn(async move {
        let mut sighup = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "cannot install SIGHUP handler; token reload disabled");
                return;
            }
        };
        while sighup.recv().await.is_some() {
            match TokensConfig::load(&config_path).and_then(TokenStore::from_config) {
                Ok(tokens) => {
                    state.replace_tokens(tokens);
                    tracing::info!("reloaded tokens on SIGHUP");
                }
                Err(e) => {
                    tracing::error!(error = %format!("{e:#}"), "token reload failed; keeping current tokens");
                }
            }
        }
    });
}

/// Resolves on SIGTERM (k8s rolling deploy) or Ctrl-C, so in-flight requests drain instead of being
/// cut mid-transaction.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutdown signal received; draining in-flight requests");
}
