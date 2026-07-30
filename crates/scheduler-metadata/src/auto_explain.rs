//! `auto_explain` support, gated on `SIM_SQL_EXPLAIN`: the container-side settings the harness
//! passes at startup, and [`with_explain`], which opts a query in so its plan reaches the postgres
//! log (`docker logs`).
//!
//! Here rather than in `network-scheduler` so the harness can use it without depending on the
//! scheduler.

use sqlx::postgres::PgConnection;

/// Whether `SIM_SQL_EXPLAIN` is set.
pub fn enabled() -> bool {
    std::env::var_os("SIM_SQL_EXPLAIN").is_some()
}

/// `auto_explain` settings to pass as `-c <setting>` at container start: load the module for every
/// session but keep it off (`log_min_duration=-1`). `log_analyze` is left off globally on purpose — it
/// instruments every statement, logged or not, and would skew the sim's timings.
pub const SESSION_SETTINGS: &[&str] = &[
    "session_preload_libraries=auto_explain",
    "auto_explain.log_min_duration=-1",
    "auto_explain.log_nested_statements=on",
    "auto_explain.log_format=text",
];

/// Notice for a container left running so its plans survive — how to read and remove it.
pub fn left_running_notice(container_id: &str) -> String {
    format!(
        "SIM_SQL_EXPLAIN: postgres container {id} left running.\n  \
         plans:  docker logs {id} 2>&1 | grep -A40 'plan:'\n  \
         remove: docker rm -f {id}",
        id = container_id,
    )
}

/// Run `run` with `auto_explain` on, so the plans of the queries it issues — with ANALYZE timings —
/// reach the postgres log without each call site writing the `SET`/`RESET`. Needs `auto_explain`
/// loaded server-side, which the harness does. The `RESET` runs even if `run` fails, so it cannot
/// leak onto later queries. Untouched when `SIM_SQL_EXPLAIN` is unset.
pub async fn with_explain<R>(
    conn: &mut PgConnection,
    run: impl AsyncFnOnce(&mut PgConnection) -> sqlx::Result<R>,
) -> sqlx::Result<R> {
    if !enabled() {
        return run(conn).await;
    }
    sqlx::raw_sql("SET auto_explain.log_analyze = on; SET auto_explain.log_min_duration = 0")
        .execute(&mut *conn)
        .await?;
    let result = run(&mut *conn).await;
    let _ = sqlx::raw_sql("RESET auto_explain.log_min_duration; RESET auto_explain.log_analyze")
        .execute(&mut *conn)
        .await;
    result
}
