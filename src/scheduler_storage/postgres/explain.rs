//! `SIM_SQL_EXPLAIN` plumbing shared by the Postgres test harness, the reshuffle-sim tool, and the
//! production [`super::debug::with_explain`] wrapper. Testcontainers-free — env checks and config
//! strings only — so it lives in the library API rather than test-only code.
//!
//! When set, the container loads `auto_explain` (see [`SESSION_SETTINGS`]) and is left running after
//! the run so its log survives for `docker logs`. The module is loaded but off; a query opts in within
//! its transaction:
//!
//! ```sql
//! SET LOCAL auto_explain.log_analyze      = on;   -- ANALYZE timings
//! SET LOCAL auto_explain.log_buffers      = on;
//! SET LOCAL auto_explain.log_min_duration = 0;    -- log this statement's plan
//! ```
//!
//! [`super::debug::with_explain`] does exactly this for the queries it wraps.

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
