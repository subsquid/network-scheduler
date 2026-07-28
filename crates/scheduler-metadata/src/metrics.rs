//! The `exec_times` metric family and the `PhaseTimer` that feeds it, used to instrument ingest SQL.

use std::sync::atomic::AtomicU64;

use prometheus_client::{
    metrics::{family::Family, gauge::Gauge},
    registry::{Registry, Unit},
};

type Labels = Vec<(&'static str, String)>;

lazy_static::lazy_static! {
    pub static ref EXEC_TIMES: Family<Labels, Gauge<f64, AtomicU64>> = Default::default();
}

pub fn register(registry: &mut Registry) {
    registry.register_with_unit(
        "exec_times",
        "Execution times of various procedures",
        Unit::Seconds,
        EXEC_TIMES.clone(),
    );
}

/// Times a Postgres storage phase. On drop emits a debug line on the `pg_timing` target
/// (`RUST_LOG=error,pg_timing=debug`) plus an `EXEC_TIMES` sample; the statement count separates an
/// N+1 storm (statements ≈ rows) from a single slow query.
pub struct PhaseTimer {
    start: std::time::Instant,
    name: &'static str,
    statements: u64,
    rows: u64,
}

impl PhaseTimer {
    pub fn new(name: &'static str) -> Self {
        Self {
            start: std::time::Instant::now(),
            name,
            statements: 0,
            rows: 0,
        }
    }

    /// Record one executed statement touching `rows` rows; call once per round-trip so a per-row
    /// loop surfaces its round-trip count.
    pub fn stmt(&mut self, rows: u64) {
        self.statements += 1;
        self.rows += rows;
    }

    /// Add to the row count without counting a statement, so a CPU phase reports `statements = 0`.
    pub fn items(&mut self, rows: u64) {
        self.rows += rows;
    }
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        EXEC_TIMES
            .get_or_create(&vec![("name", self.name.to_string())])
            .inc_by(elapsed.as_secs_f64());
        tracing::debug!(
            target: "pg_timing",
            phase = self.name,
            statements = self.statements,
            rows = self.rows,
            elapsed_ms = elapsed.as_millis() as u64,
            "pg phase complete"
        );
    }
}
