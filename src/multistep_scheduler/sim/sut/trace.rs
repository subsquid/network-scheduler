//! Debug tracing (the `trace` field / `SIM_TRACE`): per-operation worker load and holder
//! mappings. Purely observational — nothing here touches state or assertions.

use std::collections::{BTreeSet, HashMap};

use bytesize::ByteSize;

use super::super::utils::CHUNK_SIZE;
use super::{Action, SimStorage, SimUnderTest};
use crate::scheduler_storage::WorkerPk;
use crate::scheduler_storage::test_harness::inspect::Snapshot;

impl<D: SimStorage> SimUnderTest<D> {
    pub(super) fn is_tracing(&self) -> bool {
        self.trace
    }

    /// Config banner, printed once at the start of a run.
    pub(super) fn trace_config(&self) {
        if !self.is_tracing() {
            return;
        }
        println!(
            "\n════ config: workers={} (pool {}) worker_capacity={} chunk_size={} \
             min_replication={} saturation={:.2} ════",
            self.active_workers_count(),
            self.workers.len(),
            ByteSize::b(self.config.worker_capacity),
            ByteSize::b(u64::from(CHUNK_SIZE)),
            self.config.min_replication,
            self.config.saturation,
        );
    }

    pub(super) fn trace_converge_header(&self) {
        if self.is_tracing() {
            println!(
                "\n══ CheckConverged  (chunks={}) ══",
                self.total_chunk_count()
            );
        }
    }

    /// Names the op so even no-output ones stay visible; state follows in `trace_step`.
    pub(super) fn trace_operation(&self, operation: &str) {
        if self.is_tracing() {
            println!("\n▶ op: {operation}");
        }
    }

    /// Announces only the first `NoOp`: the `noop_traced` latch suppresses the idle tail the
    /// model emits after converging at saturation.
    pub(super) fn trace_noop(&mut self) {
        if self.is_tracing() && !self.noop_traced {
            self.noop_traced = true;
            println!("\n▶ op: NoOp (idle — further NoOps suppressed)");
        }
    }

    /// A generation-time decision the model made, emitted from `transition`.
    pub(super) fn trace_decision(&self, decision: &str) {
        if self.is_tracing() {
            println!("  · model: {decision}");
        }
    }

    /// Numbered block of the current state, labelled `operation`. Called with `"before"` ahead
    /// of an op and the outcome label after it, so each op shows a before→after pair.
    pub(super) fn trace_step(&mut self, operation: &str) {
        if !self.is_tracing() {
            return;
        }
        let snapshot = self.storage.snapshot();
        self.step += 1;
        let step = self.step;
        println!(
            "\n── [{step}] {operation}   chunks {}   sched {:?} ──",
            snapshot.len(),
            self.schedule_status,
        );
        self.print_load_table(&snapshot);
        self.print_holder_blocks(&snapshot);
    }

    /// `chunk → holders` inverted to per-worker chunk lists, over the whole pool (a departed
    /// worker still gets an empty row).
    fn chunks_by_worker(
        &self,
        holders_per_chunk: &[BTreeSet<WorkerPk>],
        peer_ids: &HashMap<WorkerPk, String>,
    ) -> Vec<Vec<usize>> {
        let mut chunks_per_worker: Vec<Vec<usize>> = vec![Vec::new(); self.workers.len()];
        for (chunk, holders) in holders_per_chunk.iter().enumerate() {
            for worker in holders {
                if let Some(worker_idx) =
                    peer_ids.get(worker).and_then(|peer| self.pool_index(peer))
                {
                    chunks_per_worker[worker_idx].push(chunk);
                }
            }
        }
        chunks_per_worker
    }

    /// One `Wn: C.. C..` line per worker (`-` if it holds none), in worker order.
    fn lines_by_worker(
        &self,
        holders_per_chunk: &[BTreeSet<WorkerPk>],
        peer_ids: &HashMap<WorkerPk, String>,
    ) -> Vec<String> {
        self.chunks_by_worker(holders_per_chunk, peer_ids)
            .iter()
            .enumerate()
            .map(|(worker_idx, chunks)| {
                let list = if !self.is_worker_active(worker_idx) {
                    "offline — data gone".to_string()
                } else if chunks.is_empty() {
                    "-".to_string()
                } else {
                    chunks
                        .iter()
                        .map(|chunk| format!("C{chunk}"))
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                format!("  W{worker_idx}: {list}")
            })
            .collect()
    }

    fn bytes_by_worker(
        &self,
        snapshot: &Snapshot,
        holders_per_chunk: &[BTreeSet<WorkerPk>],
    ) -> Vec<u64> {
        let mut bytes = vec![0u64; self.workers.len()];
        for (chunk, holders) in holders_per_chunk.iter().enumerate() {
            let size = u64::from(snapshot.chunk_sizes[chunk]);
            for worker in holders {
                if let Some(worker_idx) = snapshot
                    .peer_ids
                    .get(worker)
                    .and_then(|peer| self.pool_index(peer))
                {
                    bytes[worker_idx] += size;
                }
            }
        }
        bytes
    }

    /// Per-worker load table: ideal / stale / total bytes against capacity.
    fn print_load_table(&self, snapshot: &Snapshot) {
        let ideal_bytes = self.bytes_by_worker(snapshot, &snapshot.ideal);
        let stale_bytes = self.bytes_by_worker(snapshot, &snapshot.stale);
        let capacity = self.config.worker_capacity;
        let percent = |bytes: u64| bytes as f64 / capacity as f64 * 100.0;
        let cell = |bytes: u64| format!("{} ({:.0}%)", ByteSize::b(bytes), percent(bytes));
        // Only active workers contribute fleet capacity.
        let fleet_capacity = capacity * self.active_workers_count() as u64;
        println!(
            "load (capacity {}/worker, {} fleet over {} active workers):",
            ByteSize::b(capacity),
            ByteSize::b(fleet_capacity),
            self.active_workers_count(),
        );
        println!("{:7}{:>16}{:>16}{:>8}", "", "ideal", "stale", "total");
        for worker in 0..self.workers.len() {
            let (ideal, stale) = (ideal_bytes[worker], stale_bytes[worker]);
            let tag = if self.is_worker_active(worker) {
                ""
            } else {
                "  offline — data gone"
            };
            println!(
                "  W{:<3} {:>16}{:>16}{:>8}{tag}",
                worker,
                cell(ideal),
                cell(stale),
                format!("{:.0}%", percent(ideal + stale)),
            );
        }
        let total_ideal: u64 = ideal_bytes.iter().sum();
        let total_stale: u64 = stale_bytes.iter().sum();
        let fleet_pct = |bytes: u64| bytes as f64 / fleet_capacity as f64 * 100.0;
        let fleet_cell = |bytes: u64| format!("{} ({:.0}%)", ByteSize::b(bytes), fleet_pct(bytes));
        println!(
            "  {:<5}{:>16}{:>16}{:>8}",
            "all",
            fleet_cell(total_ideal),
            fleet_cell(total_stale),
            format!("{:.0}%", fleet_pct(total_ideal + total_stale)),
        );
    }

    fn print_holder_blocks(&self, snapshot: &Snapshot) {
        for (label, holders_per_chunk) in [("ideal", &snapshot.ideal), ("stale", &snapshot.stale)] {
            println!("{label}:");
            for line in self.lines_by_worker(holders_per_chunk, &snapshot.peer_ids) {
                println!("{line}");
            }
        }
    }

    /// Dump the converged placement when the floor oracle is about to fail on `chunk`. Prints
    /// regardless of `SIM_TRACE`, so a captured regression explains itself without a re-run.
    pub(super) fn dump_floor_failure(&self, snapshot: &Snapshot, chunk: usize) {
        let capacity = self.config.worker_capacity;
        let size = u64::from(snapshot.chunk_sizes[chunk]);
        let load = snapshot.byte_load(&snapshot.ideal);
        let pk_of_peer: HashMap<&str, WorkerPk> = snapshot
            .peer_ids
            .iter()
            .map(|(pk, id)| (id.as_str(), *pk))
            .collect();

        println!(
            "\n════ convergence oracle FAILED — chunk C{chunk} starved ({}/{} copies, size {}; \
             worker capacity {}) ════",
            snapshot.ideal[chunk].len(),
            self.config.min_replication,
            ByteSize::b(size),
            ByteSize::b(capacity),
        );
        println!(
            "per-worker load and room for C{chunk} (a copy needs {} free):",
            ByteSize::b(size)
        );
        for (index, pooled) in self.workers.iter().enumerate() {
            let wpk = pk_of_peer
                .get(pooled.worker.id.to_string().as_str())
                .copied();
            let used = wpk.and_then(|w| load.get(&w).copied()).unwrap_or(0);
            let note = if !pooled.active {
                "offline — data gone".to_string()
            } else if wpk.is_some_and(|w| snapshot.ideal[chunk].contains(&w)) {
                format!("holds C{chunk}")
            } else if used + size <= capacity {
                "← has room".to_string()
            } else {
                String::new()
            };
            println!(
                "  W{index}: {} / {} used (free {})  {note}",
                ByteSize::b(used),
                ByteSize::b(capacity),
                ByteSize::b(capacity.saturating_sub(used)),
            );
        }

        let sizes = (0..snapshot.len())
            .map(|c| format!("C{c}={}", ByteSize::b(u64::from(snapshot.chunk_sizes[c]))))
            .collect::<Vec<_>>()
            .join("  ");
        println!("chunk sizes: {sizes}");
        println!("converged placement:");
        for line in self.lines_by_worker(&snapshot.ideal, &snapshot.peer_ids) {
            println!("{line}");
        }
    }
}

/// Render chunk indices as `[C3 C7 …]`, summarising once there are too many to list.
pub(super) fn format_chunk_list(chunks: &[usize]) -> String {
    const MAX_SHOWN: usize = 12;
    if chunks.len() <= MAX_SHOWN {
        let ids = chunks
            .iter()
            .map(|chunk| format!("C{chunk}"))
            .collect::<Vec<_>>()
            .join(" ");
        format!("[{ids}]")
    } else {
        format!("[{} chunks]", chunks.len())
    }
}

/// One-line label for an action, for the trace banner.
pub(super) fn action_label(action: &Action) -> String {
    match action {
        Action::AddChunks(new) => format!("Add({} chunks)", new.len()),
        Action::AdvanceClock(ticks) => format!("AdvanceClock(+{ticks})"),
        Action::CheckConverged(check) => format!("CheckConverged({check:?})"),
        Action::WorkerJoined(index) => format!("AddWorker(W{index})"),
        Action::WorkerLeft(index) => format!("RemoveWorker(W{index})"),
        Action::WorkerFetchAssignment { worker, succeeds } => {
            format!(
                "WorkerFetch(W{worker}, {})",
                if *succeeds { "ok" } else { "missed" }
            )
        }
        Action::PortalFetchAssignment { succeeds } => {
            format!("PortalFetch({})", if *succeeds { "ok" } else { "missed" })
        }
        Action::RegisterCorrection {
            old_dataset,
            old_chunk_id: old_key,
            replacement,
        } => format!(
            "RegisterCorrection({old_dataset}/{old_key} → {})",
            replacement.key
        ),
        Action::SetMinReplication(min_replication) => {
            format!("SetMinReplication({min_replication})")
        }
        Action::SetDatasetSchema { dataset, .. } => format!("SetDatasetSchema({dataset})"),
        Action::NoOp => "NoOp".to_string(),
    }
}
