//! The scheduling task: the leader connection (advisory lock, migrations, leadership claim — it
//! reports the [`Epoch`] its sibling tasks connect with through its readiness channel). Each tick
//! runs one scheduling cycle producing the worker assignment, then regenerates the schema bundle
//! (every tick, shortage or not), then worker GC (after the cycle, so phase A's state-based stale
//! cleanup precedes worker-row deletion), then
//! the visibility cycle, whose promotion and stale-drain act on the current confirmation
//! watermark. This connection owns every UPDATE/DELETE of existing `sched_*` rows (see the
//! ownership map in `scheduler_storage::postgres`), which is why worker GC lives here and not
//! with the worker-status task. Everything is only computed and logged; publishing is not wired
//! yet (docs/README.md).

use std::{sync::Arc, time::Duration};

use tokio::{
    runtime::Handle,
    sync::{oneshot, watch},
};
use tokio_util::sync::CancellationToken;

use crate::{
    cli, metrics,
    scheduler_storage::{
        AssignmentId, SchedulerStorage,
        algorithm::MultistepAlgorithm,
        postgres::{Epoch, PostgresStorage},
    },
};

use super::super::now_ticks;
use super::super::ops::Heartbeat;
use super::{retry_or_die, wait_for_next_tick};

pub struct SchedulingTask {
    pub database_url: cli::Secret,
    pub claim_lock_timeout: Duration,
    pub batch_size: usize,
    pub algorithm: MultistepAlgorithm<cli::DatasetsConfig>,
    pub config: crate::multistep_scheduler::SchedulingConfig,
    pub period: Duration,
    pub drain_window_ticks: u64,
    pub worker_gc_ticks: u64,
    pub latest_published: watch::Sender<AssignmentId>,
    pub probe_interval: Duration,
    pub ping_timeout: Duration,
    pub heartbeat: Arc<Heartbeat>,
    /// Fired when the worker and chunk-discovery startup passes have seeded workers and chunks;
    /// the first cycle waits for it. Dropped unfired when startup fails.
    pub start: oneshot::Receiver<()>,
    pub handle: Handle,
    pub token: CancellationToken,
}

impl SchedulingTask {
    pub fn run(self, ready: oneshot::Sender<anyhow::Result<Epoch>>) -> anyhow::Result<()> {
        let Self {
            database_url,
            claim_lock_timeout,
            batch_size,
            algorithm,
            config,
            period,
            drain_window_ticks,
            worker_gc_ticks,
            latest_published,
            probe_interval,
            ping_timeout,
            heartbeat,
            start,
            handle,
            token,
        } = self;
        // No call-site context on connect: the storage layer already names the operation.
        let (mut storage, epoch) = match PostgresStorage::connect(
            database_url.expose_secret(),
            claim_lock_timeout,
            batch_size,
        ) {
            Ok(leader) => leader,
            Err(e) => {
                let _ = ready.send(Err(e.into()));
                return Ok(());
            }
        };
        crate::metrics::LEADERSHIP_EPOCH.set(epoch.get());
        let _ = ready.send(Ok(epoch));
        let proceed = handle.block_on(async {
            tokio::select! {
                biased;
                () = token.cancelled() => false,
                started = start => started.is_ok(),
            }
        });
        if !proceed {
            return Ok(());
        }
        loop {
            // Beat only once the whole tick landed: a task looping on retryable failures has to
            // look stalled, not healthy.
            let mut done = true;
            let cycle = {
                let _timer = metrics::Timer::new("multistep:schedule");
                storage.run_scheduling_cycle(&algorithm, &config, now_ticks(), drain_window_ticks)
            };
            match cycle {
                Ok(assignment) => {
                    // STUB: publishing serializes/uploads the assignment here once the upload
                    // path exists (see docs/README.md).
                    tracing::info!(
                        assignment_id = %assignment.id,
                        chunks_placed = assignment.chunk_workers.len(),
                        replication_by_weight = ?assignment.replication_by_weight,
                        "Multistep scheduling cycle done"
                    );
                    // Bounds the confirmation gate's trust in worker-echoed ids.
                    latest_published.send_replace(assignment.id);
                }
                Err(e) => {
                    retry_or_die(&mut storage, ping_timeout, e, "scheduling cycle")?;
                    done = false;
                }
            }
            // Unconditional, including after a Shortage: the write section stays frozen with the
            // last successful assignment, but a read-schema promoted since then has to reach the
            // very next bundle rather than wait for placement to recover.
            // STUB: publishing the bundle goes here (docs/README.md).
            match storage.generate_schema_bundle() {
                Ok(bundle) => tracing::info!(
                    schema_bundle_id = %bundle.id(),
                    write_schemas = bundle.schemas().len(),
                    read_schemas = bundle.read_schemas().len(),
                    "Schema bundle generated"
                ),
                Err(e) => {
                    retry_or_die(&mut storage, ping_timeout, e, "generate schema bundle")?;
                    done = false;
                }
            }
            // After the cycle: its phase-A cleanup has already settled the departed workers'
            // mapping state, so deleting long-inactive worker rows here never races it.
            if let Err(e) = storage.gc_inactive_workers(now_ticks(), worker_gc_ticks) {
                retry_or_die(&mut storage, ping_timeout, e, "gc inactive workers")?;
                done = false;
            }
            // Portal projection: promotion and stale-drain act on the confirmation watermark the
            // worker-status task advances between our ticks. Runs regardless of the cycle
            // outcome — a rolled-back cycle doesn't preclude promoting already-confirmed chunks.
            // STUB: publishing the promoted portal assignment goes here (docs/README.md).
            match storage.run_visibility_cycle(now_ticks()) {
                Ok(portal) => tracing::info!(
                    portal_assignment_id = portal.id,
                    portal_chunks = portal.chunks.len(),
                    "Visibility cycle done"
                ),
                Err(e) => {
                    retry_or_die(&mut storage, ping_timeout, e, "visibility cycle")?;
                    done = false;
                }
            }
            if done {
                heartbeat.beat();
            }
            if wait_for_next_tick(&handle, &token, period, probe_interval, || {
                Ok(storage.ping(ping_timeout)?)
            })?
            .is_break()
            {
                return Ok(());
            }
        }
    }
}
