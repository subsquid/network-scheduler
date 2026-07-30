//! The worker-status task: refreshes the worker set from ClickHouse and drives the confirmation
//! gate on the same ping snapshot — quorum over the assignment ids workers echo in their pings
//! (`last_applied_assignment_id`) advances the confirmation watermark. The quorum denominator is
//! the snapshot's online workers.

use std::{sync::Arc, time::Duration};

use anyhow::Context;
use tokio::{
    runtime::Handle,
    sync::{oneshot, watch},
};
use tokio_util::sync::CancellationToken;

use crate::{
    cli,
    clickhouse::{ClickhouseClient, WorkerPing},
    scheduler_storage::{
        AssignmentId, SchedulerStorage,
        postgres::{Epoch, PostgresStorage, SessionMemory},
    },
    types::WorkerStatus,
};

use super::super::now_ticks;
use super::super::ops::Heartbeat;
use super::{retry_or_die, wait_for_next_tick};

pub struct WorkerStatusTask {
    pub database_url: cli::Secret,
    /// The scheduling task's epoch: our writes stop the moment a newer leader claims one.
    pub epoch: Epoch,
    pub batch_size: usize,
    pub clickhouse: ClickhouseClient,
    pub config: Arc<cli::Config>,
    pub period: Duration,
    pub quorum_pct: u8,
    pub latest_published: watch::Receiver<AssignmentId>,
    pub probe_interval: Duration,
    pub ping_timeout: Duration,
    pub heartbeat: Arc<Heartbeat>,
    pub handle: Handle,
    pub token: CancellationToken,
}

impl WorkerStatusTask {
    pub fn run(self, ready: oneshot::Sender<anyhow::Result<()>>) -> anyhow::Result<()> {
        // The startup pass runs before reporting ready: the first scheduling cycle waits on it.
        let started = self.startup().and_then(|(mut storage, mut gate)| {
            self.tick(&mut storage, &mut gate)?;
            Ok((storage, gate))
        });
        let (mut storage, mut gate) = match started {
            Ok(started) => started,
            Err(e) => {
                let _ = ready.send(Err(e));
                return Ok(());
            }
        };
        let _ = ready.send(Ok(()));
        while wait_for_next_tick(
            &self.handle,
            &self.token,
            self.period,
            self.probe_interval,
            || Ok(storage.ping(self.ping_timeout)?),
        )?
        .is_continue()
        {
            self.tick(&mut storage, &mut gate)?;
            // Liveness: the pass returned rather than wedging, whatever its outcome. A ClickHouse
            // outage leaves `task_last_success` behind without asking for a restart.
            self.heartbeat.still_running();
        }
        Ok(())
    }

    fn startup(&self) -> anyhow::Result<(PostgresStorage, ConfirmationGate)> {
        // Worker upserts and the watermark confirm are small statements — no raised session
        // memory needed.
        let mut storage = PostgresStorage::connect_follower(
            self.database_url.expose_secret(),
            SessionMemory::ServerDefault,
            self.epoch,
            self.batch_size,
        )?;
        let confirmed = storage
            .worker_confirmation_watermark()
            .context("seed confirmation gate")?;
        Ok((
            storage,
            ConfirmationGate::new(self.quorum_pct, self.latest_published.clone(), confirmed),
        ))
    }

    /// One confirmation-gate pass plus a worker-set refresh over the same ping snapshot.
    fn tick(
        &self,
        storage: &mut PostgresStorage,
        gate: &mut ConfirmationGate,
    ) -> anyhow::Result<()> {
        let Some(pings) = self.handle.block_on(load_worker_pings(
            &self.clickhouse,
            &self.config,
            &self.token,
        )) else {
            return Ok(());
        };
        let applied: Vec<_> = pings
            .iter()
            .filter(|ping| ping.worker.status == WorkerStatus::Online)
            .map(|ping| parse_applied_id(ping.last_applied_assignment_id.as_deref()))
            .collect();
        let workers: Vec<_> = pings.into_iter().map(|ping| ping.worker).collect();
        // Both operations write only tables this connection owns (the confirmation tables and
        // `sched_workers` status columns), so neither waits on a concurrent scheduling cycle.
        gate.advance(storage, self.ping_timeout, &applied)?;
        match storage.update_worker_set(&workers, now_ticks()) {
            Ok(()) => {
                tracing::info!(workers = workers.len(), "Worker set updated");
                self.heartbeat.beat();
            }
            Err(e) => retry_or_die(storage, self.ping_timeout, e, "update worker set")?,
        }
        Ok(())
    }
}

/// `None` on shutdown or a (logged) ClickHouse error. The query has no client-side timeout, so it
/// is raced against shutdown; storage operations never are — once started they run to completion.
async fn load_worker_pings(
    clickhouse: &ClickhouseClient,
    config: &cli::Config,
    token: &CancellationToken,
) -> Option<Vec<WorkerPing>> {
    let result = tokio::select! {
        () = token.cancelled() => return None,
        result = clickhouse.active_worker_pings(config) => result,
    };
    match result {
        Ok(pings) => Some(pings),
        Err(e) => {
            tracing::error!(
                error = format!("{e:#}"),
                "Failed to load active workers, retrying next tick"
            );
            None
        }
    }
}

/// Confirmation state for one service run.
struct ConfirmationGate {
    quorum_pct: u8,
    /// Newest assignment id this run's scheduling task produced; 0 until its first cycle.
    latest_published: watch::Receiver<AssignmentId>,
    confirmed: AssignmentId,
}

impl ConfirmationGate {
    /// Seed `confirmed` with the storage's watermark, so a restart doesn't re-confirm.
    fn new(
        quorum_pct: u8,
        latest_published: watch::Receiver<AssignmentId>,
        confirmed: AssignmentId,
    ) -> Self {
        Self {
            quorum_pct,
            latest_published,
            confirmed,
        }
    }

    /// One gate pass over a ping snapshot: a new watermark with quorum is confirmed. The
    /// scheduling task picks the advanced watermark up in its next visibility cycle.
    fn advance(
        &mut self,
        storage: &mut PostgresStorage,
        ping_timeout: Duration,
        applied: &[Option<AssignmentId>],
    ) -> anyhow::Result<()> {
        let latest = *self.latest_published.borrow();
        let Some(watermark) = quorum_watermark(self.quorum_pct, latest, applied) else {
            return Ok(());
        };
        if watermark <= self.confirmed {
            return Ok(());
        }
        match storage.confirm_worker_assignment(watermark, now_ticks()) {
            Ok(()) => {
                self.confirmed = watermark;
                tracing::info!(watermark, "Confirmation watermark advanced");
            }
            Err(e) => {
                retry_or_die(
                    storage,
                    ping_timeout,
                    e,
                    &format!("confirm watermark {watermark}"),
                )?;
            }
        }
        Ok(())
    }
}

/// Highest assignment id that at least `quorum_pct`% of online workers have applied, or `None`
/// when no id has quorum.
///
/// `applied`: one entry per online worker, `None` when it echoed nothing usable. Ids above
/// `latest_published` are not ours and count as not-confirmed. `quorum_pct == 0` skips the vote
/// and returns `latest_published` (`None` while it is still 0).
fn quorum_watermark(
    quorum_pct: u8,
    latest_published: AssignmentId,
    applied: &[Option<AssignmentId>],
) -> Option<AssignmentId> {
    if quorum_pct == 0 {
        return (latest_published > 0).then_some(latest_published);
    }
    if applied.is_empty() {
        return None;
    }
    let quorum = usize::max(1, (applied.len() * quorum_pct as usize).div_ceil(100));
    let mut ids: Vec<AssignmentId> = applied
        .iter()
        .flatten()
        .copied()
        .filter(|&id| id > 0 && id <= latest_published)
        .collect();
    if ids.len() < quorum {
        return None;
    }
    ids.sort_unstable_by(|a, b| b.cmp(a));
    // The quorum-th largest id is the highest one at least `quorum` workers have reached.
    Some(ids[quorum - 1])
}

/// Worker-echoed assignment id in this scheduler's id space: the numeric storage id it was
/// published under. Non-numeric echoes (e.g. the legacy `20241008T141245_…` format) are not ours.
fn parse_applied_id(echoed: Option<&str>) -> Option<AssignmentId> {
    echoed?.parse().ok()
}

#[cfg(test)]
mod tests {
    /// Generous: these tests care about the gate's verdict, not the bound.
    const PING_TIMEOUT: Duration = Duration::from_secs(5);

    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::scheduler_storage::postgres::{DEFAULT_BATCH_SIZE, DEFAULT_CLAIM_LOCK_TIMEOUT};
    use crate::scheduler_storage::test_harness::pg_harness::fresh_db_url;
    use crate::scheduler_storage::test_harness::utils::StaticSchedulingAlgorithm;

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    /// The stateful gate against real storage: quorum persists the watermark, an unchanged
    /// quorum is a no-op, and a gate re-seeded from storage (a service restart) does not
    /// re-confirm.
    #[test]
    fn gate_confirms_persists_and_reseeds() {
        let (url, _db) = fresh_db_url("worker_status", TEST_ID.fetch_add(1, Ordering::Relaxed));
        let (mut storage, _) =
            PostgresStorage::connect(&url, DEFAULT_CLAIM_LOCK_TIMEOUT, DEFAULT_BATCH_SIZE)
                .expect("connect leader");
        let algorithm = StaticSchedulingAlgorithm {
            mapping: Vec::new(),
        };
        let assignment = storage
            .run_scheduling_cycle(&algorithm, &(), 100, 60)
            .expect("mint an assignment");

        let (latest_tx, latest_rx) = watch::channel(0);
        latest_tx.send_replace(assignment.id);
        let applied = vec![Some(assignment.id); 3];

        let mut gate = ConfirmationGate::new(90, latest_rx.clone(), 0);
        gate.advance(&mut storage, PING_TIMEOUT, &applied)
            .expect("advance");
        assert_eq!(
            storage.worker_confirmation_watermark().expect("watermark"),
            assignment.id,
            "quorum must persist the watermark"
        );

        gate.advance(&mut storage, PING_TIMEOUT, &applied)
            .expect("repeat advance is a no-op");
        assert_eq!(gate.confirmed, assignment.id);

        let seeded = storage.worker_confirmation_watermark().expect("watermark");
        let mut reseeded = ConfirmationGate::new(90, latest_rx, seeded);
        reseeded
            .advance(&mut storage, PING_TIMEOUT, &applied)
            .expect("advance on a re-seeded gate");
        assert_eq!(
            reseeded.confirmed, assignment.id,
            "a restart must not re-confirm"
        );
    }

    #[test]
    fn zero_quorum_jumps_to_latest_published() {
        assert_eq!(quorum_watermark(0, 7, &[]), Some(7));
        assert_eq!(quorum_watermark(0, 0, &[]), None);
    }

    #[test]
    fn no_online_workers_holds_the_watermark() {
        assert_eq!(quorum_watermark(90, 7, &[]), None);
    }

    #[test]
    fn nothing_echoed_holds_the_watermark() {
        assert_eq!(quorum_watermark(90, 7, &[None, None, None]), None);
    }

    #[test]
    fn full_agreement_confirms_the_echoed_id() {
        let applied = [Some(5), Some(5), Some(5)];
        assert_eq!(quorum_watermark(90, 7, &applied), Some(5));
    }

    #[test]
    fn watermark_is_the_quorum_th_largest_id() {
        // 10 workers at 90% -> the 9th largest applied id carries the quorum.
        let applied: Vec<_> = (1..=10).map(Some).collect();
        assert_eq!(quorum_watermark(90, 10, &applied), Some(2));
        // At 50%, the 5th largest.
        assert_eq!(quorum_watermark(50, 10, &applied), Some(6));
    }

    #[test]
    fn laggards_within_tolerance_do_not_stall() {
        // 9 of 10 on id 5, one silent: 90% quorum is met without the straggler.
        let mut applied = vec![Some(5); 9];
        applied.push(None);
        assert_eq!(quorum_watermark(90, 7, &applied), Some(5));
        // Two silent of 10 exceed the 10% tolerance.
        let mut applied = vec![Some(5); 8];
        applied.extend([None, None]);
        assert_eq!(quorum_watermark(90, 7, &applied), None);
    }

    #[test]
    fn ids_beyond_latest_published_are_not_trusted() {
        let applied = [Some(9), Some(9), Some(9)];
        assert_eq!(quorum_watermark(90, 7, &applied), None);
        // Mixed: only the in-range echo counts, and alone it is below a 100% quorum of 3.
        let applied = [Some(9), Some(9), Some(5)];
        assert_eq!(quorum_watermark(100, 7, &applied), None);
    }

    #[test]
    fn single_worker_fleet_confirms_alone() {
        assert_eq!(quorum_watermark(90, 7, &[Some(3)]), Some(3));
    }

    #[test]
    fn parse_rejects_legacy_and_garbage_ids() {
        assert_eq!(parse_applied_id(Some("42")), Some(42));
        assert_eq!(parse_applied_id(Some("20241008T141245_242da92f7d6c")), None);
        assert_eq!(parse_applied_id(Some("")), None);
        assert_eq!(parse_applied_id(None), None);
    }
}
