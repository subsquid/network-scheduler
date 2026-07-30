//! The chunk-discovery task: watermarks → S3 listing → insert + register.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use tokio::{runtime::Handle, sync::oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    cli,
    dataset_data_storage::S3Storage,
    scheduler_storage::postgres::{Epoch, PostgresStorage, SessionMemory},
    types::{Chunk, DatasetWatermark},
};

use super::super::ops::Heartbeat;
use super::super::{bootstrap_datasets, register_chunks};
use super::{retry_or_die, wait_for_next_tick};

pub struct ChunkDiscoveryTask {
    pub database_url: cli::Secret,
    /// The scheduling task's epoch: our writes stop the moment a newer leader claims one.
    pub epoch: Epoch,
    pub batch_size: usize,
    pub s3_storage: S3Storage,
    pub config: Arc<cli::Config>,
    pub period: Duration,
    pub probe_interval: Duration,
    pub ping_timeout: Duration,
    pub heartbeat: Arc<Heartbeat>,
    pub handle: Handle,
    pub token: CancellationToken,
}

impl ChunkDiscoveryTask {
    pub fn run(self, ready: oneshot::Sender<anyhow::Result<()>>) -> anyhow::Result<()> {
        // The startup pass runs before reporting ready: the first scheduling cycle waits on it.
        let started = self.startup().and_then(|mut storage| {
            self.tick(&mut storage)?;
            Ok(storage)
        });
        let mut storage = match started {
            Ok(storage) => storage,
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
            self.tick(&mut storage)?;
            // Liveness: the pass returned rather than wedging, whatever its outcome. An S3 outage
            // leaves `task_last_success` behind without asking for a restart.
            self.heartbeat.still_running();
        }
        Ok(())
    }

    fn startup(&self) -> anyhow::Result<PostgresStorage> {
        // Batched inserts don't need raised session memory.
        let mut storage = PostgresStorage::connect_follower(
            self.database_url.expose_secret(),
            SessionMemory::ServerDefault,
            self.epoch,
            self.batch_size,
        )?;
        bootstrap_datasets(&mut storage, &self.config)?;
        Ok(storage)
    }

    fn tick(&self, storage: &mut PostgresStorage) -> anyhow::Result<()> {
        let watermarks = match storage.datasets_with_last_chunk() {
            Ok(watermarks) => watermarks,
            Err(e) => {
                return retry_or_die(storage, self.ping_timeout, e, "read dataset watermarks");
            }
        };
        let requested = watermarks.len();
        let Some(discovered) = self.handle.block_on(list_newer_chunks(
            &self.s3_storage,
            watermarks,
            &self.config,
            &self.token,
        )) else {
            return Ok(());
        };
        let new_chunks = discovered.values().map(Vec::len).sum::<usize>();
        let datasets = discovered.len();
        match register_chunks(storage, discovered) {
            // A dataset that cannot be listed is logged and dropped from the map, so losing every
            // one of them arrives here as an empty map — indistinguishable from "nothing new" by
            // shape alone. Beating on that would report the task as having completed work while it
            // read nothing at all. Datasets that fail individually are expected (ingest creates
            // rows with no scheduler-visible S3 data), so only a total loss withholds the beat.
            Ok(()) if read_nothing(requested, datasets) => tracing::error!(
                requested,
                "Every dataset listing failed, retrying next tick"
            ),
            Ok(()) => {
                tracing::info!(new_chunks, datasets, "Chunk discovery pass done");
                self.heartbeat.beat();
            }
            Err(e) => retry_or_die(storage, self.ping_timeout, e, "register discovered chunks")?,
        }
        Ok(())
    }
}

/// Whether a pass read nothing at all, i.e. every dataset it asked for failed to list. Datasets
/// failing individually is expected — ingest creates rows with no scheduler-visible S3 data — so
/// only a total loss counts. Asking for nothing is a legitimate no-op, not a failure.
fn read_nothing(requested: usize, listed: usize) -> bool {
    requested > 0 && listed == 0
}

/// `None` on shutdown. The listing can be unbounded (`dataset_load_timeout` defaults to none), so
/// it is raced against shutdown.
///
/// The `Err` arm is currently unreachable: `load_newer_chunks` logs and skips each dataset it
/// cannot read and always returns `Ok`, so a failed listing reaches the caller as a *missing map
/// entry*, not an error — which is why the caller counts what came back rather than trusting `Ok`.
async fn list_newer_chunks(
    s3_storage: &S3Storage,
    watermarks: Vec<DatasetWatermark>,
    config: &cli::Config,
    token: &CancellationToken,
) -> Option<BTreeMap<Arc<String>, Vec<Chunk>>> {
    let loaded = tokio::select! {
        () = token.cancelled() => return None,
        loaded = s3_storage.load_newer_chunks(
            watermarks,
            config.concurrent_dataset_downloads,
            config.dataset_load_timeout,
        ) => loaded,
    };
    match loaded {
        Ok(discovered) => Some(discovered),
        Err(e) => {
            tracing::error!(
                error = format!("{e:#}"),
                "Failed to load chunks from S3, retrying next tick"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::read_nothing;

    /// The heartbeat rule. A total listing failure arrives as an empty map, shaped exactly like
    /// "nothing new to do" — beating on it would report work the task never did.
    #[test]
    fn only_a_total_listing_failure_counts_as_having_read_nothing() {
        assert!(read_nothing(7, 0), "every dataset failed");
        assert!(!read_nothing(7, 1), "one dataset listed is progress");
        assert!(
            !read_nothing(0, 0),
            "no datasets configured is a no-op, not a failure"
        );
    }
}
