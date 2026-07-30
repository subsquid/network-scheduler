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
            Ok(()) => {
                tracing::info!(new_chunks, datasets, "Chunk discovery pass done");
                self.heartbeat.beat();
            }
            Err(e) => retry_or_die(storage, self.ping_timeout, e, "register discovered chunks")?,
        }
        Ok(())
    }
}

/// `None` on shutdown or a (logged) S3 error. The listing can be unbounded
/// (`dataset_load_timeout` defaults to none), so it is raced against shutdown.
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
