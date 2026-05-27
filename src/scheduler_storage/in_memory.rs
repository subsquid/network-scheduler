use crate::cli::DatasetsConfig;
use crate::scheduler_storage::SchedulerStorage;
use crate::scheduling::{ScheduledChunk, SchedulingConfig, schedule_with_per_worker_allocations};
use crate::types::{Chunk, Worker, WorkerStatus};
use crate::weight;
use anyhow::Result;
use bytesize::ByteSize;
use itertools::Itertools;
use libp2p_identity::PeerId;
use parking_lot::RwLock;
#[cfg(test)]
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use semver::Version;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
#[cfg(test)]
use thiserror::Error;

type Dataset = String;
type ChunkId = String;
type ChunkPk = (Dataset, ChunkId);

type WorkerAssignmentId = u64;
type PortalAssignmentId = u64;
type TimeUnit = u64;
type WorkerId = u64;
type ReservationId = u64;

pub struct AssignmentWorker {
    pub peer_id: PeerId,
    pub status: WorkerStatus,
}

pub struct WorkerAssignment {
    pub chunk_workers: BTreeMap<ChunkPk, Vec<WorkerId>>,
    pub chunks: BTreeMap<ChunkPk, Chunk>,
    pub workers: BTreeMap<WorkerId, AssignmentWorker>,
}

struct Reservation {
    id: ReservationId,
    reserved_bytes: ByteSize,
    portal_assignment_id: PortalAssignmentId,
    worker_assignment_id: WorkerAssignmentId,
    fulfilled_at: Option<TimeUnit>,
}

#[derive(Default, Debug)]
struct SchedulerChunkMetadata {
    applied_at_worker_assignment_id: Option<WorkerAssignmentId>,
    applied_at_portal_assignment_id: Option<PortalAssignmentId>,
    marked_for_removal: bool,
    dropped_at_portal_assignment_id: Option<PortalAssignmentId>,
    dropped_at_worker_assignment_id: Option<WorkerAssignmentId>,
}

#[derive(Debug, Clone)]
struct WorkerEntry {
    peer_id: PeerId,
    version: Option<Version>,
    inactive_since: Option<TimeUnit>,
}

mod counter {
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default, Debug)]
    pub struct Counter(AtomicU64);

    impl Counter {
        pub fn next(&self) -> u64 {
            self.0.fetch_add(1, Ordering::Relaxed)
        }
    }
}
use counter::Counter;

// Monotonically increasing ID allocators. Each call to `<field>.next()`
// returns the current value and atomically advances the counter.
#[derive(Default, Debug)]
struct Counters {
    worker_id: Counter,
    worker_assignment_id: Counter,
    portal_assignment_id: Counter,
    reservation_id: Counter,
}

struct InMemoryStorage {
    datasets: HashSet<Dataset>,
    chunks: BTreeMap<ChunkPk, Chunk>,

    sched_chunk_metadata: BTreeMap<ChunkPk, SchedulerChunkMetadata>,
    sched_worker_assignments: BTreeSet<WorkerAssignmentId>,
    sched_portal_assignments: BTreeSet<PortalAssignmentId>,

    // The highest worker assignment confirmed by the workers
    sched_worker_assignment_confirmation: WorkerAssignmentId,

    sched_stale_mappings: BTreeMap<(ChunkPk, WorkerId), TimeUnit>,
    sched_workers: BTreeMap<WorkerId, WorkerEntry>,
    // Active chunk -> workers mapping
    sched_chunk_workers: BTreeMap<ChunkPk, Vec<WorkerId>>,

    sched_reservations: BTreeMap<ReservationId, Reservation>,

    counters: Counters,
}

impl InMemoryStorage {
    pub fn get_chunks(&self) -> Vec<&Chunk> {
        self.chunks.values().collect()
    }

    /// Return all workers currently in the registry, including inactive ones
    /// that haven't been garbage-collected yet.
    pub fn get_workers(&self) -> Vec<(WorkerId, &WorkerEntry)> {
        self.sched_workers
            .iter()
            .map(|(id, entry)| (*id, entry))
            .collect()
    }

    /// Sync the worker registry with the current active peer set.
    ///
    /// 1. Peers in `active_peer_ids` not yet tracked → insert with `inactive_since = None`.
    /// 2. Tracked workers whose peer is in `active_peer_ids` but stale → revive (clear `inactive_since`).
    /// 3. Tracked workers whose peer is absent from `active_peer_ids` and not yet stale → mark stale.
    ///    Delete their rows from `sched_stale_mappings`.
    /// 4. Workers where `inactive_since < now - gc_ticks` → remove from registry.
    ///
    /// Returns (added_worker_ids, departed_worker_ids).
    pub fn update_worker_set(
        &mut self,
        active_peer_ids: &[PeerId],
        now: TimeUnit,
        gc_ticks: TimeUnit,
    ) -> (Vec<WorkerId>, Vec<WorkerId>) {
        let mut remaining: HashSet<PeerId> = active_peer_ids.iter().copied().collect();

        let mut added = Vec::new();
        let mut departed = Vec::new();

        // Pass 1: reconcile existing workers, draining matched peers from `remaining`.
        for (_, entry) in self.sched_workers.iter_mut() {
            let is_active = remaining.remove(&entry.peer_id);
            match (is_active, entry.inactive_since) {
                (true, Some(_)) => entry.inactive_since = None,
                (false, None) => entry.inactive_since = Some(now),
                _ => {}
            }
        }

        // Pass 2: whatever remains are new peers.
        for peer_id in remaining {
            let id = self.counters.worker_id.next();
            self.sched_workers.insert(
                id,
                WorkerEntry {
                    peer_id,
                    version: None,
                    inactive_since: None,
                },
            );
            added.push(id);
        }

        // Pass 3: collect departed worker IDs (just became stale this tick)
        // and delete their stale mappings.
        for (id, entry) in &self.sched_workers {
            if entry.inactive_since == Some(now) {
                departed.push(*id);
            }
        }
        // Remove stale mappings for departed workers
        self.sched_stale_mappings
            .retain(|(_, worker_id), _| !departed.contains(worker_id));

        // Pass 4: garbage-collect workers that have been stale too long.
        let gc_cutoff = now.saturating_sub(gc_ticks);
        self.sched_workers
            .retain(|_, entry| match entry.inactive_since {
                Some(ts) if ts < gc_cutoff => false,
                _ => true,
            });

        (added, departed)
    }
}

struct WritableInMemoryStorage {
    inner: Arc<RwLock<InMemoryStorage>>,
}

#[cfg(test)]
#[derive(Debug, Error)]
pub enum InsertChunkError {
    #[error("chunk {dataset}/{chunk_id} already exists")]
    UniqueConstraintViolation { dataset: Dataset, chunk_id: ChunkId },
    #[error("dataset {dataset} not found")]
    NoDatasetFound { dataset: Dataset },
}

#[cfg(test)]
#[derive(Debug, Error)]
pub enum InsertDatasetError {
    #[error("dataset {dataset} already exists")]
    UniqueConstraintViolation { dataset: Dataset },
}

// For tests and property-based desting
#[cfg(test)]
impl WritableInMemoryStorage {
    fn inner_mut(&self) -> RwLockWriteGuard<'_, InMemoryStorage> {
        self.inner.write()
    }

    fn inner_read(&self) -> RwLockReadGuard<'_, InMemoryStorage> {
        self.inner.read()
    }

    // This API will be used by ingester, or by operator to include dataset in the system
    pub fn insert_new_datasets(&self, datasets: Vec<Dataset>) -> Result<()> {
        let mut inner = self.inner_mut();
        for dataset in datasets {
            if !inner.datasets.insert(dataset.clone()) {
                return Err(InsertDatasetError::UniqueConstraintViolation { dataset }.into());
            }
        }
        Ok(())
    }

    // This API will be used by the ingester, and to register new chunks
    pub fn insert_new_chunks(&self, chunks: Vec<Chunk>) -> Result<()> {
        use std::collections::btree_map::Entry;
        let mut inner = self.inner_mut();
        for chunk in chunks {
            let key = ((*chunk.dataset).clone(), (*chunk.id).clone());
            if !inner.datasets.contains(&key.0) {
                return Err(InsertChunkError::NoDatasetFound { dataset: key.0 }.into());
            }
            match inner.chunks.entry(key) {
                Entry::Occupied(occ) => {
                    let (dataset, chunk_id) = occ.key().clone();
                    return Err(
                        InsertChunkError::UniqueConstraintViolation { dataset, chunk_id }.into(),
                    );
                }
                Entry::Vacant(vac) => {
                    vac.insert(chunk);
                }
            }
        }
        Ok(())
    }

    fn get_chunk_with_filter<F>(&self, predicate: F) -> Vec<ChunkPk>
    where
        F: Fn(&Chunk) -> bool,
    {
        let inner = self.inner_read();
        inner
            .chunks
            .iter()
            .filter(|(_, chunk)| predicate(chunk))
            .map(|(pk, _)| pk.clone())
            .collect()
    }

    // Register new chunks in the scheduler metadata
    fn register_new_chunks(&self) -> Result<Vec<ChunkPk>> {
        let mut inner = self.inner_mut();
        let new_pks: Vec<ChunkPk> = inner
            .chunks
            .keys()
            .filter(|pk| !inner.sched_chunk_metadata.contains_key(*pk))
            .cloned()
            .collect();
        for pk in &new_pks {
            inner
                .sched_chunk_metadata
                .insert(pk.clone(), SchedulerChunkMetadata::default());
            inner.sched_chunk_workers.insert(pk.clone(), Vec::new());
        }
        Ok(new_pks)
    }

    // Reconcile the worker set with what the scheduler currently sees.
    // - Peers in `active_peers` that aren't in `sched_workers` are inserted (no stale_since).
    // - Workers whose peer is missing from `active_peers` get `stale_since = Some(now)`,
    //   unless `stale_since` was already set (in which case we leave it).
    pub fn register_active_workers(
        &self,
        active_peers: Vec<PeerId>,
        now: TimeUnit,
    ) -> Result<BTreeMap<WorkerId, PeerId>> {
        let mut inner = self.inner_mut();

        let active_set: HashSet<PeerId> = active_peers.into_iter().collect();

        // Pass 1: reconcile staleness on existing workers, recording their peers.
        // - active + currently stale  -> revive (clear stale_since)
        // - inactive + not yet stale  -> mark stale with `now`
        // - inactive + already stale  -> keep the original timestamp
        let mut tracked: HashSet<PeerId> = HashSet::new();
        for (_, entry) in inner.sched_workers.iter_mut() {
            tracked.insert(entry.peer_id);
            let is_active = active_set.contains(&entry.peer_id);
            match (is_active, &entry.inactive_since) {
                (true, Some(_)) => entry.inactive_since = None,
                (false, None) => entry.inactive_since = Some(now),
                _ => {}
            }
        }

        // Pass 2: insert active peers not yet tracked.
        for peer in active_set.difference(&tracked) {
            let id = inner.counters.worker_id.next();
            inner.sched_workers.insert(
                id,
                WorkerEntry {
                    peer_id: *peer,
                    version: None,
                    inactive_since: None,
                },
            );
        }

        Ok(inner
            .sched_workers
            .iter()
            .map(|(id, entry)| (*id, entry.peer_id))
            .collect())
    }

    // Evict workers that went stale strictly before `stale_cutoff` (i.e. have
    // been stale for too long). Workers that are not stale (None) are kept.
    // Returns the WorkerIds that were removed.
    pub fn evict_stale_workers(&self, stale_cutoff: TimeUnit) -> Result<Vec<WorkerId>> {
        let mut inner = self.inner_mut();

        let evicted: Vec<WorkerId> = inner
            .sched_workers
            .iter()
            .filter_map(|(id, entry)| match entry.inactive_since {
                Some(ts) if ts < stale_cutoff => Some(*id),
                _ => None,
            })
            .collect();
        for id in &evicted {
            inner.sched_workers.remove(id);
        }
        Ok(evicted)
    }
}

// Read only memory storage is not allowed to create new chunks or
// datasets
struct ReadOnlyMemoryStorage {
    inner: Arc<RwLock<InMemoryStorage>>,
}

impl ReadOnlyMemoryStorage {}

// Naive test implementations

/*
impl Ingester {
    fn ingest_new_chunks(
        &self,
        storage: &WritableInMemoryStorage,
        chunks: Vec<Chunk>,
    ) -> Result<()> {
        storage.insert_new_chunks(chunks)
    }
}

impl Scheduler {
    // Chunks that were previously added by ingesters or backfill jobs are registered with scheduler
    fn discover_and_register_new_chunks(
        &self,
        storage: &WritableInMemoryStorage,
    ) -> Result<Vec<ChunkPk>> {
        storage.register_new_chunks()
    }

    fn update_workers(
        &self,
        storage: &WritableInMemoryStorage,
        active_peers: Vec<PeerId>,
        now: TimeUnit,
        stale_cutoff: TimeUnit,
    ) -> Result<BTreeMap<WorkerId, PeerId>> {
        storage.evict_stale_workers(stale_cutoff)?;
        storage.register_active_workers(active_peers, now)
    }
}
*/

impl InMemoryStorage {
    pub fn run_scheduling_cycle(
        &mut self,
        datasets_config: &DatasetsConfig,
        scheduling_config: SchedulingConfig,
        now: TimeUnit,
        m_ticks: TimeUnit,
    ) -> WorkerAssignment {
        // 1. Expire stale mappings older than m_ticks
        self.sched_stale_mappings
            .retain(|_, detected_at| now.saturating_sub(*detected_at) < m_ticks);

        // 2. Compute per-worker bytes allocated by stale (not-yet-applied) mappings.
        let mut allocated_per_worker: BTreeMap<WorkerId, u64> = BTreeMap::new();
        for ((pk, worker_id), _) in &self.sched_stale_mappings {
            if let Some(chunk) = self.chunks.get(pk) {
                *allocated_per_worker.entry(*worker_id).or_default() += chunk.size as u64;
            }
        }

        // 3. Read chunks and workers from storage
        let chunks = self.get_chunks().into_iter().cloned().collect_vec();
        let workers = self.get_workers();

        // 4. Translate allocated_per_worker from WorkerId to PeerId keys
        let allocated_per_peer: BTreeMap<PeerId, u64> = allocated_per_worker
            .iter()
            .filter_map(|(worker_id, bytes)| {
                self.sched_workers
                    .get(worker_id)
                    .map(|entry| (entry.peer_id, *bytes))
            })
            .collect();

        // 5. Translate to schedule() inputs and call schedule()
        let prepared = weight::prepare_chunks(chunks, datasets_config);

        let scheduled_chunks: Vec<ScheduledChunk<'_>> = prepared
            .iter()
            .map(|(chunk, weight, min_ver)| ScheduledChunk {
                dataset: &chunk.dataset,
                chunk_id: &chunk.id,
                size: chunk.size,
                weight: *weight,
                minimum_worker_version: min_ver.as_ref(),
            })
            .collect();

        let worker_list: Vec<Worker> = workers
            .iter()
            .map(|(_, entry)| Worker {
                id: entry.peer_id,
                status: match entry.inactive_since {
                    None => WorkerStatus::Online,
                    Some(_) => WorkerStatus::Stale,
                },
                version: entry.version.clone(),
            })
            .collect();

        let assignment = schedule_with_per_worker_allocations(
            &scheduled_chunks,
            &worker_list,
            scheduling_config,
            &allocated_per_peer,
        )
        .expect("scheduling failed");

        // 6. Translate schedule() output back to storage IDs
        let peer_to_worker: BTreeMap<PeerId, WorkerId> = workers
            .iter()
            .map(|(id, entry)| (entry.peer_id, *id))
            .collect();

        let mut ideal_mappings: BTreeMap<ChunkPk, HashSet<WorkerId>> = BTreeMap::new();
        for (peer_id, chunk_indexes) in &assignment.worker_chunks {
            let worker_id = peer_to_worker[peer_id];
            for &ci in chunk_indexes {
                let chunk = &prepared[ci as usize].0;
                let pk: ChunkPk = (String::clone(&chunk.dataset), String::clone(&chunk.id));
                ideal_mappings.entry(pk).or_default().insert(worker_id);
            }
        }

        // 7. Diff ideal vs current to update sched_stale_mappings
        for (pk, current_workers) in &self.sched_chunk_workers {
            let ideal_workers = ideal_mappings.get(pk);
            for &worker_id in current_workers {
                let still_assigned = ideal_workers.map_or(false, |iw| iw.contains(&worker_id));
                if !still_assigned {
                    self.sched_stale_mappings
                        .entry((pk.clone(), worker_id))
                        .or_insert(now);
                }
            }
        }

        // 8. Resolve flip-flops: chunk re-assigned back to the same worker
        for (pk, ideal_workers) in &ideal_mappings {
            for &worker_id in ideal_workers {
                self.sched_stale_mappings.remove(&(pk.clone(), worker_id));
            }
        }

        // 9. Overwrite sched_chunk_workers with the ideal assignment
        self.sched_chunk_workers = ideal_mappings
            .into_iter()
            .map(|(pk, workers)| (pk, workers.into_iter().collect()))
            .collect();

        // 10. Build the published worker assignment: ideal ∪ unexpired stale mappings
        self.build_worker_assignment()
    }

    /// Build the published worker assignment by merging the ideal mapping
    /// (`sched_chunk_workers`) with unexpired stale mappings. This is the
    /// union that gets serialized and sent to workers.
    fn build_worker_assignment(&self) -> WorkerAssignment {
        let mut published_chunk_workers = self.sched_chunk_workers.clone();
        for ((pk, worker_id), _) in &self.sched_stale_mappings {
            published_chunk_workers
                .entry(pk.clone())
                .or_default()
                .push(*worker_id);
        }

        let published_workers: BTreeMap<WorkerId, AssignmentWorker> = self
            .sched_workers
            .iter()
            .map(|(id, entry)| {
                (
                    *id,
                    AssignmentWorker {
                        peer_id: entry.peer_id,
                        status: match entry.inactive_since {
                            None => WorkerStatus::Online,
                            Some(_) => WorkerStatus::Stale,
                        },
                    },
                )
            })
            .collect();

        WorkerAssignment {
            chunk_workers: published_chunk_workers,
            chunks: self.chunks.clone(),
            workers: published_workers,
        }
    }
}
