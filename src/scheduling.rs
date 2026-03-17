use std::{borrow::Cow, collections::BTreeMap};

use itertools::Itertools;
use libp2p_identity::PeerId;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use seahash::hash;
use tracing::instrument;

use semver::Version;

use crate::{
    replication::{ReplicationError, calc_replication_factors},
    types::{Assignment, ChunkIndex, Worker, WorkerIndex},
};

type RingIndex = u16;

const N_RINGS: usize = 6000;

#[derive(Debug, Clone)]
pub struct SchedulingConfig {
    pub worker_capacity: u64,
    pub saturation: f64,
    pub min_replication: u16,
    pub ignore_reliability: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScheduledChunk {
    pub id: String,
    pub size: u32,
    pub weight: u16,
    pub minimum_worker_version: Option<Version>,
}

pub fn schedule(
    chunks: &[ScheduledChunk],
    workers: &[Worker],
    config: SchedulingConfig,
) -> Result<Assignment, ReplicationError> {
    let _timer = crate::metrics::Timer::new("schedule");
    let mut sorted_workers = Vec::with_capacity(workers.len());
    sorted_workers.extend(workers.iter().filter(|w| w.reliable()));
    let reliable_workers = sorted_workers.len();
    sorted_workers.extend(workers.iter().filter(|w| !w.reliable()));

    if config.ignore_reliability {
        tracing::info!(
            "Scheduling {} chunks to {} workers",
            chunks.len(),
            workers.len(),
        );
        return schedule_to_workers(chunks, &sorted_workers, &config);
    }

    tracing::info!(
        "Scheduling {} chunks to {} reliable workers",
        chunks.len(),
        reliable_workers
    );
    let mut reliable = schedule_to_workers(chunks, &sorted_workers[..reliable_workers], &config)?;

    if reliable_workers == workers.len() {
        return Ok(reliable);
    }

    tracing::info!(
        "Scheduling {} chunks to {} total workers",
        chunks.len(),
        workers.len()
    );
    let all = schedule_to_workers(chunks, &sorted_workers, &config)?;

    // Use assignment from `reliable` for reliable workers and from `all` for unreliable workers
    for (worker_id, chunk_indexes) in all.worker_chunks {
        reliable
            .worker_chunks
            .entry(worker_id)
            .or_insert(chunk_indexes);
    }
    Ok(reliable)
}

fn schedule_to_workers(
    chunks: &[ScheduledChunk],
    workers: &[&Worker],
    config: &SchedulingConfig,
) -> Result<Assignment, ReplicationError> {
    let size_by_weight = chunks
        .iter()
        .map(|chunk| (chunk.weight, chunk.size as u64))
        .into_grouping_map()
        .sum()
        .into_iter()
        .collect();

    let total_capacity =
        (config.worker_capacity as f64 * workers.len() as f64 * config.saturation) as u64;

    let mapping = calc_replication_factors(
        size_by_weight,
        total_capacity,
        config.min_replication,
        workers.len() as u16,
    )?;

    validate_version_restrictions(chunks, workers, config, &mapping);

    let chunks = chunks
        .iter()
        .map(|c| ReplicatedChunk {
            id: Cow::Borrowed(&c.id),
            replication: mapping[&c.weight],
            size: c.size,
            minimum_worker_version: c.minimum_worker_version.as_ref(),
        })
        .collect_vec();

    tracing::info!(
        "Replication factors by weight: {:?}, total vchunks: {}",
        mapping,
        chunks.iter().map(|c| c.replication as u64).sum::<u64>()
    );

    let worker_chunks = distribute(&chunks, workers, config.worker_capacity);
    Ok(Assignment {
        worker_chunks,
        replication_by_weight: mapping,
    })
}

/// Validates that version-restricted data can be scheduled within the
/// saturation headroom. Only a single non-null `minimum_worker_version` value
/// is allowed across all chunks. Two checks:
/// 1. R ≤ E: replication factor must not exceed eligible worker count.
/// 2. f ≤ (1-s) × E / (s × (N-E)): the extra load on eligible workers
///    from restricted chunks must fit within the unused capacity fraction.
fn validate_version_restrictions(
    chunks: &[ScheduledChunk],
    workers: &[&Worker],
    config: &SchedulingConfig,
    replication_by_weight: &BTreeMap<u16, u16>,
) {
    let total_size: u64 = chunks.iter().map(|c| c.size as u64).sum();
    if total_size == 0 {
        return;
    }

    // Collect the single allowed minimum_worker_version
    let mut min_ver: Option<&Version> = None;
    let mut s_restricted: u64 = 0;
    let mut max_replication: u16 = 0;

    for chunk in chunks {
        if let Some(ref v) = chunk.minimum_worker_version {
            if let Some(existing) = min_ver {
                assert!(
                    existing == v,
                    "Multiple minimum_worker_version values found ({} and {}). \
                     Only a single version restriction is supported.",
                    existing,
                    v,
                );
            } else {
                min_ver = Some(v);
            }
            s_restricted += chunk.size as u64;
            max_replication = max_replication.max(replication_by_weight[&chunk.weight]);
        }
    }

    let Some(min_ver) = min_ver else { return };

    let n = workers.len();
    let s = config.saturation;
    let e = workers
        .iter()
        .filter(|w| w.version.as_ref().is_some_and(|v| v >= min_ver))
        .count();

    assert!(
        max_replication as usize <= e,
        "Not enough eligible workers for version >= {}: \
         replication factor {} exceeds {} eligible workers (need at least {})",
        min_ver,
        max_replication,
        e,
        max_replication,
    );

    if e < n {
        let f = s_restricted as f64 / total_size as f64;
        let threshold = (1.0 - s) * e as f64 / (s * (n - e) as f64);
        assert!(
            f <= threshold,
            "Version-restricted data (>= {}) exceeds saturation headroom: \
             f = {:.4} > {:.4} (E={}, N={}, s={})",
            min_ver,
            f,
            threshold,
            e,
            n,
            s,
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicatedChunk<'a> {
    id: Cow<'a, str>,
    size: u32,
    replication: u16,
    minimum_worker_version: Option<&'a Version>,
}

#[instrument(skip_all)]
fn distribute(
    chunks: &[ReplicatedChunk],
    workers: &[&Worker],
    worker_capacity: u64,
) -> BTreeMap<PeerId, Vec<ChunkIndex>> {
    let _timer = crate::metrics::Timer::new("schedule:distribute");
    let worker_ids = workers.iter().map(|w| w.id).collect_vec();
    let rings = hash_workers(&worker_ids);
    let mut orderings = hash_chunks(chunks, &rings);
    // Assign version-restricted chunks first so they get priority on eligible
    // workers' capacity before unrestricted chunks fill it. Without this,
    // unrestricted chunks (ordered by hash ring position only) could fill
    // eligible workers' capacity before restricted chunks are processed,
    // causing a panic even when total capacity would suffice.
    orderings.sort_by_key(|(chunk_index, _, _)| {
        chunks[*chunk_index as usize]
            .minimum_worker_version
            .is_none()
    });
    assign_chunks(orderings, chunks, workers, rings, worker_capacity)
}

#[instrument(skip_all)]
fn hash_workers(worker_ids: &[PeerId]) -> Vec<Vec<(u64, WorkerIndex)>> {
    tracing::info!("Hashing workers");
    let _timer = crate::metrics::Timer::new("schedule:distribute:hash_workers");

    (0..N_RINGS as RingIndex)
        .into_par_iter()
        .map(|ring_index| {
            let mut vec = worker_ids
                .iter()
                .enumerate()
                .map(|(worker_index, worker_id)| {
                    let buffer = [
                        worker_id.to_bytes().as_slice(),
                        b":",
                        &ring_index.to_le_bytes(),
                    ]
                    .concat();
                    (hash(&buffer), worker_index as WorkerIndex)
                })
                .collect_vec();
            vec.sort_unstable();
            vec
        })
        .collect()
}

#[instrument(skip_all)]
fn hash_chunks(
    chunks: &[ReplicatedChunk],
    rings: &[Vec<(u64, u16)>],
) -> Vec<(ChunkIndex, RingIndex, WorkerIndex)> {
    tracing::info!("Hashing chunks");
    let _timer = crate::metrics::Timer::new("schedule:distribute:hash_chunks");

    chunks
        .par_iter()
        .enumerate()
        .flat_map_iter(|(chunk_index, chunk)| {
            (0..chunk.replication).map(move |tag| {
                let buffer = [chunk.id.as_bytes(), b":", &tag.to_le_bytes()].concat();
                let chunk_hash = hash(&buffer);
                let ring_index = chunk_hash as usize % N_RINGS;
                let ring = &rings[ring_index];
                let first = ring.partition_point(|(x, _)| *x < chunk_hash);
                (
                    chunk_index as ChunkIndex,
                    ring_index as RingIndex,
                    first as WorkerIndex,
                )
            })
        })
        .collect()
}

#[instrument(skip_all)]
fn assign_chunks(
    orderings: Vec<(ChunkIndex, RingIndex, WorkerIndex)>,
    chunks: &[ReplicatedChunk],
    workers: &[&Worker],
    rings: Vec<Vec<(u64, WorkerIndex)>>,
    worker_capacity: u64,
) -> BTreeMap<PeerId, Vec<ChunkIndex>> {
    tracing::info!("Assigning chunks");
    let _timer = crate::metrics::Timer::new("schedule:distribute:assign_chunks");

    struct WorkerAssignment {
        chunks: Vec<ChunkIndex>,
        allocated: u64,
    }

    let mut assignments: Vec<WorkerAssignment> = workers
        .iter()
        .map(|_| WorkerAssignment {
            chunks: Vec::new(),
            allocated: 0,
        })
        .collect();
    orderings
        .into_iter()
        .for_each(|(chunk_index, ring_index, first)| {
            let chunk = &chunks[chunk_index as usize];
            let ring = &rings[ring_index as usize];
            let first = first as usize;
            let candidates = ring[first..].iter().chain(ring[..first].iter());
            for &(_, worker_index) in candidates {
                if let Some(min_ver) = chunk.minimum_worker_version {
                    let worker_ver = &workers[worker_index as usize].version;
                    if !worker_ver.as_ref().is_some_and(|v| v >= min_ver) {
                        continue;
                    }
                }

                let assignment = &mut assignments[worker_index as usize];

                if assignment.allocated + chunk.size as u64 <= worker_capacity
                    // Replicas of the same chunk stay adjacent after the stable sort
                    // (they share the same minimum_worker_version key), so checking
                    // the last assigned index is sufficient to prevent duplicates.
                    && assignment.chunks.last() != Some(&chunk_index)
                {
                    assignment.allocated += chunk.size as u64;
                    assignment.chunks.push(chunk_index);
                    return;
                }
            }
            panic!("No worker found for chunk {}", chunk.id);
        });

    assignments
        .into_iter()
        .enumerate()
        .map(|(index, a)| (workers[index].id, a.chunks))
        .collect()
}
