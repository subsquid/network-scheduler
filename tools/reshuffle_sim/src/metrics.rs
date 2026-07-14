//! Reshuffle metrics and the assignment diffing that produces them.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use crate::simulation::StepPlacement;
use crate::types::{ChunkId, ChunkOwners, ChunkSizeIndex, WorkerIdx};

pub struct ReshuffleMetrics {
    pub step: u32,
    pub new_chunks_in_step: u32,
    /// How many of this step's new chunks are version-restricted.
    pub new_restricted_in_step: u32,
    /// Chunks the `copy` and `replace` plans added this step. On the stateless path copies arrive as
    /// ordinary new chunks, so they count in `new_chunks_in_step`.
    pub copied_or_replaced_chunks: u32,
    /// The chunk funnel, widest first. `ingested` counts every chunk ever inserted, including ones
    /// rejected as overlapping and ones a correction later tombstoned: `ingested - placed` is the dead
    /// rows, and `placed - portal` the chunks still in flight to the portal.
    pub ingested_chunks: usize,
    /// Chunks at least one worker holds.
    pub placed_chunks: usize,
    /// Chunks the portal routes. `None` on the stateless path, which has no portal.
    pub portal_chunks: Option<usize>,
    /// Cumulative count of version-restricted chunks.
    pub total_restricted_chunks: usize,
    pub replication_by_weight: BTreeMap<u16, u16>,
    pub data_movement: DataMovement,
    pub total_capacity_bytes: u64,
    pub used_capacity_bytes: u64,
    /// Of `used_capacity_bytes`, the bytes held by draining stale copies (0 on the stateless path).
    pub stale_capacity_bytes: u64,
    pub eligible_workers: usize,
    /// False when scheduling failed at this step; the run stops after.
    pub scheduled: bool,
    pub failure_reason: Option<String>,
    /// Data movement restricted to version-restricted chunks only.
    pub restricted_movement: DataMovement,
    /// Wall-clock time the scheduler spent on this step.
    pub schedule_duration: Duration,
}

/// Values the renderers derive from a step, defined once so the console line, the terminal table and
/// the HTML report agree.
impl ReshuffleMetrics {
    pub fn total_download(&self) -> u64 {
        self.data_movement.total_download()
    }

    pub fn restricted_download(&self) -> u64 {
        self.restricted_movement.total_download()
    }

    pub fn freed_bytes(&self) -> u64 {
        self.data_movement.freed_bytes
    }

    /// Share of this step's download caused by chunks moving between workers.
    pub fn shuffle_share(&self) -> f64 {
        self.data_movement.shuffle_share()
    }

    pub fn free_capacity(&self) -> u64 {
        self.total_capacity_bytes
            .saturating_sub(self.used_capacity_bytes)
    }

    pub fn used_pct(&self) -> f64 {
        self.pct(self.used_capacity_bytes)
    }

    pub fn stale_pct(&self) -> f64 {
        self.pct(self.stale_capacity_bytes)
    }

    pub fn schedule_ms(&self) -> f64 {
        self.schedule_duration.as_secs_f64() * 1000.0
    }

    /// Replication factor per chunk weight, e.g. `"1:5, 3:15"`.
    pub fn replication_label(&self) -> String {
        self.replication_by_weight
            .iter()
            .map(|(weight, factor)| format!("{weight}:{factor}"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn pct(&self, part: u64) -> f64 {
        if self.total_capacity_bytes == 0 {
            return 0.0;
        }
        part as f64 / self.total_capacity_bytes as f64 * 100.0
    }
}

/// Data movement between two steps: S3 downloads in, deletions out.
///
/// The three download causes partition [`total_download`](Self::total_download) exactly — every
/// downloaded byte is new data, a move, or an extra replica.
#[derive(Default)]
pub struct DataMovement {
    /// Downloads of chunks that were not placed before.
    pub new_chunk_bytes: u64,
    /// Downloads that replaced a holder the scheduler dropped in the same step: the chunk moved.
    pub shuffled_bytes: u64,
    /// Chunks that moved at least one copy.
    pub shuffled_count: u32,
    /// Downloads beyond the holders dropped this step: the chunk gained replicas.
    pub increased_replication_bytes: u64,
    /// Replicas the scheduler shed this step without a replacement download. A decision, not a
    /// movement: those copies keep their bytes until their drain expires, and are counted in
    /// `freed_bytes` when it does.
    pub shed_replication_bytes: u64,
    /// Bytes deleted off workers this step, whatever the cause: a drain expiring, a chunk tombstoned,
    /// a worker leaving. The counterpart of `total_download`.
    pub freed_bytes: u64,
    pub workers_receiving_new: usize,
    pub workers_losing: usize,
    /// Workers that downloaded a copy of an already-placed chunk — a move or an extra replica. Which
    /// of the two is a per-chunk fact, not a per-worker one: one chunk can do both in a step.
    pub workers_receiving_existing: usize,
}

impl DataMovement {
    pub fn total_download(&self) -> u64 {
        self.new_chunk_bytes + self.shuffled_bytes + self.increased_replication_bytes
    }

    /// Percentage of this step's download that chunks moving between workers cost, as opposed to
    /// ingesting new data or adding replicas.
    pub fn shuffle_share(&self) -> f64 {
        let total = self.total_download();
        if total == 0 {
            return 0.0;
        }
        self.shuffled_bytes as f64 / total as f64 * 100.0
    }
}

/// A step's placement, as the next step diffs against it.
///
/// `owners` are the physical holders — what each worker has on disk. `stale` are the draining copies
/// among them, so `ideal = owners \ stale`. The diff needs both: bytes come from the physical side,
/// and the cause of a download from the ideal side.
#[derive(Default)]
pub struct Placement {
    pub owners: ChunkOwners,
    /// Empty on the stateless path, which has no drain.
    pub stale: ChunkOwners,
}

impl Placement {
    /// The draining copies of `chunk_id`, sorted.
    fn stale_of(&self, chunk_id: &ChunkId) -> &[WorkerIdx] {
        self.stale.get(chunk_id).map_or(&[], Vec::as_slice)
    }

    /// `None` if the chunk is not placed.
    fn holders(&self, chunk_id: &ChunkId) -> Option<Holders<'_>> {
        self.owners.get(chunk_id).map(|physical| Holders {
            physical,
            stale: self.stale_of(chunk_id),
        })
    }
}

/// Counters the success and failure metric builders share.
pub struct StepStats {
    pub step: u32,
    pub new_chunks: u32,
    pub new_restricted: u32,
    pub eligible_workers: usize,
}

/// Diffs the step's placement against the previous one. Returns the metrics and the placement itself,
/// which becomes the next step's baseline.
pub fn measure_reshuffle(
    previous: &Placement,
    chunk_sizes: &mut ChunkSizeIndex,
    placement: StepPlacement,
    restricted: &HashSet<ChunkId>,
    stats: StepStats,
    total_capacity_bytes: u64,
) -> (ReshuffleMetrics, Placement) {
    let StepPlacement {
        owners,
        stale_owners,
        chunk_sizes: sizes_this_step,
        replication_by_weight,
        used_capacity_bytes,
        stale_capacity_bytes,
        ingested_chunks,
        copied_or_replaced_chunks,
        portal_chunks,
        schedule_duration,
    } = placement;
    let current = Placement {
        owners,
        stale: stale_owners,
    };

    // Sizes accumulate across the run: a chunk a correction tombstones is gone from the placement it
    // left, yet its bytes were freed off the workers and have to be scored. Sizes never change, so
    // merging is safe.
    chunk_sizes.extend(sizes_this_step);

    let data_movement = compute_data_movement(previous, &current, chunk_sizes, |_| true);
    let restricted_movement = compute_data_movement(previous, &current, chunk_sizes, |chunk_id| {
        restricted.contains(chunk_id)
    });

    let metrics = ReshuffleMetrics {
        step: stats.step,
        new_chunks_in_step: stats.new_chunks,
        new_restricted_in_step: stats.new_restricted,
        copied_or_replaced_chunks,
        ingested_chunks,
        placed_chunks: current.owners.len(),
        portal_chunks,
        total_restricted_chunks: restricted.len(),
        replication_by_weight,
        data_movement,
        total_capacity_bytes,
        used_capacity_bytes,
        stale_capacity_bytes,
        eligible_workers: stats.eligible_workers,
        scheduled: true,
        failure_reason: None,
        restricted_movement,
        schedule_duration,
    };

    (metrics, current)
}

/// The step at which scheduling failed: zero movement, and the run stops afterwards.
pub fn failed_step_metrics(
    stats: StepStats,
    ingested_chunks: usize,
    total_restricted_chunks: usize,
    total_capacity_bytes: u64,
    reason: String,
) -> ReshuffleMetrics {
    ReshuffleMetrics {
        step: stats.step,
        new_chunks_in_step: stats.new_chunks,
        new_restricted_in_step: stats.new_restricted,
        copied_or_replaced_chunks: 0,
        ingested_chunks,
        placed_chunks: 0,
        portal_chunks: None,
        total_restricted_chunks,
        replication_by_weight: BTreeMap::new(),
        data_movement: DataMovement::default(),
        total_capacity_bytes,
        used_capacity_bytes: 0,
        stale_capacity_bytes: 0,
        eligible_workers: stats.eligible_workers,
        scheduled: false,
        failure_reason: Some(reason),
        restricted_movement: DataMovement::default(),
        schedule_duration: Duration::ZERO,
    }
}

/// One chunk's holders in a step: the physical copies on disk, and the draining ones among them. Both
/// sorted. `ideal = physical \ stale`.
#[derive(Clone, Copy)]
struct Holders<'a> {
    physical: &'a [WorkerIdx],
    stale: &'a [WorkerIdx],
}

impl Holders<'_> {
    fn is_ideal(&self, worker: WorkerIdx) -> bool {
        self.physical.binary_search(&worker).is_ok() && self.stale.binary_search(&worker).is_err()
    }
}

/// How many copies a worker gained and lost for one chunk.
struct PhysicalDelta {
    gained: u64,
    lost: u64,
}

/// Classifies movement over the chunks `keep` accepts: all of them, or just the version-restricted
/// ones.
///
/// Bytes come from the physical diff — a gained holder downloaded the chunk, a lost one deleted it —
/// so used capacity moves by exactly `total_download - freed_bytes`. The cause of a download comes from
/// the ideal diff: the scheduler drops a holder in the step its replacement is downloaded, but that
/// copy keeps its bytes until its drain expires, several steps later. Only the ideal sees the two
/// together, so only the ideal can tell a move from an extra replica.
fn compute_data_movement(
    previous: &Placement,
    current: &Placement,
    chunk_sizes: &ChunkSizeIndex,
    keep: impl Fn(&ChunkId) -> bool,
) -> DataMovement {
    let mut tally = MovementTally::default();

    for (chunk_id, physical) in current.owners.iter().filter(|(id, _)| keep(id)) {
        let size = chunk_size(chunk_sizes, chunk_id);
        let now = Holders {
            physical,
            stale: current.stale_of(chunk_id),
        };
        match previous.holders(chunk_id) {
            Some(before) => tally.record_existing(size, before, now),
            None => tally.record_added(size, physical),
        }
    }
    for (chunk_id, physical) in previous.owners.iter().filter(|(id, _)| keep(id)) {
        if !current.owners.contains_key(chunk_id) {
            tally.record_removed(chunk_size(chunk_sizes, chunk_id), physical);
        }
    }

    tally.finish()
}

fn chunk_size(chunk_sizes: &ChunkSizeIndex, chunk_id: &ChunkId) -> u64 {
    *chunk_sizes.get(chunk_id).unwrap_or(&0) as u64
}

/// Holders the scheduler dropped from the ideal this step. Distinct from the copies lost off disk: a
/// dropped copy keeps its bytes while it drains, and a re-promoted draining copy re-enters the ideal
/// without downloading anything.
fn ideal_drops(before: Holders, now: Holders) -> u64 {
    before
        .physical
        .iter()
        .filter(|&&worker| before.is_ideal(worker) && !now.is_ideal(worker))
        .count() as u64
}

/// Accumulates data movement while iterating chunks. The byte counters live in `totals`; the affected
/// workers are tracked as sets and collapse to their counts in [`MovementTally::finish`].
#[derive(Default)]
struct MovementTally {
    totals: DataMovement,
    // Distinct affected workers; only their final `.len()` is read, so unordered.
    workers_receiving_new: HashSet<WorkerIdx>,
    workers_losing: HashSet<WorkerIdx>,
    workers_receiving_existing: HashSet<WorkerIdx>,
}

impl MovementTally {
    /// A chunk placed in both steps. Each dropped holder pairs with one of the step's downloads to
    /// make a move; downloads beyond that are extra replicas, drops beyond that are shed replicas.
    fn record_existing(&mut self, size: u64, before: Holders, now: Holders) {
        let PhysicalDelta { gained, lost } = self.record_physical_delta(before, now);
        let dropped = ideal_drops(before, now);

        let shuffled = gained.min(dropped);
        if shuffled > 0 {
            self.totals.shuffled_count += 1;
            self.totals.shuffled_bytes += size * shuffled;
        }
        // shuffled + increased == gained, so the download total is exactly the physical gains.
        self.totals.increased_replication_bytes += size * gained.saturating_sub(dropped);
        self.totals.shed_replication_bytes += size * dropped.saturating_sub(gained);
        self.totals.freed_bytes += size * lost;
    }

    /// Walks the two sorted holder lists in lockstep, noting which workers downloaded the chunk and
    /// which deleted it.
    fn record_physical_delta(&mut self, before: Holders, now: Holders) -> PhysicalDelta {
        let (previous, current) = (before.physical, now.physical);
        let (mut i, mut j) = (0, 0);
        let (mut gained, mut lost) = (0, 0);
        while i < current.len() && j < previous.len() {
            match current[i].cmp(&previous[j]) {
                Ordering::Less => {
                    self.workers_receiving_existing.insert(current[i]);
                    gained += 1;
                    i += 1;
                }
                Ordering::Greater => {
                    self.workers_losing.insert(previous[j]);
                    lost += 1;
                    j += 1;
                }
                Ordering::Equal => {
                    i += 1;
                    j += 1;
                }
            }
        }
        for &worker in &current[i..] {
            self.workers_receiving_existing.insert(worker);
            gained += 1;
        }
        for &worker in &previous[j..] {
            self.workers_losing.insert(worker);
            lost += 1;
        }
        PhysicalDelta { gained, lost }
    }

    fn record_added(&mut self, size: u64, holders: &[WorkerIdx]) {
        self.totals.new_chunk_bytes += size * holders.len() as u64;
        self.workers_receiving_new.extend(holders.iter().copied());
    }

    /// The chunk left the placement entirely, so every copy is deleted. The chunk is gone, not thinned,
    /// so this is not a replication change.
    fn record_removed(&mut self, size: u64, holders: &[WorkerIdx]) {
        self.totals.freed_bytes += size * holders.len() as u64;
        self.workers_losing.extend(holders.iter().copied());
    }

    fn finish(mut self) -> DataMovement {
        self.totals.workers_receiving_new = self.workers_receiving_new.len();
        self.totals.workers_losing = self.workers_losing.len();
        self.totals.workers_receiving_existing = self.workers_receiving_existing.len();
        self.totals
    }
}
