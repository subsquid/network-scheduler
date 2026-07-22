//! Worker hash rings shared by both schedulers.
//!
//! Consistent hashing with bounded loads: [`N_RINGS`] rings, each a permutation of the workers
//! ordered by `hash(peer_id:ring_index)`. A chunk replica picks a ring by its hash and walks it from
//! there, taking the first worker that accepts.

use libp2p_identity::PeerId;
use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
use seahash::hash;
use tracing::instrument;

use crate::types::WorkerIndex;

pub(crate) type RingIndex = u16;

pub(crate) const N_RINGS: usize = 6000;

const _: () = assert!(N_RINGS <= RingIndex::MAX as usize + 1);

/// Caches the worker rings across cycles, rebuilt only when the id list changes.
#[derive(Default)]
pub struct WorkerRingCache {
    ids: Vec<PeerId>,
    rings: Rings,
}

impl WorkerRingCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rings for the workers `ids` yields; rebuilt from scratch whenever that list changes.
    pub(crate) fn rings(&mut self, ids: impl Iterator<Item = PeerId> + Clone) -> &Rings {
        if self.ids.iter().copied().ne(ids.clone()) {
            self.ids.clear();
            self.ids.extend(ids);
            self.rings = build_rings(&self.ids);
        }
        &self.rings
    }
}

/// Build every ring from scratch: hash each worker onto each ring, then sort each ring by hash.
#[instrument(skip_all)]
fn build_rings(ids: &[PeerId]) -> Rings {
    let _timer = crate::metrics::Timer::new("schedule:distribute:hash_workers");
    let n_workers = ids.len();
    let mut entries: Vec<(u64, WorkerIndex)> = vec![(0, 0); N_RINGS * n_workers];
    // `par_chunks_mut` panics on a zero stride; nothing to fill when there are no workers.
    if n_workers > 0 {
        // Encode each peer id once, then per ring reuse one scratch buffer for the
        // `peer_id:ring_index` hash input rather than allocating a fresh one per entry.
        let id_bytes: Vec<Vec<u8>> = ids.iter().map(|id| id.to_bytes()).collect();
        entries
            .par_chunks_mut(n_workers)
            .enumerate()
            .for_each(|(ring_index, ring)| {
                let ring_index_bytes = (ring_index as RingIndex).to_le_bytes();
                let mut buffer = Vec::new();
                for (position, id) in id_bytes.iter().enumerate() {
                    buffer.clear();
                    buffer.extend_from_slice(id);
                    buffer.push(b':');
                    buffer.extend_from_slice(&ring_index_bytes);
                    ring[position] = (hash(&buffer), position as WorkerIndex);
                }
                ring.sort_unstable();
            });
    }
    Rings {
        entries: entries.into_boxed_slice(),
        n_workers,
    }
}

/// All [`N_RINGS`] rings in one contiguous buffer: ring `r` is the `n_workers`-wide slice
/// `[r * n_workers .. (r + 1) * n_workers]`, sorted by hash.
#[derive(Default, Debug, PartialEq)]
pub(crate) struct Rings {
    entries: Box<[(u64, WorkerIndex)]>,
    n_workers: usize,
}

impl Rings {
    /// Sorted `(hash, worker)` slice for ring `index`; empty when there are no workers.
    pub(crate) fn ring(&self, index: usize) -> &[(u64, WorkerIndex)] {
        let start = index * self.n_workers;
        &self.entries[start..start + self.n_workers]
    }

    /// Test-only ring with a fixed, predictable geometry: every ring lists workers in index order
    /// with `hash == index`. A replica whose hash is `h` (in `0..n_workers`) therefore walks the
    /// ring starting at worker `h`, then `h+1`, … wrapping — so a test can steer each replica onto
    /// exact workers by choosing its per-tag hashes, instead of depending on the real hash geometry.
    // Only the `mvcc-chunks` reconcile tests use this; `rings` compiles without that feature, so gate
    // the helper to match its users or it reads as dead code in a default-feature build.
    #[cfg(all(test, feature = "mvcc-chunks"))]
    pub(crate) fn test_identity(n_workers: usize) -> Self {
        let one_ring: Vec<(u64, WorkerIndex)> = (0..n_workers)
            .map(|i| (i as u64, i as WorkerIndex))
            .collect();
        let entries: Vec<(u64, WorkerIndex)> = std::iter::repeat_with(|| one_ring.iter().copied())
            .take(N_RINGS)
            .flatten()
            .collect();
        Rings {
            entries: entries.into_boxed_slice(),
            n_workers,
        }
    }
}

/// Walk the ring `chunk_hash` selects, from `chunk_hash` and wrapping, returning the first worker
/// `accept` claims (or `None`).
pub(crate) fn walk_ring_from(
    rings: &Rings,
    chunk_hash: u64,
    mut accept: impl FnMut(WorkerIndex) -> bool,
) -> Option<WorkerIndex> {
    let ring = rings.ring(chunk_hash as usize % N_RINGS);
    let first = ring.partition_point(|(x, _)| *x < chunk_hash);
    ring[first..]
        .iter()
        .chain(&ring[..first])
        .map(|&(_, worker)| worker)
        .find(|&worker| accept(worker))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::input::generate_workers;

    #[test]
    fn ring_cache_matches_builder_and_rebuilds_on_change() {
        let ids_a: Vec<PeerId> = generate_workers(5);
        let ids_b: Vec<PeerId> = generate_workers(7);

        let mut cache = WorkerRingCache::default();

        let direct_a = build_rings(&ids_a);
        assert_eq!(cache.rings(ids_a.iter().copied()), &direct_a);
        assert_eq!(cache.rings(ids_a.iter().copied()), &direct_a); // cache hit

        let direct_b = build_rings(&ids_b);
        assert_eq!(cache.rings(ids_b.iter().copied()), &direct_b);
        assert_ne!(direct_a, direct_b);

        assert_eq!(cache.rings(ids_a.iter().copied()), &direct_a); // switching back
    }

    #[test]
    fn no_workers_yields_empty_rings() {
        let rings = build_rings(&[]);
        assert!(rings.ring(0).is_empty());
        assert!(rings.ring(N_RINGS - 1).is_empty());
        assert_eq!(walk_ring_from(&rings, 12345, |_| true), None);
    }

    // The cache rebuilds correctly as the worker set grows / shrinks / reorders / is replaced — a
    // reorder (same set, new order) must rebuild, since the result is order-dependent.
    #[test]
    fn cache_matches_fresh_build_through_changes() {
        let all = generate_workers(8);
        let sequences = [
            vec![0, 1, 2, 3],
            vec![0, 1, 2, 3, 4, 5], // additions
            vec![0, 2, 4, 5],       // drop from the middle
            vec![5, 4, 2, 0],       // reordered, same set
            vec![6, 7, 2],          // mostly replaced
            vec![],                 // emptied
            vec![0, 1, 2, 3, 4, 5, 6],
        ];

        let mut cache = WorkerRingCache::default();
        for seq in sequences {
            let ids: Vec<PeerId> = seq.iter().map(|&i| all[i]).collect();
            assert_eq!(
                cache.rings(ids.iter().copied()),
                &build_rings(&ids),
                "cached rings diverged from a fresh build for {seq:?}",
            );
        }
    }
}
