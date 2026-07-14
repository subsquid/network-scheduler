//! The identity types the whole run shares: chunk keys, compact worker indices, and the placement
//! maps built from them.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use libp2p_identity::PeerId;

pub(crate) type ChunkId = (Arc<String>, Arc<String>);
/// Run-stable compact worker identity
pub(crate) type WorkerIdx = u16;
pub(crate) type ChunkOwners = BTreeMap<ChunkId, Vec<WorkerIdx>>;
pub(crate) type ChunkSizeIndex = BTreeMap<ChunkId, u32>;

/// A dataset's config key: the bucket, without the `s3://` scheme.
pub(crate) fn bucket_of(dataset_id: &str) -> &str {
    dataset_id.strip_prefix("s3://").unwrap_or(dataset_id)
}

/// The dataset id a bucket is stored under.
pub(crate) fn dataset_id(bucket: &str) -> String {
    format!("s3://{bucket}")
}

/// Interns worker `PeerId`s to compact [`WorkerIdx`]es, assigned in first-seen order
/// and never reused, so a worker keeps the same index for the whole run.
#[derive(Default)]
pub(crate) struct WorkerIndex {
    to_idx: HashMap<PeerId, WorkerIdx>,
}

impl WorkerIndex {
    pub(crate) fn intern(&mut self, peer: PeerId) -> WorkerIdx {
        if let Some(&idx) = self.to_idx.get(&peer) {
            return idx;
        }
        let idx = WorkerIdx::try_from(self.to_idx.len())
            .expect("reshuffle-sim: more than u16::MAX distinct workers in one run");
        self.to_idx.insert(peer, idx);
        idx
    }

    pub(crate) fn intern_holders(
        &mut self,
        peers: impl IntoIterator<Item = PeerId>,
    ) -> Vec<WorkerIdx> {
        let mut holders: Vec<WorkerIdx> = peers.into_iter().map(|p| self.intern(p)).collect();
        holders.sort_unstable();
        holders.dedup();
        holders
    }
}
