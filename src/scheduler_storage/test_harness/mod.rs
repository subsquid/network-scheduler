//! Shared, backend-agnostic test machinery: the [`StorageInspect`](inspect::StorageInspect)
//! observation API, the chunk-correction invariant oracles, and the Postgres database harness.
//! Used by both backends' test suites and the multistep simulation.

pub mod correction_oracle;
pub mod inspect;
pub mod pg_harness;
#[cfg(test)]
pub mod utils;

use std::collections::HashSet;

use crate::scheduler_storage::{ChunkPk, PortalAssignment};

/// Assert a portal assignment's visible chunk set is *exactly* `expected` — no missing, no extra.
/// Shared by both backends' correction suites so portal-state checks stay tight rather than
/// spot-checking individual membership.
pub fn assert_portal_chunks_exact(portal: &PortalAssignment, expected: &[ChunkPk], context: &str) {
    let actual: HashSet<ChunkPk> = portal.chunks.keys().copied().collect();
    assert_eq!(
        actual,
        expected.iter().copied().collect::<HashSet<_>>(),
        "{context}: portal must contain exactly {expected:?}",
    );
}
