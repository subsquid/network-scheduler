//! Shared, backend-agnostic test machinery: the [`StorageInspect`](inspect::StorageInspect)
//! observation API, the chunk-correction invariant oracles, and the Postgres database harness.
//! Used by both backends' test suites and the multistep simulation.

// Only `pg_harness` is reachable under the `pg-testkit` feature (for reshuffle-sim); the rest is
// test-only.
pub mod pg_harness;
#[cfg(test)]
pub mod correction_oracle;
#[cfg(test)]
pub mod inspect;
#[cfg(test)]
pub mod utils;

/// Assert a portal assignment's visible chunk set is *exactly* `expected` — no missing, no extra.
/// Shared by both backends' correction suites so portal-state checks stay tight rather than
/// spot-checking individual membership.
#[cfg(test)]
pub fn assert_portal_chunks_exact(
    portal: &crate::scheduler_storage::PortalAssignment,
    expected: &[crate::scheduler_storage::ChunkPk],
    context: &str,
) {
    use std::collections::HashSet;
    let actual: HashSet<_> = portal.chunks.keys().copied().collect();
    assert_eq!(
        actual,
        expected.iter().copied().collect::<HashSet<_>>(),
        "{context}: portal must contain exactly {expected:?}",
    );
}
