//! Tests for the chunk-correction machinery (registration, firing, visibility-cycle integration).
//!
//! Split into two suites:
//! - [`machinery`]: drives the correction state machine directly through the static scheduling
//!   stub, so the test controls exactly which chunks land in each cycle.
//! - [`multistep`]: drives the same machinery through the real multistep scheduler, so the
//!   scheduler — not the test — decides placement.
//!
//! `register_correction` both inserts the replacement and links it, so the replacement chunk is
//! never seeded up front: it is created by the call and its pk is the return value.

use super::*;

mod machinery;
mod multistep;

const GRACE_PERIOD: TimeUnit = 60;
const CYCLE_INTERVAL: TimeUnit = 100;
const DELTA: TimeUnit = 50;

// ---------------------------------------------------------------------------
// Oracle bridge for PBT — the O1/O2/O3 invariants live in the backend-generic
// `scheduler_storage::test_harness::correction_oracle`, so this suite, the Postgres suite,
// and the multistep sim assert one definition.
// ---------------------------------------------------------------------------

/// Runs the shared correction oracles, surfacing their diagnostic in proptest failures.
fn corrections_safety_ok(storage: &InMemoryStorage) -> Result<(), String> {
    use crate::scheduler_storage::test_harness::inspect::StorageInspect;
    let promoted: HashSet<ChunkPk> = storage
        .get_chunks_metadata(|meta| {
            meta.applied_at_portal_assignment_id.is_some()
                && meta.dropped_at_portal_assignment_id.is_none()
        })
        .into_iter()
        .map(|meta| meta.chunk_pk)
        .collect();
    let removing: HashSet<ChunkPk> = storage
        .get_chunks_metadata(|meta| {
            meta.marked_for_removal
                || meta.dropped_at_portal_assignment_id.is_some()
                || meta.dropped_from_worker_assignment_at.is_some()
        })
        .into_iter()
        .map(|meta| meta.chunk_pk)
        .collect();
    crate::scheduler_storage::test_harness::correction_oracle::corrections_safety(
        &storage.get_corrections(|_| true),
        |pk| promoted.contains(pk),
        |pk| removing.contains(pk),
    )
}
