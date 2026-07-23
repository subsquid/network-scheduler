//! Transition validity shared by every simulation model.

use super::{Action, SimUnderTest};

/// Shared transition validity (adds/no-ops/joins always pass — storage accepts every insert,
/// recording shortages rather than rejecting). Re-checked on shrink against the replayed SUT.
pub(super) fn standard_preconditions<D: super::SimStorage>(
    sut: &SimUnderTest<D>,
    transition: &Action,
) -> bool {
    match transition {
        Action::AddChunks(_)
        | Action::NoOp
        | Action::WorkerJoined(_)
        | Action::SetDatasetSchema { .. } => true,
        Action::AdvanceClock(_) => {
            sut.has_stale_mappings() || sut.has_published_portal_assignment()
        }
        Action::CheckConverged(_) => sut.total_chunk_count() > 0,
        // Only remove a worker the fleet can absorb: floors still fit from scratch AND no visible
        // chunk loses its last durable (committed-ideal ∩ held) copy. A departure that outpaces
        // re-replication is real loss — kept out of the walk rather than excused by an oracle.
        // See [`SimUnderTest::is_removal_recoverable`].
        Action::WorkerLeft(index) => {
            sut.is_worker_active(*index) && sut.is_removal_recoverable(*index)
        }
        Action::WorkerFetchAssignment { worker, .. } => sut.is_worker_active(*worker),
        Action::PortalFetchAssignment { .. } => sut.has_published_portal_assignment(),
        // Any existing chunk is a valid pick — register_correction disregards an unsuitable one as a
        // no-op; only the replacement id must be fresh (the generator never mints a colliding chunk).
        // Re-checked on shrink: a prefix where the old chunk isn't created yet prunes the transition.
        Action::RegisterCorrection {
            old_dataset,
            old_chunk_id: old_key,
            replacement,
        } => {
            sut.chunk_exists(old_dataset, old_key)
                && !sut.chunk_exists(&replacement.dataset, &replacement.key)
        }
        // A floor above the active fleet size could never be met. Re-checked on shrink — a
        // prefix that loses a join can invalidate a raise.
        Action::SetMinReplication(min_replication) => {
            *min_replication >= 1 && usize::from(*min_replication) <= sut.active_workers_count()
        }
    }
}
