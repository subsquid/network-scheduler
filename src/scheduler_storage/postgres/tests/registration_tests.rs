//! Tests for the PG-specific registration helpers ([`PostgresStorage::copy_dataset_chunks`]).

use super::fresh_storage;
use crate::scheduler_storage::SchedulerStorage;
use crate::scheduler_storage::test_harness::utils::{chunk, chunk_with_blocks, dataset};
use crate::types::DatasetSchema;

#[test]
fn copy_dataset_chunks_clones_live_set_only() {
    let mut storage = fresh_storage("copy_ds");
    storage
        .insert_new_datasets(vec![
            (dataset("src"), DatasetSchema::default()),
            (dataset("dst"), DatasetSchema::default()),
        ])
        .expect("datasets");

    // Two live chunks plus one that loses the overlap gate (same range as the first) — the
    // rejected chunk must not be cloned.
    storage
        .insert_new_chunks(vec![
            chunk("src", 1, 100),
            chunk("src", 2, 100),
            chunk_with_blocks("src", 3, 100, 2..=3),
        ])
        .expect("insert chunks");
    let admitted = storage.register_new_chunks().expect("register");
    assert_eq!(admitted.len(), 2, "overlap loser rejected at registration");

    let copied = storage
        .copy_dataset_chunks(&dataset("src"), &dataset("dst"))
        .expect("copy dataset");
    assert_eq!(copied, 2, "only the live chunks are cloned");

    // The clones enter unregistered; the next registration admits exactly them.
    let clones = storage.register_new_chunks().expect("register clones");
    assert_eq!(clones.len(), 2, "both clones admitted into the new dataset");
}
