//! Pins the scheduling cycle's chunk feed order. Stage-1 packing is order-sensitive, so the
//! order the backend feeds the algorithm is observable behaviour: `(dataset name, first_block)`,
//! collated by the server. The fetch reproduces it client-side from the ranked dataset list
//! instead of a 6M+-row SQL sort — this test fails if that reconstruction drifts.

use sqlx::Connection;

use super::super::scheduling_cycle;
use super::fresh_storage;
use crate::scheduler_storage::SchedulerStorage;
use crate::scheduler_storage::test_harness::utils::{chunk, dataset};
use crate::types::DatasetSchema;

#[test]
fn active_chunks_feed_in_dataset_name_then_block_order() {
    let mut storage = fresh_storage("feed_order");
    // Registration order deliberately disagrees with name order, so dataset ids can't stand in
    // for name ranks; insertion interleaves the datasets and inverts block order within each.
    storage
        .insert_new_datasets(vec![
            (dataset("zeta"), DatasetSchema::default()),
            (dataset("alpha"), DatasetSchema::default()),
        ])
        .expect("insert datasets");
    let chunks = [("zeta", 7), ("alpha", 9), ("zeta", 2), ("alpha", 3)]
        .map(|(name, seed)| chunk(name, seed, 1024));
    storage
        .insert_new_chunks(chunks.to_vec())
        .expect("insert chunks");
    storage.register_new_chunks().expect("register chunks");

    let feed: Vec<(String, u64)> = storage.with_conn(async |conn| {
        let mut tx = conn.begin().await.expect("begin");
        let active = scheduling_cycle::fetch_active_chunks_with_placement(&mut tx)
            .await
            .expect("fetch active chunks");
        active
            .for_algo
            .iter()
            .map(|(_, chunk)| ((*chunk.dataset).clone(), *chunk.blocks.start()))
            .collect()
    });

    let expected: Vec<(String, u64)> = [("alpha", 3u64), ("alpha", 9), ("zeta", 2), ("zeta", 7)]
        .map(|(name, seed)| (dataset(name), seed * 2))
        .to_vec();
    assert_eq!(
        feed, expected,
        "the algorithm feed must stay in (dataset name, first_block) order"
    );
}
