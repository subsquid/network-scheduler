use std::collections::BTreeMap;

use crate::types::ChunkWeight;

/// Finds such multiplier `K` that
/// `sum(size_by_weight[w_i] * max(min_replication, round(w_i * K)))` is close to the `capacity`.
/// Returns the values `max(min_replication, round(w_i * K))` for each weight `w_i`.
///
/// In other words, distributes the total size proportionally to the weights but guarantees at least
/// `min_replication` for each chunk.
///
/// Note that the total resulting size may exceed the `capacity`.
pub fn calc_replication_factors(
    size_by_weight: BTreeMap<ChunkWeight, u64>,
    capacity: u64,
    min_replication: u16,
) -> Result<BTreeMap<ChunkWeight, u16>, ReplicationError> {
    let total_size: u64 = size_by_weight.values().sum();
    if total_size * min_replication as u64 > capacity {
        return Err(ReplicationError::NotEnoughCapacity);
    }

    let size_per_weight_sum: u64 = size_by_weight
        .iter()
        .map(|(weight, size)| *weight as u64 * size)
        .sum();
    let mut remaining_size_per_weight_sum = size_per_weight_sum;
    let mut fixed_size = 0;
    let mut multiplier = 0.;
    for (&weight, &size) in &size_by_weight {
        // All previous chunks get the minimal replication factor
        // Try to distribute what's left between the remaining chunks
        let surplus = capacity.saturating_sub(fixed_size);
        multiplier = surplus as f64 / remaining_size_per_weight_sum as f64;
        if (multiplier * weight as f64) as u64 >= min_replication as u64 {
            break;
        }
        // All chunks with the current weight should also have the minimal replication factor
        fixed_size += min_replication as u64 * size;
        remaining_size_per_weight_sum -= size * weight as u64;
    }
    tracing::debug!("Replication multiplier: {multiplier:.2}");

    assert!((*size_by_weight.last_key_value().unwrap().0 as f64 * multiplier) < 10000.);

    Ok(size_by_weight
        .into_keys()
        .map(|weight| {
            (
                weight,
                min_replication.max((weight as f64 * multiplier) as u16),
            )
        })
        .collect())
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("Not enough capacity to schedule chunks")]
    NotEnoughCapacity,
}

#[test]
fn test_replication() {
    let size_by_weight: BTreeMap<_, _> = [(1, 4), (2, 1), (6, 1), (12, 1)].into_iter().collect();
    assert!(matches!(
        calc_replication_factors(size_by_weight.clone(), 13, 2),
        Err(ReplicationError::NotEnoughCapacity)
    ));

    for (capacity, expected) in [
        (14, [2, 2, 2, 2]),
        (15, [2, 2, 2, 3]),
        (16, [2, 2, 2, 4]),
        (17, [2, 2, 2, 4]),
        (18, [2, 2, 2, 5]),
        (19, [2, 2, 3, 6]),
        (20, [2, 2, 3, 6]),
        (21, [2, 2, 3, 7]),
        (22, [2, 2, 4, 8]),
        (23, [2, 2, 4, 8]),
        (24, [2, 2, 4, 9]),
        (25, [2, 2, 5, 10]),
        (26, [2, 2, 5, 10]),
        (27, [2, 2, 5, 11]),
        (28, [2, 2, 6, 12]),
        (29, [2, 2, 6, 12]),
        (30, [2, 2, 6, 13]),
        (31, [2, 2, 6, 13]),
        (32, [2, 2, 7, 14]),
        (33, [2, 2, 7, 15]),
        (34, [2, 2, 7, 15]),
        (35, [2, 2, 8, 16]),
        (36, [2, 2, 8, 16]),
        (37, [2, 2, 8, 17]),
        (38, [2, 3, 9, 18]),
        (39, [2, 3, 9, 18]),
        (40, [2, 3, 9, 19]),
        (41, [2, 3, 9, 19]),
        (42, [2, 3, 10, 20]),
        (43, [2, 3, 10, 21]),
        (44, [2, 3, 10, 21]),
        (45, [2, 3, 11, 22]),
        (46, [2, 3, 11, 22]),
        (47, [2, 3, 11, 23]),
        (48, [2, 4, 12, 24]),
        (240, [10, 20, 60, 120]),
        (2400, [100, 200, 600, 1200]),
    ] {
        let replication_factors =
            calc_replication_factors(size_by_weight.clone(), capacity, 2).unwrap();
        assert_eq!(
            replication_factors.values().copied().collect::<Vec<_>>(),
            expected,
            "capacity: {capacity}",
        );
        let total_size: u64 = size_by_weight
            .iter()
            .map(|(weight, size)| size * replication_factors[weight] as u64)
            .sum();
        assert!(
            total_size <= capacity,
            "capacity: {capacity}, total_size: {total_size}"
        );
    }
}

#[test]
fn test_big_sizes() {
    const TB: u64 = 1 << 40;

    let size_by_weight: BTreeMap<_, _> = [(1, 10 * TB), (2, 20 * TB), (6, 10 * TB), (48, 10 * TB)]
        .into_iter()
        .collect();
    assert_eq!(
        calc_replication_factors(size_by_weight.clone(), 2000 * TB, 5)
            .unwrap()
            .into_values()
            .collect::<Vec<_>>(),
        [5, 6, 20, 161]
    );
}
