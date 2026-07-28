//! Row/param helpers shared by the ingest SQL (inserts and corrections).

use std::ops::RangeInclusive;

use crate::ids::SchemaId;
use crate::new_chunk::NewChunk;

/// A non-negative logical clock value (wall-clock seconds in production).
pub(crate) type Tick = u64;

/// Non-negative, so always fits in `i64`.
pub fn tick_to_i64(tick: Tick) -> i64 {
    i64::try_from(tick).expect("tick value overflows i64")
}

/// Split an inclusive block range into the stored `(first_block, last_block_delta)` columns.
/// Checked casts: internal callers bypass `validate_chunk` — better a loud panic than a silently
/// negative committed range.
pub(crate) fn block_range_columns(blocks: &RangeInclusive<u64>) -> (i64, i32) {
    (
        i64::try_from(*blocks.start()).expect("first_block overflows i64"),
        i32::try_from(blocks.end() - blocks.start()).expect("block span overflows i32"),
    )
}

pub(crate) fn is_unique_violation(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .is_some_and(|db| db.kind() == sqlx::error::ErrorKind::UniqueViolation)
}

/// SQLSTATE `40001` (`serialization_failure`); `sqlx::ErrorKind` has no variant for it, so match the code directly.
pub(crate) fn is_serialization_failure(err: &sqlx::Error) -> bool {
    err.as_database_error().and_then(|db| db.code()).as_deref() == Some("40001")
}

/// SQLSTATE `55P03` (`lock_not_available`): a `lock_timeout` expiry, matched by code like `40001` above.
pub(crate) fn is_lock_timeout(err: &sqlx::Error) -> bool {
    err.as_database_error().and_then(|db| db.code()).as_deref() == Some("55P03")
}

/// Column arrays for the batched chunk inserts, zipped positionally by `UNNEST`; bind order must
/// match the SQL's `UNNEST` column list. No `datasets` column: the API insert resolves the dataset
/// id up front, and the callers that join by name bind their own name array before the macro.
pub(crate) struct ChunkInsertArrays<'a> {
    pub(crate) chunk_ids: Vec<&'a str>,
    pub(crate) sizes: Vec<i32>,
    pub(crate) schema_ids: Vec<SchemaId>,
    pub(crate) tables_present: Vec<Option<bit_vec::BitVec>>,
    pub(crate) first_blocks: Vec<i64>,
    pub(crate) last_block_deltas: Vec<i32>,
    pub(crate) last_block_hashes: Vec<Option<&'a str>>,
    pub(crate) last_block_timestamps: Vec<Option<i64>>,
}

/// A macro because `sqlx` builder types share no `.bind()` trait; centralizing the bind order keeps it matching each INSERT's `UNNEST` column list.
macro_rules! bind_chunk_arrays {
    ($q:expr, $arrays:expr) => {
        $q.bind(&$arrays.chunk_ids)
            .bind(&$arrays.sizes)
            .bind(&$arrays.schema_ids)
            .bind(&$arrays.tables_present)
            .bind(&$arrays.first_blocks)
            .bind(&$arrays.last_block_deltas)
            .bind(&$arrays.last_block_hashes)
            .bind(&$arrays.last_block_timestamps)
    };
}
pub(crate) use bind_chunk_arrays;

impl<'a> ChunkInsertArrays<'a> {
    pub(crate) fn from_chunks(chunks: impl ExactSizeIterator<Item = &'a NewChunk>) -> Self {
        let n = chunks.len();
        let mut arrays = ChunkInsertArrays {
            chunk_ids: Vec::with_capacity(n),
            sizes: Vec::with_capacity(n),
            schema_ids: Vec::with_capacity(n),
            tables_present: Vec::with_capacity(n),
            first_blocks: Vec::with_capacity(n),
            last_block_deltas: Vec::with_capacity(n),
            last_block_hashes: Vec::with_capacity(n),
            last_block_timestamps: Vec::with_capacity(n),
        };
        for chunk in chunks {
            let (first_block, last_block_delta) = block_range_columns(&chunk.blocks);
            arrays.chunk_ids.push(chunk.id.as_str());
            // Checked for the same reason as block_range_columns: internal callers bypass
            // validate_chunk.
            arrays
                .sizes
                .push(i32::try_from(chunk.size).expect("chunk size overflows i32"));
            arrays.schema_ids.push(chunk.schema_id);
            arrays.tables_present.push(chunk.tables_present.clone());
            arrays.first_blocks.push(first_block);
            arrays.last_block_deltas.push(last_block_delta);
            arrays
                .last_block_hashes
                .push(chunk.last_block_hash.as_deref());
            arrays
                .last_block_timestamps
                .push(chunk.last_block_timestamp);
        }
        arrays
    }
}
