//! Schemas published alongside an assignment: the WRITE schemas its chunks pin, and the current
//! READ schemas of the datasets in play. Built from the assignment's own chunks, so the two can't
//! drift.
//!
//! [`BundleId`] covers both sections, so a promote moves it — that is what lets a portal cache the
//! bundle and re-fetch only on a change.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use super::{ReadSchemaId, SchemaId, WorkerAssignmentChunk};
#[cfg(test)]
use super::{SchedulerStorage, StorageError};
use crate::types::DatasetSchema;

/// Pins the encoding: a future format change becomes a different id, not a silent collision.
const BUNDLE_ID_DOMAIN: &[u8] = b"sqd.schema-bundle.v1";

/// SHA-256 over the bundle's sorted ids. Ids are content-deduped serials, so equal ids ⇒ equal
/// content — within one database, not across them.
///
/// Each section is hashed behind its own tag and length: `schemas.id` and `read_schemas.id` are
/// independent `SERIAL`s handing out the same integers, so a flat hash would let write-3 and
/// read-3 collide. Ids are never reused, so an id in a published bundle keeps its meaning.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BundleId([u8; 32]);

impl BundleId {
    /// Ids must arrive sorted (they do, from a `BTreeMap`); order isn't normalised here.
    pub(crate) fn from_sections(
        write_ids: impl IntoIterator<Item = SchemaId>,
        read_ids: impl IntoIterator<Item = ReadSchemaId>,
    ) -> Self {
        let write_ids: Vec<SchemaId> = write_ids.into_iter().collect();
        let read_ids: Vec<ReadSchemaId> = read_ids.into_iter().collect();
        debug_assert_sorted(write_ids.iter().map(|id| id.0), "write");
        debug_assert_sorted(read_ids.iter().map(|id| id.0), "read");

        let mut hasher = Sha256::new();
        hasher.update(BUNDLE_ID_DOMAIN);
        hasher.update(b"w");
        hasher.update((write_ids.len() as u64).to_le_bytes());
        for id in &write_ids {
            hasher.update(id.0.to_le_bytes());
        }
        hasher.update(b"r");
        hasher.update((read_ids.len() as u64).to_le_bytes());
        for id in &read_ids {
            hasher.update(id.0.to_le_bytes());
        }
        Self(hasher.finalize().into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Display for BundleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for BundleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BundleId({self})")
    }
}

fn debug_assert_sorted(ids: impl Iterator<Item = i32>, section: &str) {
    #[cfg(debug_assertions)]
    {
        let ids: Vec<i32> = ids.collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        debug_assert_eq!(ids, sorted, "{section} ids must be pre-sorted");
    }
    #[cfg(not(debug_assertions))]
    let _ = (ids, section);
}

/// Workers derive file sets from the write section; portals validate queries against the read one.
/// The two key on disjoint id spaces — see [`BundleId`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaBundle {
    id: BundleId,
    schemas: BTreeMap<SchemaId, DatasetSchema>,
    read_schemas: BTreeMap<ReadSchemaId, DatasetSchema>,
}

impl SchemaBundle {
    pub(crate) fn from_sections(
        schemas: BTreeMap<SchemaId, DatasetSchema>,
        read_schemas: BTreeMap<ReadSchemaId, DatasetSchema>,
    ) -> Self {
        let id = BundleId::from_sections(schemas.keys().copied(), read_schemas.keys().copied());
        Self {
            id,
            schemas,
            read_schemas,
        }
    }

    /// For tests; production takes the bundle from `run_scheduling_cycle`.
    #[cfg(test)]
    pub(crate) fn generate(storage: &impl SchedulerStorage) -> Result<Self, StorageError> {
        Ok(Self::from_sections(
            storage.active_schema_bundle()?,
            storage.active_read_schemas()?,
        ))
    }

    pub fn id(&self) -> BundleId {
        self.id
    }

    pub fn schemas(&self) -> &BTreeMap<SchemaId, DatasetSchema> {
        &self.schemas
    }

    pub fn read_schemas(&self) -> &BTreeMap<ReadSchemaId, DatasetSchema> {
        &self.read_schemas
    }

    pub fn get(&self, id: SchemaId) -> Option<&DatasetSchema> {
        self.schemas.get(&id)
    }

    pub fn get_read(&self, id: ReadSchemaId) -> Option<&DatasetSchema> {
        self.read_schemas.get(&id)
    }

    pub fn contains(&self, id: SchemaId) -> bool {
        self.schemas.contains_key(&id)
    }

    pub fn contains_read(&self, id: ReadSchemaId) -> bool {
        self.read_schemas.contains_key(&id)
    }

    /// The chunk's `<table>.parquet` files, from its pinned schema and `tables_present` bitmap.
    ///
    /// The bitmap is positional over that schema's tables in sorted-name order, so a length
    /// mismatch means it was resolved against a different table list — refused, not applied.
    pub fn chunk_files(
        &self,
        chunk: &WorkerAssignmentChunk,
    ) -> Result<Vec<String>, ChunkFilesError> {
        let schema_id = chunk.schema_id;
        let schema = self
            .schemas
            .get(&schema_id)
            .ok_or(ChunkFilesError::SchemaMissing { schema_id })?;
        let tables = schema.tables().len();
        if let Some(bitmap) = chunk.tables_present.as_ref()
            && bitmap.len() != tables
        {
            return Err(ChunkFilesError::BitmapArity {
                schema_id,
                bitmap_len: bitmap.len(),
                tables,
            });
        }
        Ok(chunk_files(schema, chunk.tables_present.as_ref()))
    }
}

/// Both variants must reach the caller rather than degrade to a shorter file list: a worker acts on
/// that list, and a missing file reads as legitimate absence, not as an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChunkFilesError {
    /// Pinned schema absent from the bundle — the state ADR 0002 exists to prevent.
    #[error("chunk pins schema {schema_id}, absent from the bundle — no file set can be derived")]
    SchemaMissing { schema_id: SchemaId },
    /// Bitmap length ≠ the pinned schema's table count. Reached by re-pinning a chunk without
    /// recomputing the bitmap: bits shift onto the wrong tables, and positions past the end read as
    /// "absent".
    #[error(
        "tables_present has {bitmap_len} bits but pinned schema {schema_id} has {tables} tables — \
         the bitmap indexes a different table list"
    )]
    BitmapArity {
        schema_id: SchemaId,
        bitmap_len: usize,
        tables: usize,
    },
}

/// `None` = every table present. It records no schema, so it can't be arity-checked: re-pinning a
/// NULL-bitmap chunk to a superset schema would claim files it lacks. A re-pin must therefore
/// materialise NULL into an explicit bitmap first.
///
/// Precondition (checked by the only caller): an explicit bitmap's length is `tables().len()`.
fn chunk_files(schema: &DatasetSchema, bitmap: Option<&bit_vec::BitVec>) -> Vec<String> {
    schema
        .tables()
        .keys()
        .enumerate()
        .filter(|(i, _)| bitmap.is_none_or(|p| p.get(*i).unwrap_or(false)))
        .map(|(_, table)| format!("{table}.parquet"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ops::RangeInclusive;
    use std::sync::Arc;

    use super::*;
    use crate::types::{BlockNumber, TableSchema};

    fn schema(tables: &[&str]) -> DatasetSchema {
        DatasetSchema::new(
            tables
                .iter()
                .map(|t| ((*t).to_owned(), TableSchema::default()))
                .collect(),
        )
    }

    fn bundle(entries: &[(i32, DatasetSchema)]) -> SchemaBundle {
        let schemas: BTreeMap<SchemaId, DatasetSchema> = entries
            .iter()
            .map(|(id, s)| (SchemaId(*id), s.clone()))
            .collect();
        SchemaBundle::from_sections(schemas, BTreeMap::new())
    }

    fn chunk(schema_id: i32, tables_present: Option<bit_vec::BitVec>) -> WorkerAssignmentChunk {
        WorkerAssignmentChunk {
            dataset: Arc::new("s3://a".to_owned()),
            id: Arc::new("c".to_owned()),
            size: 1,
            blocks: RangeInclusive::<BlockNumber>::new(0, 1),
            schema_id: SchemaId(schema_id),
            tables_present,
        }
    }

    #[test]
    fn table_bitmap_selects_files_in_schema_order() {
        let s = schema(&["blocks", "logs", "transactions"]);
        let mask = bit_vec::BitVec::from_fn(3, |i| i != 1);

        assert_eq!(
            chunk_files(&s, Some(&mask)),
            vec![
                "blocks.parquet".to_owned(),
                "transactions.parquet".to_owned()
            ]
        );
        // No bitmap = all tables.
        assert_eq!(chunk_files(&s, None).len(), 3);
    }

    #[test]
    fn chunk_files_resolves_schema_and_applies_bitmap() {
        let b = bundle(&[(1, schema(&["blocks", "logs", "transactions"]))]);

        // Resolves the schema by id and forwards the bitmap (drops `logs`).
        let mask = bit_vec::BitVec::from_fn(3, |i| i != 1);
        assert_eq!(
            b.chunk_files(&chunk(1, Some(mask))),
            Ok(vec![
                "blocks.parquet".to_owned(),
                "transactions.parquet".to_owned(),
            ])
        );
        assert_eq!(
            b.chunk_files(&chunk(99, None)),
            Err(ChunkFilesError::SchemaMissing {
                schema_id: SchemaId(99)
            }),
        );
    }

    /// Applying a mismatched bitmap yields a plausible-but-wrong file set with no error — the
    /// failure mode that makes re-pinning unsafe. Both directions must be refused.
    #[test]
    fn chunk_files_rejects_a_bitmap_of_the_wrong_arity() {
        let b = bundle(&[(1, schema(&["blocks", "logs", "transactions"]))]);

        // Too short, as if re-pinned from a 2-table schema: `transactions` used to read as absent.
        assert_eq!(
            b.chunk_files(&chunk(1, Some(bit_vec::BitVec::from_elem(2, true)))),
            Err(ChunkFilesError::BitmapArity {
                schema_id: SchemaId(1),
                bitmap_len: 2,
                tables: 3,
            }),
        );
        // Too long: extra bits used to be ignored, so positions could mean the wrong table.
        assert_eq!(
            b.chunk_files(&chunk(1, Some(bit_vec::BitVec::from_elem(4, true)))),
            Err(ChunkFilesError::BitmapArity {
                schema_id: SchemaId(1),
                bitmap_len: 4,
                tables: 3,
            }),
        );
        assert_eq!(
            b.chunk_files(&chunk(1, Some(bit_vec::BitVec::from_elem(3, true))))
                .map(|files| files.len()),
            Ok(3),
        );
    }

    /// NULL has no arity to check, so it passes against any schema — the residue a re-pin must close.
    #[test]
    fn chunk_files_accepts_the_null_bitmap_against_any_schema() {
        let b = bundle(&[(1, schema(&["blocks", "logs"]))]);
        assert_eq!(b.chunk_files(&chunk(1, None)).map(|f| f.len()), Ok(2));
    }

    fn write_only(ids: &[i32]) -> BundleId {
        BundleId::from_sections(ids.iter().map(|i| SchemaId(*i)), [])
    }

    fn read_only(ids: &[i32]) -> BundleId {
        BundleId::from_sections([], ids.iter().map(|i| ReadSchemaId(*i)))
    }

    #[test]
    fn bundle_id_is_content_addressed() {
        // Deterministic and set-sensitive.
        assert_eq!(write_only(&[1, 2]), write_only(&[1, 2]));
        assert_ne!(write_only(&[1]), write_only(&[1, 2]));
        assert_ne!(write_only(&[1]), write_only(&[2]));
    }

    /// Without per-section tagging, promoting a read schema whose id matches a write id would leave
    /// the fingerprint unchanged.
    #[test]
    fn bundle_id_separates_the_two_sections() {
        assert_ne!(write_only(&[3]), read_only(&[3]));
        assert_ne!(
            BundleId::from_sections([SchemaId(1)], [ReadSchemaId(2)]),
            BundleId::from_sections([SchemaId(1), SchemaId(2)], []),
        );
        // Adding a read schema moves the fingerprint even though the write section is untouched.
        assert_ne!(
            write_only(&[1]),
            BundleId::from_sections([SchemaId(1)], [ReadSchemaId(1)]),
        );
    }

    #[test]
    #[should_panic(expected = "read ids must be pre-sorted")]
    fn from_sections_rejects_unsorted_input() {
        BundleId::from_sections([], [ReadSchemaId(2), ReadSchemaId(1)]);
    }
}
