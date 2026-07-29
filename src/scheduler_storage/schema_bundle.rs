//! The dataset schemas a published assignment reference, with a content [`BundleId`] clients use
//! to detect changes. Two sections: the WRITE schemas the assignment's chunks are pinned to (built
//! from those chunks and frozen with the assignment, so the two can't drift) and the current READ
//! schemas of the datasets in play.
//!
//! The fingerprint covers both, so promoting a read schema moves it — that is what lets a portal
//! cache the bundle and re-fetch only when the set actually changed.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use super::{ReadSchemaId, SchemaId, WorkerAssignmentChunk};
#[cfg(test)]
use super::{SchedulerStorage, StorageError};
use crate::types::DatasetSchema;

/// Domain tag: keeps this fingerprint from colliding with any other SHA-256 over id lists, and
/// pins the encoding so a future format change is a different id rather than a silent collision.
const BUNDLE_ID_DOMAIN: &[u8] = b"sqd.schema-bundle.v1";

/// Content id of a [`SchemaBundle`]: SHA-256 over its sorted write- and read-schema ids. Ids are
/// content-deduped serials, so equal ids ⇒ equal content — but only within one storage instance,
/// not across DBs.
///
/// The two sections are hashed separately, each length-prefixed behind its own tag. `schemas.id`
/// and `read_schemas.id` are independent `SERIAL`s that hand out the same small integers, so a
/// flat hash over both would let write-{3} and read-{3} collide; the tags and lengths make the
/// section a value carries part of its identity. Primary keys are never reused, so an id in an
/// already-published bundle can never later mean a different schema.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BundleId([u8; 32]);

impl BundleId {
    /// `SchemaBundle` is the only production caller for now, and its ids come pre-sorted from a
    /// `BTreeMap` — order isn't normalized here, so a caller with unsorted ids must sort first
    /// (checked in debug builds).
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

/// In debug builds, catch a caller passing ids a `BTreeMap` didn't already sort.
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

/// Two sections: the WRITE schemas chunks are pinned to (a worker derives a chunk's file set from
/// them) and the current READ schemas (what a portal validates queries against). They key on
/// disjoint id spaces and are never interchangeable — see [`BundleId`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaBundle {
    id: BundleId,
    schemas: BTreeMap<SchemaId, DatasetSchema>,
    read_schemas: BTreeMap<ReadSchemaId, DatasetSchema>,
}

impl SchemaBundle {
    /// Wrap the assignment's schemas and stamp the content id.
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

    /// Snapshot of the in-play schemas for tests; production uses the bundle returned from
    /// `run_scheduling_cycle`.
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

    /// The chunk's `<table>.parquet` files from its pinned schema and `tables_present` bitmap.
    /// `None` if the schema is absent from the bundle (an invariant violation the caller surfaces).
    pub fn chunk_files(&self, chunk: &WorkerAssignmentChunk) -> Option<Vec<String>> {
        self.schemas
            .get(&chunk.schema_id)
            .map(|schema| chunk_files(schema, chunk.tables_present.as_ref()))
    }
}

/// A schema's `<table>.parquet` files, filtered by a chunk's table-presence bitmap (`None` = all
/// tables).
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
            Some(vec![
                "blocks.parquet".to_owned(),
                "transactions.parquet".to_owned(),
            ])
        );
        assert_eq!(
            b.chunk_files(&chunk(99, None)),
            None,
            "unknown schema → None"
        );
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

    /// The two id spaces are independent SERIALs handing out the same integers, so the fingerprint
    /// must distinguish which section an id sits in — otherwise promoting a read schema whose id
    /// happens to match a write id would leave the bundle looking unchanged.
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
