//! The dataset schemas of every chunk currently in play, with a content [`BundleId`] clients use
//! to detect changes. A per-cycle, DB-level snapshot ([`SchedulerStorage::active_schema_bundle`]).

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use super::{SchedulerStorage, SchemaId, StorageError, WorkerAssignmentChunk};
use crate::types::DatasetSchema;

/// Content id of a [`SchemaBundle`]: SHA-256 over its sorted `schema_id`s. Ids are content-deduped
/// serials, so equal ids ⇒ equal content — but only within one storage instance, not across DBs.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BundleId([u8; 32]);

impl BundleId {
    /// `SchemaBundle` is the only production caller for now, and its ids come pre-sorted from a
    /// `BTreeMap` — order isn't normalized here, so a caller with unsorted ids must sort first
    /// (checked in debug builds).
    pub(crate) fn from_schema_ids(ids: impl IntoIterator<Item = SchemaId>) -> Self {
        let ids: Vec<SchemaId> = ids.into_iter().collect();
        #[cfg(debug_assertions)]
        {
            let mut sorted = ids.clone();
            sorted.sort_unstable();
            debug_assert_eq!(
                ids, sorted,
                "from_schema_ids: caller must pass pre-sorted ids"
            );
        }
        let mut hasher = Sha256::new();
        for id in &ids {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaBundle {
    id: BundleId,
    schemas: BTreeMap<SchemaId, DatasetSchema>,
}

impl SchemaBundle {
    /// Load the in-play schemas (one round-trip) and stamp the content id.
    pub fn generate(storage: &impl SchedulerStorage) -> Result<Self, StorageError> {
        let schemas = storage.active_schema_bundle()?;
        let id = BundleId::from_schema_ids(schemas.keys().copied());
        Ok(Self { id, schemas })
    }

    pub fn id(&self) -> BundleId {
        self.id
    }

    pub fn schemas(&self) -> &BTreeMap<SchemaId, DatasetSchema> {
        &self.schemas
    }

    pub fn get(&self, id: SchemaId) -> Option<&DatasetSchema> {
        self.schemas.get(&id)
    }

    pub fn contains(&self, id: SchemaId) -> bool {
        self.schemas.contains_key(&id)
    }

    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
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
        let id = BundleId::from_schema_ids(schemas.keys().copied());
        SchemaBundle { id, schemas }
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

    #[test]
    fn bundle_id_is_content_addressed() {
        // Deterministic and set-sensitive.
        assert_eq!(
            BundleId::from_schema_ids([SchemaId(1), SchemaId(2)]),
            BundleId::from_schema_ids([SchemaId(1), SchemaId(2)]),
        );
        assert_ne!(
            BundleId::from_schema_ids([SchemaId(1)]),
            BundleId::from_schema_ids([SchemaId(1), SchemaId(2)]),
        );
        assert_ne!(
            BundleId::from_schema_ids([SchemaId(1)]),
            BundleId::from_schema_ids([SchemaId(2)]),
        );
    }

    #[test]
    #[should_panic(expected = "caller must pass pre-sorted ids")]
    fn from_schema_ids_rejects_unsorted_input() {
        BundleId::from_schema_ids([SchemaId(2), SchemaId(1)]);
    }
}
