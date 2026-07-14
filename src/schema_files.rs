//! Derives a chunk's parquet file set from its schema: one `<table>.parquet` per table, filtered
//! by the chunk's table-presence bitmap (`None` = all tables).

use crate::types::DatasetSchema;

// TODO(wire-format): unused until assignment serialization derives files from loaded schemas.
#[allow(dead_code)]
pub(crate) fn chunk_files(schema: &DatasetSchema, bitmap: Option<&bit_vec::BitVec>) -> Vec<String> {
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

    use crate::types::{DatasetSchema, TableSchema};

    #[test]
    fn table_bitmap_selects_files_in_schema_order() {
        let schema = DatasetSchema::new(BTreeMap::from([
            ("blocks".to_owned(), TableSchema::default()),
            ("logs".to_owned(), TableSchema::default()),
            ("transactions".to_owned(), TableSchema::default()),
        ]));
        let mask = bit_vec::BitVec::from_fn(3, |i| i != 1);

        assert_eq!(
            super::chunk_files(&schema, Some(&mask)),
            vec![
                "blocks.parquet".to_owned(),
                "transactions.parquet".to_owned()
            ]
        );
        // No bitmap = all tables.
        assert_eq!(super::chunk_files(&schema, None).len(), 3);
    }
}
