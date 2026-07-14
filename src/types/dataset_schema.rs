use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A dataset's data schema: its tables, the fields of each table, and the fields returned by
/// default. Stored once per dataset (see the `schemas` table) rather than per chunk.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSchema {
    tables: BTreeMap<String, TableSchema>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub fields: Vec<String>,
    /// The implicit projection for callers that omit a column list; a subset of `fields`.
    pub default_fields: Vec<String>,
}

impl DatasetSchema {
    pub fn new(tables: BTreeMap<String, TableSchema>) -> Self {
        Self { tables }
    }

    pub fn tables(&self) -> &BTreeMap<String, TableSchema> {
        &self.tables
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let mut lowercased: BTreeMap<String, &str> = BTreeMap::new();
        for (table, ts) in &self.tables {
            let trimmed = table.trim();
            if trimmed != table
                || trimmed.is_empty()
                || table == "."
                || table == ".."
                || table.contains(['/', '\\'])
                || table.chars().any(char::is_control)
            {
                anyhow::bail!("invalid table name {table:?}: must be a plain file stem");
            }
            if let Some(prev) = lowercased.insert(table.to_lowercase(), table) {
                anyhow::bail!("table names {prev:?} and {table:?} collide case-insensitively");
            }
            let fields: std::collections::BTreeSet<&str> =
                ts.fields.iter().map(String::as_str).collect();
            for default in &ts.default_fields {
                if !fields.contains(default.as_str()) {
                    anyhow::bail!("table {table:?}: default field {default:?} is not in `fields`");
                }
            }
        }
        Ok(())
    }

    pub fn canonicalized(&self) -> Self {
        let mut canon = self.clone();
        for table in canon.tables.values_mut() {
            table.fields.sort();
            table.fields.dedup();
            table.default_fields.sort();
            table.default_fields.dedup();
        }
        canon
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_table(name: &str, table: TableSchema) -> DatasetSchema {
        let mut schema = DatasetSchema::default();
        schema.tables.insert(name.to_owned(), table);
        schema
    }

    #[test]
    fn validate_rejects_unsafe_table_names() {
        for bad in [
            "a/b", "..", ".", "", "a\\b", "a\nb", "a\u{7}b", "   ", " a", "a ",
        ] {
            assert!(
                with_table(bad, TableSchema::default()).validate().is_err(),
                "table name {bad:?} must be rejected (it becomes a file stem)"
            );
        }
    }

    #[test]
    fn validate_rejects_case_insensitive_table_collisions() {
        let mut schema = DatasetSchema::default();
        schema
            .tables
            .insert("Blocks".into(), TableSchema::default());
        schema
            .tables
            .insert("blocks".into(), TableSchema::default());
        // `Blocks.parquet` and `blocks.parquet` are one file on a case-insensitive worker disk.
        assert!(schema.validate().is_err());
    }

    #[test]
    fn canonicalized_sorts_and_dedups_field_lists() {
        let table = TableSchema {
            fields: vec!["b".into(), "a".into(), "a".into()],
            default_fields: vec!["b".into(), "a".into()],
        };
        let canon = with_table("blocks", table).canonicalized();
        let ts = &canon.tables["blocks"];
        assert_eq!(ts.fields, vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(ts.default_fields, vec!["a".to_owned(), "b".to_owned()]);
    }

    #[test]
    fn validate_rejects_default_field_not_in_fields() {
        let table = TableSchema {
            fields: vec!["a".into()],
            default_fields: vec!["missing".into()],
        };
        assert!(with_table("blocks", table).validate().is_err());
    }
}
