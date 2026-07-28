//! Input type for dataset creation.

use crate::dataset_schema::DatasetSchema;

/// A dataset to create.
#[derive(Debug, Clone, PartialEq)]
pub struct NewDataset {
    /// Identity the scheduler groups and weights by; the `location` with any `<scheme>://` stripped.
    pub name: String,
    /// Storage path an assignment points workers at (e.g. `s3://base-sepolia`).
    pub location: String,
    /// Initial read schema.
    pub schema: DatasetSchema,
}

impl NewDataset {
    /// Create from a storage `location`; `name` is the scheme-stripped location.
    /// Use [`with_name`](Self::with_name) to set a name distinct from that.
    pub fn new(location: impl Into<String>, schema: DatasetSchema) -> Self {
        let location = location.into();
        let name = strip_scheme(&location).to_owned();
        Self {
            name,
            location,
            schema,
        }
    }

    /// Create with an explicit `name` distinct from `location`.
    pub fn with_name(
        name: impl Into<String>,
        location: impl Into<String>,
        schema: DatasetSchema,
    ) -> Self {
        Self {
            name: name.into(),
            location: location.into(),
            schema,
        }
    }

    /// Check the naming invariant: `location` must carry a `<scheme>://`, `name` must not.
    /// Constructors skip this — call it at the creation boundary (e.g. the HTTP handler) so fixtures aren't forced through.
    ///
    /// # Errors
    ///
    /// If `location` has no scheme, or `name` contains one.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.location.contains("://"),
            "location {:?} must include a scheme (e.g. s3://)",
            self.location
        );
        anyhow::ensure!(
            !self.name.contains("://"),
            "name {:?} must not include a scheme",
            self.name
        );
        Ok(())
    }
}

/// Strip a leading `<scheme>://` from a storage path; unchanged if it has none.
fn strip_scheme(location: &str) -> &str {
    location
        .split_once("://")
        .map_or(location, |(_scheme, rest)| rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_derives_name_from_location() {
        let d = NewDataset::new("s3://base-sepolia", DatasetSchema::default());
        assert_eq!(d.location, "s3://base-sepolia");
        assert_eq!(d.name, "base-sepolia");
        let d = NewDataset::new("base-sepolia", DatasetSchema::default());
        assert_eq!(d.location, "base-sepolia");
        assert_eq!(d.name, "base-sepolia");
    }

    #[test]
    fn with_name_keeps_an_explicit_name() {
        let d = NewDataset::with_name("eth", "s3://ethereum-mainnet", DatasetSchema::default());
        assert_eq!(d.name, "eth");
        assert_eq!(d.location, "s3://ethereum-mainnet");
    }

    #[test]
    fn validate_requires_scheme_on_location_but_not_name() {
        let ok = |n: &str, l: &str| {
            NewDataset::with_name(n, l, DatasetSchema::default())
                .validate()
                .is_ok()
        };
        // Canonical: location has a scheme, name doesn't.
        assert!(ok("base-sepolia", "s3://base-sepolia"));
        assert!(
            NewDataset::new("s3://base-sepolia", DatasetSchema::default())
                .validate()
                .is_ok()
        );
        assert!(!ok("base-sepolia", "base-sepolia"));
        assert!(!ok("s3://base-sepolia", "s3://base-sepolia"));
    }
}
