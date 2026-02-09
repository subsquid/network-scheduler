use std::{collections::BTreeMap, path::Path, sync::Arc};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::types::{Chunk, ChunkSummary, Worker};

/// CLI mode state file format containing workers and known chunks
#[derive(Debug, Serialize, Deserialize)]
pub struct CliState {
    pub workers: Vec<Worker>,
    #[serde(default)]
    pub known_chunks: BTreeMap<String, Vec<ChunkConfig>>,
}

/// Chunk configuration in the state file
#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkConfig {
    pub id: String,
    pub size: u32,
    pub files: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<ChunkSummary>,
}

impl CliState {
    /// Load CLI state from a YAML or JSON file
    pub fn load(path: &Path) -> Result<Self> {
        let content =
            std::fs::read_to_string(path).context("Failed to read CLI state file")?;

        serde_yaml::from_str(&content)
            .context("Failed to parse CLI state file")
    }

    /// Convert chunk configs to Chunk types grouped by dataset
    pub fn to_chunks(&self) -> Result<BTreeMap<Arc<String>, Vec<Chunk>>> {
        let mut result = BTreeMap::new();

        for (dataset_name, chunk_configs) in &self.known_chunks {
            let dataset = Arc::new(format!("s3://{}", dataset_name));
            let chunks: Vec<Chunk> = chunk_configs
                .iter()
                .map(|cc| {
                    Chunk::new(
                        dataset.clone(),
                        cc.id.clone(),
                        cc.size,
                        cc.files.clone(),
                    )
                    .map(|mut chunk| {
                        chunk.summary = cc.summary.clone();
                        chunk
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            result.insert(dataset, chunks);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_yaml_state() {
        let yaml_content = r#"
workers:
  - peer_id: "12D3KooWR1VSmCyVGTc8ewcm3LW82VeT68rjCKjBvGfpkt6ALxPx"
    status: online
  - peer_id: "12D3KooWDmLTHk3sBPHLZuN3Et3Zte7vLKwAApqXN7BNCeXijaQ2"
    status: offline

known_chunks:
  ethereum-mainnet-1:
    - id: "0018197829/0018246541-0018248424-c7ed95c9"
      size: 1000000
      files: ["blocks.parquet", "transactions.parquet"]
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();

        let state = CliState::load(temp_file.path()).unwrap();
        assert_eq!(state.workers.len(), 2);
        assert_eq!(state.known_chunks.len(), 1);
    }

    #[test]
    fn test_workers_deserialization() {
        use crate::types::WorkerStatus;

        let yaml_content = r#"
workers:
  - peer_id: "12D3KooWR1VSmCyVGTc8ewcm3LW82VeT68rjCKjBvGfpkt6ALxPx"
    status: online
known_chunks: {}
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();

        let state = CliState::load(temp_file.path()).unwrap();
        assert_eq!(state.workers.len(), 1);
        assert_eq!(state.workers[0].status, WorkerStatus::Online);
    }
}
