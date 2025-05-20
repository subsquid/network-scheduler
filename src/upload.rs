use std::io::Write;

use anyhow::Context;
use aws_sdk_s3 as s3;
use chrono::Utc;
use flate2::{Compression, write::GzEncoder};
use sha2::{Digest, Sha256};
use sqd_messages::assignments::{Assignment as EncodedAssignment, NetworkAssignment, NetworkState};

use crate::cli::Config;

pub struct Uploader {
    client: s3::Client,
}

impl Uploader {
    pub fn new(s3_config: &aws_config::SdkConfig) -> Self {
        let client = s3::Client::new(s3_config);
        Self { client }
    }

    pub async fn save_assignment(
        &self,
        assignment: EncodedAssignment,
        config: &Config,
    ) -> anyhow::Result<String> {
        tracing::debug!("Encoding assignment");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        let _ = encoder.write_all(serde_json::to_vec(&assignment).unwrap().as_slice());
        let compressed_bytes = encoder.finish().unwrap();

        tracing::debug!("Saving assignment");
        let hash = Sha256::digest(&compressed_bytes);
        let network = config.network.clone();
        let system_time = std::time::SystemTime::now();
        let chrono_time = Utc::now();
        let timestamp = chrono_time.format("%FT%T");
        let assignment_id = format!("{timestamp}_{hash:X}");
        let filename: String = format!("assignments/{network}/{assignment_id}.json.gz");
        let expiration = s3::primitives::DateTime::from(system_time + config.assignment_ttl);

        self.client
            .put_object()
            .bucket(&config.scheduler_state_bucket)
            .key(&filename)
            .expires(expiration)
            .body(compressed_bytes.into())
            .send()
            .await
            .context("Can't upload assignment")?;

        let effective_from = (system_time.duration_since(std::time::UNIX_EPOCH).unwrap()
            + config.assignment_delay)
            .as_secs();

        let assignment_url = format!("{}/{filename}", config.network_state_url);
        let network_state = NetworkState {
            network: config.network.clone(),
            assignment: NetworkAssignment {
                url: assignment_url.clone(),
                id: assignment_id,
                effective_from,
            },
        };
        let contents = serde_json::to_vec(&network_state).unwrap();
        self.client
            .put_object()
            .bucket(&config.scheduler_state_bucket)
            .key(&config.network_state_name)
            .body(contents.into())
            .send()
            .await
            .context("Can't save link to the assignment")?;

        Ok(assignment_url)
    }
}
