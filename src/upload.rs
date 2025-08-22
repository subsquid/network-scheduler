use std::io::Write;

use anyhow::Context;
use aws_sdk_s3 as s3;
use chrono::Utc;
use flate2::{Compression, write::GzEncoder};
use sha2::{Digest, Sha256};
use sqd_messages::assignments::{Assignment as EncodedAssignment, NetworkAssignment, NetworkState};
use tracing::instrument;

use crate::{
    cli,
    types::{SchedulingStatus, SchedulingStatusConfig, Worker},
    utils::CountWrite,
};

pub struct Uploader {
    config: cli::Config,
    client: s3::Client,
    time: chrono::DateTime<Utc>,
}

impl Uploader {
    pub fn new(config: cli::Config, s3_config: &aws_config::SdkConfig) -> Self {
        let client = s3::Client::new(s3_config);
        let time = Utc::now();
        crate::metrics::ASSIGNMENT_TIMESTAMP.set(time.timestamp());
        Self {
            config,
            client,
            time,
        }
    }

    #[instrument(skip_all)]
    pub async fn upload_assignment(
        &self,
        assignment: EncodedAssignment,
        fb_assignment_v0: Vec<u8>,
        fb_assignment_v1: Vec<u8>,
    ) -> anyhow::Result<String> {
        tracing::debug!("Encoding assignment");
        let compressed_json;
        {
            let _timer = crate::metrics::Timer::new("encode_assignment_json");
            let encoder = GzEncoder::new(Vec::new(), Compression::best());
            let mut count_encoder = CountWrite::new(encoder);
            serde_json::to_writer(&mut count_encoder, &assignment)?;
            let written = count_encoder.count();
            let encoder = count_encoder.into_inner();
            compressed_json = encoder.finish()?;
            crate::metrics::ASSIGNMENT_JSON_SIZE.set(written as i64);
            crate::metrics::ASSIGNMENT_COMPRESSED_JSON_SIZE.set(compressed_json.len() as i64);
        }
        let compressed_fb_v0;
        {
            let _timer = crate::metrics::Timer::new("encode_assignment");
            let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
            encoder.write_all(&fb_assignment_v0)?;
            compressed_fb_v0 = encoder.finish()?;
        }
        let compressed_fb_v1;
        {
            let _timer = crate::metrics::Timer::new("encode_assignment");
            let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
            encoder.write_all(&fb_assignment_v1)?;
            compressed_fb_v1 = encoder.finish()?;
            crate::metrics::ASSIGNMENT_FB_SIZE.set(fb_assignment_v1.len() as i64);
            crate::metrics::ASSIGNMENT_COMPRESSED_FB_SIZE.set(compressed_fb_v1.len() as i64);
        }

        let hash = {
            let _timer = crate::metrics::Timer::new("hash_assignment");
            Sha256::digest(&compressed_json)
        };

        tracing::debug!("Saving assignment");
        let _timer = crate::metrics::Timer::new("upload_assignment");
        let network = self.config.network.clone();
        let system_time = std::time::SystemTime::now();
        let timestamp = self.time.format("%FT%T");
        let assignment_id = format!("{timestamp}_{hash:X}");
        let json_filename: String = format!("assignments/{network}/{assignment_id}.json.gz");
        let fb_v0_filename: String = format!("assignments/{network}/{assignment_id}.fb.0.gz");
        let fb_v1_filename: String = format!("assignments/{network}/{assignment_id}.fb.1.gz");
        let expiration = s3::primitives::DateTime::from(system_time + self.config.assignment_ttl);

        let json_fut = self
            .client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            .key(&json_filename)
            .expires(expiration)
            .body(compressed_json.into())
            .send();
        let fb_v0_fut = self
            .client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            .key(&fb_v0_filename)
            .expires(expiration)
            .body(compressed_fb_v0.into())
            .send();
        let fb_v1_fut = self
            .client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            .key(&fb_v1_filename)
            .expires(expiration)
            .body(compressed_fb_v1.into())
            .send();
        tokio::try_join!(json_fut, fb_v0_fut, fb_v1_fut).context("Can't upload assignment")?;

        let effective_from = (system_time.duration_since(std::time::UNIX_EPOCH).unwrap()
            + self.config.assignment_delay)
            .as_secs();

        let json_url = format!("{}/{json_filename}", self.config.network_state_url);
        let fb_url_v0 = format!("{}/{fb_v0_filename}", self.config.network_state_url);
        let fb_url_v1 = format!("{}/{fb_v1_filename}", self.config.network_state_url);
        let network_state = NetworkState {
            network,
            assignment: NetworkAssignment {
                url: json_url.clone(),
                fb_url: Some(fb_url_v0),
                fb_url_v1: Some(fb_url_v1),
                id: assignment_id,
                effective_from,
            },
        };
        let contents = serde_json::to_vec(&network_state).unwrap();
        self.client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            .key(&self.config.network_state_name)
            .body(contents.into())
            .send()
            .await
            .context("Can't save link to the assignment")?;

        Ok(json_url)
    }

    #[instrument(skip_all)]
    pub async fn upload_status(&self, workers: Vec<Worker>) -> anyhow::Result<()> {
        let status = SchedulingStatus {
            config: SchedulingStatusConfig {
                supported_worker_versions: self.config.supported_worker_versions.clone(),
                recommended_worker_versions: self.config.recommended_worker_versions.clone(),
            },
            assignment_timestamp_sec: self.time.timestamp() as u64,
            workers,
        };
        let contents = serde_json::to_vec(&status)?;
        self.client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            .key(format!("scheduler/{}/status.json", self.config.network))
            .body(contents.into())
            .content_type("application/json")
            .send()
            .await
            .context("Can't upload worker status")?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn upload_metrics(&self, metrics: String) -> anyhow::Result<()> {
        let contents = metrics.into_bytes();
        self.client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            .key(format!("scheduler/{}/metrics.txt", self.config.network))
            .body(contents.into())
            .content_type("application/openmetrics-text; version=1.0.0; charset=utf-8")
            .send()
            .await
            .context("Can't upload metrics")?;
        Ok(())
    }
}
