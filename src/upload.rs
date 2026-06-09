use std::io::Write;

use anyhow::Context;
use aws_sdk_s3 as s3;
use chrono::Utc;
use flate2::{Compression, write::GzEncoder};
use sha2::{Digest, Sha256};
use sqd_assignments::{NetworkAssignment, NetworkState};
use tracing::instrument;

use crate::{
    cli,
    types::{Dataset, SchedulingStatus, SchedulingStatusConfig, Worker},
};

pub struct Uploader {
    config: cli::Config,
    client: s3::Client,
    time: chrono::DateTime<Utc>,
}

#[cfg(feature = "mvcc-chunks")]
pub struct AssignmentUrls {
    pub legacy: String,
    pub worker: String,
    pub portal: String,
}

impl Uploader {
    pub fn new(config: cli::Config, sdk_config: &aws_config::SdkConfig) -> Self {
        let s3_config = aws_sdk_s3::config::Builder::from(sdk_config)
            .force_path_style(true)
            .build();

        let client = s3::Client::from_conf(s3_config);
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
        fb_assignment_v0: Vec<u8>,
        fb_assignment_v1: Vec<u8>,
    ) -> anyhow::Result<String> {
        let assignment = self
            .upload_assignment_artifact(None, fb_assignment_v0, fb_assignment_v1)
            .await?;

        let network_state = legacy_network_state(self.config.network.clone(), assignment);
        let fb_url_v1 = assignment_fb_url_v1(&network_state.assignment)?;
        self.upload_network_state(network_state).await?;

        Ok(fb_url_v1)
    }

    #[cfg(feature = "mvcc-chunks")]
    pub async fn upload_assignments(
        &self,
        legacy: (Vec<u8>, Vec<u8>),
        worker: (Vec<u8>, Vec<u8>),
        portal: (Vec<u8>, Vec<u8>),
    ) -> anyhow::Result<AssignmentUrls> {
        let legacy = self
            .upload_assignment_artifact(None, legacy.0, legacy.1)
            .await?;
        let worker = self
            .upload_assignment_artifact(Some("worker"), worker.0, worker.1)
            .await?;
        let portal = self
            .upload_assignment_artifact(Some("portal"), portal.0, portal.1)
            .await?;

        let urls = AssignmentUrls {
            legacy: assignment_fb_url_v1(&legacy).context("Legacy assignment URL missing")?,
            worker: assignment_fb_url_v1(&worker).context("Worker assignment URL missing")?,
            portal: assignment_fb_url_v1(&portal).context("Portal assignment URL missing")?,
        };

        let network_state =
            split_network_state(self.config.network.clone(), legacy, worker, portal);
        self.upload_network_state(network_state).await?;

        Ok(urls)
    }

    #[allow(deprecated)]
    async fn upload_assignment_artifact(
        &self,
        kind: Option<&str>,
        fb_assignment_v0: Vec<u8>,
        fb_assignment_v1: Vec<u8>,
    ) -> anyhow::Result<NetworkAssignment> {
        tracing::debug!("Compressing assignment");
        let compressed_fb_v0;
        {
            let _timer = crate::metrics::Timer::new("compress_assignment");
            let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
            encoder.write_all(&fb_assignment_v0)?;
            compressed_fb_v0 = encoder.finish()?;
        }
        let compressed_fb_v1;
        {
            let _timer = crate::metrics::Timer::new("compress_assignment");
            let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
            encoder.write_all(&fb_assignment_v1)?;
            compressed_fb_v1 = encoder.finish()?;
            crate::metrics::ASSIGNMENT_FB_SIZE.set(fb_assignment_v1.len() as i64);
            crate::metrics::ASSIGNMENT_COMPRESSED_FB_SIZE.set(compressed_fb_v1.len() as i64);
        }

        let hash = {
            let _timer = crate::metrics::Timer::new("hash_assignment");
            Sha256::digest(&compressed_fb_v1)
        };

        tracing::debug!("Saving assignment");
        let _timer = crate::metrics::Timer::new("upload_assignment");
        let network = self.config.network.clone();
        let system_time = std::time::SystemTime::now();
        let timestamp = self.time.format("%FT%T");
        let assignment_id = format!("{timestamp}_{hash:X}");
        let (fb_v0_filename, fb_v1_filename) = match kind {
            Some(kind) => (
                format!("assignments/{network}/{kind}/{assignment_id}.fb.0.gz"),
                format!("assignments/{network}/{kind}/{assignment_id}.fb.1.gz"),
            ),
            None => (
                format!("assignments/{network}/{assignment_id}.fb.0.gz"),
                format!("assignments/{network}/{assignment_id}.fb.1.gz"),
            ),
        };
        let expiration = s3::primitives::DateTime::from(system_time + self.config.assignment_ttl);

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
        tokio::try_join!(fb_v0_fut, fb_v1_fut).context("Can't upload assignment")?;

        let effective_from = (system_time.duration_since(std::time::UNIX_EPOCH).unwrap()
            + self.config.assignment_delay)
            .as_secs();

        let fb_url_v0 = format!("{}/{fb_v0_filename}", self.config.network_state_url);
        let fb_url_v1 = format!("{}/{fb_v1_filename}", self.config.network_state_url);
        Ok(NetworkAssignment {
            #[cfg(not(feature = "mvcc-chunks"))]
            url: Some(String::new()),
            #[cfg(feature = "mvcc-chunks")]
            url: None,
            fb_url: Some(fb_url_v0),
            fb_url_v1: Some(fb_url_v1),
            id: assignment_id,
            effective_from,
        })
    }

    async fn upload_network_state(&self, network_state: NetworkState) -> anyhow::Result<()> {
        let contents =
            serde_json::to_vec(&network_state).context("Can't serialize network state")?;
        self.client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            .key(&self.config.network_state_name)
            .body(contents.into())
            .send()
            .await
            .context("Can't save link to the assignment")?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn upload_status(
        &self,
        workers: Vec<Worker>,
        datasets: Vec<Dataset>,
    ) -> anyhow::Result<()> {
        let system_time = std::time::SystemTime::now();
        let effective_from = (system_time.duration_since(std::time::UNIX_EPOCH).unwrap()
            + self.config.assignment_delay)
            .as_secs();

        let status = SchedulingStatus {
            config: SchedulingStatusConfig {
                supported_worker_versions: format!(
                    ">={}",
                    self.config.min_supported_worker_version
                ),
                recommended_worker_versions: format!(
                    ">={}",
                    self.config.min_recommended_worker_version
                ),
            },
            assignment_timestamp_sec: self.time.timestamp() as u64,
            effective_from,
            workers,
            datasets,
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

fn legacy_network_state(network: String, assignment: NetworkAssignment) -> NetworkState {
    NetworkState {
        network,
        assignment,
        #[cfg(feature = "mvcc-chunks")]
        worker_assignment: None,
        #[cfg(feature = "mvcc-chunks")]
        portal_assignment: None,
    }
}

fn assignment_fb_url_v1(assignment: &NetworkAssignment) -> anyhow::Result<String> {
    assignment
        .fb_url_v1
        .clone()
        .ok_or_else(|| anyhow::anyhow!("fb_url_v1 missing"))
}

#[cfg(feature = "mvcc-chunks")]
fn split_network_state(
    network: String,
    legacy: NetworkAssignment,
    worker: NetworkAssignment,
    portal: NetworkAssignment,
) -> NetworkState {
    NetworkState {
        network,
        assignment: legacy,
        worker_assignment: Some(worker),
        portal_assignment: Some(portal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(deprecated)]
    fn assignment(id: &str) -> NetworkAssignment {
        NetworkAssignment {
            #[cfg(not(feature = "mvcc-chunks"))]
            url: Some(String::new()),
            #[cfg(feature = "mvcc-chunks")]
            url: None,
            fb_url: Some(format!("https://example.test/{id}.fb.0.gz")),
            fb_url_v1: Some(format!("https://example.test/{id}.fb.1.gz")),
            id: id.to_string(),
            effective_from: 1781000000,
        }
    }

    #[test]
    fn legacy_network_state_has_compatibility_shape() {
        let state = legacy_network_state("testnet".to_string(), assignment("legacy"));
        let value = serde_json::to_value(state).unwrap();

        assert_eq!(value["network"], "testnet");
        assert_eq!(value["assignment"]["id"], "legacy");
        #[cfg(not(feature = "mvcc-chunks"))]
        assert_eq!(value["assignment"]["url"], "");
        #[cfg(feature = "mvcc-chunks")]
        assert!(value["assignment"].get("url").is_none());
        assert!(value.get("worker_assignment").is_none());
        assert!(value.get("portal_assignment").is_none());
    }

    #[cfg(feature = "mvcc-chunks")]
    #[test]
    fn split_network_state_has_worker_and_portal_assignments() {
        let state = split_network_state(
            "testnet".to_string(),
            assignment("legacy"),
            assignment("worker"),
            assignment("portal"),
        );
        let value = serde_json::to_value(state).unwrap();

        assert_eq!(value["assignment"]["id"], "legacy");
        assert_eq!(value["worker_assignment"]["id"], "worker");
        assert_eq!(value["portal_assignment"]["id"], "portal");
    }
}
