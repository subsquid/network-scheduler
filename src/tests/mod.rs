pub mod input;
mod scheduling;
mod utils;

use std::collections::BTreeMap;
use std::time::Duration;

use crate::cli;

pub fn test_config() -> cli::Config {
    cli::Config {
        datasets: BTreeMap::new(),
        worker_inactive_timeout: Duration::from_secs(600),
        ignore_reliability: false,
        worker_storage_bytes: 0,
        worker_stale_bytes: 0,
        min_replication: 1,
        saturation: 0.99,
        network: "test".to_string(),
        storage_domain: "test.io".to_string(),
        network_state_name: "test.json".to_string(),
        network_state_url: "https://test.io".to_string(),
        scheduler_state_bucket: "test".to_string(),
        cloudflare_storage_secret: "secret".to_string(),
        min_supported_worker_version: "2.0.0".parse().unwrap(),
        min_recommended_worker_version: "2.0.0".parse().unwrap(),
        assignment_delay: Duration::from_secs(60),
        assignment_ttl: Duration::from_secs(86400),
        concurrent_dataset_downloads: 1,
        strict_continuity_check: false,
        storage_allow_insecure_scheme: false,
        clear_last_block_hash: false,
    }
}
