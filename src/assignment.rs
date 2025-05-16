pub struct AssignmentStorage {
    client: s3::Client,
}

impl AssignmentStorage {
    pub async fn save_assignment(&self, assignment: Assignment) {
        tracing::debug!("Encoding assignment");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        let _ = encoder.write_all(serde_json::to_vec(&assignment).unwrap().as_slice());
        let compressed_bytes = encoder.finish().unwrap();
        tracing::debug!("Saving assignment");
        let mut hasher = Sha256::new();
        hasher.update(compressed_bytes.as_slice());
        let hash = hasher.finalize();
        let network = Config::get().network.clone();
        let current_time = Utc::now();
        let timestamp = current_time.format("%FT%T");
        let filename: String = format!("assignments/{network}/{timestamp}_{hash:X}.json.gz");

        let saving_result = self
            .client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            //.bucket("network-scheduler-state")
            .key(&filename)
            .body(compressed_bytes.into())
            .send()
            .await;
        prometheus_metrics::s3_request();
        match saving_result {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Error saving assignment: {e:?}");
                return;
            }
        }

        let system_time = std::time::SystemTime::now();
        let effective_from = (system_time.duration_since(std::time::UNIX_EPOCH).unwrap()
            + Config::get().assignment_delay)
            .as_secs();

        let network_state = NetworkState {
            network: Config::get().network.clone(),
            assignment: NetworkAssignment {
                url: format!("{}/{filename}", Config::get().network_state_url),
                id: format!("{timestamp}_{hash:X}"),
                effective_from,
            },
        };
        let contents = serde_json::to_vec(&network_state).unwrap();
        let _ = self
            .client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            //.bucket("network-scheduler-state")
            .key(Config::get().network_state_name.clone())
            .body(contents.into())
            .send()
            .await
            .map_err(|e| tracing::error!("Error saving link to assignment: {e:?}"));
        prometheus_metrics::s3_request();
    }
}
