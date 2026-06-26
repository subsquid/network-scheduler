use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_sdk_s3 as s3;
use tokio::io::AsyncSeekExt;
use tracing::instrument;

use crate::metrics;
use crate::types::Chunk;
use futures::{
    TryStreamExt,
    stream::{self, StreamExt},
};

const CONCURRENT_CHUNKS: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ContinuityGap {
    expected_block: u64,
    actual_block: u64,
}

/// Returns the next expected block after accepting this chunk.
fn advance_continuity(
    next_expected_block: Option<u64>,
    chunk_start: u64,
    chunk_end: u64,
) -> Result<u64, ContinuityGap> {
    if let Some(expected) = next_expected_block {
        if chunk_start != expected {
            return Err(ContinuityGap {
                expected_block: expected,
                actual_block: chunk_start,
            });
        }
    }
    Ok(chunk_end + 1)
}

/// How many times to attempt downloading a chunk's summary before giving up.
/// The S3 endpoint occasionally stalls mid-stream (the AWS SDK reports it as
/// "throughput of 0 B/s was observed"), which is transient and usually clears
/// on a retry. Without this, a single stall discards the whole run's progress
/// for the dataset, so a large new dataset can fail to ever finish indexing.
const SUMMARY_DOWNLOAD_ATTEMPTS: u32 = 5;

/// Base delay for the exponential backoff between summary download attempts.
const SUMMARY_RETRY_BASE_DELAY: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct S3Storage {
    client: s3::Client,
}

impl S3Storage {
    pub fn new(sdk_config: &aws_config::SdkConfig) -> Self {
        let s3_config = aws_sdk_s3::config::Builder::from(sdk_config)
            .force_path_style(true)
            .build();

        let client = s3::Client::from_conf(s3_config);
        Self { client }
    }

    pub async fn load_newer_chunks(
        &self,
        datasets: impl IntoIterator<Item = (Arc<String>, Option<&Chunk>)>,
        concurrent_downloads: usize,
        dataset_load_timeout: Option<Duration>,
    ) -> anyhow::Result<BTreeMap<Arc<String>, Vec<Chunk>>> {
        let _timer = crate::metrics::Timer::new("load_newer_chunks");
        // Log and skip datasets that fail to load instead of aborting: one unreachable bucket
        // must not freeze updates for all datasets.
        let result = stream::iter(datasets)
            .map(|(dataset, last_chunk)| {
                let storage = DatasetStorage::new(self.client.clone(), dataset);
                async move {
                    let chunks = storage
                        .list_new_chunks(last_chunk, CONCURRENT_CHUNKS, dataset_load_timeout)
                        .await;
                    (storage.dataset, chunks)
                }
            })
            .buffer_unordered(concurrent_downloads)
            .filter_map(|(dataset, chunks)| async move {
                match chunks {
                    Ok(chunks) => Some((dataset, chunks)),
                    Err(e) => {
                        tracing::error!(
                            dataset = %dataset,
                            error = ?e,
                            "Failed to load chunks for dataset; skipping it for this run"
                        );
                        None
                    }
                }
            })
            .collect::<BTreeMap<_, _>>()
            .await;
        Ok(result)
    }
}

#[derive(Clone)]
struct DatasetStorage {
    client: s3::Client,
    dataset: Arc<String>,
}

struct ChunkStream {
    client: s3::Client,
    dataset: Arc<String>,
    bucket: String,
    last_key: Option<String>,
    continuation_token: Option<String>,
    pending_objects: Vec<S3Object>,
    next_expected_block: Option<u64>,
    exhausted: bool,
}

impl ChunkStream {
    pub fn new(
        client: s3::Client,
        dataset: Arc<String>,
        bucket: String,
        last_chunk: Option<&Chunk>,
    ) -> Self {
        let next_expected_block = last_chunk.as_ref().map(|chunk| chunk.blocks.end() + 1);
        let last_key =
            last_chunk.map(|chunk| format!("{}/{}", chunk.id, chunk.files.iter().max().unwrap()));

        Self {
            client,
            dataset,
            bucket,
            last_key,
            continuation_token: None,
            pending_objects: Vec::new(),
            next_expected_block,
            exhausted: false,
        }
    }

    pub async fn next_batch(&mut self) -> anyhow::Result<Option<Vec<Chunk>>> {
        if self.exhausted {
            return Ok(None);
        }

        let objects = self.list_next_objects().await?;
        let has_more_pages = self.continuation_token.is_some();
        let batch = self.collect_chunks_from_page(objects, has_more_pages)?;

        if self.continuation_token.is_none() {
            self.exhausted = true;
        }

        Ok(Some(batch))
    }

    fn collect_chunks_from_page(
        &mut self,
        objects: Vec<S3Object>,
        has_more_pages: bool,
    ) -> anyhow::Result<Vec<Chunk>> {
        let mut batch = Vec::new();
        let mut current: Vec<S3Object> = Vec::new();
        let mut objects = objects.into_iter().peekable();

        while let Some(obj) = objects.next() {
            let group_closed = objects.peek().is_none_or(|next| next.prefix != obj.prefix);
            current.push(obj);

            if group_closed {
                let is_last_on_page = objects.peek().is_none();

                // The trailing group of a truncated page may continue on the next
                // page, so defer it instead of emitting a possibly-partial chunk.
                if has_more_pages && is_last_on_page {
                    self.pending_objects = std::mem::take(&mut current);
                } else {
                    match self.objects_to_chunk(&current)? {
                        Some(chunk) => {
                            match advance_continuity(
                                self.next_expected_block,
                                *chunk.blocks.start(),
                                *chunk.blocks.end(),
                            ) {
                                Ok(next) => {
                                    self.next_expected_block = Some(next);
                                    batch.push(chunk);
                                }
                                Err(gap) => {
                                    metrics::report_continuity_fallback(&self.dataset);
                                    tracing::warn!(
                                        dataset = %self.dataset,
                                        expected_block = gap.expected_block,
                                        actual_block = gap.actual_block,
                                        missing_through = gap.actual_block.saturating_sub(1),
                                        accepted_in_batch = batch.len(),
                                        "Continuity gap detected; falling back to last known good chunks for this dataset",
                                    );
                                    self.exhausted = true;
                                    break;
                                }
                            }
                        }
                        // The very last chunk in the bucket may still be in the
                        // process of being written (no blocks.parquet yet); that's
                        // acceptable, so skip it rather than failing.
                        None if is_last_on_page && !has_more_pages => {
                            tracing::debug!(
                                "Skipping incomplete chunk {} (blocks.parquet not yet present)",
                                current.first().map(|o| o.prefix.as_str()).unwrap_or("?")
                            );
                        }
                        None => {
                            anyhow::bail!(
                                "Chunk {} is missing required blocks.parquet",
                                current.first().map(|o| o.prefix.as_str()).unwrap_or("?")
                            );
                        }
                    }
                    current.clear();
                }
            }
        }

        Ok(batch)
    }

    pub fn exhausted(&self) -> bool {
        self.exhausted
    }

    async fn list_next_objects(&mut self) -> anyhow::Result<Vec<S3Object>> {
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .set_start_after(self.last_key.take())
            .set_continuation_token(self.continuation_token.take())
            .send()
            .await?;
        metrics::S3_REQUESTS.inc();
        metrics::S3_KEYS_LISTED.inc_by(output.key_count().unwrap() as i64);
        tracing::trace!("Got {} S3 objects", output.key_count().unwrap());

        let mut next_objects = std::mem::take(&mut self.pending_objects);
        if let Some(objects) = output.contents {
            for object in objects {
                next_objects.push(S3Object::try_from(object)?);
            }
        }
        self.continuation_token = output.next_continuation_token;

        Ok(next_objects)
    }

    fn objects_to_chunk(&self, objs: &[S3Object]) -> anyhow::Result<Option<Chunk>> {
        const REQUIRED_FILE: &str = "blocks.parquet";

        let mut full_chunk = false;
        let mut size_bytes = 0;
        let mut files = Vec::with_capacity(7);
        let mut id = None;
        for obj in objs {
            id = Some(obj.prefix.clone());
            full_chunk |= obj.file_name == REQUIRED_FILE;
            files.push(obj.file_name.clone());
            size_bytes += obj.size;
        }
        let id = id.expect("Chunk should be created from at least one object");
        if !full_chunk {
            // block.parquet is always the last file written in a chunk.
            // So if it is missing, the chunk is not complete yet.
            return Ok(None);
        }

        let chunk = Chunk::new(self.dataset.clone(), id, size_bytes, files)?;
        tracing::trace!("Downloaded chunk {chunk}");

        Ok(Some(chunk))
    }
}

impl DatasetStorage {
    pub fn new(client: s3::Client, dataset: Arc<String>) -> Self {
        Self { dataset, client }
    }

    pub fn bucket(&self) -> &str {
        self.dataset
            .strip_prefix("s3://")
            .expect("Dataset should start with s3://")
    }

    #[instrument(skip_all, level = "debug", fields(dataset = %self.dataset))]
    pub async fn list_new_chunks(
        &self,
        last_chunk: Option<&Chunk>,
        concurrent_downloads: usize,
        dataset_load_timeout: Option<Duration>,
    ) -> anyhow::Result<Vec<Chunk>> {
        tracing::debug!("Downloading chunks from {}", self.dataset);

        let deadline = dataset_load_timeout.map(|timeout| Instant::now() + timeout);
        let mut stream = ChunkStream::new(
            self.client.clone(),
            self.dataset.clone(),
            self.bucket().to_string(),
            last_chunk,
        );
        let mut chunks = Vec::new();

        while let Some(mut batch) = stream.next_batch().await? {
            stream::iter(batch.iter_mut())
                .map(anyhow::Ok)
                .try_for_each_concurrent(Some(concurrent_downloads), |ch| async move {
                    self.populate_with_summary(ch).await.map_err(|e| {
                        e.context(format!("couldn't download chunk summary for {}", ch))
                    })
                })
                .await?;

            chunks.append(&mut batch);

            if let Some(deadline) = deadline
                && Instant::now() >= deadline
                && !stream.exhausted()
            {
                tracing::warn!(
                    "Dataset {} summary population timed out after {}s. \
                         {} chunks processed. Remaining chunks will be processed on next run.",
                    self.dataset,
                    dataset_load_timeout.unwrap().as_secs(),
                    chunks.len(),
                );
                break;
            }
        }

        tracing::debug!("Downloaded {} chunks", chunks.len());

        Ok(chunks)
    }

    async fn populate_with_summary(&self, data_chunk: &mut Chunk) -> anyhow::Result<()> {
        let mut attempt = 1;
        loop {
            match self.try_populate_with_summary(data_chunk).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < SUMMARY_DOWNLOAD_ATTEMPTS => {
                    let delay = SUMMARY_RETRY_BASE_DELAY * 2u32.pow(attempt - 1);
                    tracing::warn!(
                        chunk = %data_chunk,
                        attempt,
                        error = ?e,
                        "Failed to download chunk summary; retrying in {delay:?}",
                    );
                    metrics::S3_SUMMARY_DOWNLOAD_RETRIES.inc();
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn try_populate_with_summary(&self, data_chunk: &mut Chunk) -> anyhow::Result<()> {
        let key = format!("{}/blocks.parquet", data_chunk.id);
        let temp_file = tempfile::tempfile()?;
        let mut tokio_file = tokio::fs::File::from_std(temp_file);
        self.download_object(&key, &mut tokio_file).await?;

        tokio_file.rewind().await?;
        let temp_file = tokio_file.into_std().await;
        let summary =
            tokio::task::spawn_blocking(move || crate::parquet::read_chunk_summary(temp_file))
                .await??;

        tracing::debug!("Adding summary to {}/{}: {:?}", self.bucket(), key, summary);
        data_chunk.summary = Some(summary);
        Ok(())
    }

    async fn download_object(&self, key: &str, file: &mut tokio::fs::File) -> anyhow::Result<()> {
        tracing::trace!(
            "Downloading object {}/{} to extract summary",
            self.bucket(),
            key
        );
        let response = self
            .client
            .get_object()
            .bucket(self.bucket())
            .key(key)
            .send()
            .await?;
        let mut stream = response.body.into_async_read();
        tokio::io::copy(&mut stream, file).await?;
        tracing::trace!("Downloaded object {}/{}", self.bucket(), key);
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct S3Object {
    // common prefix identifies all objects belonging to a single chunk
    prefix: String,
    file_name: String,
    size: u32,
}

impl TryFrom<s3::types::Object> for S3Object {
    type Error = anyhow::Error;

    fn try_from(obj: s3::types::Object) -> Result<Self, Self::Error> {
        let key = obj.key.ok_or(anyhow::anyhow!("Object key missing"))?;
        let (prefix, file_name) = match key.rsplit_once('/') {
            Some((prefix, file_name)) => (prefix.to_string(), file_name.to_string()),
            None => return Err(anyhow::anyhow!("Invalid key (no prefix)")),
        };
        let size = obj.size.unwrap_or_default() as u32;
        Ok(Self {
            prefix,
            file_name,
            size,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use aws_config::BehaviorVersion;
    use std::env;

    #[test]
    fn continuity_first_chunk_always_accepted() {
        assert_eq!(advance_continuity(None, 1000, 1099).unwrap(), 1100);
    }

    #[test]
    fn continuity_sequential_ok() {
        assert_eq!(advance_continuity(Some(1100), 1100, 1199).unwrap(), 1200);
    }

    #[test]
    fn continuity_gap_detected() {
        let gap = advance_continuity(Some(1200), 1500, 1599).unwrap_err();
        assert_eq!(
            gap,
            ContinuityGap {
                expected_block: 1200,
                actual_block: 1500,
            }
        );
    }

    fn test_dataset() -> Arc<String> {
        Arc::new("s3://test-dataset".to_string())
    }

    fn make_test_chunk(first: u64, last: u64) -> Chunk {
        let id = format!("{first:010}/{first:010}-{last:010}-{:08x}", first as u32);
        Chunk {
            dataset: test_dataset(),
            id: Arc::new(id),
            size: 1000,
            blocks: first..=last,
            files: Arc::new(vec!["blocks.parquet".to_string()]),
            summary: None,
        }
    }

    fn make_chunk_objects(first: u64, last: u64) -> Vec<S3Object> {
        let prefix = format!("{first:010}/{first:010}-{last:010}-{:08x}", first as u32);
        vec![S3Object {
            prefix,
            file_name: "blocks.parquet".to_string(),
            size: 100,
        }]
    }

    fn make_test_stream(last_chunk: Option<&Chunk>) -> ChunkStream {
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        let client = s3::Client::from_conf(config);
        ChunkStream::new(
            client,
            test_dataset(),
            "test-dataset".to_string(),
            last_chunk,
        )
    }

    #[test]
    fn collect_chunks_stops_at_continuity_gap_after_prefix() {
        let last_chunk = make_test_chunk(0, 199);
        let mut stream = make_test_stream(Some(&last_chunk));

        let mut objects = make_chunk_objects(200, 299);
        objects.extend(make_chunk_objects(300, 399));
        objects.extend(make_chunk_objects(500, 599));

        let batch = stream
            .collect_chunks_from_page(objects, false)
            .expect("continuity gap should not error");

        assert_eq!(batch.len(), 2);
        assert_eq!(*batch[0].blocks.start(), 200);
        assert_eq!(*batch[1].blocks.end(), 399);
        assert!(stream.exhausted());
    }

    #[test]
    fn collect_chunks_returns_empty_when_first_new_chunk_has_gap() {
        let last_chunk = make_test_chunk(0, 199);
        let mut stream = make_test_stream(Some(&last_chunk));

        let objects = make_chunk_objects(500, 599);
        let batch = stream
            .collect_chunks_from_page(objects, false)
            .expect("continuity gap should not error");

        assert!(batch.is_empty());
        assert!(stream.exhausted());
    }

    #[tokio::test]
    async fn next_batch_returns_none_after_continuity_gap() {
        let last_chunk = make_test_chunk(0, 199);
        let mut stream = make_test_stream(Some(&last_chunk));

        let objects = make_chunk_objects(500, 599);
        stream
            .collect_chunks_from_page(objects, false)
            .expect("continuity gap should not error");

        assert!(stream.exhausted());
        assert_eq!(stream.next_batch().await.unwrap(), None);
    }

    async fn make_s3_client() -> anyhow::Result<s3::Client> {
        let endpoint = env::var("AWS_ENDPOINT_URL")?;

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region("auto")
            .endpoint_url(endpoint)
            .load()
            .await;

        Ok(s3::Client::new(&config))
    }

    async fn make_storage(dataset: &str) -> anyhow::Result<DatasetStorage> {
        let s3c = make_s3_client().await?;
        Ok(DatasetStorage::new(s3c, Arc::new(dataset.to_string())))
    }

    #[tokio::test]
    #[ignore = "not a unit test"]
    // this test requires the AWS secrets and the AWS_ENDPOINT_URL variable to be set in env
    async fn test_load_chunks_from_eth_holesky() {
        test_load_chunks("ethereum-holesky-1").await;
    }

    #[tokio::test]
    #[ignore = "not a unit test"]
    // this test requires the AWS secrets and the AWS_ENDPOINT_URL variable to be set in env
    async fn test_load_chunks_from_eth_sepolia() {
        test_load_chunks("ethereum-sepolia-1").await;
    }

    #[tokio::test]
    #[ignore = "not a unit test"]
    // this test requires the AWS secrets and the AWS_ENDPOINT_URL variable to be set in env
    async fn test_load_chunks_from_sol_mainnet() {
        test_load_chunks("solana-mainnet-1").await;
    }

    #[tokio::test]
    #[ignore = "not a unit test"]
    // this test requires the AWS secrets and the AWS_ENDPOINT_URL variable to be set in env
    async fn test_load_chunks_from_hyper_testnet() {
        test_load_chunks("hyperliquid-testnet-4").await;
    }

    async fn test_load_chunks(dataset: &str) {
        let ds = make_storage(&format!("s3://{dataset}")).await.unwrap();
        let chunks = ds
            .list_new_chunks(None, CONCURRENT_CHUNKS, None)
            .await
            .unwrap();
        let expected = chunks.len();

        assert!(expected > 0);

        let have = chunks.iter().fold(
            0,
            |acc, ch| if ch.summary.is_some() { acc + 1 } else { acc },
        );

        assert_eq!(expected, have);
    }
}
