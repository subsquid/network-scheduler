use std::collections::BTreeMap;
use std::fmt::Display;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3 as s3;
use itertools::Itertools;
use tokio::io::AsyncSeekExt;
use tracing::instrument;

use crate::metrics;
use crate::types::Chunk;
use futures::{
    TryStreamExt,
    stream::{self, StreamExt},
};

#[derive(Clone)]
pub struct S3Storage {
    client: s3::Client,
}

impl S3Storage {
    pub fn new(s3_config: aws_config::SdkConfig) -> Self {
        let client = s3::Client::new(&s3_config);
        Self { client }
    }

    pub async fn load_all_chunks(
        &self,
        buckets: impl IntoIterator<Item = impl Display>,
        concurrent_downloads: usize,
    ) -> anyhow::Result<BTreeMap<Arc<str>, Vec<Chunk>>> {
        stream::iter(buckets)
            .map(|bucket| {
                let storage = DatasetStorage::new(self.client.clone(), bucket);
                async move {
                    let last_chunk = None;
                    let chunks = storage.list_new_chunks(last_chunk).await?;
                    anyhow::Ok((storage.dataset(), chunks))
                }
            })
            .buffer_unordered(concurrent_downloads)
            .try_collect()
            .await
    }
}

#[derive(Clone)]
struct DatasetStorage {
    bucket: String,
    client: s3::Client,
    dataset: Arc<str>,
}

impl DatasetStorage {
    pub fn new(client: s3::Client, bucket: impl Display) -> Self {
        Self {
            dataset: format!("s3://{bucket}").into(),
            bucket: bucket.to_string(),
            client,
        }
    }

    pub fn dataset(&self) -> Arc<str> {
        self.dataset.clone()
    }

    #[instrument(skip_all, fields(dataset = %self.dataset))]
    pub async fn list_new_chunks(&self, last_chunk: Option<Chunk>) -> anyhow::Result<Vec<Chunk>> {
        let mut next_expected_block = last_chunk.as_ref().map(|chunk| chunk.blocks.end() + 1);
        let last_key =
            last_chunk.map(|chunk| format!("{}/{}", chunk.id, chunk.files.iter().max().unwrap()));

        let objects = self.list_new_objects(last_key).await?;

        let mut chunks = Vec::new();
        for (_, objects) in objects
            .into_iter()
            .chunk_by(|obj| obj.prefix.clone())
            .into_iter()
        {
            match self.objects_to_chunk(objects)? {
                Some(chunk) => chunks.push(chunk),
                None => break,
            }
        }

        // Verify if chunks are continuous
        for chunk in chunks.iter() {
            if next_expected_block.is_some_and(|next_block| *chunk.blocks.start() != next_block) {
                anyhow::bail!(
                    "Blocks {} to {} missing from {}",
                    next_expected_block.unwrap(),
                    chunk.blocks.start() - 1,
                    self.dataset
                );
            }
            next_expected_block = Some(chunk.blocks.end() + 1);
        }

        if let Some(last_chunk) = chunks.last_mut() {
            self.populate_with_summary(last_chunk)
                .await
                .context(format!(
                    "couldn't download chunk summary for {}",
                    last_chunk
                ))?;
        }

        tracing::debug!("Downloaded {} chunks", chunks.len());

        Ok(chunks)
    }

    async fn list_new_objects(
        &self,
        mut last_key: Option<String>,
    ) -> anyhow::Result<Vec<S3Object>> {
        let mut result = Vec::new();
        let mut continuation_token = None;
        loop {
            let objects = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .set_start_after(last_key.take())
                .set_continuation_token(continuation_token)
                .send()
                .await?;
            metrics::S3_REQUESTS.inc();
            metrics::S3_KEYS_LISTED.inc_by(objects.key_count().unwrap() as i64);
            tracing::trace!("Got {} S3 objects", objects.key_count().unwrap());

            for object in objects.contents.unwrap() {
                result.push(S3Object::try_from(object)?);
            }
            continuation_token = objects.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(result)
    }

    fn objects_to_chunk(
        &self,
        objs: impl IntoIterator<Item = S3Object>,
    ) -> anyhow::Result<Option<Chunk>> {
        const REQUIRED_FILE: &str = "blocks.parquet";

        let mut full_chunk = false;
        let mut size_bytes = 0;
        let mut files = Vec::with_capacity(7);
        let mut id = None;
        for obj in objs {
            id = Some(obj.prefix);
            full_chunk |= obj.file_name == REQUIRED_FILE;
            files.push(obj.file_name);
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

    async fn populate_with_summary(&self, data_chunk: &mut Chunk) -> anyhow::Result<()> {
        let key = format!("{}/blocks.parquet", data_chunk.id);
        let temp_file = tempfile::tempfile()?;
        let mut tokio_file = tokio::fs::File::from_std(temp_file);
        self.download_object(&key, &mut tokio_file).await?;

        tokio_file.rewind().await?;
        let temp_file = tokio_file.into_std().await;
        let summary =
            tokio::task::spawn_blocking(move || crate::parquet::read_chunk_summary(temp_file))
                .await??;

        tracing::debug!("Adding summary to {}/{}: {:?}", self.bucket, key, summary);
        data_chunk.summary = Some(summary);
        Ok(())
    }

    async fn download_object(&self, key: &str, file: &mut tokio::fs::File) -> anyhow::Result<()> {
        tracing::debug!(
            "Downloading object {}/{} to extract summary",
            self.bucket,
            key
        );
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        let mut stream = response.body.into_async_read();
        tokio::io::copy(&mut stream, file).await?;
        tracing::debug!("Downloaded object {}/{}", self.bucket, key);
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

impl S3Object {
    fn key(&self) -> String {
        format!("{}/{}", self.prefix, self.file_name)
    }
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
