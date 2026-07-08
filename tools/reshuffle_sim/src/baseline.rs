use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Read,
    path::PathBuf,
    sync::Arc,
};

use flate2::read::GzDecoder;
use libp2p_identity::PeerId;
use network_scheduler::types::{BlockNumber, Chunk, Worker, WorkerStatus};
use semver::Version;
use sqd_assignments::Assignment as FbAssignment;

use crate::ChunkOwners;

pub struct DatasetInfo {
    pub dataset_id: Arc<String>,
    pub chunk_count: u32,
    pub last_block: BlockNumber,
    pub avg_block_span: u64,
    pub avg_chunk_size: u32,
}

pub struct Baseline {
    pub chunks: Vec<Chunk>,
    pub workers: Vec<Worker>,
    pub chunk_owners: ChunkOwners,
    pub datasets: Vec<DatasetInfo>,
}

fn read_flatbuffer(path: &PathBuf) -> anyhow::Result<Vec<u8>> {
    let raw = std::fs::read(path)?;
    if path.extension().is_some_and(|ext| ext == "gz") || path.to_string_lossy().ends_with(".fb.gz")
    {
        let mut decoder = GzDecoder::new(&raw[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    } else {
        Ok(raw)
    }
}

fn extract_workers(fb: &FbAssignment, worker_version: &Version) -> anyhow::Result<Vec<Worker>> {
    let mut worker_peer_ids: Vec<PeerId> = Vec::new();
    for i in 0..fb.workers().len() {
        worker_peer_ids.push(fb.get_worker_id(i as u16)?);
    }

    let workers = fb
        .workers()
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let peer_id = worker_peer_ids[i];
            let fb_worker = fb.get_worker(&peer_id).expect("Worker must exist");
            let status = match fb_worker.status() {
                sqd_assignments::WorkerStatus::Ok => WorkerStatus::Online,
                sqd_assignments::WorkerStatus::Unreliable => WorkerStatus::Offline,
                sqd_assignments::WorkerStatus::UnsupportedVersion => {
                    WorkerStatus::UnsupportedVersion
                }
                sqd_assignments::WorkerStatus::DeprecatedVersion => WorkerStatus::Online,
            };
            Worker {
                id: peer_id,
                status,
                version: Some(worker_version.clone()),
            }
        })
        .collect();

    Ok(workers)
}

/// Reconstructs chunks and their worker ownership from the flatbuffer.
/// Each chunk's last_block is inferred from the next chunk's first_block within the same dataset.
fn extract_chunks_and_owners(fb: &FbAssignment) -> anyhow::Result<(Vec<Chunk>, ChunkOwners)> {
    let mut worker_peer_ids: Vec<PeerId> = Vec::new();
    for i in 0..fb.workers().len() {
        worker_peer_ids.push(fb.get_worker_id(i as u16)?);
    }

    let mut chunks = Vec::new();
    let mut chunk_owners = BTreeMap::new();
    let mut dataset_pool: HashMap<String, Arc<String>> = HashMap::new();

    for dataset in fb.datasets() {
        let fb_chunks: Vec<_> = dataset.chunks().iter().collect();
        for (i, fb_chunk) in fb_chunks.iter().enumerate() {
            let first_block = fb_chunk.first_block();
            let last_block = if i + 1 < fb_chunks.len() {
                fb_chunks[i + 1].first_block() - 1
            } else {
                dataset.last_block()
            };

            let dataset_arc = dataset_pool
                .entry(fb_chunk.dataset_id().to_owned())
                .or_insert_with_key(|k| Arc::new(k.clone()))
                .clone();
            let chunk_id_arc = Arc::new(fb_chunk.id().to_owned());

            chunks.push(Chunk {
                dataset: dataset_arc.clone(),
                id: chunk_id_arc.clone(),
                size: fb_chunk.size(),
                blocks: first_block..=last_block,
                files: Arc::new(vec![]),
                summary: None,
            });

            let owners: BTreeSet<PeerId> = fb_chunk
                .worker_indexes()
                .iter()
                .map(|wi| worker_peer_ids[wi as usize])
                .collect();
            chunk_owners.insert((dataset_arc, chunk_id_arc), owners);
        }
    }

    Ok((chunks, chunk_owners))
}

/// Aggregates per-dataset statistics needed for proportional chunk generation:
/// chunk count, highest block number, average block span, and average chunk size.
fn compute_dataset_stats(chunks: &[Chunk]) -> Vec<DatasetInfo> {
    let mut stats: BTreeMap<Arc<String>, (u32, BlockNumber, u64, u64)> = BTreeMap::new();

    for chunk in chunks {
        let first = *chunk.blocks.start();
        let last = *chunk.blocks.end();
        let entry = stats.entry(chunk.dataset.clone()).or_insert((0, 0, 0, 0));
        entry.0 += 1;
        entry.1 = entry.1.max(last);
        entry.2 += last - first + 1;
        entry.3 += chunk.size as u64;
    }

    stats
        .into_iter()
        .map(
            |(dataset_id, (count, last_block, total_block_span, total_bytes))| DatasetInfo {
                dataset_id,
                chunk_count: count,
                last_block,
                avg_block_span: total_block_span / count.max(1) as u64,
                avg_chunk_size: (total_bytes / count.max(1) as u64) as u32,
            },
        )
        .collect()
}

pub fn load_baseline(path: &PathBuf, worker_version: &Version) -> anyhow::Result<Baseline> {
    let buf = read_flatbuffer(path)?;
    let fb = FbAssignment::from_owned_unchecked(buf);

    let workers = extract_workers(&fb, worker_version)?;
    let (chunks, chunk_owners) = extract_chunks_and_owners(&fb)?;
    let datasets = compute_dataset_stats(&chunks);

    tracing::info!(
        "Loaded {} chunks across {} datasets, {} workers",
        chunks.len(),
        datasets.len(),
        workers.len()
    );

    Ok(Baseline {
        chunks,
        workers,
        chunk_owners,
        datasets,
    })
}
