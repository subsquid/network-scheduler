/// A utility program to test the impact of last block hash in the assignment.
/// To use run:
/// cargo run -p clear-block-hash -- -c config.yaml input.fb output.fb

use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
};

use clap::Parser;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use libp2p_identity::PeerId;
use network_scheduler::types::{
    Assignment, Chunk, ChunkIndex, ChunkSummary, FbVersion, Worker, WorkerStatus,
};
use sqd_assignments::Assignment as FbAssignment;

#[derive(Parser, Debug)]
#[command(about = "Read an assignment and rewrite it with last_block_hash cleared on every chunk")]
pub struct Args {
    /// Input assignment file (flatbuffer)
    #[arg(value_name = "INPUT")]
    pub input: PathBuf,

    /// Output assignment file
    #[arg(value_name = "OUTPUT")]
    pub output: PathBuf,

    /// Scheduler config file (same one used to produce the assignment)
    #[arg(long, short)]
    pub config: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut config = network_scheduler::cli::Config::load(&args.config)?;
    config.clear_last_block_hash = true;

    let raw = std::fs::read(&args.input)?;
    let buf = if args.input.extension().is_some_and(|ext| ext == "gz") {
        let mut decoder = GzDecoder::new(&raw[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        decompressed
    } else {
        raw
    };
    let fb = FbAssignment::from_owned_unchecked(buf);

    let mut worker_ids: Vec<PeerId> = Vec::new();
    for i in 0..fb.workers().len() {
        worker_ids.push(fb.get_worker_id(i as u16)?);
    }

    let mut chunks: Vec<Chunk> = Vec::new();
    let mut worker_chunks: BTreeMap<PeerId, Vec<ChunkIndex>> = BTreeMap::new();
    let mut chunk_index: ChunkIndex = 0;

    for dataset in fb.datasets() {
        let fb_chunks: Vec<_> = dataset.chunks().iter().collect();
        for (i, fb_chunk) in fb_chunks.iter().enumerate() {
            let last_block = if i + 1 < fb_chunks.len() {
                fb_chunks[i + 1].first_block() - 1
            } else {
                dataset.last_block()
            };

            let dataset_id = fb_chunk.dataset_id().to_owned();
            let files: Vec<String> = fb_chunk
                .files()
                .iter()
                .map(|f| f.filename().to_owned())
                .collect();

            let summary = fb_chunk.last_block_hash().map(|hash| ChunkSummary {
                last_block_hash: hash.to_owned(),
                last_block_timestamp: fb_chunk.last_block_timestamp().unwrap_or(0),
            });

            let mut chunk = Chunk::new(
                Arc::new(dataset_id),
                fb_chunk.id().to_owned(),
                fb_chunk.size(),
                files,
            )?;
            chunk.blocks = fb_chunk.first_block()..=last_block;
            chunk.summary = summary;
            chunks.push(chunk);

            for wi in fb_chunk.worker_indexes().iter() {
                let peer_id = worker_ids[wi as usize];
                worker_chunks.entry(peer_id).or_default().push(chunk_index);
            }
            chunk_index += 1;
            if chunk_index % 50_000 == 0 {
                eprintln!("Processed {} chunks...", chunk_index);
            }
        }
    }

    let workers: Vec<Worker> = fb
        .workers()
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let peer_id = worker_ids[i];
            let w = fb.get_worker(&peer_id).expect("Worker must exist");
            let status = match w.status() {
                sqd_assignments::WorkerStatus::Ok => WorkerStatus::Online,
                sqd_assignments::WorkerStatus::Unreliable => WorkerStatus::Offline,
                sqd_assignments::WorkerStatus::UnsupportedVersion => {
                    WorkerStatus::UnsupportedVersion
                }
                _ => WorkerStatus::Offline,
            };
            Worker {
                id: peer_id,
                status,
                version: None,
            }
        })
        .collect();

    for chunks in worker_chunks.values_mut() {
        chunks.sort_unstable();
    }

    let assignment = Assignment {
        worker_chunks,
        replication_by_weight: BTreeMap::new(),
    };

    let fb_bytes = assignment.encode_fb(&chunks, &config, &workers, FbVersion::V1);
    std::fs::write(&args.output, &fb_bytes)?;
    eprintln!(
        "Wrote {} bytes to {}",
        fb_bytes.len(),
        args.output.display()
    );

    let gz_path = args.output.with_extension("fb.gz");
    let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
    encoder.write_all(&fb_bytes)?;
    let compressed = encoder.finish()?;
    std::fs::write(&gz_path, &compressed)?;
    eprintln!("Wrote {} bytes to {}", compressed.len(), gz_path.display());

    Ok(())
}
