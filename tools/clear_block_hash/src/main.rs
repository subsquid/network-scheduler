use std::path::PathBuf;

use clap::Parser;
use sqd_assignments::{Assignment, AssignmentBuilder};

#[derive(Parser, Debug)]
#[command(about = "Read an assignment and rewrite it with last_block_hash cleared on every chunk")]
pub struct Args {
    /// Input assignment file (flatbuffer)
    #[arg(value_name = "INPUT")]
    pub input: PathBuf,

    /// Output assignment file
    #[arg(value_name = "OUTPUT")]
    pub output: PathBuf,

    /// Cloudflare storage secret (required to generate encrypted worker headers)
    #[arg(long, env = "CLOUDFLARE_STORAGE_SECRET")]
    pub cloudflare_storage_secret: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let buf = std::fs::read(&args.input)?;
    let assignment = Assignment::from_owned_unchecked(buf);

    let mut builder = AssignmentBuilder::new(&args.cloudflare_storage_secret)
        .check_continuity(false);

    for dataset in assignment.datasets() {
        let chunks: Vec<_> = dataset.chunks().iter().collect();
        for (i, chunk) in chunks.iter().enumerate() {
            let last_block = if i + 1 < chunks.len() {
                chunks[i + 1].first_block() - 1
            } else {
                dataset.last_block()
            };

            let files: Vec<String> = chunk.files().iter().map(|f| f.filename().to_owned()).collect();
            let worker_indexes: Vec<u16> = chunk.worker_indexes().iter().collect();

            builder
                .new_chunk()
                .id(chunk.id())
                .dataset_id(chunk.dataset_id())
                .block_range(chunk.first_block()..=last_block)
                .size(chunk.size())
                .dataset_base_url(chunk.dataset_base_url())
                .worker_indexes(&worker_indexes)
                .files(&files)
                .last_block_hash("")
                .finish()?;
        }
        builder.finish_dataset();
    }

    let num_workers = assignment.workers().len();
    for i in 0..num_workers {
        let peer_id = assignment.get_worker_id(i as u16)?;
        let worker = assignment
            .get_worker(&peer_id)
            .expect("Worker must exist in assignment");
        builder.add_worker(peer_id, worker.status(), &[]);
    }

    let output = builder.finish();
    std::fs::write(&args.output, &output)?;

    eprintln!(
        "Wrote {} bytes to {}",
        output.len(),
        args.output.display()
    );

    Ok(())
}
