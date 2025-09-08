use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::Parser;
use libp2p_identity::PeerId;
use sqd_assignments::Assignment;

const GB: f64 = (1 << 30) as f64;

#[derive(Parser, Debug)]
#[command()]
pub struct Args {
    #[arg(value_name = "FILE")]
    pub old: PathBuf,

    #[arg(value_name = "FILE")]
    pub new: PathBuf,

    #[arg()]
    pub sample_workers: Option<u16>,
}

fn read_assignment(filename: &Path) -> anyhow::Result<Assignment> {
    let buf = std::fs::read(filename)?;
    Ok(Assignment::from_owned_unchecked(buf))
}

fn parse_assignment(
    assignment: Assignment,
    sample_workers: Option<u16>,
) -> BTreeMap<PeerId, Vec<(Arc<str>, u32)>> {
    let sample_workers = sample_workers.unwrap_or(assignment.workers().len() as u16);
    let worker_ids = assignment
        .workers()
        .iter()
        .take(sample_workers as usize)
        .map(|w| {
            let id: PeerId = (*w.worker_id())
                .try_into()
                .expect("Failed to convert worker ID");
            id
        })
        .collect::<Vec<_>>();

    let mut result: BTreeMap<PeerId, Vec<(Arc<str>, u32)>> = BTreeMap::new();

    for ds in assignment.datasets() {
        for chunk in ds.chunks() {
            let id = Arc::<str>::from(format!("{}/{}", chunk.dataset_id(), chunk.id()));
            for i in chunk.worker_indexes() {
                if i >= sample_workers {
                    break;
                }
                let worker_id = &worker_ids[i as usize];
                result
                    .entry(*worker_id)
                    .or_default()
                    .push((id.clone(), chunk.size()));
            }
        }
    }

    result
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let handle = std::thread::spawn(move || {
        anyhow::Ok(parse_assignment(
            read_assignment(&args.old)?,
            args.sample_workers,
        ))
    });
    let mut new = parse_assignment(read_assignment(&args.new)?, args.sample_workers);
    let old = handle.join().unwrap()?;

    for (peer_id, old_chunks) in old {
        let Some(new_chunks) = new.remove(&peer_id) else {
            let removed = old_chunks
                .iter()
                .map(|(_id, size)| *size as u64)
                .sum::<u64>() as f64
                / GB;
            println!("{peer_id}: -{removed:.2}GB");
            continue;
        };

        let old_chunks = BTreeSet::from_iter(old_chunks.into_iter());
        let new_chunks = BTreeSet::from_iter(new_chunks.into_iter());
        let only_old = old_chunks
            .difference(&new_chunks)
            .map(|(_id, size)| *size as u64)
            .sum::<u64>();
        let only_new = new_chunks
            .difference(&old_chunks)
            .map(|(_id, size)| *size as u64)
            .sum::<u64>();
        let both = old_chunks
            .intersection(&new_chunks)
            .map(|(_id, size)| *size as u64)
            .sum::<u64>();

        let before = (both + only_old) as f64 / GB;
        let after = (both + only_new) as f64 / GB;
        let removed = only_old as f64 / GB;
        let added = only_new as f64 / GB;
        println!("{peer_id}: {before:.2}GB -{removed:.2}GB +{added:.2}GB ={after:.2}GB");
    }

    for (peer_id, new_chunks) in new {
        let added = new_chunks
            .iter()
            .map(|(_id, size)| *size as u64)
            .sum::<u64>() as f64
            / GB;
        println!("{peer_id}: +{added:.2}GB");
    }

    Ok(())
}
