use std::collections::HashMap;

use itertools::Itertools;
use rand::Rng;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

fn hash(str: &str) -> u64 {
    seahash::hash(str.as_bytes())
}

const MAX_CHUNK_SIZE: usize = 200_000_000;
const N_RINGS: usize = 6000;

#[derive(Debug)]
struct Chunk {
    id: String,
    size: usize,
}

#[derive(Debug, Default)]
struct Worker<'c> {
    total_size: usize,
    chunks: Vec<&'c Chunk>,
}

impl<'l> Worker<'l> {
    fn add(&mut self, chunk: &'l Chunk) {
        self.total_size += chunk.size;
        self.chunks.push(chunk);
    }
}

fn main() {
    println!("Generating input");
    let chunks = generate_chunks(10_000_000);
    let vchunks = generate_virtual_chunks(&chunks);
    let workers = generate_workers(2_000);
    let total_size: usize = vchunks.iter().map(|(chunk, _)| chunk.size).sum();
    let worker_capacity = (total_size as f32 / workers.len() as f32 * 1.1) as usize;
    println!(
        "Total chunks: {}, virtual chunks: {}, total size: {}GB, worker capacity: {}GB",
        chunks.len(),
        vchunks.len(),
        total_size as f32 / 1000_000_000.,
        worker_capacity as f32 / 1000_000_000.,
    );

    println!("Hashing workers");
    let rings: Vec<Vec<(u64, &String)>> = (0..N_RINGS)
        .into_par_iter()
        .map(|ring_index| {
            let mut vec = workers
                .iter()
                .enumerate()
                .map(|(worker_index, worker_id)| {
                    (hash(&format!("{}:{}", worker_id, ring_index)), worker_id)
                })
                .collect_vec();
            vec.sort_unstable();
            vec
        })
        .collect();

    println!("Hashing chunks");
    let orderings: Vec<(&Chunk, &Vec<_>, usize)> = vchunks
        .par_iter()
        .map(|&(chunk, tag)| {
            let chunk_hash = hash(&format!("{}:{}", chunk.id, tag));
            let ring_index = chunk_hash as usize % N_RINGS;
            let ring = &rings[ring_index];
            let first = ring.partition_point(|(x, _)| *x < chunk_hash);
            (chunk, ring, first)
        })
        .collect();

    println!("Distributing chunks");
    let mut capacities: HashMap<String, usize> = workers
        .iter()
        .map(|worker_id| (worker_id.clone(), worker_capacity))
        .collect();
    let distribution: Vec<(&Chunk, &String)> = orderings
        .into_iter()
        .map(|(chunk, ring, first)| {
            let first = first as usize;
            let candidates = ring[first..].iter().chain(ring[..first].iter());
            for &(_, worker_id) in candidates {
                let capacity = capacities.get_mut(worker_id).unwrap();
                if *capacity >= chunk.size {
                    *capacity -= chunk.size;
                    return (chunk, worker_id);
                }
            }
            panic!("No worker found for chunk {}", chunk.id);
        })
        .collect();
    drop(rings);
    drop(capacities);
    drop(vchunks);

    println!("Packing assignments");
    let mut results: HashMap<String, Worker> = HashMap::new();
    for (chunk, worker_id) in distribution {
        results
            .entry(worker_id.clone())
            .or_default()
            .add(chunk);
    }
    results
        .into_iter()
        .sorted_by_key(|(_, worker)| worker.total_size)
        .for_each(|(id, worker)| {
            println!("{}: {} ({})", id, worker.chunks.len(), worker.total_size)
        });
}

fn generate_chunks(n: usize) -> Vec<Chunk> {
    (0..n)
        .into_par_iter()
        .map(|i| {
            let id = format!(
                "s3://solana-mainnet-0/0000000000/{:010}-{:010}-{:08x}",
                i,
                i + 1,
                i
            );
            Chunk {
                id,
                size: rand::rng().random_range(0..MAX_CHUNK_SIZE),
            }
        })
        .collect()
}

fn generate_virtual_chunks(chunks: &Vec<Chunk>) -> Vec<(&Chunk, u8)> {
    chunks
        .iter()
        .enumerate()
        .flat_map(|(_chunk_index, chunk)| {
            let mut rng = rand::rng();
            let n_virtual_chunks = rng.random_range(1..=60);
            (0..n_virtual_chunks).map(move |tag| (chunk, tag))
        })
        .collect()
}

fn generate_workers(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("12D3KooW{:044x}", i)).collect()
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    fn ring_iter<'l, T: Ord>(v: &'l [T], from: &T) -> impl Iterator<Item = &'l T> {
        let i = v.partition_point(|x| x < from);
        v[i..].iter().chain(v[..i].iter())
    }

    #[test]
    fn test_ring_iterator() {
        let v = vec![1, 3, 5, 7];

        assert_eq!(ring_iter(&v, &0).copied().collect_vec(), vec![1, 3, 5, 7]);
        assert_eq!(ring_iter(&v, &2).copied().collect_vec(), vec![3, 5, 7, 1]);
        assert_eq!(ring_iter(&v, &3).copied().collect_vec(), vec![3, 5, 7, 1]);
        assert_eq!(ring_iter(&v, &4).copied().collect_vec(), vec![5, 7, 1, 3]);
        assert_eq!(ring_iter(&v, &6).copied().collect_vec(), vec![7, 1, 3, 5]);
        assert_eq!(ring_iter(&v, &7).copied().collect_vec(), vec![7, 1, 3, 5]);
        assert_eq!(ring_iter(&v, &8).copied().collect_vec(), vec![1, 3, 5, 7]);
    }
}
