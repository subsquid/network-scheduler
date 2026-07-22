//! Sim harness helpers: env/seed/config plumbing, chunk-strategy generators, and the model-chunk
//! ⇄ storage-chunk codec with its weight table.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex};

use bytesize::ByteSize;
use proptest::prelude::*;
use proptest::statistics::RunStatistics;
use proptest::test_runner::Config;
use rand::RngCore;
use semver::Version;

use super::sut::{Action, ChunkKey, CorrectableOld, NewChunk, SimConfig, SimStorage, SimUnderTest};
use crate::scheduler_storage::Tick;
use crate::scheduler_storage::in_memory::InMemoryStorage;
use crate::scheduler_storage::postgres::PostgresStorage;
use crate::scheduler_storage::test_harness::inspect::ChunkView;
use crate::scheduler_storage::{AlgoChunk, NewChunk as StorageNewChunk};
use crate::types::{ChunkWeight, DatasetSchema, TableSchema};
use crate::weight::{SchedulingChunk, WeightStrategy};

pub(super) type Seed = [u8; 32];

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok().and_then(|value| value.parse().ok())
}

/// Proptest config honoring `PROPTEST_CASES` — unlike the `proptest!` macro, the iterative runner
/// won't pick it up on its own.
pub(super) fn proptest_config() -> Config {
    let mut config = Config::default();
    if let Some(cases) = env_usize("PROPTEST_CASES") {
        config.cases = cases as u32;
    }
    config
}

/// Cases per in-memory sweep: `SIM_IN_MEMORY_CASES` if set, else the proptest default. A dedicated
/// knob so CI can budget the sim sweeps without touching every other proptest test.
pub(super) fn in_memory_sim_cases() -> u32 {
    env_usize("SIM_IN_MEMORY_CASES").map_or_else(|| proptest_config().cases, |cases| cases as u32)
}

/// Cases per Postgres sweep: `SIM_PG_CASES` if set, else 10 — kept modest because each transition
/// is a real database round-trip.
pub(super) fn pg_sim_cases() -> u32 {
    env_usize("SIM_PG_CASES").map_or(10, |cases| cases as u32)
}

/// Per-run master seed: `SIM_SEED=<64 hex>` reproduces a prior run, otherwise fresh entropy.
pub(super) fn master_seed() -> Seed {
    env_seed("SIM_SEED").unwrap_or_else(|| {
        let mut seed = [0u8; 32];
        rand::rng().fill_bytes(&mut seed);
        seed
    })
}

/// Seed parsed from env var `key` (64 hex chars), if set and well-formed.
pub(super) fn env_seed(key: &str) -> Option<Seed> {
    seed_from_hex(std::env::var(key).ok()?.trim())
}

pub(super) fn trace_enabled() -> bool {
    std::env::var_os("SIM_TRACE").is_some()
}

pub(super) fn seed_hex(seed: &Seed) -> String {
    seed.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn seed_from_hex(text: &str) -> Option<Seed> {
    if text.len() != 64 {
        return None;
    }
    let mut seed = [0u8; 32];
    for (index, byte) in seed.iter_mut().enumerate() {
        *byte = u8::from_str_radix(text.get(2 * index..2 * index + 2)?, 16).ok()?;
    }
    Some(seed)
}

// ===========================================================================
// Sweep statistics
// ===========================================================================

/// Aggregate of the `proptest::statistics` recorded by every passing case of a sweep. The
/// iterative runner records one statistics "case" per executed *transition*, so label percentages
/// are per-transition, not per-run. Pure telemetry — `cover` is only enforced by
/// `TestRunner::run`, which the sim never calls — making generator regressions (a regime that
/// silently stops occurring) visible in the report.
#[derive(Default)]
pub(super) struct SweepStatistics {
    cases: u32,
    transitions: u32,
    labels: BTreeMap<String, u32>,
    measurements: BTreeMap<String, MeasurementAggregate>,
}

struct MeasurementAggregate {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl SweepStatistics {
    pub(super) fn absorb(&mut self, run: &RunStatistics) {
        if run.cases() == 0 {
            return;
        }
        self.cases += 1;
        self.transitions += run.cases();
        for (label, &count) in run.labels() {
            *self.labels.entry(label.to_string()).or_insert(0) += count;
        }
        for (name, summary) in run.measurements() {
            let aggregate =
                self.measurements
                    .entry(name.to_string())
                    .or_insert(MeasurementAggregate {
                        count: 0,
                        sum: 0.0,
                        min: f64::INFINITY,
                        max: f64::NEG_INFINITY,
                    });
            aggregate.count += summary.count();
            aggregate.sum += summary.sum();
            aggregate.min = aggregate.min.min(summary.min());
            aggregate.max = aggregate.max.max(summary.max());
        }
    }

    /// Print the aggregated distribution to stderr. No-op if nothing was recorded.
    pub(super) fn print(&self, name: &str) {
        if self.transitions == 0 || (self.labels.is_empty() && self.measurements.is_empty()) {
            return;
        }
        eprintln!(
            "{name}: transition statistics — {} transitions over {} passing cases",
            self.transitions, self.cases
        );
        let width = self
            .labels
            .keys()
            .chain(self.measurements.keys())
            .map(String::len)
            .max()
            .unwrap_or(0);
        for (label, &count) in &self.labels {
            let percent = f64::from(count) * 100.0 / f64::from(self.transitions);
            eprintln!("  {label:<width$}  {count:>8}  {percent:>5.1}%");
        }
        if !self.measurements.is_empty() {
            eprintln!("  measurements — count / min / mean / max:");
            for (measurement, aggregate) in &self.measurements {
                eprintln!(
                    "  {measurement:<width$}  {} / {:.2} / {:.2} / {:.2}",
                    aggregate.count,
                    aggregate.min,
                    aggregate.sum / aggregate.count as f64,
                    aggregate.max,
                );
            }
        }
    }
}

// ===========================================================================
// Simulation world constants
// ===========================================================================

pub(super) const CHUNK_SIZE: u32 = ByteSize::mib(1).as_u64() as u32;
pub(super) const MIN_CHUNK_SIZE: u32 = ByteSize::kib(512).as_u64() as u32;
pub(super) const MAX_CHUNK_SIZE: u32 = ByteSize::mib(2).as_u64() as u32;
pub(super) const WORKER_CAPACITY: u64 = ByteSize::mib(10).as_u64();
pub(super) const WEIGHTS: [u16; 3] = [1, 4, 12];
pub(super) const DATASETS: [&str; 3] = ["s3://sim-0", "s3://sim-1", "s3://sim-2"];

/// Small fixed pool `set_dataset_schema` samples from — not a general `Arbitrary`/`Strategy`
/// generator for [`DatasetSchema`] (none exists; building one is out of scope). Just enough
/// variety (empty, one table) to exercise schema-bundle consistency as a dataset's schema changes
/// mid-run. `LazyLock`, not a plain `const`, because `DatasetSchema` holds a `BTreeMap`/`String`s,
/// which aren't const-constructible.
pub(super) static SCHEMA_POOL: LazyLock<[DatasetSchema; 2]> = LazyLock::new(|| {
    [
        DatasetSchema::default(),
        DatasetSchema::new(BTreeMap::from([(
            "blocks".to_string(),
            TableSchema {
                fields: vec!["number".to_string(), "hash".to_string()],
                default_fields: vec!["number".to_string()],
            },
        )])),
    ]
});

// ---- Storage-lifecycle timing -----------------------------------------------------------------

/// Per-cycle clock advance, kept below [`M_TICKS`] so reshuffled copies accumulate as draining
/// rather than expiring at once.
pub(super) const CLOCK_STEP: Tick = 1;
/// Stale drain window: `AdvanceClock(M_TICKS)` flushes all currently-draining stale; smaller jumps
/// land mid-window.
pub(super) const M_TICKS: Tick = 5;
/// Worker GC horizon: `0` evicts a departed worker promptly, so it stops counting as scheduler
/// capacity before the next reschedule.
pub(super) const GC_TICKS: Tick = 0;

/// Per-worker capacity: `SIM_WORKER_CAPACITY=<MiB>` overrides [`WORKER_CAPACITY`] (chunks are
/// 1 MiB, so the value reads directly as chunk-slots).
pub(super) fn worker_capacity() -> u64 {
    env_usize("SIM_WORKER_CAPACITY")
        .map(|mib| ByteSize::mib(mib as u64).as_u64())
        .unwrap_or(WORKER_CAPACITY)
}

/// Owned copy of [`DATASETS`] — each config holds its own list.
pub(super) fn sim_datasets() -> Vec<String> {
    DATASETS.iter().map(|dataset| dataset.to_string()).collect()
}

/// Per-backend tuning: in-memory explores full-size fleets; Postgres uses small fleets so the
/// add→saturate→converge arc fits the reduced transition budget.
pub(super) trait SimProfile {
    fn worker_count() -> BoxedStrategy<u16>;
}

impl SimProfile for InMemoryStorage {
    fn worker_count() -> BoxedStrategy<u16> {
        match env_usize("SIM_WORKERS") {
            Some(workers) => Just(u16::try_from(workers.max(1)).unwrap_or(u16::MAX)).boxed(),
            None => (4u16..=8).boxed(),
        }
    }
}

impl SimProfile for PostgresStorage {
    fn worker_count() -> BoxedStrategy<u16> {
        // Defaults to a small fleet so test duration stays bounded; SIM_WORKERS pins a larger
        // fleet for stress runs (a bigger placement problem keeps walks doing real work instead of
        // idling once the few default levers are spent).
        match env_usize("SIM_WORKERS") {
            Some(workers) => Just(u16::try_from(workers.max(1)).unwrap_or(u16::MAX)).boxed(),
            None => (2u16..=3).boxed(),
        }
    }
}

/// Confirmation quorum per case. Both regimes matter: at 100% the strict consistency oracle holds;
/// below it the oracle scopes to caught-up workers — a fixed 100% would never explore that.
fn confirm_threshold() -> BoxedStrategy<u32> {
    prop_oneof![1 => Just(100u32), 2 => 70u32..100].boxed()
}

/// Default per-case [`SimConfig`] strategy shared by the guided and churn models.
pub(super) fn default_sim_config<D: SimProfile>() -> BoxedStrategy<SimConfig> {
    // Initial min_replication is 2..=4, never 1: recovery at saturation lowers it one step, so a
    // walk starting at 1 has no lever and idles. 1 is still explored by recovering down.
    (D::worker_count(), 2u16..=4, 70u32..=98, confirm_threshold())
        .prop_map(
            |(worker_count, min_replication, saturation_pct, confirm_threshold_pct)| SimConfig {
                worker_count,
                worker_capacity: worker_capacity(),
                min_replication: min_replication.min(worker_count),
                saturation: f64::from(saturation_pct) / 100.0,
                converge_is_terminal: false,
                chunk_cap: env_usize("SIM_CHUNKS"),
                datasets: sim_datasets(),
                confirm_threshold_pct,
            },
        )
        .boxed()
}

/// An `AdvanceClock` jump biased toward the boundaries that distinguish drain off-by-ones
/// (`M_TICKS`, `M_TICKS ± 1`, a single tick, up to two windows). Partial jumps land mid-window so
/// only early-anchored drains expire. The M clock only runs for *stamped* drains (drop confirmed);
/// a shed still pending confirmation ignores the jump entirely — the common case.
pub(super) fn advance_clock() -> BoxedStrategy<Action> {
    prop_oneof![
        3 => Just(M_TICKS),
        1 => Just(1u64),
        1 => Just(M_TICKS - 1),
        1 => Just(M_TICKS + 1),
        2 => 1..2 * M_TICKS,
    ]
    .prop_map(Action::AdvanceClock)
    .boxed()
}

/// Fetch transitions for the observation layer, as `(weight, strategy)` pairs for a model's
/// `Union`; empty when there is nothing to fetch yet. Weighted for the common within-M case; below
/// a 100% quorum the watermark-tolerated tail polls rarely and unreliably, so the consistency
/// oracle's accountability scoping carries real obligations rather than holding vacuously.
pub(super) fn fetch_choices<D: SimStorage>(
    sut: &SimUnderTest<D>,
) -> Vec<(u32, BoxedStrategy<Action>)> {
    let mut choices: Vec<(u32, BoxedStrategy<Action>)> = Vec::new();
    let lagging = sut.lagging_worker_indexes();
    let fast: Vec<usize> = sut
        .active_worker_indexes()
        .into_iter()
        .filter(|index| !lagging.contains(index))
        .collect();
    if !fast.is_empty() {
        // 9:1 success:miss gives lag without starving confirmation. Weighted up while sheds await
        // confirmation: fetches stamp a pending drop and start its M clock, putting
        // actively-draining copies in front of the boundary `AdvanceClock` jumps.
        let weight = if sut.has_pending_stale() { 10 } else { 6 };
        let worker_fetch = prop::sample::select(fast)
            .prop_flat_map(|worker| {
                fetch_succeeds(9, 1)
                    .prop_map(move |succeeds| Action::WorkerFetchAssignment { worker, succeeds })
            })
            .boxed();
        choices.push((weight, worker_fetch));
    }
    if !lagging.is_empty() {
        // Rare, mostly-missed polls: sustained stragglers the quorum advances without.
        let lagging_fetch = prop::sample::select(lagging)
            .prop_flat_map(|worker| {
                fetch_succeeds(1, 2)
                    .prop_map(move |succeeds| Action::WorkerFetchAssignment { worker, succeeds })
            })
            .boxed();
        choices.push((1, lagging_fetch));
    }
    if sut.has_published_portal_assignment() {
        // Weighted high so the portal stays within M most of the time.
        let portal_fetch = fetch_succeeds(9, 1)
            .prop_map(|succeeds| Action::PortalFetchAssignment { succeeds })
            .boxed();
        choices.push((6, portal_fetch));
    }
    choices
}

/// A fetch outcome weighted `success`:`miss` true:false — the shared Bernoulli draw behind every
/// fetch action, so the success/miss ratios live in one readable place.
fn fetch_succeeds(success: u32, miss: u32) -> BoxedStrategy<bool> {
    prop_oneof![success => Just(true), miss => Just(false)].boxed()
}

fn chunk_size_strategy() -> BoxedStrategy<u32> {
    if std::env::var_os("SIM_RANDOM_SIZES").is_some() {
        (MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).boxed()
    } else {
        Just(CHUNK_SIZE).boxed()
    }
}

/// Mint a chunk key from a numeric seed, zero-padded so ids sort by the seed. The *only* place
/// that knows the key format — everywhere else a [`ChunkKey`] is opaque.
pub(super) fn mint_key(seed: u64) -> ChunkKey {
    format!("{seed:020}")
}

const SIM_CHUNK_BLOCK_SPAN: u64 = 1_000;

/// Deterministic block range for a sim chunk. The key seed is the chunk's index; each index owns a
/// fixed-width, non-overlapping window. Generated keys are minted from `u16` seeds (`random_key`), so
/// the `as u32` narrowing is only a safety guard for explicitly-minted `u64` keys in tests. Block
/// range is independent of weight (the shared [`WeightTable`]) — orthogonal attributes, as in production.
///
/// To exercise the non-overlap gate, ~5% of chunks (index divisible by 20) instead land in a shared
/// low "collision" window: within a dataset they overlap each other, so the second and later such
/// chunks are rejected at registration. The rest keep disjoint windows.
pub(super) fn blocks_for_key(key: &str) -> std::ops::RangeInclusive<u64> {
    let seed: u64 = key
        .parse()
        .expect("sim chunk keys are minted from a u64 seed by mint_key");
    let index = u64::from(seed as u32);
    if index % 20 == 0 {
        return 0..=SIM_CHUNK_BLOCK_SPAN - 1;
    }
    let first = index * SIM_CHUNK_BLOCK_SPAN;
    first..=first + SIM_CHUNK_BLOCK_SPAN - 1
}

/// Fresh chunk key strategy. `no_shrink` keeps shrinking from collapsing two chunks onto one key.
pub(super) fn random_key() -> BoxedStrategy<ChunkKey> {
    any::<u16>()
        .no_shrink()
        .prop_map(|seed| mint_key(u64::from(seed)))
        .boxed()
}

/// Tuple → [`NewChunk`], so the `(key, size, weight, dataset)` shape every chunk strategy already
/// produces maps straight to a chunk with `.prop_map(new_chunk)` instead of a struct literal.
pub(super) fn new_chunk((key, size, weight, dataset): (ChunkKey, u32, u16, String)) -> NewChunk {
    let blocks = blocks_for_key(&key);
    NewChunk {
        key,
        size,
        weight,
        dataset,
        blocks,
    }
}

pub(super) fn add_chunk(
    key: impl Strategy<Value = ChunkKey> + 'static,
    size: impl Strategy<Value = u32> + 'static,
    weight: impl Strategy<Value = u16> + 'static,
    dataset: impl Strategy<Value = String> + 'static,
) -> BoxedStrategy<Action> {
    (key, size, weight, dataset)
        .prop_map(|shape| Action::AddChunks(vec![new_chunk(shape)]))
        .boxed()
}

/// `count` chunks in one `Add`, each field drawn per chunk.
pub(super) fn add_chunks(
    count: usize,
    size: impl Strategy<Value = u32> + 'static,
    weight: impl Strategy<Value = u16> + 'static,
    dataset: impl Strategy<Value = String> + 'static,
) -> BoxedStrategy<Action> {
    prop::collection::vec((random_key(), size, weight, dataset), count..=count)
        .prop_map(|shapes| Action::AddChunks(shapes.into_iter().map(new_chunk).collect()))
        .boxed()
}

/// An `Add` of 1..=`max` fully-random chunks — the staple move of the guided and churn walks.
pub(super) fn add_random_chunks(max: usize, datasets: &[String]) -> BoxedStrategy<Action> {
    let chunk = (
        random_key(),
        chunk_size_strategy(),
        prop::sample::select(WEIGHTS.to_vec()),
        prop::sample::select(datasets.to_vec()),
    )
        .prop_map(new_chunk);
    prop::collection::vec(chunk, 1..=max)
        .prop_map(Action::AddChunks)
        .boxed()
}

/// A `RegisterCorrection` for a random correctable chunk. `prop_oneof!` mints the replacement in
/// the old chunk's dataset (registers) or, occasionally, an unregistered foreign one (rejected),
/// so the walk exercises both outcomes. The replacement inherits the superseded chunk's block range
/// (`old.blocks`), so the swap is 1:1 same-range and a chain stays on the chain root's range.
pub(super) fn register_correction(olds: Vec<CorrectableOld>) -> BoxedStrategy<Action> {
    prop::sample::select(olds)
        .prop_flat_map(|old| {
            let repl_dataset = prop_oneof![
                4 => Just(old.dataset.clone()),
                1 => Just(format!("{}-foreign", old.dataset)),
            ];
            (
                random_key(),
                chunk_size_strategy(),
                prop::sample::select(WEIGHTS.to_vec()),
                repl_dataset,
            )
                .prop_map(move |(key, size, weight, repl_dataset)| {
                    Action::RegisterCorrection {
                        old_dataset: old.dataset.clone(),
                        old_chunk_id: old.chunk_id.clone(),
                        replacement: NewChunk {
                            key,
                            size,
                            weight,
                            dataset: repl_dataset,
                            blocks: old.blocks.clone(),
                        },
                    }
                })
        })
        .boxed()
}

/// A `SetDatasetSchema` for a random dataset, drawing its new schema from the canned
/// [`SCHEMA_POOL`]. Needs no precondition data (unlike [`register_correction`]) — any dataset
/// accepts any schema at any time, so this can always be offered.
pub(super) fn set_dataset_schema(datasets: &[String]) -> BoxedStrategy<Action> {
    (
        prop::sample::select(datasets.to_vec()),
        prop::sample::select(SCHEMA_POOL.to_vec()),
    )
        .prop_map(|(dataset, schema)| Action::SetDatasetSchema { dataset, schema })
        .boxed()
}

// ===========================================================================
// Model chunk ⇄ storage chunk, and the sim's weight strategy.
//
// A storage `Chunk` has no weight field — the scheduler derives weight through a `WeightStrategy`.
// Rather than smuggling weight through the block range, the sim records it in a shared table, so
// one dataset can hold chunks of any weight and the storage id only has to stay unique/sortable.
// ===========================================================================

/// Shared `chunk-id → weight` table, cloned so the SUT and the scheduler's [`WeightStrategy`] see
/// the same map. `Arc<Mutex<…>>` rather than a thread-local because proptest may run a case's
/// steps on any thread. Append-only.
#[derive(Clone, Default)]
pub(super) struct WeightTable(Arc<Mutex<HashMap<String, ChunkWeight>>>);

impl WeightTable {
    fn record(&self, id: &str, weight: ChunkWeight) {
        self.0
            .lock()
            .expect("weight table not poisoned")
            .insert(id.to_string(), weight);
    }

    fn weight_of(&self, id: &str) -> ChunkWeight {
        *self
            .0
            .lock()
            .expect("weight table not poisoned")
            .get(id)
            .expect("every storage chunk records its weight in storage_chunk")
    }
}

/// Sim-only: lets insert-input chunks flow through [`WeightStrategy::prepare`] directly.
impl SchedulingChunk for StorageNewChunk {
    fn dataset(&self) -> &std::sync::Arc<String> {
        &self.dataset
    }
    fn id(&self) -> &std::sync::Arc<String> {
        &self.id
    }
    fn blocks(&self) -> &std::ops::RangeInclusive<crate::types::BlockNumber> {
        &self.blocks
    }
    fn size(&self) -> u32 {
        self.size
    }
}

impl WeightStrategy for WeightTable {
    fn prepare<T: SchedulingChunk>(
        &self,
        chunks: Vec<T>,
    ) -> Vec<(T, ChunkWeight, Option<Version>)> {
        chunks
            .into_iter()
            .map(|chunk| {
                let weight = self.weight_of(chunk.id());
                (chunk, weight, None)
            })
            .collect()
    }
}

/// A chunk's storage primary key, `(dataset, key)`. Weight isn't encoded — it lives in the table.
#[cfg(test)]
pub(super) fn chunk_pk(chunk: &NewChunk) -> (String, String) {
    (chunk.dataset.clone(), chunk.key.clone())
}

/// Build the storage insert input for a model chunk. Weight is *not* recorded here — call
/// [`record_weights`] separately.
pub(super) fn storage_chunk(chunk: &NewChunk) -> StorageNewChunk {
    StorageNewChunk {
        dataset: Arc::new(chunk.dataset.clone()),
        id: Arc::new(chunk.key.clone()),
        size: chunk.size,
        blocks: chunk.blocks.clone(),
        schema_id: None,
        tables_present: None,
    }
}

/// Record each chunk's weight so the scheduler's [`WeightStrategy`] can return it.
pub(super) fn record_weights(weights: &WeightTable, chunks: &[NewChunk]) {
    for chunk in chunks {
        weights.record(&chunk.key, chunk.weight);
    }
}

/// Read-path counterpart of [`storage_chunk`]: the algorithm's view of a chunk already in storage.
pub(super) fn chunk_from_view(view: &ChunkView) -> AlgoChunk {
    AlgoChunk {
        dataset: Arc::new(view.dataset.clone()),
        id: Arc::new(view.chunk_id.clone()),
        size: view.size,
        blocks: view.blocks.clone(),
    }
}
