use anyhow::Context;
use network_scheduler::types::Worker;
use rand::seq::SliceRandom;
use semver::Version;

/// Target version workers upgrade to, and which restricted chunks require.
/// Chosen well above real worker versions so only upgraded workers serve
/// restricted data. Workers keep their original (loaded) version until upgraded.
pub fn new_version() -> Version {
    Version::new(10, 0, 0)
}

/// Ascending `(step, target_fraction)` breakpoints. The fraction is held
/// constant (piecewise-constant) from each breakpoint's step until the next.
#[derive(Debug, Clone, Default)]
pub struct UpgradeSchedule {
    breakpoints: Vec<(u32, f64)>,
}

impl UpgradeSchedule {
    /// Parses `"1:0,5:0.25,50:0.5"`. An empty string yields no breakpoints.
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let mut breakpoints: Vec<(u32, f64)> = Vec::new();
        for entry in s.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let (step_str, frac_str) = entry.split_once(':').with_context(|| {
                format!("Invalid schedule entry '{entry}', expected 'step:fraction'")
            })?;
            let step: u32 = step_str
                .trim()
                .parse()
                .with_context(|| format!("Invalid step '{step_str}'"))?;
            let frac: f64 = frac_str
                .trim()
                .parse()
                .with_context(|| format!("Invalid fraction '{frac_str}'"))?;
            anyhow::ensure!(
                frac.is_finite(),
                "Fraction '{frac_str}' is not a finite number"
            );
            anyhow::ensure!(
                (0.0..=1.0).contains(&frac),
                "Fraction {frac} out of range [0, 1]"
            );
            if let Some(&(last_step, _)) = breakpoints.last() {
                anyhow::ensure!(
                    step > last_step,
                    "Schedule steps must be strictly ascending (got {step} after {last_step})"
                );
            }
            breakpoints.push((step, frac));
        }
        Ok(Self { breakpoints })
    }

    /// The fraction of the latest breakpoint whose step is `<= step`, or 0.0 before the first.
    pub fn target_fraction(&self, step: u32) -> f64 {
        let mut frac = 0.0;
        for &(bp_step, bp_frac) in &self.breakpoints {
            if step >= bp_step {
                frac = bp_frac;
            } else {
                break;
            }
        }
        frac
    }
}

/// Tracks which workers are on the new version. Upgrades are monotonic: a
/// worker that reaches the new version never reverts. Workers are upgraded in a
/// fixed, seed-determined shuffle order.
pub struct WorkerVersionState {
    order: Vec<usize>,
    n: usize,
    upgraded: usize,
}

fn frac_to_count(fraction: f64, n: usize) -> usize {
    ((fraction * n as f64).round() as usize).min(n)
}

impl WorkerVersionState {
    pub fn new(n: usize, initial_new_fraction: f64, rng: &mut impl rand::Rng) -> Self {
        debug_assert!(
            (0.0..=1.0).contains(&initial_new_fraction),
            "initial_new_fraction {initial_new_fraction} out of range [0, 1]"
        );
        let mut order: Vec<usize> = (0..n).collect();
        order.shuffle(rng);
        Self {
            order,
            n,
            upgraded: frac_to_count(initial_new_fraction, n),
        }
    }

    /// Never decreases the count: a monotonic high-water mark.
    pub fn advance(&mut self, schedule: &UpgradeSchedule, step: u32) {
        let target = frac_to_count(schedule.target_fraction(step), self.n);
        self.upgraded = self.upgraded.max(target);
    }

    pub fn eligible_count(&self) -> usize {
        self.upgraded
    }

    /// Leaves every other worker on its original (loaded) version. Idempotent across steps, because
    /// the upgraded set only grows.
    pub fn apply(&self, workers: &mut [Worker], new: &Version) {
        debug_assert_eq!(
            workers.len(),
            self.n,
            "apply: workers slice length {} != state n {}",
            workers.len(),
            self.n
        );
        for &i in &self.order[..self.upgraded] {
            workers[i].version = Some(new.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p_identity::PeerId;
    use network_scheduler::types::{Worker, WorkerStatus};
    use rand::{SeedableRng, rngs::StdRng};

    fn make_test_workers(n: usize) -> Vec<Worker> {
        (0..n)
            .map(|_| Worker {
                id: PeerId::random(),
                status: WorkerStatus::Online,
                version: None,
            })
            .collect()
    }

    #[test]
    fn initial_fraction_sets_starting_upgraded_count() {
        let mut rng = StdRng::seed_from_u64(1);
        let st = WorkerVersionState::new(100, 0.2, &mut rng);
        assert_eq!(st.eligible_count(), 20);
    }

    #[test]
    fn advance_is_monotonic_and_respects_initial_floor() {
        let mut rng = StdRng::seed_from_u64(1);
        let mut st = WorkerVersionState::new(100, 0.2, &mut rng);
        let sched = UpgradeSchedule::parse("1:0,5:0.5").unwrap();
        st.advance(&sched, 1); // target 0, but floor is the initial 20
        assert_eq!(st.eligible_count(), 20);
        st.advance(&sched, 5); // target 50
        assert_eq!(st.eligible_count(), 50);
        st.advance(&sched, 6); // holds at 50 (monotonic)
        assert_eq!(st.eligible_count(), 50);
    }

    #[test]
    fn apply_stamps_exactly_eligible_count_with_new() {
        let original = Version::new(2, 10, 1);
        let mut rng = StdRng::seed_from_u64(7);
        let st = WorkerVersionState::new(10, 0.3, &mut rng);
        let mut workers = make_test_workers(10);
        for w in &mut workers {
            w.version = Some(original.clone());
        }
        st.apply(&mut workers, &new_version());
        let new_count = workers
            .iter()
            .filter(|w| w.version.as_ref() == Some(&new_version()))
            .count();
        assert_eq!(new_count, 3);
        // The rest keep their original version (not overwritten).
        assert_eq!(
            workers
                .iter()
                .filter(|w| w.version.as_ref() == Some(&original))
                .count(),
            7
        );
    }

    #[test]
    fn target_fraction_holds_between_breakpoints() {
        let s = UpgradeSchedule::parse("1:0,5:0.25,50:0.5").unwrap();
        assert_eq!(s.target_fraction(1), 0.0);
        assert_eq!(s.target_fraction(4), 0.0);
        assert_eq!(s.target_fraction(5), 0.25);
        assert_eq!(s.target_fraction(49), 0.25);
        assert_eq!(s.target_fraction(50), 0.5);
        assert_eq!(s.target_fraction(1000), 0.5);
    }

    #[test]
    fn target_fraction_before_first_breakpoint_is_zero() {
        let s = UpgradeSchedule::parse("5:0.25").unwrap();
        assert_eq!(s.target_fraction(1), 0.0);
    }

    #[test]
    fn empty_schedule_is_always_zero() {
        let s = UpgradeSchedule::parse("").unwrap();
        assert_eq!(s.target_fraction(100), 0.0);
    }

    #[test]
    fn rejects_descending_steps() {
        assert!(UpgradeSchedule::parse("5:0.25,2:0.5").is_err());
    }

    #[test]
    fn rejects_out_of_range_fraction() {
        assert!(UpgradeSchedule::parse("1:1.5").is_err());
    }

    #[test]
    fn rejects_malformed_entry() {
        assert!(UpgradeSchedule::parse("1-0").is_err());
    }

    #[test]
    fn rejects_non_finite_fraction() {
        assert!(UpgradeSchedule::parse("1:NaN").is_err());
        assert!(UpgradeSchedule::parse("1:inf").is_err());
    }

    #[test]
    fn rejects_duplicate_steps() {
        assert!(UpgradeSchedule::parse("5:0.25,5:0.5").is_err());
    }

    #[test]
    fn apply_with_zero_upgraded_leaves_originals_untouched() {
        let original = Version::new(2, 10, 1);
        let mut rng = StdRng::seed_from_u64(3);
        let st = WorkerVersionState::new(5, 0.0, &mut rng);
        let mut workers = make_test_workers(5);
        for w in &mut workers {
            w.version = Some(original.clone());
        }
        st.apply(&mut workers, &new_version());
        assert!(
            workers
                .iter()
                .all(|w| w.version.as_ref() == Some(&original))
        );
    }
}
