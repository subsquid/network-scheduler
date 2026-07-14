//! Per-step chunk ingestion rate — how many chunks to generate at each step.
//!
//! A [`ChunkRate`] is a pure function `count(step, total_steps) -> u32`,
//! evaluated once per step. Steps are 1-indexed.

use anyhow::{Context, bail, ensure};

/// Default per-step count for the steady/peak shapes.
const DEFAULT_COUNT: u32 = 1000;
/// Default size of a bulk insert (`pulse`, `bursts`).
const DEFAULT_BULK: u32 = 10_000;
/// Default period for `bursts`.
const DEFAULT_EVERY: u32 = 5;

/// A shape describing how many chunks to generate at each step. Every parameter
/// has a sane default and can be overridden in the spec.
#[derive(Debug, Clone, PartialEq)]
pub enum ChunkRate {
    /// `n` chunks every step.
    Constant { n: u32 },
    /// Linear from `from` (step 1) to `to` (last step).
    Ramp { from: u32, to: u32 },
    /// Linear `0 → peak` up to `center`, then `peak → 0` (default center: middle).
    Triangle { peak: u32, center: Option<u32> },
    /// Bell curve `peak · e^(−(step − center)² / 2σ²)`. Defaults: center =
    /// middle, sigma = `total_steps / 6`.
    Gaussian {
        peak: u32,
        sigma: Option<f64>,
        center: Option<u32>,
    },
    /// `base` everywhere, but `size` for `width` steps starting at `at` (default
    /// `at`: middle).
    Pulse {
        size: u32,
        at: Option<u32>,
        base: u32,
        width: u32,
    },
    /// `size` on every `every`-th step (up to `until`), else `base`.
    Bursts {
        size: u32,
        every: u32,
        base: u32,
        until: Option<u32>,
    },
    /// Piecewise-linear interpolation between `(step, count)` breakpoints.
    Points { breakpoints: Vec<(u32, u32)> },
}

impl ChunkRate {
    /// Parses a shape spec like `triangle:peak=5000,center=50` or
    /// `points:1=0,50=5000,100=0`.
    pub fn parse(spec: &str) -> anyhow::Result<Self> {
        let (name, rest) = match spec.split_once(':') {
            Some((name, rest)) => (name.trim(), rest.trim()),
            None => (spec.trim(), ""),
        };
        let params = Params::parse(rest)?;

        let rate = match name {
            "constant" => ChunkRate::Constant {
                n: params.u32_or("n", DEFAULT_COUNT)?,
            },
            "ramp" => ChunkRate::Ramp {
                from: params.u32_or("from", 0)?,
                to: params.u32_or("to", DEFAULT_COUNT)?,
            },
            "triangle" => ChunkRate::Triangle {
                peak: params.u32_or("peak", DEFAULT_COUNT)?,
                center: params.opt_u32("center")?,
            },
            "gaussian" => ChunkRate::Gaussian {
                peak: params.u32_or("peak", DEFAULT_COUNT)?,
                sigma: params.opt_f64("sigma")?,
                center: params.opt_u32("center")?,
            },
            "pulse" => ChunkRate::Pulse {
                size: params.u32_or("size", DEFAULT_BULK)?,
                at: params.opt_u32("at")?,
                base: params.u32_or("base", 0)?,
                width: params.u32_or("width", 1)?,
            },
            "bursts" => ChunkRate::Bursts {
                size: params.u32_or("size", DEFAULT_BULK)?,
                every: params.u32_or("every", DEFAULT_EVERY)?,
                base: params.u32_or("base", 0)?,
                until: params.opt_u32("until")?,
            },
            "points" => ChunkRate::Points {
                breakpoints: params.breakpoints()?,
            },
            other => bail!("unknown chunks shape '{other}'"),
        };
        rate.validate()?;
        Ok(rate)
    }

    fn validate(&self) -> anyhow::Result<()> {
        match self {
            ChunkRate::Gaussian {
                sigma: Some(sigma), ..
            } => ensure!(*sigma > 0.0, "gaussian sigma must be positive"),
            ChunkRate::Pulse { width, .. } => ensure!(*width > 0, "pulse width must be positive"),
            ChunkRate::Bursts { every, .. } => ensure!(*every > 0, "bursts every must be positive"),
            _ => {}
        }
        Ok(())
    }

    /// Number of chunks to generate at `step` (1-indexed) of `total_steps`.
    pub fn count_at(&self, step: u32, total_steps: u32) -> u32 {
        match self {
            ChunkRate::Constant { n } => *n,
            ChunkRate::Ramp { from, to } => lerp(*from, *to, step, 1, total_steps),
            ChunkRate::Triangle { peak, center } => {
                let center = middle_or(*center, total_steps);
                if step <= center {
                    lerp(0, *peak, step, 1, center)
                } else {
                    lerp(*peak, 0, step, center, total_steps)
                }
            }
            ChunkRate::Gaussian {
                peak,
                sigma,
                center,
            } => {
                let center = middle_or(*center, total_steps) as f64;
                let sigma = sigma.unwrap_or((total_steps as f64 / 6.0).max(1.0));
                let exponent = -((step as f64 - center).powi(2)) / (2.0 * sigma * sigma);
                (*peak as f64 * exponent.exp()).round() as u32
            }
            ChunkRate::Pulse {
                size,
                at,
                base,
                width,
            } => {
                let at = middle_or(*at, total_steps);
                if step >= at && step < at + width {
                    *size
                } else {
                    *base
                }
            }
            ChunkRate::Bursts {
                size,
                every,
                base,
                until,
            } => {
                let within = until.is_none_or(|u| step <= u);
                if within && step % every == 0 {
                    *size
                } else {
                    *base
                }
            }
            ChunkRate::Points { breakpoints } => interpolate(breakpoints, step),
        }
    }
}

/// Resolves an optional step position, defaulting to the middle of the run.
fn middle_or(value: Option<u32>, total_steps: u32) -> u32 {
    value.unwrap_or_else(|| total_steps.div_ceil(2)).max(1)
}

/// Linearly interpolates a count from `(x0, y0)` to `(x1, y1)`, clamped to the
/// `[x0, x1]` range. Handles decreasing segments (`y1 < y0`).
fn lerp(y0: u32, y1: u32, x: u32, x0: u32, x1: u32) -> u32 {
    if x1 <= x0 {
        return y1;
    }
    let x = x.clamp(x0, x1);
    let t = (x - x0) as f64 / (x1 - x0) as f64;
    (y0 as f64 + (y1 as f64 - y0 as f64) * t).round() as u32
}

/// Piecewise-linear interpolation between ascending `(step, count)` breakpoints;
/// flat before the first and after the last.
fn interpolate(breakpoints: &[(u32, u32)], step: u32) -> u32 {
    let first = breakpoints[0];
    let last = *breakpoints.last().unwrap();
    if step <= first.0 {
        return first.1;
    }
    if step >= last.0 {
        return last.1;
    }
    for segment in breakpoints.windows(2) {
        let (s0, c0) = segment[0];
        let (s1, c1) = segment[1];
        if step <= s1 {
            return lerp(c0, c1, step, s0, s1);
        }
    }
    last.1
}

/// Parsed `key=value` parameters of a shape spec.
struct Params(Vec<(String, String)>);

impl Params {
    fn parse(rest: &str) -> anyhow::Result<Self> {
        let mut pairs = Vec::new();
        for entry in rest.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let (key, value) = entry
                .split_once('=')
                .with_context(|| format!("expected key=value in '{entry}'"))?;
            pairs.push((key.trim().to_string(), value.trim().to_string()));
        }
        Ok(Params(pairs))
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    fn u32_or(&self, key: &str, default: u32) -> anyhow::Result<u32> {
        match self.get(key) {
            Some(value) => value.parse().with_context(|| format!("invalid '{key}'")),
            None => Ok(default),
        }
    }

    fn opt_u32(&self, key: &str) -> anyhow::Result<Option<u32>> {
        self.get(key)
            .map(|value| value.parse().with_context(|| format!("invalid '{key}'")))
            .transpose()
    }

    fn opt_f64(&self, key: &str) -> anyhow::Result<Option<f64>> {
        self.get(key)
            .map(|value| value.parse().with_context(|| format!("invalid '{key}'")))
            .transpose()
    }

    /// Interprets every `key=value` pair as a `step=count` breakpoint.
    fn breakpoints(&self) -> anyhow::Result<Vec<(u32, u32)>> {
        let mut points = Vec::new();
        for (key, value) in &self.0 {
            let step: u32 = key
                .parse()
                .with_context(|| format!("invalid step '{key}'"))?;
            let count: u32 = value
                .parse()
                .with_context(|| format!("invalid count '{value}'"))?;
            points.push((step, count));
        }
        ensure!(
            !points.is_empty(),
            "points shape needs at least one 'step=count'"
        );
        points.sort_by_key(|(step, _)| *step);
        Ok(points)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_is_flat() {
        let rate = ChunkRate::parse("constant:n=1000").unwrap();
        assert_eq!(rate.count_at(1, 10), 1000);
        assert_eq!(rate.count_at(7, 10), 1000);
    }

    #[test]
    fn ramp_interpolates_endpoints() {
        let rate = ChunkRate::parse("ramp:from=0,to=100").unwrap();
        assert_eq!(rate.count_at(1, 11), 0);
        assert_eq!(rate.count_at(6, 11), 50);
        assert_eq!(rate.count_at(11, 11), 100);
    }

    #[test]
    fn triangle_peaks_in_the_middle() {
        let rate = ChunkRate::parse("triangle:peak=100,center=5").unwrap();
        assert_eq!(rate.count_at(1, 10), 0);
        assert_eq!(rate.count_at(3, 10), 50);
        assert_eq!(rate.count_at(5, 10), 100);
        assert_eq!(rate.count_at(10, 10), 0);
    }

    #[test]
    fn gaussian_peaks_at_center() {
        let rate = ChunkRate::parse("gaussian:peak=100,sigma=2,center=5").unwrap();
        assert_eq!(rate.count_at(5, 10), 100);
        assert!(rate.count_at(1, 10) < rate.count_at(3, 10));
        assert!(rate.count_at(3, 10) < rate.count_at(5, 10));
    }

    #[test]
    fn pulse_spikes_once() {
        let rate = ChunkRate::parse("pulse:size=500,at=5").unwrap();
        assert_eq!(rate.count_at(4, 10), 0);
        assert_eq!(rate.count_at(5, 10), 500);
        assert_eq!(rate.count_at(6, 10), 0);
    }

    #[test]
    fn bursts_repeat_until_limit() {
        let rate = ChunkRate::parse("bursts:size=10,every=3,until=6").unwrap();
        assert_eq!(rate.count_at(2, 10), 0);
        assert_eq!(rate.count_at(3, 10), 10);
        assert_eq!(rate.count_at(6, 10), 10);
        assert_eq!(rate.count_at(9, 10), 0); // past `until`
    }

    #[test]
    fn points_interpolate_piecewise() {
        let rate = ChunkRate::parse("points:1=0,5=100,9=0").unwrap();
        assert_eq!(rate.count_at(1, 10), 0);
        assert_eq!(rate.count_at(3, 10), 50);
        assert_eq!(rate.count_at(5, 10), 100);
        assert_eq!(rate.count_at(7, 10), 50);
        assert_eq!(rate.count_at(9, 10), 0);
    }

    #[test]
    fn defaults_apply_without_params() {
        assert_eq!(ChunkRate::parse("constant").unwrap().count_at(3, 10), 1000);
        assert_eq!(ChunkRate::parse("ramp").unwrap().count_at(10, 10), 1000);
        assert_eq!(ChunkRate::parse("triangle").unwrap().count_at(5, 10), 1000);
        assert_eq!(ChunkRate::parse("gaussian").unwrap().count_at(5, 10), 1000);
        assert_eq!(ChunkRate::parse("pulse").unwrap().count_at(5, 10), 10_000);
        assert_eq!(ChunkRate::parse("pulse").unwrap().count_at(1, 10), 0);
        assert_eq!(ChunkRate::parse("bursts").unwrap().count_at(5, 10), 10_000);
    }

    #[test]
    fn rejects_malformed_specs() {
        for spec in [
            "spiral:peak=1",           // unknown shape
            "points",                  // no breakpoints
            "gaussian:peak=1,sigma=0", // sigma must be positive
        ] {
            assert!(ChunkRate::parse(spec).is_err(), "{spec} must be rejected");
        }
    }
}
