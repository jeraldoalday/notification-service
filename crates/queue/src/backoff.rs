use chrono::Duration;
use rand::Rng;

const BASE_SECS: i64 = 2;
const MAX_SECS: i64 = 15 * 60;

/// The deterministic exponential curve before jitter is layered on: 2s,
/// 4s, 8s, 16s, ... capped at 15 minutes so a notification with a large
/// `max_attempts` doesn't end up scheduled days out.
fn base_delay_secs(attempt_count: u32) -> i64 {
    let exponent = attempt_count.saturating_sub(1).min(16);
    let secs = BASE_SECS.saturating_mul(1i64 << exponent);
    secs.min(MAX_SECS)
}

/// Delay before the next retry, given the notification's `attempt_count`
/// after the failed attempt (1 for the first failure, 2 for the second,
/// ...).
///
/// Every notification on the same channel that fails at the same moment
/// computes the exact same exponential delay from the exact same
/// `attempt_count`, so a plain exponential curve reschedules a whole
/// outage's backlog to retry in lockstep -- the resulting burst looks
/// just like the failure that caused it, and repeats every cycle rather
/// than spreading out. "Equal jitter" (half the computed delay, plus a
/// random amount up to the other half) keeps the curve's lower bound, so
/// a flapping provider still gets real breathing room, while spreading
/// the upper half of each retry across a window instead of a single
/// instant.
pub fn delay(attempt_count: u32) -> Duration {
    jittered_delay(attempt_count, &mut rand::thread_rng())
}

fn jittered_delay(attempt_count: u32, rng: &mut impl Rng) -> Duration {
    let base = base_delay_secs(attempt_count);
    let half = base / 2;
    Duration::seconds(half + rng.gen_range(0..=half))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn doubles_each_attempt_before_jitter() {
        assert_eq!(base_delay_secs(1), 2);
        assert_eq!(base_delay_secs(2), 4);
        assert_eq!(base_delay_secs(3), 8);
        assert_eq!(base_delay_secs(4), 16);
    }

    #[test]
    fn caps_at_fifteen_minutes() {
        assert_eq!(base_delay_secs(10), MAX_SECS);
        assert_eq!(base_delay_secs(100), MAX_SECS);
        for _ in 0..50 {
            assert!(delay(10).num_seconds() <= MAX_SECS);
            assert!(delay(100).num_seconds() <= MAX_SECS);
        }
    }

    #[test]
    fn zero_attempts_is_around_the_base_delay() {
        for _ in 0..50 {
            let secs = delay(0).num_seconds();
            assert!((1..=2).contains(&secs), "expected 1 or 2, got {secs}");
        }
    }

    #[test]
    fn jittered_delay_never_falls_below_half_the_deterministic_curve() {
        for attempt in 1..=12 {
            let base = base_delay_secs(attempt);
            for _ in 0..50 {
                let secs = delay(attempt).num_seconds();
                assert!(
                    secs >= base / 2 && secs <= base,
                    "attempt {attempt}: {secs} not in [{}, {}]",
                    base / 2,
                    base
                );
            }
        }
    }

    #[test]
    fn jitter_actually_varies_the_delay_across_calls() {
        let values: BTreeSet<i64> = (0..200).map(|_| delay(6).num_seconds()).collect();
        assert!(values.len() > 1, "expected jitter to vary the delay across calls, got {values:?}");
    }
}
