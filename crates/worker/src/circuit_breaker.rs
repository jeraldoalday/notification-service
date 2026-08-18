use std::collections::HashMap;
use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};
use ns_core::ChannelKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Sends are allowed; failures accumulate toward `failure_threshold`.
    Closed,
    /// Sends are denied until `reset_after` has elapsed since the breaker
    /// tripped.
    Open,
    /// The cooldown has elapsed and exactly one trial send is in flight;
    /// its outcome decides whether the breaker closes or reopens.
    HalfOpen,
}

struct Breaker {
    state: State,
    consecutive_failures: u32,
    opened_at: Option<DateTime<Utc>>,
}

impl Breaker {
    fn new() -> Self {
        Self { state: State::Closed, consecutive_failures: 0, opened_at: None }
    }
}

/// Per-channel circuit breaker guarding against hammering a provider that's
/// already down. `RateLimiter` throttles volume regardless of provider
/// health; this reacts to it -- after `failure_threshold` consecutive
/// failed sends on a channel, the breaker trips `Open` and denies further
/// sends until `reset_after` has elapsed, then goes `HalfOpen` and lets a
/// single trial through: success closes it, failure reopens it and resets
/// the cooldown. Interior mutability (`Mutex`) lets a single
/// `CircuitBreaker` be shared (typically via `Arc`) across concurrently
/// dispatching workers without callers needing `&mut self`.
pub struct CircuitBreaker {
    failure_threshold: u32,
    reset_after: Duration,
    breakers: Mutex<HashMap<ChannelKind, Breaker>>,
}

impl CircuitBreaker {
    /// Trips a channel's breaker after `failure_threshold` consecutive
    /// failures, holding it open for `reset_after` before trialing again.
    pub fn new(failure_threshold: u32, reset_after: Duration) -> Self {
        Self { failure_threshold, reset_after, breakers: Mutex::new(HashMap::new()) }
    }

    /// How long a tripped breaker stays open before trialing again.
    /// `Worker` uses this to decide how far ahead to defer work it
    /// declined to send, so deferred notifications come back around about
    /// when the breaker is ready to trial one.
    pub fn cooldown(&self) -> Duration {
        self.reset_after
    }

    /// A breaker that never trips, for deployments (or tests) that don't
    /// want this protection.
    pub fn disabled() -> Self {
        Self::new(u32::MAX, Duration::zero())
    }

    /// Returns `true` if a send on `kind` may proceed right now. Callers
    /// are expected to report the outcome via `record_success` or
    /// `record_failure` afterward so the breaker can react to it.
    pub fn allow(&self, kind: ChannelKind, now: DateTime<Utc>) -> bool {
        let mut breakers = self.breakers.lock().unwrap();
        let breaker = breakers.entry(kind).or_insert_with(Breaker::new);

        match breaker.state {
            State::Closed | State::HalfOpen => true,
            State::Open => {
                let opened_at = breaker.opened_at.expect("Open state always records opened_at");
                if now - opened_at >= self.reset_after {
                    breaker.state = State::HalfOpen;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Reports a successful send on `kind`: closes the breaker and clears
    /// its failure count, whatever state it was previously in.
    pub fn record_success(&self, kind: ChannelKind) {
        let mut breakers = self.breakers.lock().unwrap();
        let breaker = breakers.entry(kind).or_insert_with(Breaker::new);
        breaker.state = State::Closed;
        breaker.consecutive_failures = 0;
        breaker.opened_at = None;
    }

    /// Reports a failed send on `kind`. In `HalfOpen`, this immediately
    /// reopens the breaker and restarts the cooldown. In `Closed`, it
    /// increments the failure count and trips the breaker once
    /// `failure_threshold` is reached.
    pub fn record_failure(&self, kind: ChannelKind, now: DateTime<Utc>) {
        let mut breakers = self.breakers.lock().unwrap();
        let breaker = breakers.entry(kind).or_insert_with(Breaker::new);

        match breaker.state {
            State::HalfOpen => {
                breaker.state = State::Open;
                breaker.opened_at = Some(now);
            }
            State::Closed => {
                breaker.consecutive_failures += 1;
                if breaker.consecutive_failures >= self.failure_threshold {
                    breaker.state = State::Open;
                    breaker.opened_at = Some(now);
                }
            }
            State::Open => {
                // A failure shouldn't normally land here -- a denied
                // `allow` means no send was attempted -- but refresh the
                // cooldown defensively rather than leaving it stale.
                breaker.opened_at = Some(now);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_breaker_allows_sends() {
        let breaker = CircuitBreaker::new(3, Duration::seconds(30));
        assert!(breaker.allow(ChannelKind::Webhook, Utc::now()));
    }

    #[test]
    fn trips_open_after_consecutive_failures() {
        let breaker = CircuitBreaker::new(3, Duration::seconds(30));
        let now = Utc::now();

        breaker.record_failure(ChannelKind::Sms, now);
        breaker.record_failure(ChannelKind::Sms, now);
        assert!(breaker.allow(ChannelKind::Sms, now), "still below threshold");

        breaker.record_failure(ChannelKind::Sms, now);
        assert!(!breaker.allow(ChannelKind::Sms, now), "threshold reached, breaker should be open");
    }

    #[test]
    fn success_resets_the_failure_count() {
        let breaker = CircuitBreaker::new(3, Duration::seconds(30));
        let now = Utc::now();

        breaker.record_failure(ChannelKind::Sms, now);
        breaker.record_failure(ChannelKind::Sms, now);
        breaker.record_success(ChannelKind::Sms);
        breaker.record_failure(ChannelKind::Sms, now);
        breaker.record_failure(ChannelKind::Sms, now);

        assert!(breaker.allow(ChannelKind::Sms, now), "count should have reset after the success");
    }

    #[test]
    fn half_opens_and_allows_a_trial_after_cooldown() {
        let breaker = CircuitBreaker::new(1, Duration::seconds(30));
        let now = Utc::now();

        breaker.record_failure(ChannelKind::Push, now);
        assert!(!breaker.allow(ChannelKind::Push, now));

        let later = now + Duration::seconds(31);
        assert!(breaker.allow(ChannelKind::Push, later), "cooldown elapsed, should allow a trial send");
    }

    #[test]
    fn successful_trial_closes_the_breaker_and_resets_the_failure_count() {
        let breaker = CircuitBreaker::new(2, Duration::seconds(30));
        let now = Utc::now();

        breaker.record_failure(ChannelKind::Push, now);
        breaker.record_failure(ChannelKind::Push, now);
        assert!(!breaker.allow(ChannelKind::Push, now), "threshold reached, breaker should be open");

        let later = now + Duration::seconds(31);
        assert!(breaker.allow(ChannelKind::Push, later), "cooldown elapsed, should allow a trial send");

        breaker.record_success(ChannelKind::Push);
        assert!(breaker.allow(ChannelKind::Push, later));
        // The close reset the failure count, so one more failure alone
        // must not retrip a breaker with a threshold of two.
        breaker.record_failure(ChannelKind::Push, later);
        assert!(breaker.allow(ChannelKind::Push, later), "a single failure after a close must not retrip");
    }

    #[test]
    fn failed_trial_reopens_and_restarts_the_cooldown() {
        let breaker = CircuitBreaker::new(1, Duration::seconds(30));
        let now = Utc::now();

        breaker.record_failure(ChannelKind::Push, now);
        let trial_at = now + Duration::seconds(31);
        assert!(breaker.allow(ChannelKind::Push, trial_at));

        breaker.record_failure(ChannelKind::Push, trial_at);
        assert!(!breaker.allow(ChannelKind::Push, trial_at), "failed trial should reopen the breaker");

        // Cooldown restarted from trial_at, so it's not over yet at
        // trial_at + 10s even though it would have been relative to `now`.
        assert!(!breaker.allow(ChannelKind::Push, trial_at + Duration::seconds(10)));
        assert!(breaker.allow(ChannelKind::Push, trial_at + Duration::seconds(31)));
    }

    #[test]
    fn channels_are_independent() {
        let breaker = CircuitBreaker::new(1, Duration::seconds(30));
        let now = Utc::now();

        breaker.record_failure(ChannelKind::Sms, now);
        assert!(!breaker.allow(ChannelKind::Sms, now));
        assert!(breaker.allow(ChannelKind::Email, now), "email breaker must be unaffected by sms");
    }

    #[test]
    fn disabled_breaker_never_trips() {
        let breaker = CircuitBreaker::disabled();
        let now = Utc::now();
        for _ in 0..1000 {
            breaker.record_failure(ChannelKind::Webhook, now);
        }
        assert!(breaker.allow(ChannelKind::Webhook, now));
    }
}
