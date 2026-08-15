use std::collections::HashMap;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use ns_core::ChannelKind;

/// A single channel's token bucket: `capacity` tokens, refilled continuously
/// at `refill_per_sec`, one consumed per successful `try_acquire`. Takes
/// `now` explicitly rather than reading the clock itself so callers (and
/// tests) can drive refill deterministically.
struct Bucket {
    capacity: f64,
    refill_per_sec: f64,
    tokens: f64,
    last_refill: DateTime<Utc>,
}

impl Bucket {
    fn new(capacity: u32, refill_per_sec: f64, now: DateTime<Utc>) -> Self {
        Self { capacity: capacity as f64, refill_per_sec, tokens: capacity as f64, last_refill: now }
    }

    fn try_acquire(&mut self, now: DateTime<Utc>) -> bool {
        let elapsed_secs = (now - self.last_refill).num_milliseconds().max(0) as f64 / 1000.0;
        self.tokens = (self.tokens + elapsed_secs * self.refill_per_sec).min(self.capacity);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Per-channel token-bucket rate limiter, so a burst against one provider
/// (e.g. the SMS gateway) can't starve sends to another (e.g. webhooks).
/// `Worker` consults this before dispatching each claimed notification;
/// a denied acquire leaves the notification claimed as `Sending` for this
/// pass, to be retried on the next poll rather than burning a delivery
/// attempt. Interior mutability (`Mutex`) lets a single `RateLimiter` be
/// shared (typically via `Arc`) across concurrently-running workers
/// without callers needing `&mut self`.
#[derive(Default)]
pub struct RateLimiter {
    limits: HashMap<ChannelKind, (u32, f64)>,
    buckets: Mutex<HashMap<ChannelKind, Bucket>>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Caps `kind` at `capacity` in-flight tokens, refilling at
    /// `refill_per_sec` tokens/second. A channel with no configured limit
    /// is unrestricted.
    pub fn with_limit(mut self, kind: ChannelKind, capacity: u32, refill_per_sec: f64) -> Self {
        self.limits.insert(kind, (capacity, refill_per_sec));
        self
    }

    /// Returns `true` and consumes a token if `kind` may send right now;
    /// `false` if it's currently rate-limited. Always `true` for a channel
    /// with no configured limit.
    pub fn try_acquire(&self, kind: ChannelKind, now: DateTime<Utc>) -> bool {
        let Some(&(capacity, refill_per_sec)) = self.limits.get(&kind) else {
            return true;
        };

        let mut buckets = self.buckets.lock().unwrap();
        let bucket = buckets.entry(kind).or_insert_with(|| Bucket::new(capacity, refill_per_sec, now));
        bucket.try_acquire(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn unlimited_channel_always_acquires() {
        let limiter = RateLimiter::new();
        let now = Utc::now();
        for _ in 0..100 {
            assert!(limiter.try_acquire(ChannelKind::Email, now));
        }
    }

    #[test]
    fn exhausts_capacity_then_denies() {
        let limiter = RateLimiter::new().with_limit(ChannelKind::Sms, 2, 1.0);
        let now = Utc::now();

        assert!(limiter.try_acquire(ChannelKind::Sms, now));
        assert!(limiter.try_acquire(ChannelKind::Sms, now));
        assert!(!limiter.try_acquire(ChannelKind::Sms, now));
    }

    #[test]
    fn refills_over_time() {
        let limiter = RateLimiter::new().with_limit(ChannelKind::Sms, 1, 1.0);
        let now = Utc::now();

        assert!(limiter.try_acquire(ChannelKind::Sms, now));
        assert!(!limiter.try_acquire(ChannelKind::Sms, now));

        let later = now + Duration::seconds(2);
        assert!(limiter.try_acquire(ChannelKind::Sms, later));
    }

    #[test]
    fn refill_never_exceeds_capacity() {
        let limiter = RateLimiter::new().with_limit(ChannelKind::Push, 3, 100.0);
        let now = Utc::now();
        let much_later = now + Duration::hours(1);

        // Capacity caps the refill regardless of how much time passed, so
        // only 3 tokens are available, not an unbounded backlog.
        assert!(limiter.try_acquire(ChannelKind::Push, much_later));
        assert!(limiter.try_acquire(ChannelKind::Push, much_later));
        assert!(limiter.try_acquire(ChannelKind::Push, much_later));
        assert!(!limiter.try_acquire(ChannelKind::Push, much_later));
    }

    #[test]
    fn channels_are_independent() {
        let limiter =
            RateLimiter::new().with_limit(ChannelKind::Sms, 1, 1.0).with_limit(ChannelKind::Push, 1, 1.0);
        let now = Utc::now();

        assert!(limiter.try_acquire(ChannelKind::Sms, now));
        assert!(!limiter.try_acquire(ChannelKind::Sms, now));
        assert!(limiter.try_acquire(ChannelKind::Push, now), "push bucket must be unaffected by sms");
    }
}
