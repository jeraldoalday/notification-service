use chrono::Duration;

const BASE_SECS: i64 = 2;
const MAX_SECS: i64 = 15 * 60;

/// Delay before the next retry, given the notification's `attempt_count`
/// after the failed attempt (1 for the first failure, 2 for the second,
/// ...). Exponential -- 2s, 4s, 8s, 16s, ... -- capped at 15 minutes so a
/// notification with a large `max_attempts` doesn't end up scheduled days
/// out.
pub fn delay(attempt_count: u32) -> Duration {
    let exponent = attempt_count.saturating_sub(1).min(16);
    let secs = BASE_SECS.saturating_mul(1i64 << exponent);
    Duration::seconds(secs.min(MAX_SECS))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_each_attempt() {
        assert_eq!(delay(1), Duration::seconds(2));
        assert_eq!(delay(2), Duration::seconds(4));
        assert_eq!(delay(3), Duration::seconds(8));
        assert_eq!(delay(4), Duration::seconds(16));
    }

    #[test]
    fn caps_at_fifteen_minutes() {
        assert_eq!(delay(10), Duration::seconds(MAX_SECS));
        assert_eq!(delay(100), Duration::seconds(MAX_SECS));
    }

    #[test]
    fn zero_attempts_is_base_delay() {
        assert_eq!(delay(0), Duration::seconds(BASE_SECS));
    }
}
