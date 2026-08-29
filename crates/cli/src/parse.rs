use serde::de::DeserializeOwned;

use ns_core::{ChannelKind, DeliveryStatus, Priority};

/// Parses one of the domain's string-valued enums by round-tripping
/// through its `serde` representation.
///
/// Doing it this way rather than deriving `clap::ValueEnum` on the types
/// keeps `ns-core` free of a CLI dependency, and guarantees the values
/// accepted on the command line are exactly the ones the HTTP API and the
/// stored JSON use -- there's no second spelling of `dead_lettered` to
/// drift out of sync.
fn parse_enum<T: DeserializeOwned>(raw: &str, valid: &[&str]) -> Result<T, String> {
    serde_json::from_value(serde_json::Value::String(raw.to_string()))
        .map_err(|_| format!("expected one of: {}", valid.join(", ")))
}

pub fn channel(raw: &str) -> Result<ChannelKind, String> {
    parse_enum(raw, &ChannelKind::all().map(|c| c.as_str()))
}

pub fn status(raw: &str) -> Result<DeliveryStatus, String> {
    parse_enum(raw, &DeliveryStatus::all().map(|s| s.as_str()))
}

/// Parses a `--scope` value. `ns-auth::Scope` already has a `FromStr`
/// whose error names the accepted values, so this just forwards to it
/// rather than restating the list in a second place.
pub fn scope(raw: &str) -> Result<ns_auth::Scope, String> {
    raw.parse()
}

/// Parses a `--reason` value. Forwards to the domain's own `FromStr`,
/// whose error already names the accepted values.
pub fn suppression_reason(raw: &str) -> Result<ns_core::SuppressionReason, String> {
    raw.parse().map_err(|err: ns_core::CoreError| err.to_string())
}

/// Parses an `--idempotency-key` value. Forwards to the domain's own
/// validation, so the CLI accepts exactly the keys the HTTP API does --
/// a key that works through one has to work through the other, or an
/// operator reproducing a client's submission by hand gets a different
/// answer than the client did.
pub fn idempotency_key(raw: &str) -> Result<ns_core::IdempotencyKey, String> {
    raw.parse().map_err(|err: ns_core::CoreError| err.to_string())
}

pub fn priority(raw: &str) -> Result<Priority, String> {
    parse_enum(raw, &["low", "normal", "high", "critical"])
}

/// Parses a `--var name=value` pair. Only the first `=` separates, so a
/// value may itself contain `=` (URLs with query strings turn up in
/// template variables often enough to matter).
pub fn key_value(raw: &str) -> Result<(String, String), String> {
    let Some((key, value)) = raw.split_once('=') else {
        return Err("expected name=value".to_string());
    };
    if key.is_empty() {
        return Err("variable name cannot be empty".to_string());
    }
    Ok((key.to_string(), value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_scope_auth_defines() {
        for value in ns_auth::Scope::all() {
            assert_eq!(scope(value.as_str()).unwrap(), value);
        }
        assert!(scope("superuser").unwrap_err().contains("read, write, admin"));
    }

    #[test]
    fn parses_every_channel_the_domain_defines() {
        for kind in ChannelKind::all() {
            assert_eq!(channel(kind.as_str()).unwrap(), kind);
        }
    }

    #[test]
    fn parses_every_status_the_domain_defines() {
        for value in DeliveryStatus::all() {
            assert_eq!(status(value.as_str()).unwrap(), value);
        }
    }

    #[test]
    fn parses_every_suppression_reason_the_domain_defines() {
        for value in ns_core::SuppressionReason::all() {
            assert_eq!(suppression_reason(value.as_str()).unwrap(), value);
        }
        assert!(suppression_reason("annoyed").is_err());
    }

    #[test]
    fn parses_the_idempotency_keys_the_api_accepts() {
        assert_eq!(idempotency_key("order-1041").unwrap().as_str(), "order-1041");
        assert!(idempotency_key("has spaces").is_err());
        assert!(idempotency_key("").is_err());
    }

    #[test]
    fn parses_priorities() {
        assert_eq!(priority("critical").unwrap(), Priority::Critical);
        assert_eq!(priority("normal").unwrap(), Priority::Normal);
    }

    #[test]
    fn unknown_values_list_the_valid_ones() {
        let err = channel("carrier-pigeon").unwrap_err();
        assert!(err.contains("email") && err.contains("webhook"), "got {err}");
    }

    #[test]
    fn key_value_splits_on_the_first_equals_only() {
        let (key, value) = key_value("url=https://example.com/?a=1&b=2").unwrap();
        assert_eq!(key, "url");
        assert_eq!(value, "https://example.com/?a=1&b=2");
    }

    #[test]
    fn key_value_accepts_an_empty_value() {
        assert_eq!(key_value("note=").unwrap(), ("note".to_string(), String::new()));
    }

    #[test]
    fn key_value_rejects_a_pair_without_an_equals() {
        assert!(key_value("justakey").is_err());
    }

    #[test]
    fn key_value_rejects_an_empty_name() {
        assert!(key_value("=value").is_err());
    }
}
