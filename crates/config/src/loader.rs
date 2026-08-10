use std::collections::HashMap;
use std::path::Path;

use crate::error::ConfigError;
use crate::settings::Settings;

/// Loads settings from a TOML file on disk, falling back to defaults for
/// any keys not present. Returns `Settings::default()` unmodified if the
/// file doesn't exist yet, since a fresh checkout shouldn't require one.
pub fn load_from_file(path: impl AsRef<Path>) -> Result<Settings, ConfigError> {
    let path = path.as_ref();
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(toml::from_str(&contents)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Settings::default()),
        Err(err) => Err(ConfigError::Io {
            path: path.display().to_string(),
            source: err,
        }),
    }
}

/// Applies a set of `NS_*`-style overrides on top of already-loaded
/// settings. Takes a plain map rather than reading `std::env` directly so
/// callers (and tests) can supply a deterministic, injectable source
/// instead of mutating global process state.
///
/// Recognized keys: `NS_SERVER_HOST`, `NS_SERVER_PORT`, `NS_DATABASE_URL`,
/// `NS_DATABASE_MAX_CONNECTIONS`, `NS_QUEUE_POLL_INTERVAL_MS`,
/// `NS_QUEUE_BATCH_SIZE`, `NS_SMTP_HOST`, `NS_SMTP_PORT`, `NS_SMTP_USERNAME`.
/// Unrecognized keys are ignored so callers can pass through the full
/// process environment safely.
pub fn apply_overrides(mut settings: Settings, overrides: &HashMap<String, String>) -> Result<Settings, ConfigError> {
    macro_rules! parse_override {
        ($key:expr, $target:expr) => {
            if let Some(raw) = overrides.get($key) {
                $target = raw.parse().map_err(|_| ConfigError::InvalidOverride {
                    key: $key.to_string(),
                    reason: format!("could not parse '{raw}'"),
                })?;
            }
        };
    }

    if let Some(host) = overrides.get("NS_SERVER_HOST") {
        settings.server.host = host.clone();
    }
    parse_override!("NS_SERVER_PORT", settings.server.port);

    if let Some(url) = overrides.get("NS_DATABASE_URL") {
        settings.database.url = url.clone();
    }
    parse_override!("NS_DATABASE_MAX_CONNECTIONS", settings.database.max_connections);
    parse_override!("NS_QUEUE_POLL_INTERVAL_MS", settings.queue.poll_interval_ms);
    parse_override!("NS_QUEUE_BATCH_SIZE", settings.queue.batch_size);

    if let Some(host) = overrides.get("NS_SMTP_HOST") {
        settings.smtp.host = host.clone();
    }
    parse_override!("NS_SMTP_PORT", settings.smtp.port);
    if let Some(user) = overrides.get("NS_SMTP_USERNAME") {
        settings.smtp.username = user.clone();
    }

    Ok(settings)
}

/// Convenience wrapper: load from file, then apply the current process
/// environment as overrides.
pub fn load(path: impl AsRef<Path>) -> Result<Settings, ConfigError> {
    let base = load_from_file(path)?;
    let env: HashMap<String, String> = std::env::vars().collect();
    apply_overrides(base, &env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn missing_file_yields_defaults() {
        let settings = load_from_file("/nonexistent/path/config.toml").unwrap();
        assert_eq!(settings, Settings::default());
    }

    #[test]
    fn loads_partial_toml_with_defaults_filled_in() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "[server]\nport = 9090\n").unwrap();

        let settings = load_from_file(file.path()).unwrap();
        assert_eq!(settings.server.port, 9090);
        assert_eq!(settings.server.host, "127.0.0.1"); // default preserved
        assert_eq!(settings.database.max_connections, 10); // default preserved
    }

    #[test]
    fn overrides_apply_known_keys() {
        let mut overrides = HashMap::new();
        overrides.insert("NS_SERVER_PORT".to_string(), "1234".to_string());
        overrides.insert("NS_DATABASE_URL".to_string(), "sqlite://test.db".to_string());

        let settings = apply_overrides(Settings::default(), &overrides).unwrap();
        assert_eq!(settings.server.port, 1234);
        assert_eq!(settings.database.url, "sqlite://test.db");
    }

    #[test]
    fn overrides_ignore_unknown_keys() {
        let mut overrides = HashMap::new();
        overrides.insert("PATH".to_string(), "/usr/bin".to_string());
        overrides.insert("HOME".to_string(), "/home/user".to_string());

        let settings = apply_overrides(Settings::default(), &overrides).unwrap();
        assert_eq!(settings, Settings::default());
    }

    #[test]
    fn invalid_numeric_override_errors() {
        let mut overrides = HashMap::new();
        overrides.insert("NS_SERVER_PORT".to_string(), "not-a-number".to_string());

        let err = apply_overrides(Settings::default(), &overrides).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidOverride { .. }));
    }

    #[test]
    fn malformed_toml_errors() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "this is not valid toml [[[").unwrap();

        let err = load_from_file(file.path()).unwrap_err();
        assert!(matches!(err, ConfigError::Parse(_)));
    }
}
