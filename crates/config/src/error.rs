use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse config as TOML: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("invalid override for key '{key}': {reason}")]
    InvalidOverride { key: String, reason: String },
}
