use thiserror::Error;

pub type CliResult<T> = Result<T, CliError>;

/// Every failure the CLI can exit on. Each lower crate's error is carried
/// transparently rather than flattened to a string, so `ns` can keep
/// printing the same wording the API and worker log for the same problem
/// -- one vocabulary for the whole service.
#[derive(Debug, Error)]
pub enum CliError {
    #[error(transparent)]
    Config(#[from] ns_config::ConfigError),

    #[error(transparent)]
    Core(#[from] ns_core::CoreError),

    #[error(transparent)]
    Storage(#[from] ns_storage::StorageError),

    #[error(transparent)]
    Queue(#[from] ns_queue::QueueError),

    #[error(transparent)]
    Template(#[from] ns_templates::TemplateError),

    #[error("could not read {path}: {source}")]
    Io { path: String, source: std::io::Error },

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("{0}")]
    Invalid(String),
}
