use ns_core::ChannelKind;
use thiserror::Error;

pub type TemplateResult<T> = Result<T, TemplateError>;

/// Everything that can go wrong defining or rendering a template.
///
/// The split matters to callers: `Validation` and the two syntax errors are
/// authoring mistakes, caught once when a template is created, whereas
/// `MissingVariable` and `ChannelMismatch` are per-notification mistakes
/// that only surface at render time -- `ns-worker` treats the latter as an
/// ordinary delivery failure (backoff, then dead-letter) rather than
/// something to abort a batch over.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TemplateError {
    #[error("validation failed: {0}")]
    Validation(String),

    #[error("unclosed placeholder: `{{{{` at byte {offset} is never closed")]
    UnclosedPlaceholder { offset: usize },

    #[error("invalid placeholder name `{0}`: expected letters, digits, `_`, `-` or `.`")]
    InvalidPlaceholderName(String),

    #[error("template `{template_id}` requires variable `{name}`, which was not supplied")]
    MissingVariable { template_id: String, name: String },

    #[error("template `{template_id}` is for the {expected} channel, not {actual}")]
    ChannelMismatch { template_id: String, expected: ChannelKind, actual: ChannelKind },
}
