//! Reusable notification templates.
//!
//! A `Template` is a named, tenant-scoped subject/body pair containing
//! `{{placeholder}}` markers. A `Notification` refers to one by
//! `template_id` and carries the `template_vars` to fill it with, so the
//! message text lives in one editable place instead of being duplicated
//! into every notification at submit time.
//!
//! Rendering is deliberately kept in this crate rather than in `ns-core`:
//! it's a presentation concern with its own error vocabulary, and `ns-core`
//! stays free of anything that isn't the delivery domain itself.

pub mod error;
pub mod render;
pub mod template;

pub use error::{TemplateError, TemplateResult};
pub use render::{missing_variables, placeholders, render, Segment};
pub use template::{RenderedMessage, Template};
