//! Admin CLI for the notification service.
//!
//! The command implementations live in the library rather than in
//! `main.rs` so they can be exercised directly against a temporary
//! database in tests, without spawning a process or parsing stdout. Each
//! one takes an already-connected `SqliteStorage`, does its work, and
//! returns the text to print -- `main` owns all the I/O.

pub mod cli;
pub mod commands;
pub mod error;
pub mod output;
pub mod parse;

pub use cli::{Cli, Command};
pub use error::{CliError, CliResult};
pub use output::OutputFormat;
