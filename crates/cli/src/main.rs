use clap::Parser;
use ns_cli::cli::{Cli, Command, QueueCommand};
use ns_cli::commands;
use ns_cli::error::CliResult;
use ns_storage::SqliteStorage;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        // Errors go to stderr and exit non-zero so `ns` composes properly
        // in a shell pipeline; only command output lands on stdout.
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> CliResult<()> {
    let cli = Cli::parse();

    let settings = ns_config::load(&cli.config)?;
    let database_url = cli.database_url.clone().unwrap_or(settings.database.url);
    let storage = SqliteStorage::connect(&database_url).await?;

    let rendered = match cli.command {
        Command::Queue(QueueCommand::Stats { tenant }) => {
            commands::queue::stats(&storage, tenant, cli.output).await?
        }
    };

    print!("{rendered}");
    Ok(())
}
