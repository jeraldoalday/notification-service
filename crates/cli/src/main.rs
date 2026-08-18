use clap::Parser;
use ns_cli::cli::QueueCommand;
use ns_cli::cli::{Cli, Command, NotificationCommand, SubmitArgs};
use ns_cli::commands;
use ns_cli::commands::notification::parse_timestamp;
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
        Command::Notification(NotificationCommand::Submit(args)) => {
            commands::notification::submit(&storage, submit_args(args)?, cli.output).await?
        }
        Command::Notification(NotificationCommand::Show { id }) => {
            commands::notification::show(&storage, id, cli.output).await?
        }
        Command::Notification(NotificationCommand::List { tenant, status, limit }) => {
            commands::notification::list(&storage, tenant, status, limit, cli.output).await?
        }
        Command::Notification(NotificationCommand::Cancel { id }) => {
            commands::notification::cancel(&storage, id, cli.output).await?
        }
        Command::Notification(NotificationCommand::Replay { id }) => {
            commands::notification::replay(&storage, id, cli.output).await?
        }
        Command::Queue(QueueCommand::Stats { tenant }) => {
            commands::queue::stats(&storage, tenant, cli.output).await?
        }
    };

    print!("{rendered}");
    Ok(())
}

/// Translates the parsed command line into the shape the command layer
/// takes. Only the timestamp needs real work; clap has already validated
/// everything else.
fn submit_args(args: SubmitArgs) -> CliResult<commands::notification::SubmitArgs> {
    Ok(commands::notification::SubmitArgs {
        tenant_id: args.tenant,
        recipient_id: args.recipient,
        channel: args.channel,
        priority: args.priority,
        subject: args.subject,
        body: args.body,
        template_id: args.template,
        template_vars: args.vars.into_iter().collect(),
        max_attempts: args.max_attempts,
        scheduled_for: args.schedule.as_deref().map(parse_timestamp).transpose()?,
    })
}
