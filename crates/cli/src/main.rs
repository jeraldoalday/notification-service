use clap::Parser;
use ns_cli::cli::{
    ApikeyCommand, PreferenceCommand, QueueCommand, RecipientCommand, SuppressionCommand, TemplateCommand,
    TemplateSaveArgs,
};
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
        Command::Notification(NotificationCommand::ByKey { tenant, key }) => {
            commands::notification::by_key(&storage, tenant, &key, cli.output).await?
        }
        Command::Notification(NotificationCommand::List { tenant, status, limit }) => {
            commands::notification::list(&storage, tenant, status, limit, cli.output).await?
        }
        Command::Notification(NotificationCommand::Attempts { id, limit }) => {
            commands::notification::attempts(&storage, id, limit, cli.output).await?
        }
        Command::Notification(NotificationCommand::Cancel { id }) => {
            commands::notification::cancel(&storage, id, cli.output).await?
        }
        Command::Notification(NotificationCommand::Replay { id }) => {
            commands::notification::replay(&storage, id, cli.output).await?
        }
        Command::Recipient(RecipientCommand::Add {
            tenant,
            display_name,
            email,
            phone,
            push_token,
            webhook_url,
        }) => {
            let args = commands::recipient::AddArgs {
                tenant_id: tenant,
                display_name,
                email,
                phone,
                push_token,
                webhook_url,
            };
            commands::recipient::add(&storage, args, cli.output).await?
        }
        Command::Recipient(RecipientCommand::Show { id }) => {
            commands::recipient::show(&storage, id, cli.output).await?
        }
        Command::Template(TemplateCommand::Create(args)) => {
            commands::template::create(&storage, template_args(args), cli.output).await?
        }
        Command::Template(TemplateCommand::Update(args)) => {
            commands::template::update(&storage, template_args(args), cli.output).await?
        }
        Command::Template(TemplateCommand::Show { tenant, id }) => {
            commands::template::show(&storage, tenant, &id, cli.output).await?
        }
        Command::Template(TemplateCommand::List { tenant, limit }) => {
            commands::template::list(&storage, tenant, limit, cli.output).await?
        }
        Command::Template(TemplateCommand::Delete { tenant, id }) => {
            commands::template::delete(&storage, tenant, &id, cli.output).await?
        }
        Command::Template(TemplateCommand::Render { tenant, id, vars }) => {
            commands::template::render(&storage, tenant, &id, vars.into_iter().collect(), cli.output).await?
        }
        Command::Queue(QueueCommand::Stats { tenant }) => {
            commands::queue::stats(&storage, tenant, cli.output).await?
        }
        Command::Queue(QueueCommand::Due { limit }) => {
            commands::queue::due(&storage, limit, cli.output).await?
        }
        Command::Queue(QueueCommand::Replay { tenant, limit }) => {
            commands::queue::replay(&storage, tenant, limit, cli.output).await?
        }
        Command::Queue(QueueCommand::PurgeAttempts { older_than_days }) => {
            commands::queue::purge_attempts(&storage, older_than_days, cli.output).await?
        }
        Command::Suppression(SuppressionCommand::Add { tenant, channel, address, reason, note }) => {
            commands::suppression::add(&storage, tenant, channel, address, reason, note, cli.output).await?
        }
        Command::Suppression(SuppressionCommand::List { tenant, limit }) => {
            commands::suppression::list(&storage, tenant, limit, cli.output).await?
        }
        Command::Suppression(SuppressionCommand::Check { tenant, channel, address }) => {
            commands::suppression::check(&storage, tenant, channel, address, cli.output).await?
        }
        Command::Suppression(SuppressionCommand::Remove { tenant, channel, address }) => {
            commands::suppression::remove(&storage, tenant, channel, address, cli.output).await?
        }
        Command::Preference(PreferenceCommand::Set { tenant, recipient, topic, channel, setting }) => {
            commands::preference::set(&storage, tenant, recipient, topic, channel, setting, cli.output)
                .await?
        }
        Command::Preference(PreferenceCommand::List { tenant, recipient, limit }) => {
            commands::preference::list(&storage, tenant, recipient, limit, cli.output).await?
        }
        Command::Preference(PreferenceCommand::Check { tenant, recipient, topic, channel }) => {
            commands::preference::check(&storage, tenant, recipient, topic, channel, cli.output).await?
        }
        Command::Preference(PreferenceCommand::Clear { tenant, recipient, topic, channel }) => {
            commands::preference::clear(&storage, tenant, recipient, topic, channel, cli.output).await?
        }
        Command::Apikey(ApikeyCommand::Create { tenant, name, scopes }) => {
            commands::apikey::create(&storage, tenant, name, scopes, cli.output).await?
        }
        Command::Apikey(ApikeyCommand::List { tenant, limit }) => {
            commands::apikey::list(&storage, tenant, limit, cli.output).await?
        }
        Command::Apikey(ApikeyCommand::Revoke { id }) => {
            commands::apikey::revoke(&storage, id, cli.output).await?
        }
    };

    print!("{rendered}");
    Ok(())
}

fn template_args(args: TemplateSaveArgs) -> commands::template::SaveArgs {
    commands::template::SaveArgs {
        tenant_id: args.tenant,
        id: args.id,
        name: args.name,
        channel: args.channel,
        subject: args.subject,
        body: args.body,
        body_file: args.body_file,
    }
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
        topic: args.topic,
        idempotency_key: args.idempotency_key,
        max_attempts: args.max_attempts,
        scheduled_for: args.schedule.as_deref().map(parse_timestamp).transpose()?,
    })
}
