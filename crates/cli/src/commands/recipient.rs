use ns_core::{Recipient, RecipientAddress};
use ns_storage::{RecipientRepository, SqliteStorage};
use uuid::Uuid;

use crate::error::{CliError, CliResult};
use crate::output::OutputFormat;

/// The addresses `recipient add` can attach, one per channel. Each is
/// validated by `ns-core` before the recipient is stored, so a typo'd
/// phone number is caught here rather than by a provider rejection three
/// delivery attempts later.
#[derive(Debug, Default)]
pub struct AddArgs {
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub push_token: Option<String>,
    pub webhook_url: Option<String>,
}

impl AddArgs {
    fn addresses(&self) -> Vec<RecipientAddress> {
        let mut addresses = Vec::new();
        if let Some(email) = &self.email {
            addresses.push(RecipientAddress::Email(email.clone()));
        }
        if let Some(phone) = &self.phone {
            addresses.push(RecipientAddress::Phone(phone.clone()));
        }
        if let Some(token) = &self.push_token {
            addresses.push(RecipientAddress::PushToken(token.clone()));
        }
        if let Some(url) = &self.webhook_url {
            addresses.push(RecipientAddress::WebhookUrl(url.clone()));
        }
        addresses
    }
}

pub async fn add(storage: &SqliteStorage, args: AddArgs, output: OutputFormat) -> CliResult<String> {
    let addresses = args.addresses();
    if addresses.is_empty() {
        // A recipient with no addresses can't receive anything, so
        // creating one is almost certainly a mistyped flag.
        return Err(CliError::Invalid(
            "a recipient needs at least one of --email, --phone, --push-token or --webhook-url".into(),
        ));
    }

    let mut recipient = Recipient::new(Uuid::new_v4());
    if let Some(name) = args.display_name {
        recipient = recipient.with_display_name(name);
    }
    for address in addresses {
        recipient = recipient.with_address(address)?;
    }

    storage.insert_recipient(&recipient).await?;
    output.render(&recipient, || describe(&recipient))
}

pub async fn show(storage: &SqliteStorage, id: Uuid, output: OutputFormat) -> CliResult<String> {
    let recipient = storage.get_recipient(id).await?;
    output.render(&recipient, || describe(&recipient))
}

fn describe(recipient: &Recipient) -> String {
    let mut out = format!("id: {}\n", recipient.id);
    if let Some(name) = &recipient.display_name {
        out.push_str(&format!("name: {name}\n"));
    }
    let channels =
        recipient.supported_channels().iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ");
    out.push_str(&format!("channels: {channels}\n"));
    out
}
