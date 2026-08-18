use std::collections::HashMap;
use std::path::PathBuf;

use chrono::Utc;
use ns_core::ChannelKind;
use ns_storage::{SqliteStorage, TemplateRepository};
use ns_templates::Template;
use uuid::Uuid;

use crate::error::{CliError, CliResult};
use crate::output::{table, OutputFormat};

#[derive(Debug)]
pub struct SaveArgs {
    pub tenant_id: Uuid,
    pub id: String,
    pub name: Option<String>,
    pub channel: Option<ChannelKind>,
    pub subject: Option<String>,
    pub body: Option<String>,
    pub body_file: Option<PathBuf>,
}

impl SaveArgs {
    /// Resolves the body from `--body` or `--body-file`. A file is the
    /// practical option for anything longer than a line: shell quoting
    /// mangles multi-line message text, and templates are exactly the
    /// thing an operator wants to keep in version control anyway.
    fn resolve_body(&self) -> CliResult<String> {
        match (&self.body, &self.body_file) {
            (Some(_), Some(_)) => {
                Err(CliError::Invalid("pass either --body or --body-file, not both".into()))
            }
            (Some(body), None) => Ok(body.clone()),
            (None, Some(path)) => std::fs::read_to_string(path)
                .map_err(|source| CliError::Io { path: path.display().to_string(), source }),
            (None, None) => Err(CliError::Invalid("a template needs --body or --body-file".into())),
        }
    }

    fn build(&self) -> CliResult<Template> {
        let mut template = Template::new(self.tenant_id, self.id.clone(), self.resolve_body()?)?;
        if let Some(name) = &self.name {
            template = template.with_name(name.clone());
        }
        if let Some(channel) = self.channel {
            template = template.with_channel(channel);
        }
        if let Some(subject) = &self.subject {
            template = template.with_subject(subject.clone())?;
        }
        Ok(template)
    }
}

pub async fn create(storage: &SqliteStorage, args: SaveArgs, output: OutputFormat) -> CliResult<String> {
    let template = args.build()?;
    storage.insert_template(&template).await?;
    output.render(&template, || describe(&template))
}

/// Replaces an existing template's content. Like the API's PUT, this
/// revises content only -- id, tenant and creation time are identity.
pub async fn update(storage: &SqliteStorage, args: SaveArgs, output: OutputFormat) -> CliResult<String> {
    let body = args.resolve_body()?;
    let mut template = storage.get_template(args.tenant_id, &args.id).await?;
    template.revise(args.subject.clone(), body, Utc::now())?;
    if args.name.is_some() {
        template.name = args.name.clone();
    }
    if args.channel.is_some() {
        template.channel = args.channel;
    }

    storage.update_template(&template).await?;
    output.render(&template, || describe(&template))
}

pub async fn show(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    id: &str,
    output: OutputFormat,
) -> CliResult<String> {
    let template = storage.get_template(tenant_id, id).await?;
    output.render(&template, || describe(&template))
}

pub async fn list(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    limit: u32,
    output: OutputFormat,
) -> CliResult<String> {
    let templates = storage.list_templates(tenant_id, limit).await?;
    output.render(&templates, || {
        let rows: Vec<Vec<String>> = templates
            .iter()
            .map(|template| {
                vec![
                    template.id.clone(),
                    template.channel.map(|c| c.to_string()).unwrap_or_else(|| "any".into()),
                    template
                        .variables()
                        .map(|names| names.into_iter().collect::<Vec<_>>().join(","))
                        .unwrap_or_else(|_| "<unparseable>".into()),
                    template.updated_at.to_rfc3339(),
                ]
            })
            .collect();
        table(&["id", "channel", "variables", "updated"], &rows)
    })
}

pub async fn delete(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    id: &str,
    output: OutputFormat,
) -> CliResult<String> {
    storage.delete_template(tenant_id, id).await?;
    output.render(&serde_json::json!({"deleted": id}), || format!("deleted template {id}\n"))
}

/// Renders a template against variables supplied on the command line,
/// without sending anything -- the local equivalent of the API's preview
/// endpoint, for checking wording before real notifications reference it.
pub async fn render(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    id: &str,
    vars: HashMap<String, String>,
    output: OutputFormat,
) -> CliResult<String> {
    let template = storage.get_template(tenant_id, id).await?;

    let missing = template.missing_variables(&vars)?;
    if !missing.is_empty() {
        // One message naming every gap, rather than making the operator
        // rerun the command once per missing variable.
        return Err(CliError::Invalid(format!(
            "missing variables for template `{id}`: {}",
            missing.into_iter().collect::<Vec<_>>().join(", ")
        )));
    }

    let rendered = template.render(&vars)?;
    output.render(&rendered, || match &rendered.subject {
        Some(subject) => format!("subject: {subject}\n\n{}\n", rendered.body),
        None => format!("{}\n", rendered.body),
    })
}

fn describe(template: &Template) -> String {
    let mut out = String::new();
    out.push_str(&format!("id: {}\n", template.id));
    out.push_str(&format!("tenant: {}\n", template.tenant_id));
    if let Some(name) = &template.name {
        out.push_str(&format!("name: {name}\n"));
    }
    out.push_str(&format!(
        "channel: {}\n",
        template.channel.map(|c| c.to_string()).unwrap_or_else(|| "any".into())
    ));
    if let Some(subject) = &template.subject {
        out.push_str(&format!("subject: {subject}\n"));
    }
    let variables = template
        .variables()
        .map(|names| names.into_iter().collect::<Vec<_>>().join(", "))
        .unwrap_or_else(|_| "<unparseable>".into());
    out.push_str(&format!("variables: {variables}\n"));
    out.push_str(&format!("updated: {}\n", template.updated_at.to_rfc3339()));
    out.push_str(&format!("body:\n{}\n", template.body));
    out
}
