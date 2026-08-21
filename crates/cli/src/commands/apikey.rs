use chrono::Utc;
use ns_auth::{ApiKey, Scope};
use ns_storage::{ApiKeyRepository, SqliteStorage};
use serde::Serialize;
use uuid::Uuid;

use crate::error::{CliError, CliResult};
use crate::output::{table, OutputFormat};

/// A newly minted key, together with the one and only copy of its token.
///
/// The token is on the *creation* response and nowhere else: `list` can't
/// show it, `show` doesn't exist, and the database doesn't hold it. An
/// operator who loses it mints a replacement and revokes this one.
#[derive(Debug, Serialize)]
pub struct CreatedKey {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub scopes: Vec<Scope>,
    pub token: String,
}

#[derive(Debug, Serialize)]
pub struct KeySummary {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub scopes: Vec<Scope>,
    pub fingerprint: String,
    pub created_at: String,
    pub revoked_at: Option<String>,
}

impl From<&ApiKey> for KeySummary {
    fn from(key: &ApiKey) -> Self {
        Self {
            id: key.id,
            tenant_id: key.tenant_id,
            name: key.name.clone(),
            scopes: key.scopes.iter().copied().collect(),
            fingerprint: key.fingerprint(),
            created_at: key.created_at.to_rfc3339(),
            revoked_at: key.revoked_at.map(|at| at.to_rfc3339()),
        }
    }
}

fn render_scopes(scopes: &[Scope]) -> String {
    if scopes.is_empty() {
        // Distinguishable at a glance from a key whose scopes just didn't
        // fit the column, because a scopeless key authenticates fine and
        // then fails every request, which is confusing to debug.
        return "(none)".to_string();
    }
    scopes.iter().map(Scope::as_str).collect::<Vec<_>>().join(",")
}

/// Mints a key and prints its token once.
pub async fn create(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    name: String,
    scopes: Vec<Scope>,
    output: OutputFormat,
) -> CliResult<String> {
    if scopes.is_empty() {
        // Refused rather than created: a key with no scopes authenticates
        // and then fails every request, which looks like a service fault
        // rather than the mistyped command it is.
        return Err(CliError::Invalid("a key needs at least one --scope (read, write or admin)".into()));
    }

    let (key, token) = ApiKey::generate(tenant_id, name, scopes, Utc::now());
    storage.insert_api_key(&key).await?;

    let created = CreatedKey {
        id: key.id,
        tenant_id: key.tenant_id,
        name: key.name.clone(),
        scopes: key.scopes.iter().copied().collect(),
        token: token.expose().to_string(),
    };

    output.render(&created, || {
        format!(
            "id: {}\ntenant: {}\nname: {}\nscopes: {}\ntoken: {}\n\n\
             This token is shown once and is not recoverable. Store it now.\n",
            created.id,
            created.tenant_id,
            created.name,
            render_scopes(&created.scopes),
            created.token,
        )
    })
}

/// Lists a tenant's keys, newest first, including revoked ones -- an audit
/// of who had access has to include the access that was taken away.
pub async fn list(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    limit: u32,
    output: OutputFormat,
) -> CliResult<String> {
    let keys = storage.list_api_keys(tenant_id, limit).await?;
    let summaries: Vec<KeySummary> = keys.iter().map(KeySummary::from).collect();

    output.render(&summaries, || {
        let rows: Vec<Vec<String>> = summaries
            .iter()
            .map(|key| {
                vec![
                    key.id.to_string(),
                    key.name.clone(),
                    render_scopes(&key.scopes),
                    key.fingerprint.clone(),
                    key.revoked_at.as_ref().map_or("active", |_| "revoked").to_string(),
                ]
            })
            .collect();
        table(&["id", "name", "scopes", "fingerprint", "status"], &rows)
    })
}

/// Revokes a key. Idempotent, because the operator running this in a hurry
/// shouldn't have to care whether it already ran -- and because rewriting
/// the timestamp would lose when the credential actually stopped working.
pub async fn revoke(storage: &SqliteStorage, id: Uuid, output: OutputFormat) -> CliResult<String> {
    let mut key = storage.get_api_key(id).await?;
    let already_revoked = key.is_revoked();

    key.revoke(Utc::now());
    if !already_revoked {
        storage.update_api_key(&key).await?;
    }

    let summary = KeySummary::from(&key);
    output.render(&summary, || {
        let when = summary.revoked_at.clone().unwrap_or_default();
        if already_revoked {
            format!("key {} was already revoked at {when}\n", summary.id)
        } else {
            format!("revoked key {} ({}) at {when}\n", summary.id, summary.name)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_render_as_a_comma_list() {
        assert_eq!(render_scopes(&[Scope::Read, Scope::Write]), "read,write");
    }

    #[test]
    fn an_empty_scope_list_is_spelled_out() {
        assert_eq!(render_scopes(&[]), "(none)");
    }

    #[test]
    fn a_summary_carries_no_secret_material() {
        let (key, token) = ApiKey::generate(Uuid::new_v4(), "ci", [Scope::Read], Utc::now());
        let summary = KeySummary::from(&key);
        let rendered = serde_json::to_string(&summary).unwrap();

        assert!(!rendered.contains(token.expose()), "{rendered}");
        assert!(!rendered.contains(&key.token_digest), "not even the digest belongs in operator output");
        assert!(rendered.contains(&key.fingerprint()));
    }
}
