use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use ns_auth::{ApiKey, Scope};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::Caller;
use crate::error::{ApiError, ApiJson, ApiResult};
use crate::state::{AppState, Repositories};

const DEFAULT_LIST_LIMIT: u32 = 50;
const MAX_LIST_LIMIT: u32 = 500;

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
    pub scopes: Vec<Scope>,
}

#[derive(Debug, Deserialize)]
pub struct ListApiKeysQuery {
    pub limit: Option<u32>,
}

/// What a key looks like to an operator reading it back: everything except
/// anything that would help present it. Not even the digest, which is a
/// value derived from a live secret and has no reason to travel.
#[derive(Debug, Serialize)]
pub struct ApiKeyResponse {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub scopes: Vec<Scope>,
    /// The last few characters of the digest, stable for the key's life,
    /// so a key can be matched to a log line or a revocation request
    /// without anyone holding the secret next to it.
    pub fingerprint: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

impl From<&ApiKey> for ApiKeyResponse {
    fn from(key: &ApiKey) -> Self {
        Self {
            id: key.id,
            tenant_id: key.tenant_id,
            name: key.name.clone(),
            scopes: key.scopes.iter().copied().collect(),
            fingerprint: key.fingerprint(),
            created_at: key.created_at,
            revoked_at: key.revoked_at,
        }
    }
}

/// The creation response, and the only place a token is ever returned.
#[derive(Debug, Serialize)]
pub struct CreatedApiKeyResponse {
    #[serde(flatten)]
    pub key: ApiKeyResponse,
    pub token: String,
}

/// Mints a key for the caller's own tenant.
///
/// There is no `tenant_id` field to supply. An admin key acts for exactly
/// one tenant, so letting the request name one could only ever agree with
/// the credential or be refused -- and a field whose sole legal value is
/// the one the server already knows is a field that eventually gets
/// filled in wrong.
pub async fn create_api_key<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    ApiJson(request): ApiJson<CreateApiKeyRequest>,
) -> ApiResult<(StatusCode, Json<CreatedApiKeyResponse>)>
where
    R: Repositories,
{
    caller.require(Scope::Admin)?;
    let tenant_id = caller.tenant_for(None)?;

    if request.name.trim().is_empty() {
        return Err(ApiError::BadRequest("an api key needs a name to be recognised by later".into()));
    }
    if request.scopes.is_empty() {
        // Same reasoning as the CLI: a scopeless key authenticates and
        // then fails everything, which is indistinguishable from a broken
        // service to whoever was handed it.
        return Err(ApiError::BadRequest("an api key needs at least one scope".into()));
    }

    let (key, token) = ApiKey::generate(tenant_id, request.name, request.scopes, Utc::now());
    state.repo.insert_api_key(&key).await?;

    Ok((
        StatusCode::CREATED,
        Json(CreatedApiKeyResponse { key: ApiKeyResponse::from(&key), token: token.expose().to_string() }),
    ))
}

pub async fn list_api_keys<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Query(query): Query<ListApiKeysQuery>,
) -> ApiResult<Json<Vec<ApiKeyResponse>>>
where
    R: Repositories,
{
    caller.require(Scope::Admin)?;
    let tenant_id = caller.tenant_for(None)?;
    let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT).min(MAX_LIST_LIMIT);

    let keys = state.repo.list_api_keys(tenant_id, limit).await?;
    Ok(Json(keys.iter().map(ApiKeyResponse::from).collect()))
}

/// Revokes a key. Idempotent: revoking an already-revoked key answers 200
/// with the original revocation time rather than a conflict, because the
/// caller's intent -- that this credential not work -- already holds.
pub async fn revoke_api_key<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<ApiKeyResponse>>
where
    R: Repositories,
{
    caller.require(Scope::Admin)?;

    let mut key = state.repo.get_api_key(id).await?;
    // 404 rather than 403, as everywhere else an id could otherwise be
    // used to probe for what exists.
    if let Some(tenant_id) = caller.tenant_id() {
        if key.tenant_id != tenant_id {
            return Err(ApiError::NotFound(format!("api key {id} not found")));
        }
    }

    if !key.is_revoked() {
        key.revoke(Utc::now());
        state.repo.update_api_key(&key).await?;
    }

    Ok(Json(ApiKeyResponse::from(&key)))
}
