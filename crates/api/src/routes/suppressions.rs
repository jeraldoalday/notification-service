use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use ns_auth::Scope;
use ns_core::{ChannelKind, RecipientAddress, Suppression, SuppressionReason};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::Caller;
use crate::error::{ApiError, ApiJson, ApiResult};
use crate::state::{AppState, Repositories};

const DEFAULT_LIST_LIMIT: u32 = 100;
const MAX_LIST_LIMIT: u32 = 1000;

/// An address is identified by the pair (channel, value) rather than by an
/// id, because that is genuinely what it is -- there is nothing else to
/// name it by, and minting a surrogate id would only mean callers had to
/// look one up before they could unsuppress an address they already know.
///
/// It rides in the query string rather than in the path because addresses
/// are not path-shaped: a webhook URL is itself a URL, and an email
/// address is full of characters that a path segment would have to escape
/// and every client would have to remember to escape identically.
#[derive(Debug, Deserialize)]
pub struct SuppressionAddressQuery {
    pub tenant_id: Option<Uuid>,
    pub channel: ChannelKind,
    pub address: String,
}

#[derive(Debug, Deserialize)]
pub struct ListSuppressionsQuery {
    pub tenant_id: Option<Uuid>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct CreateSuppressionRequest {
    pub tenant_id: Option<Uuid>,
    pub channel: ChannelKind,
    pub address: String,
    pub reason: SuppressionReason,
    pub note: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SuppressionResponse {
    pub channel: ChannelKind,
    /// The address as it was supplied, not the normalised key it is
    /// matched under. An operator reading the list should see what they
    /// typed; the normalisation is the storage layer's business.
    pub address: String,
    pub reason: SuppressionReason,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<Suppression> for SuppressionResponse {
    fn from(suppression: Suppression) -> Self {
        Self {
            channel: suppression.channel(),
            address: suppression.address.value().to_string(),
            reason: suppression.reason,
            note: suppression.note,
            created_at: suppression.created_at,
        }
    }
}

/// Adds an address to the caller's suppression list.
///
/// `Write` rather than `Admin`: this changes what the service will send,
/// which is exactly the blast radius `write` describes, and requiring
/// `admin` would mean an unsubscribe handler needed a key that could also
/// mint credentials.
///
/// Answers 201 whether or not the address was already suppressed. The
/// request asked for an address to be unsendable and it is; reporting a
/// 409 would make a retried webhook delivery look like a failure, and
/// bounce and complaint notifications are retried constantly.
pub async fn create_suppression<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    ApiJson(request): ApiJson<CreateSuppressionRequest>,
) -> ApiResult<(StatusCode, Json<SuppressionResponse>)>
where
    R: Repositories,
{
    caller.require(Scope::Write)?;
    let tenant_id = caller.tenant_for(request.tenant_id)?;

    let address = RecipientAddress::for_channel(request.channel, request.address);
    let mut suppression = Suppression::new(tenant_id, address, request.reason, Utc::now())?;
    if let Some(note) = request.note {
        suppression = suppression.with_note(note);
    }

    state.repo.insert_suppression(&suppression).await?;
    Ok((StatusCode::CREATED, Json(suppression.into())))
}

pub async fn list_suppressions<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Query(query): Query<ListSuppressionsQuery>,
) -> ApiResult<Json<Vec<SuppressionResponse>>>
where
    R: Repositories,
{
    caller.require(Scope::Read)?;
    let tenant_id = caller.tenant_for(query.tenant_id)?;
    let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT).min(MAX_LIST_LIMIT);

    let suppressions = state.repo.list_suppressions(tenant_id, limit).await?;
    Ok(Json(suppressions.into_iter().map(SuppressionResponse::from).collect()))
}

/// Looks one address up without listing the whole tenant.
///
/// Worth its own route because the list can be long and the question a
/// client actually has is almost always about one address: an unsubscribe
/// page rendering a checkbox needs this, not a thousand-row download it
/// then has to search.
pub async fn get_suppression<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Query(query): Query<SuppressionAddressQuery>,
) -> ApiResult<Json<SuppressionResponse>>
where
    R: Repositories,
{
    caller.require(Scope::Read)?;
    let tenant_id = caller.tenant_for(query.tenant_id)?;
    let address = RecipientAddress::for_channel(query.channel, query.address);

    match state.repo.find_suppression(tenant_id, &address).await? {
        Some(suppression) => Ok(Json(suppression.into())),
        None => Err(not_suppressed(&address)),
    }
}

/// Removes an address from the list, so it can be sent to again.
///
/// 404 when there was nothing to remove, even though the repository is
/// happy to no-op. Deleting a suppression re-enables sending to someone
/// who at some point asked not to be sent to, and a caller that got the
/// address slightly wrong should find that out rather than be told the
/// unsuppress succeeded.
pub async fn delete_suppression<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Query(query): Query<SuppressionAddressQuery>,
) -> ApiResult<StatusCode>
where
    R: Repositories,
{
    caller.require(Scope::Write)?;
    let tenant_id = caller.tenant_for(query.tenant_id)?;
    let address = RecipientAddress::for_channel(query.channel, query.address);

    if state.repo.delete_suppression(tenant_id, &address).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(not_suppressed(&address))
    }
}

/// Names the address rather than hiding behind an opaque id, unlike the
/// 404s elsewhere in this API. There is nothing to leak: the caller
/// supplied the address, so the response tells it only what it already
/// said.
fn not_suppressed(address: &RecipientAddress) -> ApiError {
    ApiError::NotFound(format!("{} address `{}` is not suppressed", address.channel_kind(), address.value()))
}
