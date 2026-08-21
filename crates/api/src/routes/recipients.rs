use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use ns_auth::Scope;
use ns_core::{Recipient, RecipientAddress};
use ns_storage::{ApiKeyRepository, NotificationRepository, RecipientRepository, TemplateRepository};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::Caller;
use crate::error::{ApiError, ApiJson, ApiResult};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct CreateRecipientRequest {
    /// Optional when authenticated: the key names the tenant.
    pub tenant_id: Option<Uuid>,
    pub display_name: Option<String>,
    #[serde(default)]
    pub addresses: Vec<RecipientAddress>,
}

#[derive(Debug, Serialize)]
pub struct RecipientResponse {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub display_name: Option<String>,
    pub supported_channels: Vec<ns_core::ChannelKind>,
}

impl From<Recipient> for RecipientResponse {
    fn from(recipient: Recipient) -> Self {
        Self {
            id: recipient.id,
            tenant_id: recipient.tenant_id,
            display_name: recipient.display_name.clone(),
            supported_channels: recipient.supported_channels(),
        }
    }
}

/// A recipient is reachable by its own tenant only. With authentication
/// off there is no tenant to compare against, so every recipient is
/// reachable -- which is the same trade that mode makes everywhere else.
fn allowed(caller: &Caller, recipient: &Recipient) -> bool {
    match caller.tenant_id() {
        Some(tenant_id) => recipient.belongs_to(tenant_id),
        None => true,
    }
}

pub async fn create_recipient<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    ApiJson(request): ApiJson<CreateRecipientRequest>,
) -> ApiResult<(StatusCode, Json<RecipientResponse>)>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + ApiKeyRepository + Clone,
{
    caller.require(Scope::Write)?;
    let tenant_id = caller.tenant_for(request.tenant_id)?;

    let mut recipient = Recipient::new(Uuid::new_v4(), tenant_id);
    if let Some(name) = request.display_name {
        recipient = recipient.with_display_name(name);
    }
    for address in request.addresses {
        recipient = recipient.with_address(address)?;
    }

    state.repo.insert_recipient(&recipient).await?;
    Ok((StatusCode::CREATED, Json(recipient.into())))
}

pub async fn get_recipient<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<RecipientResponse>>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + ApiKeyRepository + Clone,
{
    caller.require(Scope::Read)?;
    let recipient = state.repo.get_recipient(id).await?;
    // 404 rather than 403, for the same reason a notification belonging to
    // another tenant is: the id is opaque, so "not yours" and "not real"
    // should be indistinguishable from outside.
    if !allowed(&caller, &recipient) {
        return Err(ApiError::NotFound(format!("recipient {id} not found")));
    }
    Ok(Json(recipient.into()))
}
