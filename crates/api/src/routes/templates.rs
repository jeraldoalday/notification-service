use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use ns_core::ChannelKind;
use ns_storage::{NotificationRepository, RecipientRepository, TemplateRepository};
use ns_templates::{Template, TemplateError};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{ApiError, ApiJson, ApiResult};
use crate::state::AppState;

/// Mirrors `DEFAULT_LIST_LIMIT` on the notifications routes: an omitted
/// `limit` shouldn't mean "everything".
const DEFAULT_LIST_LIMIT: u32 = 50;

/// Templates are keyed by (tenant_id, id), so every item route needs the
/// tenant alongside the id in the path. It rides as a query parameter
/// rather than a second path segment to keep `{id}` the resource's
/// identity and the tenant a scope, matching `GET /notifications`.
#[derive(Debug, Deserialize)]
pub struct TenantQuery {
    pub tenant_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct ListTemplatesQuery {
    pub tenant_id: Uuid,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct CreateTemplateRequest {
    pub tenant_id: Uuid,
    pub id: String,
    pub name: Option<String>,
    pub channel: Option<ChannelKind>,
    pub subject: Option<String>,
    pub body: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateTemplateRequest {
    pub name: Option<String>,
    pub channel: Option<ChannelKind>,
    pub subject: Option<String>,
    pub body: String,
}

#[derive(Debug, Serialize)]
pub struct TemplateResponse {
    pub id: String,
    pub tenant_id: Uuid,
    pub name: Option<String>,
    pub channel: Option<ChannelKind>,
    pub subject: Option<String>,
    pub body: String,
    /// Every variable this template interpolates, so a caller building a
    /// submit request knows exactly which `template_vars` to supply
    /// without having to parse the body themselves.
    pub variables: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// Not a `From` impl: deriving the variable list means parsing the body,
// which can fail for a template that was written into storage by an older
// (or hand-edited) version of the service.
fn to_response(template: Template) -> ApiResult<TemplateResponse> {
    let variables = template.variables()?.into_iter().collect();
    Ok(TemplateResponse {
        id: template.id,
        tenant_id: template.tenant_id,
        name: template.name,
        channel: template.channel,
        subject: template.subject,
        body: template.body,
        variables,
        created_at: template.created_at,
        updated_at: template.updated_at,
    })
}

fn build_template(request: CreateTemplateRequest) -> ApiResult<Template> {
    let mut template = Template::new(request.tenant_id, request.id, request.body)?;
    if let Some(name) = request.name {
        template = template.with_name(name);
    }
    if let Some(channel) = request.channel {
        template = template.with_channel(channel);
    }
    if let Some(subject) = request.subject {
        template = template.with_subject(subject)?;
    }
    Ok(template)
}

pub async fn create_template<R>(
    State(state): State<AppState<R>>,
    ApiJson(request): ApiJson<CreateTemplateRequest>,
) -> ApiResult<(StatusCode, Json<TemplateResponse>)>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    let template = build_template(request)?;
    // A 409 rather than an overwrite: silently replacing a template would
    // change the wording of every notification already queued against it.
    state.repo.insert_template(&template).await?;
    Ok((StatusCode::CREATED, Json(to_response(template)?)))
}

pub async fn get_template<R>(
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
    Query(query): Query<TenantQuery>,
) -> ApiResult<Json<TemplateResponse>>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    let template = state.repo.get_template(query.tenant_id, &id).await?;
    Ok(Json(to_response(template)?))
}

pub async fn list_templates<R>(
    State(state): State<AppState<R>>,
    Query(query): Query<ListTemplatesQuery>,
) -> ApiResult<Json<Vec<TemplateResponse>>>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    let templates =
        state.repo.list_templates(query.tenant_id, query.limit.unwrap_or(DEFAULT_LIST_LIMIT)).await?;
    templates.into_iter().map(to_response).collect::<ApiResult<Vec<_>>>().map(Json)
}

/// Replaces a template's content in place. The id, tenant and creation
/// time are identity and can't be changed here -- renaming a template
/// means creating the new id and deleting the old one, so notifications
/// still referencing the old id keep resolving until they're drained.
pub async fn update_template<R>(
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
    Query(query): Query<TenantQuery>,
    ApiJson(request): ApiJson<UpdateTemplateRequest>,
) -> ApiResult<Json<TemplateResponse>>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    let mut template = state.repo.get_template(query.tenant_id, &id).await?;
    template.revise(request.subject, request.body, Utc::now())?;
    template.name = request.name;
    template.channel = request.channel;

    state.repo.update_template(&template).await?;
    Ok(Json(to_response(template)?))
}

pub async fn delete_template<R>(
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
    Query(query): Query<TenantQuery>,
) -> ApiResult<StatusCode>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    state.repo.delete_template(query.tenant_id, &id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
pub struct PreviewTemplateRequest {
    #[serde(default)]
    pub vars: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct PreviewTemplateResponse {
    pub subject: Option<String>,
    pub body: String,
}

/// Renders a template against a candidate set of variables without
/// sending anything, so callers can check their wording (and that they're
/// supplying every variable) before submitting real notifications against
/// it. Gaps are reported all at once rather than one failed render at a
/// time, since fixing them one round-trip each is the slow way to find out
/// a template needs four variables.
pub async fn preview_template<R>(
    State(state): State<AppState<R>>,
    Path(id): Path<String>,
    Query(query): Query<TenantQuery>,
    ApiJson(request): ApiJson<PreviewTemplateRequest>,
) -> ApiResult<Json<PreviewTemplateResponse>>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    let template = state.repo.get_template(query.tenant_id, &id).await?;

    let missing = template.missing_variables(&request.vars)?;
    if !missing.is_empty() {
        let names = missing.into_iter().collect::<Vec<_>>().join(", ");
        return Err(ApiError::Template(TemplateError::Validation(format!(
            "missing variables for template `{id}`: {names}"
        ))));
    }

    let rendered = template.render(&request.vars)?;
    Ok(Json(PreviewTemplateResponse { subject: rendered.subject, body: rendered.body }))
}
