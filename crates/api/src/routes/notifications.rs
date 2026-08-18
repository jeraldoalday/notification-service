use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use ns_core::{ChannelKind, DeliveryStatus, Notification, Priority};
use ns_storage::{NotificationRepository, RecipientRepository, TemplateRepository};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{ApiJson, ApiResult};
use crate::state::AppState;

/// Applied when a `GET /notifications` request omits `limit`, so an
/// unbounded query can't be used to pull an entire tenant's history in one
/// request.
const DEFAULT_LIST_LIMIT: u32 = 50;

#[derive(Debug, Deserialize)]
pub struct SubmitNotificationRequest {
    pub tenant_id: Uuid,
    pub recipient_id: Uuid,
    pub channel: ChannelKind,
    #[serde(default)]
    pub priority: Priority,
    pub subject: Option<String>,
    pub body: Option<String>,
    pub template_id: Option<String>,
    #[serde(default)]
    pub template_vars: HashMap<String, String>,
    pub max_attempts: Option<u32>,
    pub scheduled_for: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct NotificationResponse {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub recipient_id: Uuid,
    pub channel: ChannelKind,
    pub priority: Priority,
    pub status: DeliveryStatus,
    pub subject: Option<String>,
    pub attempt_count: u32,
    pub max_attempts: u32,
    pub created_at: DateTime<Utc>,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl From<Notification> for NotificationResponse {
    fn from(n: Notification) -> Self {
        Self {
            id: n.id,
            tenant_id: n.tenant_id,
            recipient_id: n.recipient_id,
            channel: n.channel,
            priority: n.priority,
            status: n.status,
            subject: n.subject,
            attempt_count: n.attempt_count,
            max_attempts: n.max_attempts,
            created_at: n.created_at,
            scheduled_for: n.scheduled_for,
            updated_at: n.updated_at,
        }
    }
}

/// Submits a notification for delivery.
///
/// Both up-front checks exist for the same reason: a notification that
/// can never be delivered should be rejected here, synchronously, rather
/// than being accepted, retried on backoff until its attempts run out and
/// dead-lettered minutes later where nobody is watching. So an unknown
/// recipient is a 404, and a `template_id` is resolved and test-rendered
/// against the supplied `template_vars` before the notification is
/// queued.
///
/// The rendered text is deliberately thrown away rather than stored on
/// the notification: `ns-worker` renders again at send time, so an edit to
/// the template still reaches everything queued behind it. This is purely
/// a validity check.
pub async fn submit_notification<R>(
    State(state): State<AppState<R>>,
    ApiJson(request): ApiJson<SubmitNotificationRequest>,
) -> ApiResult<(StatusCode, Json<NotificationResponse>)>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    // Fail fast with a 404 rather than letting a bad recipient_id surface
    // only once the worker tries (and fails) to dispatch the notification.
    state.repo.get_recipient(request.recipient_id).await?;

    let mut builder = Notification::builder(request.tenant_id, request.recipient_id, request.channel)
        .priority(request.priority);
    if let Some(subject) = request.subject {
        builder = builder.subject(subject);
    }
    if let Some(body) = request.body {
        builder = builder.body(body);
    }
    if let Some(template_id) = request.template_id {
        builder = builder.template(template_id);
    }
    for (key, value) in request.template_vars {
        builder = builder.template_var(key, value);
    }
    if let Some(max_attempts) = request.max_attempts {
        builder = builder.max_attempts(max_attempts);
    }
    if let Some(scheduled_for) = request.scheduled_for {
        builder = builder.scheduled_for(scheduled_for);
    }

    let notification = builder.build()?;

    if let Some(template_id) = &notification.template_id {
        let template = state.repo.get_template(notification.tenant_id, template_id).await?;
        template.render_for(&notification)?;
    }

    let submitted = state.queue.submit(notification, Utc::now()).await?;
    Ok((StatusCode::CREATED, Json(submitted.into())))
}

pub async fn get_notification<R>(
    State(state): State<AppState<R>>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<NotificationResponse>>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    let notification = state.repo.get_notification(id).await?;
    Ok(Json(notification.into()))
}

/// Cancels a notification that hasn't started sending yet. `Notification`'s
/// own state machine (`DeliveryStatus::can_transition_to`) is what actually
/// decides whether that's legal -- only `Pending` and `Queued` can move to
/// `Cancelled`, so a notification already `Sending` or terminal returns a
/// 409 via `CoreError::InvalidTransition` rather than silently no-opping.
pub async fn cancel_notification<R>(
    State(state): State<AppState<R>>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<NotificationResponse>>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    let mut notification = state.repo.get_notification(id).await?;
    notification.transition(DeliveryStatus::Cancelled, Utc::now())?;
    state.repo.update_notification(&notification).await?;
    Ok(Json(notification.into()))
}

#[derive(Debug, Deserialize)]
pub struct ListNotificationsQuery {
    pub tenant_id: Uuid,
    pub status: Option<DeliveryStatus>,
    pub limit: Option<u32>,
}

pub async fn list_notifications<R>(
    State(state): State<AppState<R>>,
    Query(query): Query<ListNotificationsQuery>,
) -> ApiResult<Json<Vec<NotificationResponse>>>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    let notifications = state
        .repo
        .list_by_tenant(query.tenant_id, query.status, query.limit.unwrap_or(DEFAULT_LIST_LIMIT))
        .await?;
    Ok(Json(notifications.into_iter().map(NotificationResponse::from).collect()))
}
