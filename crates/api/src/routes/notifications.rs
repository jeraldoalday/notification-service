use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use ns_core::{ChannelKind, DeliveryStatus, Notification, Priority};
use ns_storage::{NotificationRepository, RecipientRepository};
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

pub async fn submit_notification<R>(
    State(state): State<AppState<R>>,
    ApiJson(request): ApiJson<SubmitNotificationRequest>,
) -> ApiResult<(StatusCode, Json<NotificationResponse>)>
where
    R: NotificationRepository + RecipientRepository + Clone,
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
    let submitted = state.queue.submit(notification, Utc::now()).await?;
    Ok((StatusCode::CREATED, Json(submitted.into())))
}

pub async fn get_notification<R>(
    State(state): State<AppState<R>>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<NotificationResponse>>
where
    R: NotificationRepository + RecipientRepository + Clone,
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
    R: NotificationRepository + RecipientRepository + Clone,
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
    R: NotificationRepository + RecipientRepository + Clone,
{
    let notifications = state
        .repo
        .list_by_tenant(query.tenant_id, query.status, query.limit.unwrap_or(DEFAULT_LIST_LIMIT))
        .await?;
    Ok(Json(notifications.into_iter().map(NotificationResponse::from).collect()))
}
