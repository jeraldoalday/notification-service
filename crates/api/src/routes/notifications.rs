use std::collections::HashMap;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use ns_core::{ChannelKind, DeliveryStatus, Notification, Priority};
use ns_storage::{NotificationRepository, RecipientRepository};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ApiResult;
use crate::state::AppState;

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
    Json(request): Json<SubmitNotificationRequest>,
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
