use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use ns_core::CoreError;
use ns_queue::QueueError;
use ns_storage::StorageError;
use ns_templates::TemplateError;
use serde::Serialize;
use thiserror::Error;

pub type ApiResult<T> = Result<T, ApiError>;

/// Maps domain/storage/queue errors onto HTTP status codes so handlers can
/// just `?`-propagate whatever the lower crates return, rather than each
/// handler hand-rolling its own status mapping.
#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Core(#[from] CoreError),

    #[error(transparent)]
    Queue(#[from] QueueError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Template(#[from] TemplateError),

    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    BadRequest(String),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

/// Drop-in replacement for `axum::Json` as a request extractor: on success
/// it behaves identically, but on failure (malformed JSON, wrong
/// `Content-Type`, a value that doesn't match the target type) it reports
/// through the same `{"error": "..."}` envelope as every other failure
/// mode in this API instead of `axum::Json`'s plain-text rejection body,
/// while preserving the status code axum would have chosen (400/415/422).
pub struct ApiJson<T>(pub T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    Json<T>: FromRequest<S, Rejection = JsonRejection>,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(ApiJson(value)),
            Err(rejection) => {
                let body = ErrorBody { error: rejection.body_text() };
                Err((rejection.status(), Json(body)).into_response())
            }
        }
    }
}

fn status_for_core(err: &CoreError) -> StatusCode {
    match err {
        CoreError::Validation(_) => StatusCode::BAD_REQUEST,
        CoreError::MissingAddress(_) => StatusCode::UNPROCESSABLE_ENTITY,
        CoreError::InvalidTransition { .. } => StatusCode::CONFLICT,
        CoreError::Serialization(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Authoring mistakes are the caller's request being wrong (400); a
/// missing variable or a channel-pinned template used on another channel
/// is a well-formed request the service understood but can't act on (422),
/// matching how a recipient missing an address for its channel is reported.
fn status_for_template(err: &TemplateError) -> StatusCode {
    match err {
        TemplateError::Validation(_)
        | TemplateError::UnclosedPlaceholder { .. }
        | TemplateError::InvalidPlaceholderName(_) => StatusCode::BAD_REQUEST,
        TemplateError::MissingVariable { .. } | TemplateError::ChannelMismatch { .. } => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
    }
}

fn status_for_storage(err: &StorageError) -> StatusCode {
    match err {
        StorageError::TemplateNotFound { .. } => StatusCode::NOT_FOUND,
        StorageError::TemplateAlreadyExists { .. } => StatusCode::CONFLICT,
        StorageError::NotificationNotFound(_) | StorageError::RecipientNotFound(_) => StatusCode::NOT_FOUND,
        // UnknownStatus means a row this service wrote can no longer be
        // read back, which is a server-side problem, not a bad request.
        StorageError::Database(_)
        | StorageError::Migration(_)
        | StorageError::Serialization(_)
        | StorageError::UnknownStatus(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::Core(err) => status_for_core(err),
            ApiError::Queue(QueueError::Core(err)) => status_for_core(err),
            ApiError::Queue(QueueError::Storage(err)) => status_for_storage(err),
            ApiError::Storage(err) => status_for_storage(err),
            ApiError::Template(err) => status_for_template(err),
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
        };

        (status, Json(ErrorBody { error: self.to_string() })).into_response()
    }
}
