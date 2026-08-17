use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use ns_core::CoreError;
use ns_queue::QueueError;
use ns_storage::StorageError;
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

    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    BadRequest(String),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

fn status_for_core(err: &CoreError) -> StatusCode {
    match err {
        CoreError::Validation(_) => StatusCode::BAD_REQUEST,
        CoreError::MissingAddress(_) => StatusCode::UNPROCESSABLE_ENTITY,
        CoreError::InvalidTransition { .. } => StatusCode::CONFLICT,
        CoreError::Serialization(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn status_for_storage(err: &StorageError) -> StatusCode {
    match err {
        StorageError::NotificationNotFound(_) | StorageError::RecipientNotFound(_) => StatusCode::NOT_FOUND,
        StorageError::Database(_) | StorageError::Migration(_) | StorageError::Serialization(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::Core(err) => status_for_core(err),
            ApiError::Queue(QueueError::Core(err)) => status_for_core(err),
            ApiError::Queue(QueueError::Storage(err)) => status_for_storage(err),
            ApiError::Storage(err) => status_for_storage(err),
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
        };

        (status, Json(ErrorBody { error: self.to_string() })).into_response()
    }
}
