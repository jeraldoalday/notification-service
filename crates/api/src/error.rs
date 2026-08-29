use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Request};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use ns_auth::AuthError;
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

    /// The recipient's address for this channel is on the tenant's
    /// suppression list.
    ///
    /// 422 rather than 403: the request was well-formed, understood, and
    /// the caller is perfectly entitled to make it -- the service just
    /// won't act on it. That matches how a recipient missing an address
    /// for its channel is reported, which is the same shape of problem.
    #[error("recipient's {reason} address is suppressed for this tenant")]
    Suppressed { reason: ns_core::SuppressionReason },

    /// The caller reused an idempotency key for a different message.
    ///
    /// 409 rather than 200-with-the-original: the two requests disagree
    /// about what should be sent, and answering with the first one would
    /// drop the second message without anything ever reporting a failure.
    /// A key derived from something less unique than the caller thought is
    /// a bug in the caller, and it is only ever discoverable here.
    #[error("idempotency key `{key}` was already used for a different notification")]
    IdempotencyConflict { key: String },

    #[error(transparent)]
    Auth(#[from] AuthError),

    /// A fault on this side of the wire, reported without detail. Used
    /// where an invariant the wiring is supposed to guarantee turns out
    /// not to hold -- a route mounted without its authentication layer,
    /// say -- which the caller can do nothing about.
    #[error("internal error")]
    Internal(String),
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
        StorageError::TemplateAlreadyExists { .. }
        // Only reachable if the winner of a keyed race has since
        // vanished, which nothing in this service does -- but it is still
        // a collision on a key the caller chose, not a server fault.
        | StorageError::DuplicateIdempotencyKey { .. } => StatusCode::CONFLICT,
        StorageError::NotificationNotFound(_)
        | StorageError::RecipientNotFound(_)
        | StorageError::ApiKeyNotFound(_) => StatusCode::NOT_FOUND,
        // A token that resolves to no key is a credential problem, not a
        // missing resource: the caller asked about itself, and the answer
        // is that it isn't anyone.
        StorageError::UnknownApiKey => StatusCode::UNAUTHORIZED,
        // UnknownStatus means a row this service wrote can no longer be
        // read back, which is a server-side problem, not a bad request.
        StorageError::Database(_)
        | StorageError::Migration(_)
        | StorageError::Serialization(_)
        | StorageError::UnknownStatus(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// 401 for "you presented no usable credential", 403 for "your credential
/// isn't allowed to do that". The split matters to a client: a 401 says
/// try again with a credential, a 403 says a different credential of the
/// same kind won't help.
fn status_for_auth(err: &AuthError) -> StatusCode {
    if err.is_unauthenticated() {
        StatusCode::UNAUTHORIZED
    } else {
        StatusCode::FORBIDDEN
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::Core(err) => status_for_core(err),
            ApiError::Queue(QueueError::Core(err)) => status_for_core(err),
            ApiError::Queue(QueueError::Storage(err)) => status_for_storage(err),
            // Asking to replay something that isn't dead-lettered is a
            // conflict with the notification's current state, the same way
            // cancelling an already-sending notification is.
            ApiError::Queue(QueueError::NotReplayable { .. }) => StatusCode::CONFLICT,
            ApiError::Storage(err) => status_for_storage(err),
            ApiError::Template(err) => status_for_template(err),
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Suppressed { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            ApiError::IdempotencyConflict { .. } => StatusCode::CONFLICT,
            ApiError::Auth(err) => status_for_auth(err),
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        // Details of an internal fault go to the operator's log and no
        // further; the caller gets the bare status.
        if let ApiError::Internal(detail) = &self {
            tracing::error!(detail = %detail, "internal error serving request");
        }

        let body = Json(ErrorBody { error: self.to_string() });

        // RFC 9110 requires a challenge on a 401, and it's what tells a
        // client library which scheme to retry under.
        if status == StatusCode::UNAUTHORIZED {
            return (status, [(header::WWW_AUTHENTICATE, "Bearer")], body).into_response();
        }
        (status, body).into_response()
    }
}
