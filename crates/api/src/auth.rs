use axum::extract::{FromRequestParts, Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use ns_auth::{AuthContext, AuthError, Scope};
use uuid::Uuid;

use crate::error::{ApiError, ApiResult};
use crate::state::{AppState, Repositories};

/// Whether the API checks credentials at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// Every request to a data endpoint must present a valid API key, and
    /// acts only for that key's tenant.
    Required,
    /// No credential is checked. A request acts for whichever tenant it
    /// names, which is only safe where the network already decides who may
    /// reach the service -- a single-tenant deployment on a private
    /// subnet, or a test. It is not the default, and the serve binary logs
    /// a warning at startup when it is in effect.
    Disabled,
}

/// Who a request is acting as, resolved before any handler runs.
///
/// The `Unauthenticated` variant exists only for `AuthMode::Disabled`;
/// with authentication required, the middleware rejects the request before
/// a handler is reached, so a handler holding a `Caller` in an
/// authenticated deployment always holds a real identity.
#[derive(Debug, Clone)]
pub enum Caller {
    Authenticated(Box<AuthContext>),
    Unauthenticated,
}

impl Caller {
    /// Enforces that this caller carries `required`.
    pub fn require(&self, required: Scope) -> ApiResult<()> {
        match self {
            Caller::Authenticated(context) => Ok(context.require(required)?),
            Caller::Unauthenticated => Ok(()),
        }
    }

    /// The tenant this request acts for, given whatever tenant the request
    /// itself named.
    ///
    /// This is the single place tenant identity is decided, and the reason
    /// it isn't left to each handler. An authenticated caller acts for its
    /// key's tenant, full stop: a `tenant_id` in the body or query string
    /// is accepted only if it agrees, and disagreeing is a 403 rather than
    /// being quietly overridden, so a client sending the wrong tenant
    /// finds out instead of silently writing somewhere unexpected.
    pub fn tenant_for(&self, requested: Option<Uuid>) -> ApiResult<Uuid> {
        match self {
            Caller::Authenticated(context) => {
                if let Some(requested) = requested {
                    context.require_tenant(requested)?;
                }
                Ok(context.tenant_id)
            }
            Caller::Unauthenticated => requested.ok_or_else(|| {
                ApiError::BadRequest(
                    "tenant_id is required when the API is running with authentication disabled".into(),
                )
            }),
        }
    }

    /// Enforces that this caller may touch data belonging to `tenant_id`,
    /// for the reads where the tenant comes from a stored record rather
    /// than from the request (fetching one notification by id, say).
    pub fn require_tenant(&self, tenant_id: Uuid) -> ApiResult<()> {
        match self {
            Caller::Authenticated(context) => Ok(context.require_tenant(tenant_id)?),
            Caller::Unauthenticated => Ok(()),
        }
    }

    pub fn tenant_id(&self) -> Option<Uuid> {
        match self {
            Caller::Authenticated(context) => Some(context.tenant_id),
            Caller::Unauthenticated => None,
        }
    }
}

/// Extracts the `Caller` the middleware resolved.
///
/// Its absence is treated as a server error rather than as "no
/// credentials": it can only mean a route was mounted without the
/// authentication layer, and failing open there would turn a wiring
/// mistake into a silent authorization bypass.
impl<S: Send + Sync> FromRequestParts<S> for Caller {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Caller>()
            .cloned()
            .ok_or_else(|| ApiError::Internal("route is missing its authentication layer".into()))
    }
}

/// Resolves the request's credential and attaches the resulting `Caller`.
///
/// The lookup is by digest, so an unknown token costs exactly one indexed
/// point query -- the same cost as a known one, which keeps a flood of
/// invalid tokens from being cheaper to serve than real traffic, and keeps
/// the response time from distinguishing the two.
pub async fn authenticate<R>(State(state): State<AppState<R>>, mut request: Request, next: Next) -> Response
where
    R: Repositories,
{
    let caller = match state.auth {
        AuthMode::Disabled => Caller::Unauthenticated,
        AuthMode::Required => {
            // Cloned so that nothing borrowed from `request` -- whose body
            // is not `Sync` -- is alive across the storage lookup below.
            let headers = request.headers().clone();
            match authenticated_caller(&state, &headers).await {
                Ok(caller) => caller,
                Err(err) => return err.into_response(),
            }
        }
    };

    request.extensions_mut().insert(caller);
    next.run(request).await
}

/// Takes the header map rather than the `Request` because the token has
/// to be lifted out of the headers *before* the storage lookup is awaited:
/// `axum::body::Body` isn't `Sync`, so a borrow of the whole request held
/// across an await would make this middleware's future non-`Send`, which
/// no tower `Service` will accept.
async fn authenticated_caller<R>(state: &AppState<R>, headers: &HeaderMap) -> Result<Caller, ApiError>
where
    R: Repositories,
{
    let header = headers
        .get(AUTHORIZATION)
        .ok_or(AuthError::MissingCredentials)?
        .to_str()
        // A header that isn't even valid ASCII can't be a bearer token, so
        // it's the same class of failure as one with the wrong shape.
        .map_err(|_| AuthError::MalformedToken)?;
    let token = ns_auth::token::parse_bearer(header)?.to_string();

    // Translated rather than propagated: StorageError::UnknownApiKey has
    // its own message, and letting it through would answer "no api key
    // matches the presented token" for an unrecognised token while a
    // revoked one got ns-auth's uniform "invalid credentials" -- which is
    // exactly the distinction the uniform wording exists to hide.
    let key =
        state.repo.find_api_key_by_digest(&ns_auth::token::digest(&token)).await.map_err(
            |err| match err {
                ns_storage::StorageError::UnknownApiKey => ApiError::Auth(AuthError::UnknownKey),
                other => ApiError::Storage(other),
            },
        )?;
    key.authenticate(&token)?;
    Ok(Caller::Authenticated(Box::new(AuthContext::from(&key))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn authenticated(tenant_id: Uuid, scopes: &[Scope]) -> Caller {
        let scopes: BTreeSet<Scope> = scopes.iter().copied().collect();
        Caller::Authenticated(Box::new(AuthContext::new(Uuid::new_v4(), tenant_id, scopes)))
    }

    #[test]
    fn an_authenticated_caller_acts_for_its_own_tenant() {
        let tenant = Uuid::new_v4();
        let caller = authenticated(tenant, &[Scope::Write]);

        assert_eq!(caller.tenant_for(None).unwrap(), tenant);
        assert_eq!(caller.tenant_for(Some(tenant)).unwrap(), tenant);
    }

    #[test]
    fn naming_another_tenant_is_refused_rather_than_overridden() {
        let caller = authenticated(Uuid::new_v4(), &[Scope::Write]);
        let err = caller.tenant_for(Some(Uuid::new_v4())).unwrap_err();
        assert!(matches!(err, ApiError::Auth(AuthError::WrongTenant { .. })), "{err:?}");
    }

    #[test]
    fn scopes_are_enforced_through_the_caller() {
        let caller = authenticated(Uuid::new_v4(), &[Scope::Read]);
        assert!(caller.require(Scope::Read).is_ok());
        assert!(matches!(
            caller.require(Scope::Write).unwrap_err(),
            ApiError::Auth(AuthError::InsufficientScope { required: Scope::Write })
        ));
    }

    #[test]
    fn an_unauthenticated_caller_must_name_a_tenant() {
        let caller = Caller::Unauthenticated;
        let tenant = Uuid::new_v4();
        assert_eq!(caller.tenant_for(Some(tenant)).unwrap(), tenant);

        let err = caller.tenant_for(None).unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)), "{err:?}");
    }

    #[test]
    fn an_unauthenticated_caller_carries_every_scope() {
        let caller = Caller::Unauthenticated;
        for scope in Scope::all() {
            assert!(caller.require(scope).is_ok());
        }
        assert!(caller.require_tenant(Uuid::new_v4()).is_ok());
        assert_eq!(caller.tenant_id(), None);
    }
}
