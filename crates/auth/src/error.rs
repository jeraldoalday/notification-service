use thiserror::Error;

pub type AuthResult<T> = Result<T, AuthError>;

/// Why a request was refused.
///
/// The variants distinguish *unauthenticated* (no usable credential:
/// missing, malformed, unknown, revoked) from *unauthorized* (a valid
/// credential that isn't allowed to do this), because they map onto
/// different HTTP responses -- 401 tells a caller to present a credential,
/// 403 tells it that presenting a different one won't help.
///
/// What the variants deliberately don't do is distinguish *which* of the
/// unauthenticated cases applied in their `Display` text: "unknown key"
/// versus "revoked key" would confirm to whoever is probing that a token
/// they hold was once real. The service logs the specific variant and
/// tells the caller only that the credential wasn't accepted.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("missing credentials")]
    MissingCredentials,

    #[error("invalid credentials")]
    MalformedToken,

    #[error("invalid credentials")]
    UnknownKey,

    #[error("invalid credentials")]
    RevokedKey,

    #[error("this API key does not have the `{required}` scope")]
    InsufficientScope { required: crate::scope::Scope },

    #[error("this API key is not permitted to act for tenant {tenant_id}")]
    WrongTenant { tenant_id: uuid::Uuid },
}

impl AuthError {
    /// Whether this is "you presented no usable credential" (401) rather
    /// than "your credential isn't allowed to do that" (403).
    pub fn is_unauthenticated(&self) -> bool {
        matches!(
            self,
            AuthError::MissingCredentials
                | AuthError::MalformedToken
                | AuthError::UnknownKey
                | AuthError::RevokedKey
        )
    }

    /// A stable, non-leaking word for logs and metrics labels. Unlike
    /// `Display`, this *does* separate the unauthenticated cases -- it
    /// goes to the operator, not to the caller.
    pub fn reason(&self) -> &'static str {
        match self {
            AuthError::MissingCredentials => "missing_credentials",
            AuthError::MalformedToken => "malformed_token",
            AuthError::UnknownKey => "unknown_key",
            AuthError::RevokedKey => "revoked_key",
            AuthError::InsufficientScope { .. } => "insufficient_scope",
            AuthError::WrongTenant { .. } => "wrong_tenant",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scope::Scope;

    #[test]
    fn unauthenticated_cases_are_classified_together() {
        for err in [
            AuthError::MissingCredentials,
            AuthError::MalformedToken,
            AuthError::UnknownKey,
            AuthError::RevokedKey,
        ] {
            assert!(err.is_unauthenticated(), "{err:?}");
        }
    }

    #[test]
    fn authorization_failures_are_not_unauthenticated() {
        assert!(!AuthError::InsufficientScope { required: Scope::Write }.is_unauthenticated());
        assert!(!AuthError::WrongTenant { tenant_id: uuid::Uuid::nil() }.is_unauthenticated());
    }

    #[test]
    fn credential_failures_are_indistinguishable_to_the_caller() {
        let messages: Vec<String> = [AuthError::MalformedToken, AuthError::UnknownKey, AuthError::RevokedKey]
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(messages[0], messages[1]);
        assert_eq!(messages[1], messages[2]);
    }

    #[test]
    fn reasons_are_distinct_for_the_operator() {
        let reasons = [
            AuthError::MissingCredentials.reason(),
            AuthError::MalformedToken.reason(),
            AuthError::UnknownKey.reason(),
            AuthError::RevokedKey.reason(),
        ];
        let unique: std::collections::BTreeSet<_> = reasons.iter().collect();
        assert_eq!(unique.len(), reasons.len());
    }
}
