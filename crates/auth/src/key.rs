use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{AuthError, AuthResult};
use crate::scope::Scope;
use crate::token::{self, SecretToken};

/// A credential belonging to one tenant.
///
/// The token itself is never stored -- only `token_digest` -- so an
/// `ApiKey` is safe to serialize into a response, a log line or the `data`
/// column of a row. The clear token exists once, as the second half of
/// `ApiKey::generate`'s return value, and if the caller doesn't keep it
/// there is no way to recover it; the operator mints a replacement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: Uuid,
    pub tenant_id: Uuid,
    /// Operator-facing label ("ci-deploy", "billing-service"), so a key
    /// can be recognised and revoked without anyone having to hold the
    /// secret next to it.
    pub name: String,
    pub token_digest: String,
    /// A `BTreeSet` so that a key's scopes serialize in a stable order and
    /// two keys granted the same scopes compare equal regardless of the
    /// order they were listed in.
    pub scopes: BTreeSet<Scope>,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

impl ApiKey {
    /// Mints a key and its one-time token. The token is returned
    /// separately rather than stored on the struct so that there's no
    /// field an unwary caller could persist or log it through.
    pub fn generate(
        tenant_id: Uuid,
        name: impl Into<String>,
        scopes: impl IntoIterator<Item = Scope>,
        now: DateTime<Utc>,
    ) -> (Self, SecretToken) {
        let secret = token::generate();
        let key = Self {
            id: Uuid::new_v4(),
            tenant_id,
            name: name.into(),
            token_digest: secret.digest(),
            scopes: scopes.into_iter().collect(),
            created_at: now,
            revoked_at: None,
        };
        (key, secret)
    }

    pub fn is_revoked(&self) -> bool {
        self.revoked_at.is_some()
    }

    /// Marks the key revoked. Revoking an already-revoked key keeps the
    /// original timestamp: the first revocation is when the credential
    /// stopped working, and a retried or duplicated request shouldn't
    /// rewrite that.
    pub fn revoke(&mut self, now: DateTime<Utc>) {
        self.revoked_at.get_or_insert(now);
    }

    /// Whether this key's scopes cover `required`, following the
    /// implication ladder in `Scope::satisfies`.
    pub fn allows(&self, required: Scope) -> bool {
        self.scopes.iter().any(|scope| scope.satisfies(required))
    }

    /// Whether this key may act on data belonging to `tenant_id`. Keys are
    /// single-tenant by construction; this is the one place that check is
    /// spelled out, so handlers call it rather than each comparing two
    /// uuids and hoping they got the direction right.
    pub fn may_act_for(&self, tenant_id: Uuid) -> bool {
        self.tenant_id == tenant_id
    }

    /// Full check of a presented token against this key: the digest has to
    /// match and the key has to still be live.
    pub fn authenticate(&self, presented: &str) -> AuthResult<()> {
        if !token::verify(presented, &self.token_digest) {
            return Err(AuthError::UnknownKey);
        }
        if self.is_revoked() {
            return Err(AuthError::RevokedKey);
        }
        Ok(())
    }

    /// The last eight characters of the digest, for identifying a key in
    /// CLI output. Derived from the *digest* rather than the token so that
    /// displaying it reveals nothing usable, and it's stable for the life
    /// of the key.
    pub fn fingerprint(&self) -> String {
        let digest = &self.token_digest;
        digest[digest.len().saturating_sub(8)..].to_string()
    }
}

/// The identity a request is acting under, once its credential has been
/// resolved. Handlers take this rather than an `ApiKey` so they can't
/// accidentally read a digest, and so that a future second credential type
/// (a signed service-to-service token, say) has an obvious place to
/// produce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub key_id: Uuid,
    pub tenant_id: Uuid,
    pub scopes: BTreeSet<Scope>,
}

impl AuthContext {
    pub fn new(key_id: Uuid, tenant_id: Uuid, scopes: BTreeSet<Scope>) -> Self {
        Self { key_id, tenant_id, scopes }
    }

    pub fn allows(&self, required: Scope) -> bool {
        self.scopes.iter().any(|scope| scope.satisfies(required))
    }

    /// Returns `Ok(())` if this context carries `required`, or the error
    /// that should become a 403.
    pub fn require(&self, required: Scope) -> AuthResult<()> {
        if self.allows(required) {
            Ok(())
        } else {
            Err(AuthError::InsufficientScope { required })
        }
    }

    /// Returns `Ok(())` if this context may act on `tenant_id`'s data.
    ///
    /// This is what stops one tenant reading another's notifications by
    /// simply passing someone else's `tenant_id` in a query string, which
    /// is the failure mode a service like this one gets wrong most often:
    /// authenticating the caller and then trusting the caller's own claim
    /// about who it's acting for.
    pub fn require_tenant(&self, tenant_id: Uuid) -> AuthResult<()> {
        if self.tenant_id == tenant_id {
            Ok(())
        } else {
            Err(AuthError::WrongTenant { tenant_id })
        }
    }
}

impl From<&ApiKey> for AuthContext {
    fn from(key: &ApiKey) -> Self {
        Self { key_id: key.id, tenant_id: key.tenant_id, scopes: key.scopes.clone() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn key_with(scopes: &[Scope]) -> (ApiKey, SecretToken) {
        ApiKey::generate(Uuid::new_v4(), "ci", scopes.iter().copied(), Utc::now())
    }

    #[test]
    fn a_generated_key_authenticates_with_its_own_token_only() {
        let (key, token) = key_with(&[Scope::Write]);
        assert_eq!(key.authenticate(token.expose()), Ok(()));
        assert_eq!(key.authenticate("nsk_not-the-token"), Err(AuthError::UnknownKey));
    }

    #[test]
    fn the_clear_token_is_not_recoverable_from_the_key() {
        let (key, token) = key_with(&[Scope::Read]);
        let serialized = serde_json::to_string(&key).unwrap();
        assert!(!serialized.contains(token.expose()), "{serialized}");
        assert!(!format!("{key:?}").contains(token.expose()));
    }

    #[test]
    fn revoked_keys_stop_authenticating() {
        let (mut key, token) = key_with(&[Scope::Write]);
        key.revoke(Utc::now());
        assert!(key.is_revoked());
        assert_eq!(key.authenticate(token.expose()), Err(AuthError::RevokedKey));
    }

    #[test]
    fn revoking_twice_keeps_the_first_timestamp() {
        let (mut key, _) = key_with(&[Scope::Read]);
        let first = Utc::now();
        key.revoke(first);
        key.revoke(first + Duration::hours(1));
        assert_eq!(key.revoked_at, Some(first));
    }

    #[test]
    fn a_wrong_token_on_a_revoked_key_still_reads_as_unknown() {
        let (mut key, _) = key_with(&[Scope::Read]);
        key.revoke(Utc::now());
        assert_eq!(
            key.authenticate("nsk_wrong"),
            Err(AuthError::UnknownKey),
            "the digest check comes first, so revocation status leaks nothing to a guesser"
        );
    }

    #[test]
    fn scope_implication_applies_to_a_whole_key() {
        let (key, _) = key_with(&[Scope::Write]);
        assert!(key.allows(Scope::Read));
        assert!(key.allows(Scope::Write));
        assert!(!key.allows(Scope::Admin));
    }

    #[test]
    fn a_key_with_no_scopes_allows_nothing() {
        let (key, _) = key_with(&[]);
        for scope in Scope::all() {
            assert!(!key.allows(scope));
        }
    }

    #[test]
    fn scopes_are_deduplicated_and_ordered() {
        let (key, _) =
            ApiKey::generate(Uuid::new_v4(), "ci", [Scope::Write, Scope::Read, Scope::Write], Utc::now());
        assert_eq!(key.scopes.iter().copied().collect::<Vec<_>>(), vec![Scope::Read, Scope::Write]);
    }

    #[test]
    fn a_key_acts_only_for_its_own_tenant() {
        let (key, _) = key_with(&[Scope::Write]);
        assert!(key.may_act_for(key.tenant_id));
        assert!(!key.may_act_for(Uuid::new_v4()));
    }

    #[test]
    fn fingerprint_comes_from_the_digest_not_the_token() {
        let (key, token) = key_with(&[Scope::Read]);
        let fingerprint = key.fingerprint();
        assert_eq!(fingerprint.len(), 8);
        assert!(key.token_digest.ends_with(&fingerprint));
        assert!(!token.expose().contains(&fingerprint) || fingerprint.is_empty());
    }

    #[test]
    fn auth_context_enforces_scope_and_tenant() {
        let (key, _) = key_with(&[Scope::Write]);
        let context = AuthContext::from(&key);

        assert_eq!(context.tenant_id, key.tenant_id);
        assert_eq!(context.require(Scope::Read), Ok(()));
        assert_eq!(
            context.require(Scope::Admin),
            Err(AuthError::InsufficientScope { required: Scope::Admin })
        );
        assert_eq!(context.require_tenant(key.tenant_id), Ok(()));

        let other = Uuid::new_v4();
        assert_eq!(context.require_tenant(other), Err(AuthError::WrongTenant { tenant_id: other }));
    }
}
