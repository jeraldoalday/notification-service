use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::RngCore;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::{AuthError, AuthResult};

/// Every issued token starts with this, which makes a leaked one
/// recognisable in a log or a paste -- both to a secret scanner and to the
/// person who has to work out what they just pasted.
pub const TOKEN_PREFIX: &str = "nsk_";

/// Bytes of entropy behind a token. 32 bytes is the same budget as the
/// SHA-256 digest they're stored under, so the hash is not the weak link.
const SECRET_BYTES: usize = 32;

/// A freshly generated API token, in the clear.
///
/// This value exists exactly once, at creation time: the service stores
/// only the digest, so a token that isn't captured by the caller here is
/// gone. `Debug` is implemented by hand to redact it, so that a token
/// can't reach a log through a `#[derive(Debug)]` on some enclosing
/// request or response struct -- which is the way secrets usually leak.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretToken(String);

impl SecretToken {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Yields the token text. Named to make its use conspicuous at the
    /// call site rather than reading like an ordinary accessor.
    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn digest(&self) -> String {
        digest(&self.0)
    }
}

impl std::fmt::Debug for SecretToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretToken({}***redacted***)", TOKEN_PREFIX)
    }
}

/// Mints a new token: `nsk_` followed by 32 random bytes in unpadded
/// URL-safe base64, so the whole thing is copy-pasteable and survives a
/// query string or a shell without quoting.
pub fn generate() -> SecretToken {
    let mut bytes = [0u8; SECRET_BYTES];
    rand::thread_rng().fill_bytes(&mut bytes);
    SecretToken(format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

/// The lowercase hex SHA-256 of a token, which is what gets stored.
///
/// A plain digest rather than a password hash (bcrypt, argon2) on purpose.
/// Those exist to make *guessing* expensive, which matters when the secret
/// is a human-chosen password with maybe 30 bits of entropy behind it.
/// These tokens carry 256 bits from the system CSPRNG, so an attacker
/// holding the digests has nothing to guess at, and a per-verification
/// cost measured in milliseconds would instead be paid on every single
/// authenticated request.
pub fn digest(token: &str) -> String {
    let hash = Sha256::digest(token.as_bytes());
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Whether `token` hashes to `expected_digest`, compared in constant time.
///
/// The comparison is constant-time even though both sides are digests: a
/// byte-at-a-time comparison against a stored digest leaks, through
/// timing, how long a prefix matched, which is enough to reconstruct a
/// target digest one byte at a time given enough attempts.
pub fn verify(token: &str, expected_digest: &str) -> bool {
    let actual = digest(token);
    actual.as_bytes().ct_eq(expected_digest.as_bytes()).into()
}

/// Extracts the token from an `Authorization: Bearer <token>` header.
///
/// The scheme match is case-insensitive because RFC 7235 says auth schemes
/// are, and clients do send `bearer`. The token itself is not trimmed
/// beyond the single separating space: a token with stray whitespace is a
/// token the caller got wrong, and quietly repairing it would mean the
/// same credential has more than one accepted spelling.
pub fn parse_bearer(header: &str) -> AuthResult<&str> {
    let (scheme, token) = header.split_once(' ').ok_or(AuthError::MalformedToken)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(AuthError::MalformedToken);
    }
    if token.is_empty() || !token.starts_with(TOKEN_PREFIX) {
        return Err(AuthError::MalformedToken);
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_tokens_carry_the_prefix_and_are_url_safe() {
        let token = generate();
        assert!(token.expose().starts_with(TOKEN_PREFIX));
        let body = &token.expose()[TOKEN_PREFIX.len()..];
        assert_eq!(URL_SAFE_NO_PAD.decode(body).unwrap().len(), SECRET_BYTES);
        assert!(body.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn generated_tokens_are_distinct() {
        let tokens: std::collections::BTreeSet<String> =
            (0..64).map(|_| generate().expose().to_string()).collect();
        assert_eq!(tokens.len(), 64);
    }

    #[test]
    fn debug_redacts_the_secret() {
        let token = generate();
        let rendered = format!("{token:?}");
        assert!(!rendered.contains(&token.expose()[TOKEN_PREFIX.len()..]), "{rendered}");
        assert!(rendered.contains("redacted"), "{rendered}");
    }

    #[test]
    fn digest_is_stable_hex_sha256() {
        // The SHA-256 of the empty string, as a canary that this is the
        // algorithm and encoding it claims to be.
        assert_eq!(digest(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assert_eq!(digest("nsk_abc").len(), 64);
        assert_eq!(digest("nsk_abc"), digest("nsk_abc"));
        assert_ne!(digest("nsk_abc"), digest("nsk_abd"));
    }

    #[test]
    fn verify_accepts_the_matching_token_only() {
        let token = generate();
        let stored = token.digest();
        assert!(verify(token.expose(), &stored));
        assert!(!verify("nsk_something-else", &stored));
        assert!(!verify(token.expose(), ""));
    }

    #[test]
    fn parse_bearer_accepts_any_case_of_the_scheme() {
        assert_eq!(parse_bearer("Bearer nsk_abc").unwrap(), "nsk_abc");
        assert_eq!(parse_bearer("bearer nsk_abc").unwrap(), "nsk_abc");
        assert_eq!(parse_bearer("BEARER nsk_abc").unwrap(), "nsk_abc");
    }

    #[test]
    fn parse_bearer_rejects_other_schemes_and_shapes() {
        for header in ["Basic nsk_abc", "nsk_abc", "Bearer ", "Bearer abc", ""] {
            assert_eq!(parse_bearer(header), Err(AuthError::MalformedToken), "accepted {header:?}");
        }
    }
}
