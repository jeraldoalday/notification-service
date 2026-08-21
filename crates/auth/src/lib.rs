//! Authentication and tenant isolation for the notification service.
//!
//! A caller presents a bearer API key; the service resolves it to an
//! `AuthContext` carrying the key's tenant and scopes, and every handler
//! works from that context rather than from anything the request said
//! about itself. That distinction is the whole point of this crate: the
//! failure mode a multi-tenant service falls into is authenticating the
//! caller correctly and then believing the caller's own claim about which
//! tenant it is acting for.
//!
//! Tokens are stored as SHA-256 digests and compared in constant time.
//! They are never persisted, logged or serialized in the clear -- the one
//! moment a token exists as text is the return value of
//! `ApiKey::generate`, and `SecretToken`'s `Debug` redacts it even then.

pub mod error;
pub mod key;
pub mod scope;
pub mod token;

pub use error::{AuthError, AuthResult};
pub use key::{ApiKey, AuthContext};
pub use scope::Scope;
pub use token::SecretToken;
