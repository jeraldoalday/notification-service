use serde::{Deserialize, Serialize};
use std::fmt;

/// What an API key is allowed to do.
///
/// Three coarse scopes rather than one per endpoint: the service has a
/// small, stable surface, and a permission model finer than the thing it
/// protects mostly produces keys whose scope lists nobody reads. The
/// division follows blast radius -- `Read` can observe, `Write` can cause
/// messages to be sent, `Admin` can mint further credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// Read notifications, recipients and templates.
    Read,
    /// Submit, cancel and replay notifications; create and edit templates
    /// and recipients.
    Write,
    /// Manage API keys.
    Admin,
}

impl Scope {
    pub fn as_str(&self) -> &'static str {
        match self {
            Scope::Read => "read",
            Scope::Write => "write",
            Scope::Admin => "admin",
        }
    }

    pub fn all() -> [Scope; 3] {
        [Scope::Read, Scope::Write, Scope::Admin]
    }

    /// Whether holding `self` satisfies a requirement for `required`.
    ///
    /// `Write` implies `Read` and `Admin` implies both: a key that can
    /// submit a notification can obviously already observe the effect of
    /// doing so, so requiring callers to list `read` alongside `write`
    /// would only produce keys that always carry both. `Admin` is separate
    /// from that ladder in intent but not in power -- it can mint a key
    /// with any scope, so denying it those scopes directly would be a
    /// formality rather than a restriction.
    pub fn satisfies(&self, required: Scope) -> bool {
        match self {
            Scope::Admin => true,
            Scope::Write => required != Scope::Admin,
            Scope::Read => required == Scope::Read,
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for Scope {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Scope::all()
            .into_iter()
            .find(|scope| scope.as_str() == value)
            .ok_or_else(|| format!("unknown scope `{value}`, expected one of read, write, admin"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_and_parse_round_trip() {
        for scope in Scope::all() {
            assert_eq!(scope.to_string().parse::<Scope>().unwrap(), scope);
        }
    }

    #[test]
    fn parse_rejects_unknown_scopes_with_a_helpful_message() {
        let err = "superuser".parse::<Scope>().unwrap_err();
        assert!(err.contains("read, write, admin"), "{err}");
    }

    #[test]
    fn serde_uses_the_same_spelling_as_display() {
        for scope in Scope::all() {
            assert_eq!(serde_json::to_string(&scope).unwrap(), format!("\"{scope}\""));
        }
    }

    #[test]
    fn write_implies_read_but_not_admin() {
        assert!(Scope::Write.satisfies(Scope::Read));
        assert!(Scope::Write.satisfies(Scope::Write));
        assert!(!Scope::Write.satisfies(Scope::Admin));
    }

    #[test]
    fn read_implies_only_read() {
        assert!(Scope::Read.satisfies(Scope::Read));
        assert!(!Scope::Read.satisfies(Scope::Write));
        assert!(!Scope::Read.satisfies(Scope::Admin));
    }

    #[test]
    fn admin_satisfies_everything() {
        for required in Scope::all() {
            assert!(Scope::Admin.satisfies(required));
        }
    }

    #[test]
    fn scopes_order_by_increasing_power() {
        let mut scopes = vec![Scope::Admin, Scope::Read, Scope::Write];
        scopes.sort();
        assert_eq!(scopes, vec![Scope::Read, Scope::Write, Scope::Admin]);
    }
}
