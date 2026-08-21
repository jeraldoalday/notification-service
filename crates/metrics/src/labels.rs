use std::fmt::Write as _;

/// The label set identifying one time series within a metric family, e.g.
/// `{channel="email", outcome="delivered"}`.
///
/// Pairs are kept sorted by name, which does two jobs at once: it makes
/// `Labels` a stable `BTreeMap` key (so `{a="1",b="2"}` and `{b="2",a="1"}`
/// address the *same* series rather than silently splitting a counter in
/// two), and it gives the exposition output a deterministic column order
/// that diffs cleanly between scrapes.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Labels {
    pairs: Vec<(String, String)>,
}

impl Labels {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces the value for `name`. Replacing rather than
    /// appending keeps the invariant that a series has at most one value
    /// per label, which the exposition format assumes.
    pub fn with(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        let value = value.into();
        match self.pairs.binary_search_by(|(existing, _)| existing.as_str().cmp(&name)) {
            Ok(index) => self.pairs[index].1 = value,
            Err(index) => self.pairs.insert(index, (name, value)),
        }
        self
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.pairs
            .binary_search_by(|(existing, _)| existing.as_str().cmp(name))
            .ok()
            .map(|index| self.pairs[index].1.as_str())
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.pairs.iter().map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Renders as the `{name="value",...}` suffix an exposition line
    /// carries, or the empty string when there are no labels (a bare
    /// `metric_name 1` line, not `metric_name{} 1`).
    pub fn render(&self) -> String {
        if self.pairs.is_empty() {
            return String::new();
        }

        let mut out = String::from("{");
        for (index, (name, value)) in self.pairs.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            let _ = write!(out, "{name}=\"{}\"", escape_value(value));
        }
        out.push('}');
        out
    }
}

/// Label *values* are arbitrary strings -- a webhook URL in an error label,
/// a Windows path in a version label -- so the three characters that would
/// otherwise terminate or corrupt the quoted value have to be escaped, per
/// the Prometheus text format. Label *names* are not escaped because every
/// name in this service is a compile-time constant identifier.
fn escape_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Convenience for the overwhelmingly common single-label series.
pub fn label(name: &str, value: impl Into<String>) -> Labels {
    Labels::new().with(name, value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_labels_render_as_nothing() {
        assert_eq!(Labels::new().render(), "");
        assert!(Labels::new().is_empty());
    }

    #[test]
    fn renders_sorted_pairs() {
        let labels = Labels::new().with("outcome", "delivered").with("channel", "email");
        assert_eq!(labels.render(), "{channel=\"email\",outcome=\"delivered\"}");
    }

    #[test]
    fn insertion_order_does_not_change_identity() {
        let a = Labels::new().with("channel", "sms").with("outcome", "failed");
        let b = Labels::new().with("outcome", "failed").with("channel", "sms");
        assert_eq!(a, b, "the same pairs in a different order address the same series");
    }

    #[test]
    fn with_replaces_rather_than_duplicating() {
        let labels = Labels::new().with("channel", "email").with("channel", "sms");
        assert_eq!(labels.len(), 1);
        assert_eq!(labels.get("channel"), Some("sms"));
    }

    #[test]
    fn escapes_quotes_backslashes_and_newlines() {
        let labels = label("error", "said \"no\"\nat C:\\tmp");
        assert_eq!(labels.render(), "{error=\"said \\\"no\\\"\\nat C:\\\\tmp\"}");
    }

    #[test]
    fn get_returns_none_for_absent_label() {
        assert_eq!(label("channel", "push").get("outcome"), None);
    }

    #[test]
    fn iter_yields_sorted_pairs() {
        let labels = Labels::new().with("z", "1").with("a", "2");
        let collected: Vec<_> = labels.iter().collect();
        assert_eq!(collected, vec![("a", "2"), ("z", "1")]);
    }
}
