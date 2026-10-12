use serde::{Deserialize, Serialize};

/// Delivery priority. Higher-priority notifications are dequeued first by
/// the worker's scheduler (see `ns-queue`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl Priority {
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Priority::Low => "low",
            Priority::Normal => "normal",
            Priority::High => "high",
            Priority::Critical => "critical",
        }
    }

    /// Every variant, lowest severity first. Lets a caller that needs to
    /// enumerate priorities (the CLI's parser, in particular) forward to
    /// this rather than keeping its own second copy of the list, the same
    /// way `ChannelKind::all()` and `DeliveryStatus::all()` already do.
    pub fn all() -> [Priority; 4] {
        [Priority::Low, Priority::Normal, Priority::High, Priority::Critical]
    }
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_by_severity() {
        assert!(Priority::Critical > Priority::High);
        assert!(Priority::High > Priority::Normal);
        assert!(Priority::Normal > Priority::Low);
    }

    #[test]
    fn default_is_normal() {
        assert_eq!(Priority::default(), Priority::Normal);
    }

    #[test]
    fn sorts_descending_for_dequeue() {
        let mut priorities = vec![Priority::Low, Priority::Critical, Priority::Normal, Priority::High];
        priorities.sort_by(|a, b| b.cmp(a));
        assert_eq!(priorities, vec![Priority::Critical, Priority::High, Priority::Normal, Priority::Low]);
    }

    #[test]
    fn display_matches_as_str() {
        for priority in Priority::all() {
            assert_eq!(priority.to_string(), priority.as_str());
        }
    }

    #[test]
    fn serde_round_trip_matches_as_str() {
        for priority in Priority::all() {
            let json = serde_json::to_string(&priority).unwrap();
            assert_eq!(json, format!("\"{}\"", priority.as_str()));
            assert_eq!(serde_json::from_str::<Priority>(&json).unwrap(), priority);
        }
    }
}
