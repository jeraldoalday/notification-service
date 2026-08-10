use serde::{Deserialize, Serialize};

/// Delivery priority. Higher-priority notifications are dequeued first by
/// the worker's scheduler (see `ns-queue`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl Default for Priority {
    fn default() -> Self {
        Priority::Normal
    }
}

impl Priority {
    pub fn as_u8(&self) -> u8 {
        *self as u8
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
        assert_eq!(
            priorities,
            vec![Priority::Critical, Priority::High, Priority::Normal, Priority::Low]
        );
    }
}
