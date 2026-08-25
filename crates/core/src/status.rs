use serde::{Deserialize, Serialize};

use crate::error::{CoreError, CoreResult};

/// Lifecycle state of a single notification delivery attempt.
///
/// Transitions are one-directional and enforced by `can_transition_to` /
/// `transition_to` so that storage and worker code can't silently corrupt
/// history (e.g. moving a `Delivered` notification back to `Pending`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    #[default]
    Pending,
    Queued,
    Sending,
    Delivered,
    Failed,
    DeadLettered,
    Cancelled,
    /// The recipient's address is on the tenant's suppression list, so
    /// nothing was sent and nothing will be.
    ///
    /// Terminal, and deliberately distinct from both neighbours it could
    /// have been folded into. It isn't `Cancelled`, which means somebody
    /// decided to stop this particular message; it isn't `DeadLettered`,
    /// which means delivery was tried and kept failing -- and which is
    /// replayable, whereas replaying a suppressed notification would send
    /// to an address that asked not to be sent to. Keeping it separate is
    /// also what lets an operator count opt-outs without them being
    /// indistinguishable from provider failures.
    Suppressed,
}

impl DeliveryStatus {
    /// Every status, in lifecycle order. Reporting code (the admin CLI's
    /// queue summary) iterates this so its output has a stable, meaningful
    /// order instead of whatever a hash map happened to yield.
    pub fn all() -> [DeliveryStatus; 8] {
        use DeliveryStatus::*;
        [Pending, Queued, Sending, Delivered, Failed, DeadLettered, Cancelled, Suppressed]
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DeliveryStatus::Pending => "pending",
            DeliveryStatus::Queued => "queued",
            DeliveryStatus::Sending => "sending",
            DeliveryStatus::Delivered => "delivered",
            DeliveryStatus::Failed => "failed",
            DeliveryStatus::DeadLettered => "dead_lettered",
            DeliveryStatus::Cancelled => "cancelled",
            DeliveryStatus::Suppressed => "suppressed",
        }
    }

    /// Terminal states never transition further.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            DeliveryStatus::Delivered
                | DeliveryStatus::DeadLettered
                | DeliveryStatus::Cancelled
                | DeliveryStatus::Suppressed
        )
    }

    pub fn can_transition_to(&self, next: DeliveryStatus) -> bool {
        use DeliveryStatus::*;
        match (self, next) {
            (Pending, Queued) => true,
            (Pending, Cancelled) => true,
            (Queued, Sending) => true,
            (Queued, Cancelled) => true,
            (Sending, Delivered) => true,
            (Sending, Failed) => true,
            // Only from Sending: the check happens when the worker has
            // the recipient's address in hand, which is the first moment
            // the service knows which address this notification would
            // actually reach.
            (Sending, Suppressed) => true,
            (Failed, Queued) => true, // retry
            (Failed, DeadLettered) => true,
            (_, _) => false,
        }
    }

    pub fn transition_to(self, next: DeliveryStatus) -> CoreResult<DeliveryStatus> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(CoreError::InvalidTransition { from: self, to: next })
        }
    }
}

impl std::fmt::Display for DeliveryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use DeliveryStatus::*;

    #[test]
    fn all_lists_every_status_once() {
        let all = DeliveryStatus::all();
        assert_eq!(all.len(), 8);
        for status in all {
            assert_eq!(all.iter().filter(|s| **s == status).count(), 1);
        }
    }

    #[test]
    fn as_str_matches_the_serde_representation() {
        for status in DeliveryStatus::all() {
            let json = serde_json::to_string(&status).unwrap();
            assert_eq!(json, format!("\"{}\"", status.as_str()));
        }
    }

    #[test]
    fn display_matches_as_str() {
        for status in DeliveryStatus::all() {
            assert_eq!(status.to_string(), status.as_str());
        }
    }

    #[test]
    fn happy_path_transitions() {
        let mut state = Pending;
        state = state.transition_to(Queued).unwrap();
        state = state.transition_to(Sending).unwrap();
        state = state.transition_to(Delivered).unwrap();
        assert_eq!(state, Delivered);
        assert!(state.is_terminal());
    }

    #[test]
    fn retry_path_reenters_queue() {
        let state = Sending.transition_to(Failed).unwrap();
        let state = state.transition_to(Queued).unwrap();
        assert_eq!(state, Queued);
    }

    #[test]
    fn exhausted_retries_dead_letter() {
        let state = Failed.transition_to(DeadLettered).unwrap();
        assert!(state.is_terminal());
    }

    #[test]
    fn a_send_can_end_in_suppression() {
        let state = Sending.transition_to(Suppressed).unwrap();
        assert!(state.is_terminal());
    }

    #[test]
    fn suppression_is_only_reachable_from_a_claimed_send() {
        for from in DeliveryStatus::all() {
            assert_eq!(
                from.can_transition_to(Suppressed),
                from == Sending,
                "{from:?} should not be able to move straight to suppressed"
            );
        }
    }

    #[test]
    fn a_suppressed_notification_cannot_be_revived() {
        // The distinction from DeadLettered that matters most: replay
        // moves a notification back onto the queue, and doing that to a
        // suppressed one would send to an address that asked not to be.
        for target in DeliveryStatus::all() {
            assert!(!Suppressed.can_transition_to(target), "suppressed -> {target:?}");
        }
    }

    #[test]
    fn rejects_illegal_transition() {
        let err = Delivered.transition_to(Pending).unwrap_err();
        assert!(matches!(err, CoreError::InvalidTransition { .. }));
    }

    #[test]
    fn rejects_skipping_states() {
        assert!(!Pending.can_transition_to(Delivered));
        assert!(!Pending.can_transition_to(Sending));
    }

    #[test]
    fn terminal_states_never_transition() {
        for terminal in [Delivered, DeadLettered, Cancelled, Suppressed] {
            for target in DeliveryStatus::all() {
                if terminal == target {
                    continue;
                }
                assert!(
                    !terminal.can_transition_to(target),
                    "{terminal:?} should not transition to {target:?}"
                );
            }
        }
    }
}
