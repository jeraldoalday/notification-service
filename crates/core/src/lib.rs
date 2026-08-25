pub mod attempt;
pub mod channel;
pub mod error;
pub mod notification;
pub mod priority;
pub mod recipient;
pub mod status;

pub use attempt::{AttemptResult, DeliveryAttempt};
pub use channel::{Channel, ChannelKind};
pub use error::{CoreError, CoreResult};
pub use notification::{Notification, NotificationBuilder};
pub use priority::Priority;
pub use recipient::{Recipient, RecipientAddress};
pub use status::DeliveryStatus;
