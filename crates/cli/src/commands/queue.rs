use chrono::Utc;
use ns_core::DeliveryStatus;
use ns_queue::NotificationQueue;
use ns_storage::{NotificationRepository, SqliteStorage};
use serde::Serialize;
use uuid::Uuid;

use crate::commands::notification::summary_table;
use crate::error::CliResult;
use crate::output::{table, OutputFormat};

#[derive(Debug, Serialize)]
pub struct QueueStats {
    pub tenant_id: Option<Uuid>,
    /// One entry per `DeliveryStatus`, including the empty ones. The
    /// repository omits statuses with no rows (it's a plain `GROUP BY`),
    /// but a status summary that silently drops "dead_lettered: 0" reads
    /// as if the question was never asked.
    pub counts: Vec<StatusCount>,
    pub total: u64,
}

#[derive(Debug, Serialize)]
pub struct StatusCount {
    pub status: DeliveryStatus,
    pub count: u64,
}

pub async fn stats(
    storage: &SqliteStorage,
    tenant_id: Option<Uuid>,
    output: OutputFormat,
) -> CliResult<String> {
    let counted = storage.count_by_status(tenant_id).await?;

    let counts: Vec<StatusCount> = DeliveryStatus::all()
        .into_iter()
        .map(|status| StatusCount { status, count: counted.get(&status).copied().unwrap_or(0) })
        .collect();
    let total = counts.iter().map(|entry| entry.count).sum();

    let stats = QueueStats { tenant_id, counts, total };
    output.render(&stats, || {
        let rows: Vec<Vec<String>> = stats
            .counts
            .iter()
            .map(|entry| vec![entry.status.to_string(), entry.count.to_string()])
            .chain(std::iter::once(vec!["total".to_string(), stats.total.to_string()]))
            .collect();
        table(&["status", "count"], &rows)
    })
}

/// Shows the work the next worker poll would claim, in the order it would
/// claim it. This is `due_for_delivery` rather than "everything queued":
/// notifications waiting behind a schedule or a backoff delay are exactly
/// what an operator is trying to tell apart from work that's genuinely
/// stuck.
pub async fn due(storage: &SqliteStorage, limit: u32, output: OutputFormat) -> CliResult<String> {
    let notifications = storage.due_for_delivery(Utc::now(), limit).await?;
    output.render(&notifications, || summary_table(&notifications))
}

/// Re-queues a tenant's dead-lettered notifications as fresh copies. The
/// originals stay dead-lettered as the record of what happened, so running
/// this twice replays the same work twice rather than becoming a no-op --
/// which is why it's capped by --limit rather than defaulting to
/// everything.
pub async fn replay(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    limit: u32,
    output: OutputFormat,
) -> CliResult<String> {
    let queue = NotificationQueue::new(storage.clone());
    let replayed = queue.replay_dead_lettered(tenant_id, limit, Utc::now()).await?;

    output.render(&replayed, || {
        if replayed.is_empty() {
            return "nothing dead-lettered to replay\n".to_string();
        }
        format!("replayed {} notification(s)\n{}", replayed.len(), summary_table(&replayed))
    })
}
