use ns_core::DeliveryStatus;
use ns_storage::{NotificationRepository, SqliteStorage};
use serde::Serialize;
use uuid::Uuid;

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
