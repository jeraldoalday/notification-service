//! In-process metrics for the notification service.
//!
//! Deliberately self-contained rather than pulling in a metrics facade:
//! the service needs a handful of counters, gauges and latency histograms
//! and one `GET /metrics` endpoint, and owning that outright keeps the
//! exposition format, the label conventions and the naming under this
//! crate's tests instead of spread across a facade, a recorder and an
//! exporter.
//!
//! Instruments are handed out as `Arc<T>` so hot paths (`ns-worker`'s
//! dispatch loop, `ns-api`'s middleware) resolve a metric once at startup
//! and afterwards only touch a relaxed atomic -- the registry's lock is
//! taken when a series is first created and at scrape time, never on the
//! delivery path.

pub mod instrument;
pub mod labels;
pub mod prometheus;
pub mod registry;
pub mod service;

pub use instrument::{Counter, Gauge, Histogram, DEFAULT_LATENCY_BUCKETS};
pub use labels::{label, Labels};
pub use registry::{Family, MetricKind, Registry, Series};
pub use service::{AttemptOutcome, ServiceMetrics, SkipReason};
