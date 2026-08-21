use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::instrument::{Counter, Gauge, Histogram};
use crate::labels::Labels;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    /// The word the Prometheus `# TYPE` line uses.
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }
}

/// One time series: a label set paired with the instrument recording it.
#[derive(Clone)]
pub enum Series {
    Counter(Arc<Counter>),
    Gauge(Arc<Gauge>),
    Histogram(Arc<Histogram>),
}

impl Series {
    pub fn kind(&self) -> MetricKind {
        match self {
            Series::Counter(_) => MetricKind::Counter,
            Series::Gauge(_) => MetricKind::Gauge,
            Series::Histogram(_) => MetricKind::Histogram,
        }
    }
}

/// A metric family: one name, one help string, one type, and every label
/// set observed under it so far.
#[derive(Clone)]
pub struct Family {
    pub name: String,
    pub help: String,
    pub kind: MetricKind,
    pub series: BTreeMap<Labels, Series>,
}

/// A collection of metric families, keyed by name.
///
/// Instruments are handed out as `Arc` clones, so a caller resolves a
/// series once (typically at startup, into a `ServiceMetrics`) and never
/// touches this lock again on the hot path -- the lock is held only when a
/// series is first created and while a scrape walks the families.
///
/// Families are a `BTreeMap` rather than a `HashMap` for the same reason
/// `Labels` sorts its pairs: exposition output should be byte-stable
/// between scrapes of an unchanged process, which makes both eyeballing
/// and golden-file testing straightforward.
#[derive(Default)]
pub struct Registry {
    families: Mutex<BTreeMap<String, Family>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolves (creating on first use) the counter named `name` with
    /// `labels`. Repeated calls with the same name and labels return the
    /// same instrument.
    pub fn counter(&self, name: &str, help: &str, labels: Labels) -> Arc<Counter> {
        match self
            .series(name, help, MetricKind::Counter, labels, || Series::Counter(Arc::new(Counter::new())))
        {
            Series::Counter(counter) => counter,
            other => kind_mismatch(name, MetricKind::Counter, other.kind()),
        }
    }

    pub fn gauge(&self, name: &str, help: &str, labels: Labels) -> Arc<Gauge> {
        match self.series(name, help, MetricKind::Gauge, labels, || Series::Gauge(Arc::new(Gauge::new()))) {
            Series::Gauge(gauge) => gauge,
            other => kind_mismatch(name, MetricKind::Gauge, other.kind()),
        }
    }

    /// Resolves a histogram over `bounds`. The bounds only apply the first
    /// time a given `name`/`labels` pair is created; a later call with
    /// different bounds returns the original instrument, since a family's
    /// buckets have to agree across its series for the exposition to make
    /// sense.
    pub fn histogram(&self, name: &str, help: &str, bounds: &[f64], labels: Labels) -> Arc<Histogram> {
        match self.series(name, help, MetricKind::Histogram, labels, || {
            Series::Histogram(Arc::new(Histogram::new(bounds)))
        }) {
            Series::Histogram(histogram) => histogram,
            other => kind_mismatch(name, MetricKind::Histogram, other.kind()),
        }
    }

    fn series(
        &self,
        name: &str,
        help: &str,
        kind: MetricKind,
        labels: Labels,
        create: impl FnOnce() -> Series,
    ) -> Series {
        let mut families = self.families.lock().expect("metrics registry lock poisoned");
        let family = families.entry(name.to_string()).or_insert_with(|| Family {
            name: name.to_string(),
            help: help.to_string(),
            kind,
            series: BTreeMap::new(),
        });

        if family.kind != kind {
            kind_mismatch(name, kind, family.kind);
        }

        family.series.entry(labels).or_insert_with(create).clone()
    }

    /// A point-in-time copy of every family, for rendering. The copy is of
    /// the `Arc` handles rather than the values behind them, so this is
    /// cheap, but it also means a scrape is not an atomic snapshot of the
    /// whole registry -- two counters read microseconds apart can reflect
    /// different instants. That is inherent to sampling a live process and
    /// is what every Prometheus client does too.
    pub fn families(&self) -> Vec<Family> {
        let families = self.families.lock().expect("metrics registry lock poisoned");
        families.values().cloned().collect()
    }

    /// How many distinct families have been registered. Mostly useful to
    /// assert that a `ServiceMetrics` wired up everything it meant to.
    pub fn family_count(&self) -> usize {
        self.families.lock().expect("metrics registry lock poisoned").len()
    }
}

/// Reusing one metric name for two different instrument types would
/// produce exposition output no scraper can parse. Every metric name in
/// this service is a constant declared in one place, so a mismatch is an
/// authoring mistake that should fail loudly at startup rather than
/// silently degrade a dashboard.
fn kind_mismatch(name: &str, requested: MetricKind, existing: MetricKind) -> ! {
    panic!(
        "metric `{name}` is already registered as a {} and cannot be reused as a {}",
        existing.as_str(),
        requested.as_str()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::label;

    #[test]
    fn resolving_the_same_series_twice_returns_the_same_instrument() {
        let registry = Registry::new();
        let a = registry.counter("ns_sends", "sends", label("channel", "email"));
        let b = registry.counter("ns_sends", "sends", label("channel", "email"));
        a.inc();
        b.inc();
        assert_eq!(a.get(), 2, "both handles point at one counter");
        assert_eq!(registry.family_count(), 1);
    }

    #[test]
    fn different_labels_are_independent_series_of_one_family() {
        let registry = Registry::new();
        registry.counter("ns_sends", "sends", label("channel", "email")).inc_by(3);
        registry.counter("ns_sends", "sends", label("channel", "sms")).inc();

        let families = registry.families();
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].series.len(), 2);
    }

    #[test]
    fn unlabelled_series_are_supported() {
        let registry = Registry::new();
        registry.gauge("ns_queue_depth", "depth", Labels::new()).set(7.0);
        let families = registry.families();
        assert_eq!(families[0].series.len(), 1);
        match families[0].series.values().next().unwrap() {
            Series::Gauge(gauge) => assert_eq!(gauge.get(), 7.0),
            _ => panic!("expected a gauge"),
        }
    }

    #[test]
    fn families_are_returned_in_name_order() {
        let registry = Registry::new();
        registry.counter("ns_zebra", "z", Labels::new());
        registry.counter("ns_alpha", "a", Labels::new());
        let names: Vec<_> = registry.families().into_iter().map(|f| f.name).collect();
        assert_eq!(names, vec!["ns_alpha", "ns_zebra"]);
    }

    #[test]
    fn histogram_bounds_are_fixed_by_the_first_caller() {
        let registry = Registry::new();
        let first = registry.histogram("ns_latency", "latency", &[1.0, 2.0], Labels::new());
        let second = registry.histogram("ns_latency", "latency", &[9.0], Labels::new());
        assert_eq!(first.bounds(), second.bounds());
    }

    #[test]
    #[should_panic(expected = "already registered as a counter")]
    fn reusing_a_name_for_another_kind_panics() {
        let registry = Registry::new();
        registry.counter("ns_thing", "thing", Labels::new());
        registry.gauge("ns_thing", "thing", Labels::new());
    }

    #[test]
    fn registry_is_shareable_across_threads() {
        let registry = Arc::new(Registry::new());
        let handles: Vec<_> = (0..4)
            .map(|index| {
                let registry = Arc::clone(&registry);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        registry.counter("ns_sends", "sends", label("worker", index.to_string())).inc();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        let families = registry.families();
        assert_eq!(families[0].series.len(), 4);
        for series in families[0].series.values() {
            match series {
                Series::Counter(counter) => assert_eq!(counter.get(), 100),
                _ => panic!("expected counters"),
            }
        }
    }
}
