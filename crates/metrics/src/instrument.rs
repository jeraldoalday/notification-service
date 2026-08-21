use std::sync::atomic::{AtomicU64, Ordering};

/// Bucket upper bounds (in seconds) used by every latency histogram in the
/// service. Spans 5ms to 10s, which brackets both a local webhook POST and
/// an SMTP handshake to a slow relay; anything slower lands in `+Inf`.
pub const DEFAULT_LATENCY_BUCKETS: &[f64] = &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

/// A monotonically increasing count.
///
/// All instruments here use relaxed atomics and are shared as `Arc<T>`
/// rather than living behind the registry's lock. Metrics sit on the
/// delivery path, so the cost of recording one has to stay near zero, and
/// the ordering between two unrelated counters is not something any
/// consumer of a scrape can observe: a scrape is a sample of a moving
/// system either way.
#[derive(Debug, Default)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc(&self) {
        self.inc_by(1);
    }

    pub fn inc_by(&self, amount: u64) {
        self.value.fetch_add(amount, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// A value that can go up and down -- queue depth, in-flight requests.
///
/// Stored as an `AtomicU64` holding the bit pattern of an `f64` so that a
/// gauge can carry fractional values (a rate-limiter's remaining tokens,
/// say) without a second instrument type; `f64::to_bits`/`from_bits` round
/// trip exactly.
#[derive(Debug, Default)]
pub struct Gauge {
    bits: AtomicU64,
}

impl Gauge {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, value: f64) {
        self.bits.store(value.to_bits(), Ordering::Relaxed);
    }

    pub fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
    }

    pub fn inc(&self) {
        self.add(1.0);
    }

    pub fn dec(&self) {
        self.add(-1.0);
    }

    /// Compare-and-swap loop rather than a plain load/store, so two threads
    /// incrementing concurrently can't lose one of the increments.
    pub fn add(&self, delta: f64) {
        let mut current = self.bits.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(current) + delta).to_bits();
            match self.bits.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }
}

/// A cumulative histogram over fixed bucket boundaries, plus the running
/// sum and count needed to derive an average.
///
/// Buckets are stored as *per-bucket* counts and accumulated only at
/// render time. Recording therefore touches exactly one bucket rather than
/// every bucket at or above the observed value, which matters because
/// `observe` runs on the delivery path and rendering runs once per scrape.
#[derive(Debug)]
pub struct Histogram {
    bounds: Vec<f64>,
    /// One slot per bound, plus a final slot for observations above the
    /// last bound (rendered as the `+Inf` bucket).
    counts: Vec<AtomicU64>,
    sum_bits: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    /// Panics if `bounds` is empty or not strictly ascending -- both are
    /// authoring mistakes in a `const` bucket list, caught at startup
    /// rather than silently producing an unusable histogram.
    pub fn new(bounds: &[f64]) -> Self {
        assert!(!bounds.is_empty(), "a histogram needs at least one bucket bound");
        assert!(
            bounds.windows(2).all(|pair| pair[0] < pair[1]),
            "histogram bucket bounds must be strictly ascending"
        );

        Self {
            bounds: bounds.to_vec(),
            counts: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum_bits: AtomicU64::new(0.0f64.to_bits()),
            count: AtomicU64::new(0),
        }
    }

    pub fn with_default_buckets() -> Self {
        Self::new(DEFAULT_LATENCY_BUCKETS)
    }

    pub fn observe(&self, value: f64) {
        let index = self.bounds.iter().position(|bound| value <= *bound).unwrap_or(self.bounds.len());
        self.counts[index].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.add_to_sum(value);
    }

    fn add_to_sum(&self, value: f64) {
        let mut current = self.sum_bits.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(current) + value).to_bits();
            match self.sum_bits.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    pub fn bounds(&self) -> &[f64] {
        &self.bounds
    }

    pub fn sum(&self) -> f64 {
        f64::from_bits(self.sum_bits.load(Ordering::Relaxed))
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Cumulative counts, one per bound in ascending order: entry `i` is
    /// how many observations were `<= bounds[i]`. The `+Inf` bucket is not
    /// included, since it always equals `count()`.
    pub fn cumulative_counts(&self) -> Vec<u64> {
        let mut running = 0;
        self.counts
            .iter()
            .take(self.bounds.len())
            .map(|slot| {
                running += slot.load(Ordering::Relaxed);
                running
            })
            .collect()
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::with_default_buckets()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_starts_at_zero_and_increments() {
        let counter = Counter::new();
        assert_eq!(counter.get(), 0);
        counter.inc();
        counter.inc_by(4);
        assert_eq!(counter.get(), 5);
    }

    #[test]
    fn gauge_sets_and_moves_in_both_directions() {
        let gauge = Gauge::new();
        gauge.set(10.5);
        assert_eq!(gauge.get(), 10.5);
        gauge.inc();
        gauge.dec();
        gauge.dec();
        assert_eq!(gauge.get(), 9.5);
    }

    #[test]
    fn gauge_round_trips_fractional_values() {
        let gauge = Gauge::new();
        gauge.set(0.1 + 0.2);
        assert_eq!(gauge.get(), 0.1 + 0.2);
    }

    #[test]
    fn histogram_places_observations_in_the_lowest_bucket_that_contains_them() {
        let hist = Histogram::new(&[1.0, 2.0, 3.0]);
        hist.observe(0.5);
        hist.observe(1.0); // boundary is inclusive
        hist.observe(2.5);
        hist.observe(99.0); // +Inf

        assert_eq!(hist.cumulative_counts(), vec![2, 2, 3]);
        assert_eq!(hist.count(), 4);
        assert_eq!(hist.sum(), 0.5 + 1.0 + 2.5 + 99.0);
    }

    #[test]
    fn empty_histogram_reports_zeroes() {
        let hist = Histogram::new(&[1.0, 2.0]);
        assert_eq!(hist.cumulative_counts(), vec![0, 0]);
        assert_eq!(hist.count(), 0);
        assert_eq!(hist.sum(), 0.0);
    }

    #[test]
    fn default_buckets_are_ascending() {
        assert!(DEFAULT_LATENCY_BUCKETS.windows(2).all(|pair| pair[0] < pair[1]));
        let hist = Histogram::with_default_buckets();
        assert_eq!(hist.bounds().len(), DEFAULT_LATENCY_BUCKETS.len());
    }

    #[test]
    #[should_panic(expected = "strictly ascending")]
    fn rejects_unsorted_bounds() {
        Histogram::new(&[1.0, 0.5]);
    }

    #[test]
    #[should_panic(expected = "at least one bucket bound")]
    fn rejects_empty_bounds() {
        Histogram::new(&[]);
    }

    #[test]
    fn concurrent_increments_are_not_lost() {
        let counter = std::sync::Arc::new(Counter::new());
        let gauge = std::sync::Arc::new(Gauge::new());

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let counter = std::sync::Arc::clone(&counter);
                let gauge = std::sync::Arc::clone(&gauge);
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        counter.inc();
                        gauge.inc();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.get(), 8000);
        assert_eq!(gauge.get(), 8000.0);
    }
}
