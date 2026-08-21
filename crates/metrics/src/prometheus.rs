use std::fmt::Write as _;

use crate::registry::{Family, Registry, Series};

/// The `Content-Type` a Prometheus scraper expects from an exposition
/// endpoint. Served alongside the body by `ns-api`'s `/metrics` handler.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Renders every family in `registry` in the Prometheus text exposition
/// format (version 0.0.4):
///
/// ```text
/// # HELP ns_notifications_sent_total Delivery attempts that reached a provider.
/// # TYPE ns_notifications_sent_total counter
/// ns_notifications_sent_total{channel="email",outcome="delivered"} 12
/// ```
///
/// Families come out in name order and each family's series in label
/// order, so scraping an idle process twice produces byte-identical
/// output.
pub fn encode(registry: &Registry) -> String {
    let mut out = String::new();
    for family in registry.families() {
        encode_family(&mut out, &family);
    }
    out
}

fn encode_family(out: &mut String, family: &Family) {
    let _ = writeln!(out, "# HELP {} {}", family.name, escape_help(&family.help));
    let _ = writeln!(out, "# TYPE {} {}", family.name, family.kind.as_str());

    for (labels, series) in &family.series {
        match series {
            Series::Counter(counter) => {
                let _ = writeln!(out, "{}{} {}", family.name, labels.render(), counter.get());
            }
            Series::Gauge(gauge) => {
                let _ = writeln!(out, "{}{} {}", family.name, labels.render(), format_float(gauge.get()));
            }
            Series::Histogram(histogram) => {
                let counts = histogram.cumulative_counts();
                for (bound, count) in histogram.bounds().iter().zip(counts) {
                    let with_le = labels.clone().with("le", format_float(*bound));
                    let _ = writeln!(out, "{}_bucket{} {}", family.name, with_le.render(), count);
                }
                // The +Inf bucket is every observation, by definition --
                // including the ones above the last explicit bound, which
                // is exactly what makes it non-redundant.
                let with_inf = labels.clone().with("le", "+Inf");
                let _ = writeln!(out, "{}_bucket{} {}", family.name, with_inf.render(), histogram.count());
                let _ =
                    writeln!(out, "{}_sum{} {}", family.name, labels.render(), format_float(histogram.sum()));
                let _ = writeln!(out, "{}_count{} {}", family.name, labels.render(), histogram.count());
            }
        }
    }
}

/// Help text is terminated by the newline, so an embedded one would turn
/// the remainder into an unparseable line. Backslashes are escaped so the
/// escaping itself round-trips. Quotes need no escaping here (unlike in a
/// label value) because help text isn't quoted.
fn escape_help(help: &str) -> String {
    help.replace('\\', "\\\\").replace('\n', "\\n")
}

/// Prometheus accepts an integer where a float is expected, so whole
/// values render without a trailing `.0` -- both because it's what every
/// client library emits and because it keeps a counter-like gauge readable
/// at a glance. The non-finite spellings are the ones the format
/// specifies; Rust's own `f64` Display produces `inf`/`NaN`, which a
/// scraper rejects.
pub fn format_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_positive() { "+Inf".to_string() } else { "-Inf".to_string() };
    }
    if value == value.trunc() && value.abs() < 1e15 {
        return format!("{}", value as i64);
    }
    format!("{value}")
}

/// Convenience for callers that only want one family's text (the CLI's
/// metrics view renders families individually).
pub fn encode_one(registry: &Registry, name: &str) -> Option<String> {
    registry.families().into_iter().find(|family| family.name == name).map(|family| {
        let mut out = String::new();
        encode_family(&mut out, &family);
        out
    })
}

/// Parses exposition text back into `(name+labels, value)` pairs. Only
/// used by tests and by the CLI's summary view, so it is deliberately
/// forgiving: comment lines are skipped and an unparseable value line is
/// dropped rather than failing the whole parse.
pub fn parse_samples(text: &str) -> Vec<(String, f64)> {
    text.lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .filter_map(|line| {
            let (key, value) = line.rsplit_once(' ')?;
            Some((key.to_string(), value.parse().ok()?))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::{label, Labels};

    #[test]
    fn renders_a_counter_family() {
        let registry = Registry::new();
        registry.counter("ns_sends_total", "Sends attempted.", label("channel", "email")).inc_by(3);

        assert_eq!(
            encode(&registry),
            "# HELP ns_sends_total Sends attempted.\n\
             # TYPE ns_sends_total counter\n\
             ns_sends_total{channel=\"email\"} 3\n"
        );
    }

    #[test]
    fn renders_an_unlabelled_gauge_without_empty_braces() {
        let registry = Registry::new();
        registry.gauge("ns_depth", "Queue depth.", Labels::new()).set(4.0);
        assert!(encode(&registry).contains("\nns_depth 4\n"), "{}", encode(&registry));
    }

    #[test]
    fn renders_a_histogram_with_cumulative_buckets_and_inf() {
        let registry = Registry::new();
        let hist = registry.histogram("ns_latency_seconds", "Send latency.", &[0.5, 1.0], Labels::new());
        hist.observe(0.25);
        hist.observe(0.75);
        hist.observe(30.0);

        assert_eq!(
            encode(&registry),
            "# HELP ns_latency_seconds Send latency.\n\
             # TYPE ns_latency_seconds histogram\n\
             ns_latency_seconds_bucket{le=\"0.5\"} 1\n\
             ns_latency_seconds_bucket{le=\"1\"} 2\n\
             ns_latency_seconds_bucket{le=\"+Inf\"} 3\n\
             ns_latency_seconds_sum 31\n\
             ns_latency_seconds_count 3\n"
        );
    }

    #[test]
    fn histogram_buckets_keep_the_series_labels() {
        let registry = Registry::new();
        registry
            .histogram("ns_latency_seconds", "Send latency.", &[1.0], label("channel", "sms"))
            .observe(0.5);

        let text = encode(&registry);
        assert!(text.contains("ns_latency_seconds_bucket{channel=\"sms\",le=\"1\"} 1"), "{text}");
        assert!(text.contains("ns_latency_seconds_count{channel=\"sms\"} 1"), "{text}");
    }

    #[test]
    fn output_is_stable_across_repeated_scrapes() {
        let registry = Registry::new();
        registry.counter("ns_b", "b", label("x", "2")).inc();
        registry.counter("ns_a", "a", label("x", "1")).inc();
        assert_eq!(encode(&registry), encode(&registry));
        assert!(encode(&registry).find("ns_a").unwrap() < encode(&registry).find("ns_b").unwrap());
    }

    #[test]
    fn help_text_newlines_are_escaped() {
        let registry = Registry::new();
        registry.counter("ns_thing", "first line\nsecond line", Labels::new());
        let text = encode(&registry);
        assert!(text.contains("# HELP ns_thing first line\\nsecond line\n"), "{text}");
        assert_eq!(text.lines().count(), 3, "the embedded newline must not split the HELP line");
    }

    #[test]
    fn floats_render_in_the_formats_prometheus_accepts() {
        assert_eq!(format_float(1.0), "1");
        assert_eq!(format_float(0.005), "0.005");
        assert_eq!(format_float(-2.5), "-2.5");
        assert_eq!(format_float(f64::INFINITY), "+Inf");
        assert_eq!(format_float(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_float(f64::NAN), "NaN");
    }

    #[test]
    fn encode_one_renders_just_that_family() {
        let registry = Registry::new();
        registry.counter("ns_a", "a", Labels::new()).inc();
        registry.counter("ns_b", "b", Labels::new()).inc();

        let text = encode_one(&registry, "ns_a").unwrap();
        assert!(text.contains("ns_a"));
        assert!(!text.contains("ns_b"));
        assert!(encode_one(&registry, "ns_missing").is_none());
    }

    #[test]
    fn parse_samples_round_trips_encoded_output() {
        let registry = Registry::new();
        registry.counter("ns_sends_total", "sends", label("channel", "push")).inc_by(9);
        registry.gauge("ns_depth", "depth", Labels::new()).set(1.5);

        let samples = parse_samples(&encode(&registry));
        assert_eq!(
            samples,
            vec![("ns_depth".to_string(), 1.5), ("ns_sends_total{channel=\"push\"}".to_string(), 9.0)]
        );
    }
}
