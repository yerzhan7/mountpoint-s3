//! Test utilities for metrics validation

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default, Clone)]
pub struct TestRecorder {
    metrics: Arc<Mutex<HashMap<Key, Arc<Metric>>>>,
}

impl TestRecorder {
    pub fn clear(&self) {
        let mut metrics = self.metrics.lock().unwrap();
        metrics.clear();
    }

    pub fn print_metrics(&self) {
        let metrics = self.metrics.lock().unwrap();
        for (key, metric) in metrics.iter() {
            match metric.as_ref() {
                Metric::Histogram(h) => {
                    let h = h.lock().unwrap();
                    println!("{key}: {h:?}");
                }
                Metric::Counter(c) => {
                    let c = c.lock().unwrap();
                    println!("{key}: {c}");
                }
                Metric::Gauge(g) => {
                    let g = g.lock().unwrap();
                    println!("{key}: {g}");
                }
            }
        }
    }

    pub fn get(&self, name: &str, labels: &[(&str, &str)]) -> Option<Arc<Metric>> {
        let metrics = self.metrics.lock().unwrap();
        metrics
            .iter()
            .find(|(key, _)| {
                if key.name() != name {
                    return false;
                }

                let actual_labels: Vec<_> = key.labels().map(|l| (l.key(), l.value())).collect();

                // Must have exact same number of labels
                if actual_labels.len() != labels.len() {
                    return false;
                }

                // Every expected label must be in the actual labels
                labels.iter().all(|(k, v)| actual_labels.contains(&(*k, *v)))
            })
            .map(|(_, metric)| Arc::clone(metric))
    }
}

#[derive(Debug)]
pub enum Metric {
    Histogram(Mutex<Vec<f64>>),
    Counter(Mutex<u64>),
    Gauge(Mutex<f64>),
}

impl Metric {
    pub fn gauge(&self) -> f64 {
        match self {
            Metric::Gauge(g) => *g.lock().unwrap(),
            _ => panic!("expected gauge"),
        }
    }

    pub fn counter(&self) -> u64 {
        match self {
            Metric::Counter(c) => *c.lock().unwrap(),
            _ => panic!("expected counter"),
        }
    }

    pub fn histogram(&self) -> Vec<f64> {
        match self {
            Metric::Histogram(h) => h.lock().unwrap().clone(),
            _ => panic!("expected histogram"),
        }
    }
}

impl Recorder for TestRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let mut metrics = self.metrics.lock().unwrap();
        let metric = metrics
            .entry(key.clone())
            .or_insert(Arc::new(Metric::Counter(Default::default())));
        Counter::from_arc(metric.clone())
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        let mut metrics = self.metrics.lock().unwrap();
        let metric = metrics
            .entry(key.clone())
            .or_insert(Arc::new(Metric::Gauge(Default::default())));
        Gauge::from_arc(metric.clone())
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        let mut metrics = self.metrics.lock().unwrap();
        let metric = metrics
            .entry(key.clone())
            .or_insert(Arc::new(Metric::Histogram(Default::default())));
        Histogram::from_arc(metric.clone())
    }
}

// Implement the metric traits
impl CounterFn for Metric {
    fn increment(&self, value: u64) {
        let Metric::Counter(counter) = self else {
            panic!("expected counter");
        };
        *counter.lock().unwrap() += value;
    }

    fn absolute(&self, value: u64) {
        let Metric::Counter(counter) = self else {
            panic!("expected counter");
        };
        *counter.lock().unwrap() = value;
    }
}

impl GaugeFn for Metric {
    fn increment(&self, value: f64) {
        let Metric::Gauge(gauge) = self else {
            panic!("expected gauge");
        };
        *gauge.lock().unwrap() += value;
    }

    fn decrement(&self, value: f64) {
        let Metric::Gauge(gauge) = self else {
            panic!("expected gauge");
        };
        *gauge.lock().unwrap() -= value;
    }

    fn set(&self, value: f64) {
        let Metric::Gauge(gauge) = self else {
            panic!("expected gauge");
        };

        *gauge.lock().unwrap() = value;
    }
}

impl HistogramFn for Metric {
    fn record(&self, value: f64) {
        let Metric::Histogram(histogram) = self else {
            panic!("expected histogram");
        };
        histogram.lock().unwrap().push(value);
    }
}

/// HDR-backed recorder used by stress tests. Histogram values are recorded as `u64`
/// microseconds-ish: every `record(v)` clamps `v` to `[0, hist.high()]` and casts to `u64`.
/// Counters and gauges behave identically to [`TestRecorder`].
#[cfg(feature = "stress_tests")]
pub mod stress {
    use super::*;
    use hdrhistogram::Histogram as HdrHistogram;

    /// Upper bound of the HDR histogram — 10 minutes expressed as µs. Records beyond this
    /// are clamped down so we never panic during recording.
    pub const HDR_HIGH: u64 = 600_000_000;

    fn new_hdr() -> HdrHistogram<u64> {
        HdrHistogram::<u64>::new_with_bounds(1, HDR_HIGH, 3).expect("HDR bounds valid")
    }

    #[derive(Debug, Default, Clone)]
    pub struct StressTestRecorder {
        metrics: Arc<Mutex<HashMap<Key, Arc<StressMetric>>>>,
    }

    impl StressTestRecorder {
        pub fn get(&self, name: &str, labels: &[(&str, &str)]) -> Option<Arc<StressMetric>> {
            let metrics = self.metrics.lock().unwrap();
            metrics
                .iter()
                .find(|(key, _)| {
                    if key.name() != name {
                        return false;
                    }
                    let actual: Vec<_> = key.labels().map(|l| (l.key(), l.value())).collect();
                    if actual.len() != labels.len() {
                        return false;
                    }
                    labels.iter().all(|(k, v)| actual.contains(&(*k, *v)))
                })
                .map(|(_, metric)| Arc::clone(metric))
        }

        /// Clone out the current state of every registered metric. Holds the registry lock
        /// only long enough to clone the per-metric `Arc`s; per-metric state is cloned
        /// without holding the registry lock.
        pub fn snapshot_all(&self) -> Vec<(Key, StressMetricSnapshot)> {
            let entries: Vec<(Key, Arc<StressMetric>)> = {
                let metrics = self.metrics.lock().unwrap();
                metrics.iter().map(|(k, m)| (k.clone(), Arc::clone(m))).collect()
            };
            entries.into_iter().map(|(k, m)| (k, m.snapshot())).collect()
        }
    }

    /// State behind a [`StressMetric::Gauge`]. Holds both the latest point-in-time value and
    /// a history of every value the gauge has taken on since installation. The history lets
    /// tests assert true peak/percentiles of the gauge without sampling races; the current
    /// value is still needed for teardown invariants ("did everything get released?").
    #[derive(Debug)]
    pub struct GaugeState {
        pub current: f64,
        pub history: HdrHistogram<u64>,
    }

    impl Default for GaugeState {
        fn default() -> Self {
            Self {
                current: 0.0,
                history: new_hdr(),
            }
        }
    }

    #[derive(Debug)]
    pub enum StressMetric {
        Histogram(Mutex<HdrHistogram<u64>>),
        Counter(Mutex<u64>),
        Gauge(Mutex<GaugeState>),
    }

    impl Default for StressMetric {
        fn default() -> Self {
            StressMetric::Histogram(Mutex::new(new_hdr()))
        }
    }

    #[derive(Debug, Clone)]
    pub enum StressMetricSnapshot {
        Histogram(HdrHistogram<u64>),
        Counter(u64),
        /// `(current_value, history_of_every_value_seen)`.
        Gauge { current: f64, history: HdrHistogram<u64> },
    }

    impl StressMetric {
        pub fn gauge(&self) -> f64 {
            match self {
                StressMetric::Gauge(g) => g.lock().unwrap().current,
                _ => panic!("expected gauge"),
            }
        }

        /// Clone of the full history of values this gauge has been set to since installation.
        /// Use `.max()` on the returned histogram to get the true peak without sampling races.
        pub fn gauge_history(&self) -> HdrHistogram<u64> {
            match self {
                StressMetric::Gauge(g) => g.lock().unwrap().history.clone(),
                _ => panic!("expected gauge"),
            }
        }

        pub fn counter(&self) -> u64 {
            match self {
                StressMetric::Counter(c) => *c.lock().unwrap(),
                _ => panic!("expected counter"),
            }
        }

        pub fn histogram_clone(&self) -> HdrHistogram<u64> {
            match self {
                StressMetric::Histogram(h) => h.lock().unwrap().clone(),
                _ => panic!("expected histogram"),
            }
        }

        fn snapshot(&self) -> StressMetricSnapshot {
            match self {
                StressMetric::Histogram(h) => StressMetricSnapshot::Histogram(h.lock().unwrap().clone()),
                StressMetric::Counter(c) => StressMetricSnapshot::Counter(*c.lock().unwrap()),
                StressMetric::Gauge(g) => {
                    let g = g.lock().unwrap();
                    StressMetricSnapshot::Gauge {
                        current: g.current,
                        history: g.history.clone(),
                    }
                }
            }
        }
    }

    impl Recorder for StressTestRecorder {
        fn describe_counter(&self, _k: KeyName, _u: Option<Unit>, _d: SharedString) {}
        fn describe_gauge(&self, _k: KeyName, _u: Option<Unit>, _d: SharedString) {}
        fn describe_histogram(&self, _k: KeyName, _u: Option<Unit>, _d: SharedString) {}

        fn register_counter(&self, key: &Key, _m: &Metadata<'_>) -> Counter {
            let mut metrics = self.metrics.lock().unwrap();
            let metric = metrics
                .entry(key.clone())
                .or_insert_with(|| Arc::new(StressMetric::Counter(Mutex::new(0))));
            Counter::from_arc(metric.clone())
        }

        fn register_gauge(&self, key: &Key, _m: &Metadata<'_>) -> Gauge {
            let mut metrics = self.metrics.lock().unwrap();
            let metric = metrics
                .entry(key.clone())
                .or_insert_with(|| Arc::new(StressMetric::Gauge(Mutex::new(GaugeState::default()))));
            Gauge::from_arc(metric.clone())
        }

        fn register_histogram(&self, key: &Key, _m: &Metadata<'_>) -> Histogram {
            let mut metrics = self.metrics.lock().unwrap();
            let metric = metrics
                .entry(key.clone())
                .or_insert_with(|| Arc::new(StressMetric::Histogram(Mutex::new(new_hdr()))));
            Histogram::from_arc(metric.clone())
        }
    }

    impl CounterFn for StressMetric {
        fn increment(&self, value: u64) {
            let StressMetric::Counter(c) = self else {
                panic!("expected counter");
            };
            *c.lock().unwrap() += value;
        }
        fn absolute(&self, value: u64) {
            let StressMetric::Counter(c) = self else {
                panic!("expected counter");
            };
            *c.lock().unwrap() = value;
        }
    }

    impl GaugeFn for StressMetric {
        fn increment(&self, value: f64) {
            let StressMetric::Gauge(g) = self else {
                panic!("expected gauge");
            };
            let mut g = g.lock().unwrap();
            g.current += value;
            let current = g.current;
            record_gauge_sample(&mut g.history, current);
        }
        fn decrement(&self, value: f64) {
            let StressMetric::Gauge(g) = self else {
                panic!("expected gauge");
            };
            let mut g = g.lock().unwrap();
            g.current -= value;
            let current = g.current;
            record_gauge_sample(&mut g.history, current);
        }
        fn set(&self, value: f64) {
            let StressMetric::Gauge(g) = self else {
                panic!("expected gauge");
            };
            let mut g = g.lock().unwrap();
            g.current = value;
            let current = g.current;
            record_gauge_sample(&mut g.history, current);
        }
    }

    /// Record a gauge's post-mutation value into its history histogram.
    ///
    /// The history is an HDR histogram of `u64`, so we clamp to `[0, hist.high()]` and cast.
    /// A gauge that goes negative is clamped to 0 in the history (uncommon for the metrics
    /// stress tests observe, but noted).
    fn record_gauge_sample(history: &mut HdrHistogram<u64>, value: f64) {
        let high = history.high() as f64;
        let clamped = if value.is_nan() || value < 0.0 {
            0
        } else if value > high {
            history.high()
        } else {
            value as u64
        };
        history.record(clamped).ok();
    }

    impl HistogramFn for StressMetric {
        fn record(&self, value: f64) {
            let StressMetric::Histogram(h) = self else {
                panic!("expected histogram");
            };
            let mut h = h.lock().unwrap();
            let high = h.high() as f64;
            let clamped = if value.is_nan() || value < 0.0 {
                0
            } else if value > high {
                h.high()
            } else {
                value as u64
            };
            // Cannot fail: `clamped` is within [0, high()].
            h.record(clamped).ok();
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use metrics::Recorder;

        fn meta() -> metrics::Metadata<'static> {
            metrics::Metadata::new("test", metrics::Level::INFO, None)
        }

        #[test]
        fn histogram_roundtrips_percentiles() {
            let rec = StressTestRecorder::default();
            let key = Key::from_name("h");
            let h = rec.register_histogram(&key, &meta());
            for v in 1..=1000 {
                h.record(v as f64);
            }
            let got = rec.get("h", &[]).unwrap();
            let hist = got.histogram_clone();
            assert_eq!(hist.len(), 1000);
            // p50 ~ 500, p99 ~ 990, p100 = 1000 (within HDR quantization).
            assert!(hist.value_at_quantile(0.50) >= 490 && hist.value_at_quantile(0.50) <= 510);
            assert!(hist.value_at_quantile(0.99) >= 980);
            assert_eq!(hist.max(), 1000);
        }

        #[test]
        fn histogram_clamps_out_of_range() {
            let rec = StressTestRecorder::default();
            let key = Key::from_name("h");
            let h = rec.register_histogram(&key, &meta());
            // None of these should panic; all should be recorded (clamped internally).
            h.record(-1.0);
            h.record(f64::NAN);
            h.record((HDR_HIGH as f64) * 2.0);
            let got = rec.get("h", &[]).unwrap();
            let hist = got.histogram_clone();
            assert_eq!(hist.len(), 3);
        }

        #[test]
        fn counter_and_gauge_behave() {
            let rec = StressTestRecorder::default();
            let c = rec.register_counter(&Key::from_name("c"), &meta());
            let g = rec.register_gauge(&Key::from_name("g"), &meta());
            c.increment(5);
            c.increment(3);
            g.set(42.5);
            assert_eq!(rec.get("c", &[]).unwrap().counter(), 8);
            assert_eq!(rec.get("g", &[]).unwrap().gauge(), 42.5);
        }

        #[test]
        fn gauge_history_tracks_peak_and_current() {
            let rec = StressTestRecorder::default();
            let g = rec.register_gauge(&Key::from_name("mem.bytes_reserved"), &meta());
            // Simulate reserve-spike-release: peak at 1000, then back to 0.
            g.increment(100.0);
            g.increment(900.0); // peak of 1000
            g.decrement(900.0);
            g.decrement(100.0); // now 0
            let got = rec.get("mem.bytes_reserved", &[]).unwrap();
            assert_eq!(got.gauge(), 0.0, "current should be zero after full release");
            let hist = got.gauge_history();
            assert_eq!(hist.len(), 4, "four mutations recorded");
            assert_eq!(hist.max(), 1000, "peak should reflect the spike, not the current value");
        }
    }
}
