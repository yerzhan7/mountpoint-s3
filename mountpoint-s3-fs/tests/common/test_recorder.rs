//! Test utilities for metrics validation

use dashmap::DashMap;
use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use std::sync::{Arc, Mutex};

/// Return `Some(Arc<M>)` for the unique entry in `map` whose name is `name` and whose label
/// set exactly matches `labels`. Shared by `TestRecorder` and `HdrRecorder`.
fn lookup<M>(map: &DashMap<Key, Arc<M>>, name: &str, labels: &[(&str, &str)]) -> Option<Arc<M>> {
    map.iter()
        .find(|entry| {
            let key = entry.key();
            if key.name() != name {
                return false;
            }
            let actual: Vec<_> = key.labels().map(|l| (l.key(), l.value())).collect();
            if actual.len() != labels.len() {
                return false;
            }
            labels.iter().all(|(k, v)| actual.contains(&(*k, *v)))
        })
        .map(|entry| Arc::clone(entry.value()))
}

#[derive(Debug, Default, Clone)]
pub struct TestRecorder {
    metrics: Arc<DashMap<Key, Arc<Metric>>>,
}

impl TestRecorder {
    pub fn clear(&self) {
        self.metrics.clear();
    }

    pub fn print_metrics(&self) {
        for entry in self.metrics.iter() {
            let (key, metric) = (entry.key(), entry.value());
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
        lookup(&self.metrics, name, labels)
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
        let metric = self
            .metrics
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Metric::Counter(Default::default())))
            .clone();
        Counter::from_arc(metric)
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        let metric = self
            .metrics
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Metric::Gauge(Default::default())))
            .clone();
        Gauge::from_arc(metric)
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        let metric = self
            .metrics
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Metric::Histogram(Default::default())))
            .clone();
        Histogram::from_arc(metric)
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

/// HDR-backed recorder used by stress tests. Histograms use `hdrhistogram` for bounded-memory
/// percentile storage; counters are lock-free `AtomicU64` (matching prod); gauges keep both a
/// current value and a full HDR history so assertions can read the true peak without sampling
/// races (no prod analogue — see `HdrMetric::Gauge`).
#[cfg(feature = "stress_tests")]
pub mod stress {
    use super::*;
    use hdrhistogram::Histogram as HdrHistogram;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Upper bound of the HDR histogram — 10 minutes expressed as µs. Records beyond this
    /// are clamped down so we never panic during recording.
    pub const HDR_HIGH: u64 = 600_000_000;

    fn new_hdr() -> HdrHistogram<u64> {
        HdrHistogram::<u64>::new_with_bounds(1, HDR_HIGH, 3).expect("HDR bounds valid")
    }

    /// Clamp an `f64` into `[0, hist.high()]` as `u64`. NaN and negatives become 0; values
    /// above the histogram's upper bound are clamped down so recording can never fail.
    fn clamp(value: f64, hist: &HdrHistogram<u64>) -> u64 {
        let high = hist.high();
        if value.is_nan() || value < 0.0 {
            0
        } else if value > high as f64 {
            high
        } else {
            value as u64
        }
    }

    #[derive(Debug, Default, Clone)]
    pub struct HdrRecorder {
        metrics: Arc<DashMap<Key, Arc<HdrMetric>>>,
    }

    impl HdrRecorder {
        pub fn get(&self, name: &str, labels: &[(&str, &str)]) -> Option<Arc<HdrMetric>> {
            lookup(&self.metrics, name, labels)
        }

        /// Call `f` for every registered metric. Holds a read-side `DashMap` reference per
        /// entry — callers must not re-enter the recorder from `f`.
        pub fn for_each(&self, mut f: impl FnMut(&Key, &HdrMetric)) {
            for entry in self.metrics.iter() {
                f(entry.key(), entry.value());
            }
        }
    }

    /// State behind a [`HdrMetric::Gauge`]. Holds both the latest point-in-time value and
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
    pub enum HdrMetric {
        Histogram(Mutex<HdrHistogram<u64>>),
        Counter(AtomicU64),
        Gauge(Mutex<GaugeState>),
    }

    impl HdrMetric {
        pub fn gauge(&self) -> f64 {
            match self {
                HdrMetric::Gauge(g) => g.lock().unwrap().current,
                _ => panic!("expected gauge"),
            }
        }

        /// Clone of the full history of values this gauge has been set to since installation.
        /// Use `.max()` on the returned histogram to get the true peak without sampling races.
        pub fn gauge_history(&self) -> HdrHistogram<u64> {
            match self {
                HdrMetric::Gauge(g) => g.lock().unwrap().history.clone(),
                _ => panic!("expected gauge"),
            }
        }

        pub fn counter(&self) -> u64 {
            match self {
                HdrMetric::Counter(c) => c.load(Ordering::Relaxed),
                _ => panic!("expected counter"),
            }
        }

        pub fn histogram_clone(&self) -> HdrHistogram<u64> {
            match self {
                HdrMetric::Histogram(h) => h.lock().unwrap().clone(),
                _ => panic!("expected histogram"),
            }
        }
    }

    impl Recorder for HdrRecorder {
        fn describe_counter(&self, _k: KeyName, _u: Option<Unit>, _d: SharedString) {}
        fn describe_gauge(&self, _k: KeyName, _u: Option<Unit>, _d: SharedString) {}
        fn describe_histogram(&self, _k: KeyName, _u: Option<Unit>, _d: SharedString) {}

        fn register_counter(&self, key: &Key, _m: &Metadata<'_>) -> Counter {
            let metric = self
                .metrics
                .entry(key.clone())
                .or_insert_with(|| Arc::new(HdrMetric::Counter(AtomicU64::new(0))))
                .clone();
            Counter::from_arc(metric)
        }

        fn register_gauge(&self, key: &Key, _m: &Metadata<'_>) -> Gauge {
            let metric = self
                .metrics
                .entry(key.clone())
                .or_insert_with(|| Arc::new(HdrMetric::Gauge(Mutex::new(GaugeState::default()))))
                .clone();
            Gauge::from_arc(metric)
        }

        fn register_histogram(&self, key: &Key, _m: &Metadata<'_>) -> Histogram {
            let metric = self
                .metrics
                .entry(key.clone())
                .or_insert_with(|| Arc::new(HdrMetric::Histogram(Mutex::new(new_hdr()))))
                .clone();
            Histogram::from_arc(metric)
        }
    }

    impl CounterFn for HdrMetric {
        fn increment(&self, value: u64) {
            let HdrMetric::Counter(c) = self else {
                panic!("expected counter");
            };
            c.fetch_add(value, Ordering::Relaxed);
        }
        fn absolute(&self, value: u64) {
            let HdrMetric::Counter(c) = self else {
                panic!("expected counter");
            };
            c.store(value, Ordering::Relaxed);
        }
    }

    impl GaugeFn for HdrMetric {
        fn increment(&self, value: f64) {
            let HdrMetric::Gauge(g) = self else {
                panic!("expected gauge");
            };
            let mut g = g.lock().unwrap();
            g.current += value;
            let current = g.current;
            record_gauge_sample(&mut g.history, current);
        }
        fn decrement(&self, value: f64) {
            let HdrMetric::Gauge(g) = self else {
                panic!("expected gauge");
            };
            let mut g = g.lock().unwrap();
            g.current -= value;
            let current = g.current;
            record_gauge_sample(&mut g.history, current);
        }
        fn set(&self, value: f64) {
            let HdrMetric::Gauge(g) = self else {
                panic!("expected gauge");
            };
            let mut g = g.lock().unwrap();
            g.current = value;
            let current = g.current;
            record_gauge_sample(&mut g.history, current);
        }
    }

    /// Record a gauge's post-mutation value into its history histogram.
    fn record_gauge_sample(history: &mut HdrHistogram<u64>, value: f64) {
        let clamped = clamp(value, history);
        history.record(clamped).ok();
    }

    impl HistogramFn for HdrMetric {
        fn record(&self, value: f64) {
            let HdrMetric::Histogram(h) = self else {
                panic!("expected histogram");
            };
            let mut h = h.lock().unwrap();
            let clamped = clamp(value, &h);
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
            let rec = HdrRecorder::default();
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
            let rec = HdrRecorder::default();
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
            let rec = HdrRecorder::default();
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
            let rec = HdrRecorder::default();
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
