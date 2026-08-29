//! Shipping what a run measured to a collector, while the capture is still going
//!
//! A capture writes its artifact at the very end, and a full one is about two hours. Nothing could
//! be charted until it finished, which is what made a dashboard over these numbers impossible to
//! evaluate against the explorer ([F29](../../../../docs/src/features/benchmark-explorer.md)).
//! This ships one point per workload per run as each finishes, so a chart fills in over the two
//! hours instead of appearing at the end of them.
//!
//! # Why this is not on the query path
//!
//! The obvious thing is a histogram fed by [`crate::workloads::workload::Measurement::record`],
//! which would give the shape of latency *within* a run. It is deliberately not done, because that
//! call is on the measured path: recording into an instrument per query would make the traced
//! capture slower than the untraced one by an amount that is a property of this module rather than
//! of the database. Everything here reads the finished [`MacroCaptureV2`] instead, so it runs after
//! the server has stopped and cannot appear in a number.
//!
//! The within-run shape is not lost — it is what the spans are for. A sampled trace of the same run
//! carries per query timing with the causal structure a histogram would have thrown away.

use std::time::Duration;

use opentelemetry::metrics::{Counter, Gauge, Meter, MeterProvider as _};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use shoal::Conf;
use shoal::server::conf::OtlpMetrics;
use shoal::tracing::{event, Level};

use crate::model::macro_layer::MacroCaptureV2;

/// The name this process reports its metrics under
const SERVICE_NAME: &str = "shoal-workload";

/// How often to ship whatever has been recorded, when the config does not say
///
/// A workload run is tens of seconds at full scale, so this is short enough that a point does not
/// wait for the next workload to be recorded before it leaves.
const DEFAULT_INTERVAL_SECS: u64 = 10;

/// How long a single export may take before it is abandoned, when the config does not say
const DEFAULT_TIMEOUT_SECS: u64 = 10;

/// Ships one workload run's measurements to an OTLP metrics sink
///
/// Built once per process and dropped at the end of it. [`Meters::flush`] is what actually gets a
/// point off the box, because the periodic reader's interval is usually longer than the tail of a
/// run that is about to exit.
pub struct Meters {
    /// The provider to flush and shut down
    provider: SdkMeterProvider,
    /// The capture this process's run belongs to, carried on every point
    label: String,
    /// Rows moved per second, summed across every counter a workload kept
    rows_per_sec: Gauge<f64>,
    /// Queries answered per second, which is the figure a throughput comparison wants
    ops_per_sec: Gauge<f64>,
    /// How long the measured phase took, in seconds
    wall_clock: Gauge<f64>,
    /// One operation's latency at one percentile, in milliseconds
    latency: Gauge<f64>,
    /// How many rows a run moved, per counter
    rows: Counter<u64>,
    /// One per workload run that produced an artifact
    ///
    /// Counted against the length of [`crate::workload_ids::IDS`], this is how far through a
    /// capture a dashboard is looking at.
    completed: Counter<u64>,
}

impl Meters {
    /// Installs a metrics pipeline, if the configuration names a sink
    ///
    /// Returns `None` when nothing is configured, which is the default and is what a capture whose
    /// numbers are going to be committed should be taking.
    ///
    /// # Arguments
    ///
    /// * `conf` - The config to read the metrics sink out of
    /// * `label` - The capture this run belongs to, if it was given one
    pub fn install(conf: &Conf, label: Option<&str>) -> Option<Meters> {
        // nothing configured means nothing installed, rather than a pipeline pointed at localhost
        let sink = conf.tracing.metrics_sink()?;
        // build an exporter pointed at the configured endpoint
        let exporter = match Self::exporter(&sink) {
            Ok(exporter) => exporter,
            Err(error) => {
                // a sink we cannot build is not a reason to fail a run that would otherwise measure
                // something, so say so and carry on without one
                event!(
                    Level::ERROR,
                    msg = "Failed to build the metrics exporter, this run reports no metrics",
                    endpoint = sink.endpoint,
                    error = error.to_string(),
                );
                return None;
            }
        };
        // ship on a timer, so a long workload reports before it has finished
        let interval = Duration::from_secs(sink.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS));
        let reader = PeriodicReader::builder(exporter)
            .with_interval(interval)
            .build();
        // describe this process the way the trace half does, so the two join on one resource
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(Resource::builder().with_service_name(SERVICE_NAME).build())
            .build();
        let meter = provider.meter("shoal-bench");
        event!(
            Level::INFO,
            msg = "Sending run metrics to an OTLP/HTTP sink",
            endpoint = sink.endpoint,
        );
        Some(Meters {
            label: label.unwrap_or_default().to_string(),
            rows_per_sec: Self::gauge(&meter, "shoal_bench.rows_per_sec", "{row}/s"),
            ops_per_sec: Self::gauge(&meter, "shoal_bench.ops_per_sec", "{query}/s"),
            wall_clock: Self::gauge(&meter, "shoal_bench.wall_clock", "s"),
            latency: Self::gauge(&meter, "shoal_bench.latency", "ms"),
            rows: meter
                .u64_counter("shoal_bench.rows")
                .with_description("How many rows a workload run moved")
                .with_unit("{row}")
                .build(),
            completed: meter
                .u64_counter("shoal_bench.run.completed")
                .with_description("One per workload run that produced an artifact")
                .build(),
            provider,
        })
    }

    /// Builds the OTLP metrics exporter a sink describes
    ///
    /// # Arguments
    ///
    /// * `sink` - The metrics sink settings to export with
    fn exporter(sink: &OtlpMetrics) -> Result<opentelemetry_otlp::MetricExporter, anyhow::Error> {
        // how long a single export may take before it is abandoned
        let timeout = Duration::from_secs(sink.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
        // protobuf over HTTP, which is the same transport the trace half uses
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(&sink.endpoint)
            .with_headers(sink.headers.clone())
            .with_timeout(timeout)
            .build()?;
        Ok(exporter)
    }

    /// Builds one float gauge
    ///
    /// # Arguments
    ///
    /// * `meter` - The meter to build the instrument on
    /// * `name` - What to call the instrument
    /// * `unit` - The unit its values are in
    fn gauge(meter: &Meter, name: &'static str, unit: &'static str) -> Gauge<f64> {
        meter.f64_gauge(name).with_unit(unit).build()
    }

    /// Records everything one finished run measured
    ///
    /// Deliberately takes the artifact rather than the live measurement, so that nothing here can
    /// run while the thing being measured is running. See this module's header.
    ///
    /// # Arguments
    ///
    /// * `capture` - What the run produced, which holds exactly one workload
    pub fn record(&self, capture: &MacroCaptureV2) {
        // one process runs one workload, but iterate rather than assume it
        for (id, workload) in &capture.workloads {
            // the attributes every point of this workload carries, which is what a query slices on
            let mut attributes = vec![
                KeyValue::new("workload", id.clone()),
                KeyValue::new("label", self.label.clone()),
                KeyValue::new("seed", workload.seed.to_string()),
                KeyValue::new("timing", workload.timing.as_str().to_string()),
            ];
            // the server settings, when this workload needed a server at all
            if let Some(conf) = &workload.conf {
                attributes.push(KeyValue::new("shards", conf.shards.to_string()));
                attributes.push(KeyValue::new("durability", conf.durability.clone()));
            }
            // the throughput figures, both of them, because they answer different questions
            self.rows_per_sec.record(workload.rows_per_sec(), &attributes);
            if let Some(ops) = workload.ops_per_sec() {
                self.ops_per_sec.record(ops, &attributes);
            }
            // the wall clock of the measured phase, in the unit a dashboard reads
            let wall = workload.median_wall_clock_ns() as f64 / 1_000_000_000.0;
            self.wall_clock.record(wall, &attributes);
            // every operation's distribution, one point per percentile
            for (op, stats) in &workload.ops {
                for (percentile, parts) in [
                    ("p50", &stats.p50),
                    ("p90", &stats.p90),
                    ("p99", &stats.p99),
                    ("avg", &stats.avg),
                ] {
                    // the percentile is an attribute rather than four instruments, so one panel
                    // can draw the band
                    let mut latency = attributes.clone();
                    latency.push(KeyValue::new("op", op.clone()));
                    latency.push(KeyValue::new("percentile", percentile));
                    self.latency
                        .record(parts.as_nanos_f64() / 1_000_000.0, &latency);
                }
            }
            // what the run moved, per counter
            for (counter, moved) in &workload.counters {
                let mut counted = attributes.clone();
                counted.push(KeyValue::new("counter", counter.clone()));
                self.rows.add(*moved, &counted);
            }
            // and the progress signal, which is what says how far through a capture this is
            self.completed.add(1, &attributes);
        }
    }

    /// Ships whatever has been recorded, now
    ///
    /// The periodic reader's interval is usually longer than what is left of a process that has
    /// just written its artifact, so without this the last run of a capture reports nothing.
    pub fn flush(&self) {
        // a sink that will not take this is logged rather than fatal: the artifact is already
        // written, and losing a dashboard point is not losing a measurement
        if let Err(error) = self.provider.force_flush() {
            event!(
                Level::WARN,
                msg = "Failed to flush this run's metrics",
                error = error.to_string(),
            );
        }
    }
}

impl Drop for Meters {
    /// Shut the provider down, flushing once more on the way out
    fn drop(&mut self) {
        // idempotent with an explicit flush, the same way the trace guard's drop is
        if let Err(error) = self.provider.shutdown() {
            event!(
                Level::WARN,
                msg = "Failed to shut the metrics provider down",
                error = error.to_string(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use shoal::Conf;
    use shoal::server::conf::{OtlpMetrics, OtlpTracing};

    use super::Meters;

    #[test]
    /// A configuration naming no sink installs no pipeline
    ///
    /// This is the safety property of the whole module. A capture whose numbers are going to be
    /// committed names nothing, and must get nothing - not a pipeline pointed at a default
    /// endpoint that then spends the run failing to reach it.
    fn nothing_configured_installs_nothing() {
        // the default configuration, which is what a file with no tracing section resolves to
        assert!(Meters::install(&Conf::default(), None).is_none());
    }

    #[test]
    /// A trace sink alone is enough to install one
    ///
    /// The derivation is `Tracing::metrics_sink`'s and is tested there; this asserts that this
    /// module actually goes through it rather than requiring an explicit block.
    fn a_trace_sink_alone_installs_a_pipeline() {
        let mut conf = Conf::default();
        conf.tracing = conf
            .tracing
            .clone()
            .otlp(OtlpTracing::new("http://127.0.0.1:4318/v1/traces"));
        // built, not connected - the exporter is lazy and an unreachable collector is a warning
        // per export rather than a failure to install
        assert!(Meters::install(&conf, Some("cap")).is_some());
    }

    #[test]
    /// An explicit metrics sink installs one with no trace sink at all
    ///
    /// Metrics without spans is a real configuration: a dashboard over a capture's throughput does
    /// not need a trace backend behind it, and requiring one would make the cheap half of this
    /// depend on the expensive half.
    fn a_metrics_sink_alone_installs_a_pipeline() {
        let mut conf = Conf::default();
        conf.tracing = conf
            .tracing
            .clone()
            .metrics(OtlpMetrics::new("http://127.0.0.1:4318/v1/metrics"));
        assert!(conf.tracing.remote.is_none());
        assert!(Meters::install(&conf, None).is_some());
    }
}
