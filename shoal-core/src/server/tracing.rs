//! Enables trace logging for shoal to some sink

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_stdout as stdout;
use tracing::{error, span};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

/// Setup basic tracing
pub fn setup() {
    // Create a new OpenTelemetry trace pipeline to stdout
    let provider = TracerProvider::builder()
        .with_simple_exporter(stdout::SpanExporter::default())
        .build();
    // instance a tracer for shoal
    let tracer = provider.tracer("Shoal");
    // setup our telemetry layer
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    // setup a registry for our telemetry layers
    let subscriber = Registry::default().with(telemetry);
}
