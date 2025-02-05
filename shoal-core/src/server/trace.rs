//! Enables trace logging for shoal to some sink

use tracing::level_filters::LevelFilter;
use tracing_subscriber::filter::Filtered;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::Layer as LayerFmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, Registry};

use super::conf::{Conf, Tracing};

/// Setup local tracing to stdout
fn setup_local(conf: &Tracing) -> Filtered<LayerFmt<Registry>, LevelFilter, Registry> {
    //-> Filtered<tracing_subscriber::fmt::Layer<_>, LevelFilter, _> {
    tracing_subscriber::fmt::layer()
        //.with_span_events(FmtSpan::FULL)
        .with_filter(conf.level.to_filter())
}

//fn setup_grpc() {
//    // setup  exporter
//    let exporter = opentelemetry_otlp::new_exporter()
//        .tonic()
//        .with_endpoint(endpoint);
//    // setup tracer
//    opentelemetry_otlp::new_pipeline().tracing().with_exporter(exporter).with_trace_config(opentelemetry::sdk::)
//}

/// Setup basic tracing
pub fn setup(conf: &Conf) {
    // setup our local tracer
    let local = setup_local(&conf.tracing);
    // setup our registry
    tracing_subscriber::registry()
        .with(local)
        .try_init()
        .unwrap();
    //// Create a new OpenTelemetry trace pipeline to stdout
    //let provider = TracerProvider::builder()
    //    .with_simple_exporter(stdout::SpanExporter::default())
    //    .build();
    //// instance a tracer for shoal
    //let tracer = provider.tracer("Shoal");
    //// setup our telemetry layer
    //let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    //// setup a registry for our telemetry layers
    //let subscriber = Registry::default().with(telemetry);
}
