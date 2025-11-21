//! Enables trace logging for shoal to some sink

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{
    BatchConfigBuilder, BatchSpanProcessor, BatchSpanProcessorBuilder, SdkTracerProvider,
};
use opentelemetry_sdk::Resource;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::filter::Filtered;
use tracing_subscriber::fmt::Layer as LayerFmt;
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;

use crate::server::conf::RemoteTracing;

use super::conf::{Conf, Tracing};

/// Setup local tracing to stdout
fn setup_local(conf: &Tracing) -> Filtered<LayerFmt<Registry>, LevelFilter, Registry> {
    //-> Filtered<tracing_subscriber::fmt::Layer<_>, LevelFilter, _> {
    tracing_subscriber::fmt::layer()
        //.with_span_events(FmtSpan::FULL)
        .with_filter(conf.level.to_filter())
}

fn setup_remote(
    name: &str,
    endpoint: &str,
    registry: Layered<Filtered<LayerFmt<Registry>, LevelFilter, Registry>, Registry>,
) -> Option<SdkTracerProvider> {
    // setup an exporter
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .expect("Failed to setup tracing grpc exporter");
    let batch_config = BatchConfigBuilder::default()
        .with_max_queue_size(2048 * 100)
        .build();
    let processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(batch_config)
        .build();
    // setup our tracer provider
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(Resource::builder().with_service_name("Shoal").build())
        //.with_simple_exporter(exporter)
        .build();
    // build a tracer
    let tracer = provider.tracer(name.to_owned());
    // define our layer filter
    let filtered = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(LevelFilter::INFO);
    // init our tracing registry
    registry
        .with(filtered)
        .try_init()
        .expect("Failed to register opentelemetry tracers/subscribers");
    println!("Sending traces for {name} to gRPC trace sink at {endpoint}",);
    Some(provider)
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
pub fn setup(conf: &Conf) -> Option<SdkTracerProvider> {
    // setup our local tracer
    let local = setup_local(&conf.tracing);
    // setup our registry
    let registry = tracing_subscriber::registry().with(local);
    // if we have grpc tracing settings then set that up too
    match &conf.tracing.remote {
        Some(RemoteTracing::Grpc(endpoint)) => setup_remote("Shoal", endpoint, registry),
        None => {
            // setup our local tracer no matter what
            registry.try_init().unwrap();
            // local tracers do not need to return a provider
            None
        }
    }
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

/// Shutdown this tracer
///
/// # Arguments
///
/// * `provider` - The tracing provider to shutdown
pub fn shutdown(provider: Option<SdkTracerProvider>) {
    // if we have a provider shut it down
    if let Some(provider) = provider {
        // shutdown this provider
        provider
            .shutdown()
            .expect("Failed to shutdown tracing provider");
    }
}
