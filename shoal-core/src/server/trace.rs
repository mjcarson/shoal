//! Enables trace logging for shoal to some sink

use std::time::Duration;

use opentelemetry::trace::{
    SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::trace::{
    BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider,
};
use opentelemetry_sdk::Resource;
use tracing::{event, Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{EnvFilter, Filtered};
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;

use super::conf::{Conf, OtlpTracing, Tracing};
use crate::shared::protocol::trace::TraceContext;

/// The name this service reports itself as to a collector
const SERVICE_NAME: &str = "Shoal";

/// How often the batch processor ships whatever it has, when the config does not say
///
/// The SDK's own default is five seconds, which is longer than a short run lives, so everything
/// a one second example emits would only ever leave on shutdown. A second is short enough that a
/// run killed part way through has still shipped most of what it produced.
const DEFAULT_BATCH_DELAY_MS: u64 = 1_000;

/// How many spans may be queued before new ones are dropped, when the config does not say
///
/// This used to be 2048 * 100. A queue that large drops spans under load instead of applying
/// backpressure, and holds a quarter of a million `SpanData` while it does.
const DEFAULT_MAX_QUEUE_SIZE: usize = 8_192;

/// How long a single export may take before it is abandoned, when the config does not say
const DEFAULT_TIMEOUT_SECS: u64 = 10;

/// The type of the console layer, which is filtered per target rather than by level alone
///
/// Boxed because the writer is a runtime choice: a process whose stdout is an artifact cannot put
/// log lines on it, and the two writers are different types.
type LocalLayer = Filtered<Box<dyn Layer<Registry> + Send + Sync>, EnvFilter, Registry>;

/// The registry the remote layer is stacked on top of
type LocalRegistry = Layered<LocalLayer, Registry>;

/// The filter directives every layer in this subscriber is built from
///
/// The configured level is the default and `RUST_LOG` replaces it entirely, per target. That
/// override is what makes a trace export problem diagnosable: the OTLP exporter reports every step
/// of a POST at `DEBUG` on its own targets, and without per target control the only way to see
/// those lines is to turn the whole workspace up to `DEBUG` and read the flood.
///
/// **Both layers are built from this one string, and that is not tidiness.** They used to be
/// filtered from different sources - the console from `RUST_LOG`, the OTLP layer from
/// `conf.level` - and a per layer filter decides what a layer can *see*, not only what it emits.
/// A span admitted to the console and hidden from the OTLP layer is a span
/// `tracing-opentelemetry` cannot find when a child names it as a parent, and a parent it cannot
/// find becomes an empty context - which is a **new trace id**. So setting `RUST_LOG` to
/// investigate a fragmented trace was one of the things that could fragment it
/// ([Resolved #90](../../../docs/src/appendix/resolved/divergent-layer-filters.md)).
///
/// `EnvFilter` is not `Clone`, so this returns the directives rather than the filter. Two filters
/// built from one string cannot disagree; two built from two sources can.
///
/// # Arguments
///
/// * `conf` - The tracing settings to read a level out of
fn filter_directives(conf: &Tracing) -> String {
    // the environment is read here and nowhere else, so the decision itself stays testable
    directives_from(std::env::var("RUST_LOG").ok(), conf)
}

/// Decide the filter directives from an override and a configured level
///
/// Split out from [`filter_directives`] so it can be tested without mutating the environment of
/// every other test in this binary.
///
/// # Arguments
///
/// * `env` - What `RUST_LOG` was set to, if it was set at all
/// * `conf` - The tracing settings to fall back to
fn directives_from(env: Option<String>, conf: &Tracing) -> String {
    // RUST_LOG replaces the configured level entirely when it is set to anything
    match env {
        Some(directives) if !directives.trim().is_empty() => directives,
        _ => conf.level.to_filter().to_string(),
    }
}

/// Setup local tracing to the console
///
/// # Arguments
///
/// * `directives` - The filter directives every layer of this subscriber shares
/// * `stderr` - Whether to write to stderr rather than stdout
fn setup_local(directives: &str, stderr: bool) -> LocalLayer {
    // the same directives the remote layer is built from, so the two can never disagree
    let filter = EnvFilter::new(directives);
    // pick the stream this process can afford to write log lines to
    let layer: Box<dyn Layer<Registry> + Send + Sync> = if stderr {
        Box::new(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
    } else {
        Box::new(tracing_subscriber::fmt::layer())
    };
    layer.with_filter(filter)
}

/// Describes this process to the collector
///
/// One sink serves many runs, so these attributes are what let a query ask about one of them.
/// Without them every span of a capture arrives under one identical resource, and a chart of them
/// can only say that Shoal was busy.
///
/// # Arguments
///
/// * `options` - How this process wants to report itself
fn resource(options: &TraceOptions) -> Resource {
    // the service name plus whatever the caller wants every span to carry
    Resource::builder()
        .with_service_name(options.service_name.clone())
        .with_attributes(
            options
                .attributes
                .iter()
                .map(|(key, value)| KeyValue::new(key.clone(), value.clone())),
        )
        .build()
}

/// The sampler a sink's settings ask for
///
/// A configured ratio exports that fraction of traces, since a queue that overflows drops spans
/// rather than choosing which ones to keep. No ratio means the SDK's own default, so a config
/// written before this existed behaves exactly as it did.
///
/// # Arguments
///
/// * `otlp` - The OTLP sink settings to read a sample ratio out of
fn sampler(otlp: &OtlpTracing) -> Sampler {
    match otlp.sample_ratio {
        // export the asked for fraction, decided on the trace id so a trace is kept whole
        Some(ratio) => Sampler::TraceIdRatioBased(ratio),
        // every trace whose parent was sampled, which is what this always did
        None => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
    }
}

/// Setup remote tracing to an OTLP over HTTP sink
///
/// This never speaks gRPC, whatever the config variant that reaches it is called. Spans are
/// serialized as protobuf and POSTed to `endpoint`, which is expected to carry the `/v1/traces`
/// path already because the exporter uses a builder supplied endpoint verbatim.
///
/// # Arguments
///
/// * `name` - The name to give the tracer this builds
/// * `directives` - The filter directives every layer of this subscriber shares
/// * `otlp` - The OTLP sink settings to export with
/// * `options` - The service name and resource attributes to report this process as
/// * `registry` - The console registry to stack the remote layer on top of
fn setup_remote(
    name: &str,
    directives: &str,
    otlp: &OtlpTracing,
    options: &TraceOptions,
    registry: LocalRegistry,
) -> Option<SdkTracerProvider> {
    // how long a single export may take before it is abandoned
    let timeout = Duration::from_secs(otlp.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
    // setup an exporter pointed at the configured endpoint
    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(&otlp.endpoint)
        .with_headers(otlp.headers.clone())
        .with_timeout(timeout)
        .build()
    {
        Ok(exporter) => exporter,
        Err(error) => {
            // a sink we cannot build is not a reason to refuse to start, so fall back to stdout
            registry.try_init().ok();
            event!(
                Level::ERROR,
                msg = "Failed to build the remote trace exporter, tracing to stdout only",
                endpoint = otlp.endpoint,
                error = error.to_string(),
            );
            return None;
        }
    };
    // batch spans rather than exporting them one at a time
    let batch_config = BatchConfigBuilder::default()
        .with_max_queue_size(otlp.max_queue_size.unwrap_or(DEFAULT_MAX_QUEUE_SIZE))
        .with_scheduled_delay(Duration::from_millis(
            otlp.batch_delay_ms.unwrap_or(DEFAULT_BATCH_DELAY_MS),
        ))
        .build();
    let processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(batch_config)
        .build();
    // setup our tracer provider
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_sampler(sampler(otlp))
        .with_resource(resource(options))
        .build();
    // build a tracer
    let tracer = provider.tracer(name.to_owned());
    // filter the remote layer with the same directives the console layer uses
    //
    // this must stay the same source as `setup_local`'s: a span one layer can see and the other
    // cannot re-roots every span that names it as a parent - see `filter_directives`
    let filtered = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(EnvFilter::new(directives));
    // init our tracing registry
    if let Err(error) = registry.with(filtered).try_init() {
        // something already installed a subscriber, so ours emits nothing and neither does the
        // exporter behind it. that is worth saying, and is not worth dying over
        event!(
            Level::WARN,
            msg = "A tracing subscriber was already installed, remote tracing is not active",
            error = error.to_string(),
        );
        return None;
    }
    // now that a subscriber exists this can be an event rather than a println
    event!(
        Level::INFO,
        msg = "Sending traces to an OTLP/HTTP sink",
        name = name,
        endpoint = otlp.endpoint,
    );
    Some(provider)
}

/// Holds the tracer provider for as long as spans are being emitted
///
/// `SdkTracerProvider` has no `Drop` of its own, so a caller that returns early between [`setup`]
/// and [`shutdown`] discards everything the batch processor has queued without saying so. Holding
/// the provider in a guard makes the final flush unconditional.
pub struct TraceGuard {
    /// The provider to flush and shut down, taken by an explicit [`shutdown`]
    provider: Option<SdkTracerProvider>,
}

impl TraceGuard {
    /// Creates a guard around a tracer provider
    ///
    /// # Arguments
    ///
    /// * `provider` - The provider to flush when this guard is dropped
    fn new(provider: Option<SdkTracerProvider>) -> Self {
        TraceGuard { provider }
    }

    /// Flush everything queued and shut this provider down
    ///
    /// This logs rather than panics on either step. A trace sink that has gone away must not
    /// take the database with it.
    fn flush(&mut self) {
        // if we never had a provider then there is nothing queued anywhere
        let Some(provider) = self.provider.take() else {
            return;
        };
        // ship whatever is queued before asking the processor to stop
        if let Err(error) = provider.force_flush() {
            event!(
                Level::ERROR,
                msg = "Failed to flush queued spans to the remote trace sink",
                error = error.to_string(),
            );
        }
        // then stop the processor, which flushes once more and joins its thread
        if let Err(error) = provider.shutdown() {
            event!(
                Level::ERROR,
                msg = "Failed to shutdown the tracing provider",
                error = error.to_string(),
            );
        }
    }
}

impl Drop for TraceGuard {
    /// Flush any queued spans when this guard goes out of scope
    fn drop(&mut self) {
        // flush is idempotent, so an explicit shutdown followed by this drop is not a double flush
        self.flush();
    }
}

/// How a caller wants its subscriber built
///
/// [`setup`] uses [`TraceOptions::default`], which is what every caller wanted before this existed.
/// A process that is not a server wants the other two fields.
#[derive(Clone, Debug)]
pub struct TraceOptions {
    /// The name this process reports itself as to a collector
    pub service_name: String,
    /// Extra resource attributes every span this process emits carries
    ///
    /// One collector serves many runs, so these are what let a query ask about one of them. A
    /// benchmark names the workload, the capture label and the scale here.
    pub attributes: Vec<(String, String)>,
    /// Whether the console layer writes to stderr rather than stdout
    ///
    /// **A process whose stdout is an artifact must set this.** `shoal-bench` harvests the hotpath
    /// profile from the last line a workload writes to stdout, so a log line on that stream
    /// silently corrupts the profile it collects.
    pub stderr: bool,
}

impl Default for TraceOptions {
    /// The options every caller had before this type existed
    fn default() -> Self {
        TraceOptions {
            service_name: SERVICE_NAME.to_string(),
            attributes: Vec::new(),
            stderr: false,
        }
    }
}

impl TraceOptions {
    /// Creates options reporting under a service name
    ///
    /// # Arguments
    ///
    /// * `service_name` - The name to report this process as
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_core::server::trace::TraceOptions;
    ///
    /// TraceOptions::new("shoal-workload")
    ///     .attribute("shoal.workload", "macro/insert_unsorted")
    ///     .stderr(true);
    /// ```
    pub fn new<N: Into<String>>(service_name: N) -> Self {
        TraceOptions {
            service_name: service_name.into(),
            attributes: Vec::new(),
            stderr: false,
        }
    }

    /// Add a resource attribute every span this process emits carries
    ///
    /// # Arguments
    ///
    /// * `key` - The attribute name to set
    /// * `value` - The value to set it to
    pub fn attribute<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        // append rather than replace, since attributes are a set rather than a single setting
        self.attributes.push((key.into(), value.into()));
        self
    }

    /// Set whether the console layer writes to stderr rather than stdout
    ///
    /// # Arguments
    ///
    /// * `stderr` - Whether to write to stderr
    pub fn stderr(mut self, stderr: bool) -> Self {
        self.stderr = stderr;
        self
    }
}

/// Setup basic tracing
///
/// A console layer is always installed. If `tracing.remote` names a sink then an OTLP layer is
/// installed on top of it, and the returned guard is what ships whatever that layer queued.
///
/// **Hold the returned guard for as long as spans are being emitted.** Dropping it flushes, and
/// letting it drop early stops the export.
///
/// # Arguments
///
/// * `conf` - The config to read tracing settings out of
pub fn setup(conf: &Conf) -> TraceGuard {
    // the defaults are what every caller of this used before the options form existed
    setup_with(conf, &TraceOptions::default())
}

/// Setup tracing, reporting this process the way a caller asks
///
/// The same pipeline [`setup`] installs, with the service name, resource attributes and console
/// stream chosen rather than assumed. Everything [`setup`] documents about the returned guard
/// applies here unchanged.
///
/// # Arguments
///
/// * `conf` - The config to read tracing settings out of
/// * `options` - How this process wants to report itself
pub fn setup_with(conf: &Conf, options: &TraceOptions) -> TraceGuard {
    // work out what every layer of this subscriber filters on, once
    let directives = filter_directives(&conf.tracing);
    // setup our local tracer, on whichever stream this process can afford to write to
    let local = setup_local(&directives, options.stderr);
    // setup our registry
    let registry = tracing_subscriber::registry().with(local);
    // if we have a remote sink configured then set that up too
    match &conf.tracing.remote {
        Some(remote) => TraceGuard::new(setup_remote(
            &options.service_name,
            &directives,
            &remote.otlp(),
            options,
            registry,
        )),
        None => {
            // setup our local tracer no matter what
            if let Err(error) = registry.try_init() {
                // there is no subscriber to report this through, so stderr is all there is
                eprintln!("Failed to install a tracing subscriber: {error}");
            }
            // local tracers do not need a provider
            TraceGuard::new(None)
        }
    }
}

/// Shutdown this tracer
///
/// Dropping the guard does the same thing. This exists for callers that want the flush to happen
/// at a point they name rather than at the end of a scope.
///
/// # Arguments
///
/// * `guard` - The tracing guard to shutdown
pub fn shutdown(mut guard: TraceGuard) {
    // flush and stop the provider this guard is holding
    guard.flush();
}

/// Make a span the child of a trace a peer opened in another process
///
/// This is the whole of the server's half of [F35](../../../docs/src/features/wire-trace-context.md).
/// The span stays an explicit root as far as `tracing` is concerned - the parent set here is an
/// **OpenTelemetry** parent, resolved by the layer rather than by the registry, and the two are
/// independent. That is why `client_rx_relay` keeps its `parent: None` and calls this afterwards.
///
/// The context is marked remote, which is what makes a parent based sampler take the sender's
/// decision instead of making its own. A trace half of which was sampled is worse than either
/// answer, so the peer that started the trace decides for all of it.
///
/// This does nothing at all when no subscriber with an OpenTelemetry layer is installed, because
/// there is nothing to set a parent on. That is the ordinary case for a server whose config names
/// no remote sink, and it is not worth reporting.
///
/// # Arguments
///
/// * `span` - The span to reparent onto the peer's trace
/// * `wire` - The trace context the peer sent
pub fn adopt_remote_parent(span: &Span, wire: &TraceContext) {
    // rebuild the peer's span context out of the bytes it sent
    //
    // `TraceContext` refuses to hold an id that names no parent, so neither of these can be the
    // invalid id that would make this a new trace rather than a join
    let context = SpanContext::new(
        TraceId::from_bytes(wire.trace_id()),
        SpanId::from_bytes(wire.span_id()),
        TraceFlags::new(wire.flags()),
        true,
        TraceState::default(),
    );
    // and hang this span off it
    span.set_parent(Context::current().with_remote_span_context(context));
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::future::BoxFuture;
    use opentelemetry::trace::{Tracer, TracerProvider as _};
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::trace::{
        BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider, SpanData, SpanExporter,
    };
    use opentelemetry_sdk::Resource;

    use super::{OtlpTracing, TraceGuard, TraceOptions, Tracing};
    use crate::server::conf::TraceLevel;
    use crate::shared::protocol::trace::TraceContext;

    /// An exporter that keeps the names it was handed instead of sending them anywhere
    ///
    /// The SDK ships one of these behind a `testing` feature. Implementing it here instead keeps
    /// the feature, and the lockfile churn that enabling it causes, out of the tree.
    #[derive(Debug, Default, Clone)]
    struct RecordingExporter {
        /// The names of every span this exporter has been handed
        exported: Arc<Mutex<Vec<String>>>,
        /// The resource the provider told this exporter it was exporting for
        resource: Arc<Mutex<Option<Resource>>>,
    }

    impl RecordingExporter {
        /// The names of every span exported so far
        fn names(&self) -> Vec<String> {
            self.exported
                .lock()
                .expect("the recording exporter was poisoned")
                .clone()
        }

        /// The value a resource attribute was set to, if it carries one
        ///
        /// # Arguments
        ///
        /// * `key` - The attribute to look up
        fn attribute(&self, key: &str) -> Option<String> {
            // the provider hands the resource over once, before anything is exported
            let resource = self
                .resource
                .lock()
                .expect("the recording exporter was poisoned");
            resource
                .as_ref()?
                .get(&opentelemetry::Key::from(key.to_owned()))
                .map(|value| value.to_string())
        }
    }

    impl SpanExporter for RecordingExporter {
        /// Record the names in this batch
        ///
        /// # Arguments
        ///
        /// * `batch` - The spans being exported
        fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, OTelSdkResult> {
            // keep the name of everything in this batch
            let exported = Arc::clone(&self.exported);
            let names = batch.into_iter().map(|span| span.name.to_string());
            exported
                .lock()
                .expect("the recording exporter was poisoned")
                .extend(names);
            Box::pin(std::future::ready(Ok(())))
        }

        /// Keep the resource the provider is exporting for
        ///
        /// # Arguments
        ///
        /// * `resource` - What the provider describes this process as
        fn set_resource(&mut self, resource: &Resource) {
            // the provider calls this once at build time, so the last value is the only value
            *self
                .resource
                .lock()
                .expect("the recording exporter was poisoned") = Some(resource.clone());
        }
    }

    /// Build a provider that only exports when it is told to
    ///
    /// The scheduled delay is longer than the test, so nothing leaves on the timer. Whatever the
    /// exporter ends up holding got there because something flushed it.
    fn provider() -> (RecordingExporter, SdkTracerProvider) {
        // an exporter that keeps what it is given
        let exporter = RecordingExporter::default();
        // batch with a delay no test will reach
        let batch_config = BatchConfigBuilder::default()
            .with_scheduled_delay(Duration::from_secs(3600))
            .build();
        let processor = BatchSpanProcessor::builder(exporter.clone())
            .with_batch_config(batch_config)
            .build();
        // and a provider over it
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .build();
        (exporter, provider)
    }

    #[test]
    /// Dropping the guard ships whatever the batch processor still has queued
    ///
    /// `SdkTracerProvider` has no `Drop` of its own, so before the guard existed a caller that
    /// returned early between `setup` and `shutdown` discarded every queued span in silence. That
    /// is the whole reason this type exists.
    fn dropping_the_guard_flushes_queued_spans() {
        let (exporter, provider) = provider();
        // emit a span, which lands in the queue and goes no further on its own
        let tracer = provider.tracer("test");
        tracer.in_span("queued", |_| {});
        // nothing has been exported yet, because the timer is an hour out
        assert!(exporter.names().is_empty(), "a span left before any flush");
        // dropping the guard is the only thing that ships it
        drop(TraceGuard::new(Some(provider)));
        assert_eq!(exporter.names(), vec!["queued".to_owned()]);
    }

    #[test]
    /// An explicit shutdown flushes, and the drop that follows it does not export again
    ///
    /// Both paths run for every caller that names its shutdown point, so the second one has to be
    /// a no-op rather than a second flush.
    fn an_explicit_shutdown_flushes_once() {
        let (exporter, provider) = provider();
        let tracer = provider.tracer("test");
        tracer.in_span("queued", |_| {});
        // shut down explicitly, which consumes the guard and drops it inside
        super::shutdown(TraceGuard::new(Some(provider)));
        assert_eq!(exporter.names(), vec!["queued".to_owned()]);
    }

    #[test]
    /// A guard holding no provider is harmless
    ///
    /// This is what a stdout only configuration hands back, and it is dropped on every run that
    /// names no remote sink.
    fn a_guard_without_a_provider_does_nothing() {
        // neither path may panic
        let mut guard = TraceGuard::new(None);
        guard.flush();
        drop(guard);
    }

    #[test]
    /// The service name and every attribute a caller asks for reach the exporter
    ///
    /// This is what makes one collector usable by more than one kind of run. Without it a
    /// benchmark's spans and a server's spans arrive under the same resource and neither can be
    /// asked about on its own.
    fn resource_attributes_reach_the_exporter() {
        // the options a benchmark workload builds
        let options = TraceOptions::new("shoal-workload")
            .attribute("shoal.workload", "macro/insert_unsorted")
            .attribute("shoal.label", "trace-check");
        // build a provider over a recording exporter, describing itself the way setup_remote does
        let exporter = RecordingExporter::default();
        let batch_config = BatchConfigBuilder::default()
            .with_scheduled_delay(Duration::from_secs(3600))
            .build();
        let processor = BatchSpanProcessor::builder(exporter.clone())
            .with_batch_config(batch_config)
            .build();
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .with_resource(super::resource(&options))
            .build();
        // emit something so there is a batch, then flush it
        provider.tracer("test").in_span("attributed", |_| {});
        super::shutdown(TraceGuard::new(Some(provider)));
        // the span left, and it left describing which run produced it
        assert_eq!(exporter.names(), vec!["attributed".to_owned()]);
        assert_eq!(
            exporter.attribute("service.name").as_deref(),
            Some("shoal-workload")
        );
        assert_eq!(
            exporter.attribute("shoal.workload").as_deref(),
            Some("macro/insert_unsorted")
        );
        assert_eq!(
            exporter.attribute("shoal.label").as_deref(),
            Some("trace-check")
        );
    }

    #[test]
    /// Both layers are built from one set of directives, whichever source decides them
    ///
    /// This is the whole of item 90. A per layer filter decides what a layer can *see*, not only
    /// what it emits, so a span the console layer is shown and the OTLP layer is not is a span
    /// `tracing-opentelemetry` cannot find when a child names it as a parent - and a parent it
    /// cannot find becomes an empty context, which is a new trace id. The two used to read
    /// different sources, so `RUST_LOG` alone could fragment every exported trace.
    fn one_source_decides_what_every_layer_filters_on() {
        // the level a config names is the default
        let conf = Tracing::default().level(TraceLevel::Warn);
        assert_eq!(super::directives_from(None, &conf), "warn");
        // and RUST_LOG replaces it entirely rather than raising or lowering it
        assert_eq!(
            super::directives_from(Some("shoal_core=debug".to_owned()), &conf),
            "shoal_core=debug"
        );
        // an empty override is not an override, or a shell exporting RUST_LOG= would silence
        // everything rather than change nothing
        assert_eq!(super::directives_from(Some(String::new()), &conf), "warn");
        assert_eq!(super::directives_from(Some("  ".to_owned()), &conf), "warn");
    }

    #[test]
    /// A configured ratio samples, and no ratio keeps the behaviour every existing config has
    ///
    /// The second half matters more than the first. A default that started sampling would quietly
    /// throw away spans for every deployment that has never heard of this setting.
    fn a_sample_ratio_selects_a_sampler() {
        // a sink that names a fraction
        let sampled = OtlpTracing::new("http://127.0.0.1:4318/v1/traces").sample_ratio(0.25);
        assert!(
            matches!(super::sampler(&sampled), Sampler::TraceIdRatioBased(ratio) if ratio == 0.25)
        );
        // and one that does not, which has to stay the SDK's default rather than becoming a ratio
        let unsampled = OtlpTracing::new("http://127.0.0.1:4318/v1/traces");
        assert!(matches!(
            super::sampler(&unsampled),
            Sampler::ParentBased(_)
        ));
    }

    #[test]
    /// A span handed a peer's trace context joins that peer's trace
    ///
    /// This is the server's whole half of the join. The parent set here is an OpenTelemetry one,
    /// resolved by the layer rather than by the registry, which is why a test of it has to install
    /// a layer and read the span's context back rather than read `Attributes::parent`.
    ///
    /// The context is marked **remote**, and that is not cosmetic: it is what makes a parent based
    /// sampler take the sender's sampling decision instead of making a fresh one, so a trace is
    /// kept whole rather than exported in halves.
    fn adopting_a_remote_parent_joins_the_peers_trace() {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        use tracing_subscriber::layer::SubscriberExt;

        // the context a peer would have put on the wire
        let trace_id = [
            0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19,
        ];
        let span_id = [0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28];
        let wire = TraceContext::new(trace_id, span_id, 1).expect("a valid context was refused");
        // a subscriber with a layer that can resolve a trace at all, scoped to this test
        let (_exporter, provider) = provider();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer().with_tracer(provider.tracer("adopt_remote_parent")),
        );
        tracing::subscriber::with_default(subscriber, || {
            // a root span, the way `client_rx_relay` opens one
            let span = tracing::info_span!(parent: None, "Shoal::request");
            // which is in a trace of its own until it is handed the peer's context
            span.set_parent(opentelemetry::Context::new());
            super::adopt_remote_parent(&span, &wire);
            // it is now in the peer's trace rather than in one of its own
            //
            // `Span::context` resolves the context of the span itself, so what this reads is the
            // trace id the layer gave it - which is the peer's, and is the whole join. The span id
            // beside it is this span's own and is expected to differ from the peer's. That the
            // peer's span is its *parent* is asserted end to end on exported spans, in
            // `shoal/tests/trace_propagation.rs`, since a parent is not readable from here
            let context = span.context();
            let resolved = context.span();
            let resolved = resolved.span_context();
            assert_eq!(resolved.trace_id().to_bytes(), trace_id);
            assert_ne!(resolved.span_id().to_bytes(), span_id);
            // and the sender's sampling decision came with it, rather than being made again here
            assert!(resolved.is_sampled());
        });
    }

    #[test]
    /// A peer that did not sample its trace does not have one sampled for it here
    ///
    /// The sampling decision moving to the client is the consequence of this feature that an
    /// operator has to know about: `sample_ratio` on a server now governs traces with no remote
    /// parent, and nothing else. A trace sampled at one end and not the other is worse than either
    /// answer, so this asserts the flags travel rather than being re-decided.
    fn an_unsampled_peer_stays_unsampled() {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        use tracing_subscriber::layer::SubscriberExt;

        // a context whose sender chose not to sample it
        let wire = TraceContext::new([5; 16], [6; 8], 0).expect("a valid context was refused");
        let (_exporter, provider) = provider();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer().with_tracer(provider.tracer("adopt_remote_parent")),
        );
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(parent: None, "Shoal::request");
            span.set_parent(opentelemetry::Context::new());
            super::adopt_remote_parent(&span, &wire);
            // the trace joined, and the sender's decision not to sample it came with it
            let context = span.context();
            let resolved = context.span();
            let resolved = resolved.span_context();
            assert_eq!(resolved.trace_id().to_bytes(), [5; 16]);
            assert!(!resolved.is_sampled());
        });
    }
}
