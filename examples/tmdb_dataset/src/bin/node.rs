//! A Shoal node serving the TMDB dataset schema, for `tmdb-dataset-loader cluster` to deploy
//!
//! ```sh
//! tmdb-dataset-node serve --conf shoal.yml
//! tmdb-dataset-node claim --conf shoal.yml
//! ```
//!
//! This is the program an inventory's `server:` names. It has no settings of its own: every host
//! runs it under the systemd unit `cluster bootstrap` installs, against the `shoal.yml` the
//! deployment rendered for that node - its cores, memory, storage, ports, TLS and admin
//! credential ([F54](../../../../docs/src/features/tmdb-dataset-deployment.md)).

/// The allocator every Shoal server program in this workspace runs with
///
/// Left out by the `system-allocator` feature, which a sanitizer build needs: AddressSanitizer
/// watches the system allocator, and mimalloc's own heap is invisible to it.
#[cfg(not(any(feature = "system-allocator", feature = "jemalloc-prof")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// jemalloc with heap profiling, for finding what a node's memory holds
///
/// Profiling is off until `_RJEM_MALLOC_CONF` turns it on, for example
/// `prof:true,lg_prof_sample:19,lg_prof_interval:30,prof_prefix:/tmp/jeprof`, which samples an
/// allocation every 512 KiB on average and dumps the live samples every gibibyte allocated.
#[cfg(feature = "jemalloc-prof")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Serve or claim a node of the TMDB dataset schema
fn main() -> Result<(), shoal::server::ServerError> {
    shoal::server::node::main::<tmdb_dataset::Tmdb>()
}
