//! A Shoal node serving the TMDB dataset schema, for `tmdb-dataset-loader cluster` to deploy
//!
//! ```sh
//! tmdb-dataset-node serve --conf shoal.yml
//! tmdb-dataset-node claim --conf shoal.yml
//! ```
//!
//! This is the program an inventory's `server:` names. It has no settings of its own: every host
//! runs it under the systemd unit `shoaladm deploy` installs, against the `shoal.yml` the
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
/// Profiling is on from the start, as `MALLOC_CONF` below says, and `_RJEM_MALLOC_CONF` can change
/// it; `target/lab/r15/prof/heap.py` reads the dumps.
#[cfg(feature = "jemalloc-prof")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// The profiling settings a `jemalloc-prof` node starts with, the C string jemalloc reads
///
/// Profiling on, an allocation sampled every 512 KiB on average, the live samples dumped every
/// 2 GiB allocated under `/var/tmp/shoal-heap.*`, so a lab host needs no environment set for it.
/// `_RJEM_MALLOC_CONF` is read after this and overrides it.
#[cfg(feature = "jemalloc-prof")]
static PROFILE_CONF: &[u8] =
    b"prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:31,prof_prefix:/var/tmp/shoal-heap\0";

/// jemalloc's weak `malloc_conf`, pointed at [`PROFILE_CONF`]
#[cfg(feature = "jemalloc-prof")]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static MALLOC_CONF: Option<&'static u8> = Some(&PROFILE_CONF[0]);

/// Serve or claim a node of the TMDB dataset schema
fn main() -> Result<(), shoal::server::ServerError> {
    shoal::server::node::main::<tmdb_dataset::Tmdb>()
}
