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

use mimalloc::MiMalloc;

/// The allocator every Shoal server program in this workspace runs with
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Serve or claim a node of the TMDB dataset schema
fn main() -> Result<(), shoal::server::ServerError> {
    shoal::server::node::main::<tmdb_dataset::Tmdb>()
}
