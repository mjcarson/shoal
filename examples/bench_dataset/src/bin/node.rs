//! The catalog's node program: `shoal.yml` in, a node serving the catalog out

/// The allocator every Shoal server program in this workspace runs with
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Serve the catalog
fn main() -> Result<(), shoal::server::ServerError> {
    shoal::server::node::main::<bench_dataset::Catalog>()
}
