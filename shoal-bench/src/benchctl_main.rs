//! shoalctl built against the bench schema: its terminal UI, and `cluster` to deploy
//! `shoal-node` to hosts over ssh ([F51](../../docs/src/features/cluster-deployment.md))

/// Query a bench cluster, or deploy one
#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    shoalctl::cli::main::<shoal_bench::workloads::schema::BenchClient>().await
}
