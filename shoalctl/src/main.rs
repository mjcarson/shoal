//! shoalctl - A terminal UI for querying Shoal databases
//!
//! This binary is a placeholder. To use shoalctl, create your own binary
//! that imports the shoalctl library and provides your database types.
//!
//! # Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use shoal_core::client::Shoal;
//!
//! // Import your database types
//! use my_db::{MyDbClient};
//!
//! #[tokio::main]
//! async fn main() -> color_eyre::Result<()> {
//!     // Create your Shoal client
//!     let shoal = Arc::new(Shoal::<MyDbClient>::new("127.0.0.1:12000").await?);
//!     // Run shoalctl with your database
//!     shoalctl::run(shoal).await
//! }
//! ```

fn main() {
    eprintln!("shoalctl requires a database type to be specified at compile time.");
    eprintln!();
    eprintln!("Create your own binary that imports shoalctl and provides your database types:");
    eprintln!();
    eprintln!("    use std::sync::Arc;");
    eprintln!("    use shoal_core::client::Shoal;");
    eprintln!("    use my_db::MyDbClient;");
    eprintln!();
    eprintln!("    #[tokio::main]");
    eprintln!("    async fn main() -> color_eyre::Result<()> {{");
    eprintln!("        let shoal = Arc::new(Shoal::<MyDbClient>::new(\"127.0.0.1:12000\").await?);");
    eprintln!("        shoalctl::run(shoal).await");
    eprintln!("    }}");
    std::process::exit(1);
}
