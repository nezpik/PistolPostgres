//! `pistol` binary — thin entry point over the library crate.

use clap::Parser;
use pistol::cli;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = cli::Cli::parse();
    cli::dispatch(cli).await
}
