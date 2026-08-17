use std::net::SocketAddr;

use ns_api::{build_router, AppState};
use ns_storage::SqliteStorage;
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG_PATH: &str = "config/local.toml";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let config_path = std::env::var("NS_CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
    let settings = ns_config::load(&config_path)?;

    let storage = SqliteStorage::connect(&settings.database.url).await?;
    let router = build_router(AppState::new(storage));

    let addr: SocketAddr = format!("{}:{}", settings.server.host, settings.server.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "ns-api listening");
    axum::serve(listener, router).await?;

    Ok(())
}
