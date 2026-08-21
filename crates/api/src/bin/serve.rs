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

    let state = if settings.auth.enabled {
        AppState::new(storage)
    } else {
        // Loud on purpose. An API that sends messages on anyone's behalf
        // with no credential is a thing an operator should have chosen,
        // not something they discover from a bill.
        tracing::warn!(
            "authentication is DISABLED: every request can act as any tenant. \
             Set auth.enabled = true (or NS_AUTH_ENABLED=true) unless the network \
             already controls who can reach this service."
        );
        AppState::new(storage).without_auth()
    };
    let router = build_router(state);

    let addr: SocketAddr = format!("{}:{}", settings.server.host, settings.server.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "ns-api listening");
    axum::serve(listener, router).await?;

    Ok(())
}
