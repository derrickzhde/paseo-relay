mod config;
mod handshake;
mod metrics;
mod ops;
mod peer;
mod protocol;
mod room;
mod state;
mod ws;

use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;

use axum::routing::{any, get};
use axum::Router;

use crate::config::Config;
use crate::state::AppState;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match Config::load(std::env::var("PASEO_RELAY_CONFIG").ok().as_deref()) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("invalid configuration: {message}");
            return ExitCode::FAILURE;
        }
    };

    let address = SocketAddr::new(config.host, config.port);
    let allowlist = config.allowed_server_ids.len();
    let state = Arc::new(AppState::new(config));

    let app = Router::new()
        .route("/ws", any(ws::upgrade))
        .route("/health", get(ops::health))
        .route("/ready", get(ops::ready))
        .route("/metrics", get(ops::metrics))
        .fallback(ops::not_found)
        .with_state(Arc::clone(&state));

    let listener = match tokio::net::TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("cannot bind {address}: {error}");
            return ExitCode::FAILURE;
        }
    };

    eprintln!(
        "paseo-relay listening on {address} (allowlist: {}, draining: {})",
        if allowlist == 0 { "open".to_string() } else { format!("{allowlist} serverIds") },
        state.draining(),
    );

    let shutdown = shutdown_signal(Arc::clone(&state));
    if let Err(error) = axum::serve(listener, app).with_graceful_shutdown(shutdown).await {
        eprintln!("server error: {error}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

/// SIGTERM starts a drain so `/ready` fails before the process goes away; SIGINT exits.
async fn shutdown_signal(state: Arc<AppState>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(_) => return std::future::pending().await,
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {
                state.begin_drain();
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = state;
        let _ = tokio::signal::ctrl_c().await;
    }
}
