use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tinyredis_core::Database;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::watch;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use tinyredis_server::config::ReplicationRole;
use tinyredis_server::{
    config::ServerConfig, handler, replication::sync_replica_from_primary, AppState,
};

#[derive(Debug, Parser)]
#[command(name = "tinyredis-server", about = "TinyRedis async TCP server")]
struct Cli {
    #[arg(long, env = "TINYREDIS_HOST")]
    host: Option<String>,

    #[arg(long, env = "TINYREDIS_PORT")]
    port: Option<u16>,

    #[arg(long, env = "TINYREDIS_DATA_DIR")]
    data_dir: Option<std::path::PathBuf>,

    #[arg(long, env = "TINYREDIS_MAX_MEMORY")]
    max_memory: Option<usize>,

    #[arg(long, env = "TINYREDIS_PASSWORD")]
    password: Option<String>,

    #[arg(long, env = "TINYREDIS_LOG_LEVEL", default_value = "info")]
    log_level: String,

    #[arg(long, default_value_t = false)]
    no_health: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut config = ServerConfig::from_env();

    if let Some(host) = cli.host {
        config.host = host;
    }
    if let Some(port) = cli.port {
        config.port = port;
    }
    if let Some(data_dir) = cli.data_dir {
        config.data_dir = data_dir;
    }
    if let Some(max_memory) = cli.max_memory {
        config.max_memory = Some(max_memory);
    }
    if let Some(password) = cli.password {
        config.password = Some(password);
    }
    config.log_level = cli.log_level;
    config.health_check = !cli.no_health;

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let mut db = Database::new();
    if let Some(max) = config.max_memory {
        db.set_max_memory(Some(max));
    }

    let state = Arc::new(AppState::new(config.clone(), db).context("initialize app state")?);

    state
        .persistence
        .ensure_data_dir()
        .await
        .context("create data directory")?;

    match state.persistence.load(&mut *state.db.write().await).await {
        Ok(report) => {
            info!(
                snapshot = report.snapshot_loaded,
                keys = report.keys_from_snapshot,
                aof_replayed = report.aof_records_replayed,
                aof_skipped = report.aof_records_skipped,
                "persistence recovery complete"
            );
            if report.snapshot_corrupt {
                warn!("snapshot was corrupt or missing; continued with AOF/new database");
            }
        }
        Err(e) => {
            warn!(error = %e, "persistence recovery failed, starting empty");
        }
    }

    if state.replication.role() == ReplicationRole::Replica {
        if let Some((host, port)) = state.replication.primary_addr() {
            let mut db = state.db.write().await;
            if let Err(e) =
                sync_replica_from_primary(&state.replication, &mut db, &host, port).await
            {
                warn!(error = %e, %host, port, "initial replica sync failed");
            }
        }
    }

    state.refresh_memory_metric().await;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    if config.health_check {
        let health_addr = config.health_addr();
        let health_shutdown = shutdown_rx.clone();
        let health_addr_spawn = health_addr.clone();
        tokio::spawn(async move {
            if let Err(e) = run_health_server(&health_addr_spawn, health_shutdown).await {
                error!(error = %e, "health server exited");
            }
        });
        info!(addr = %health_addr, "health check HTTP listening");
    }

    let listener = TcpListener::bind(config.listen_addr())
        .await
        .with_context(|| format!("bind {}", config.listen_addr()))?;
    info!(addr = %config.listen_addr(), "TinyRedis listening");

    let state_for_accept = Arc::clone(&state);
    let mut shutdown_rx = shutdown_rx;
    let accept_loop = async move {
        loop {
            tokio::select! {
                biased;
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        break;
                    }
                }
                accept = listener.accept() => {
                    match accept {
                        Ok((stream, addr)) => {
                            debug_log_accept(addr.to_string());
                            let state = Arc::clone(&state_for_accept);
                            tokio::spawn(async move {
                                if let Err(e) = handler::handle_connection(stream, state).await {
                                    warn!(%addr, error = %e, "connection error");
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "accept failed");
                        }
                    }
                }
            }
        }
    };

    tokio::select! {
        _ = accept_loop => {},
        _ = shutdown_signal() => {
            info!("shutdown signal received");
            let _ = shutdown_tx.send(true);
        }
    }

    info!("TinyRedis stopped");
    Ok(())
}

fn debug_log_accept(addr: String) {
    tracing::debug!(%addr, "accepted connection");
}

async fn shutdown_signal() {
    if signal::ctrl_c().await.is_ok() {
        return;
    }
}

async fn run_health_server(addr: &str, mut shutdown: watch::Receiver<bool>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind health endpoint {addr}"))?;

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            accept = listener.accept() => {
                match accept {
                    Ok((mut stream, _)) => {
                        tokio::spawn(async move {
                            let _ = handle_health_request(&mut stream).await;
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "health accept failed");
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_health_request(stream: &mut TcpStream) -> anyhow::Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let request = String::from_utf8_lossy(&buf[..n]);
    let status = if request.starts_with("GET /health") || request.starts_with("GET / ") {
        "200 OK"
    } else {
        "404 Not Found"
    };
    let body = if status.starts_with("200") {
        "OK"
    } else {
        "Not Found"
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}
