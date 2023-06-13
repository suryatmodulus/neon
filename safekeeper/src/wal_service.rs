//!
//!   WAL service listens for client connections and
//!   receive WAL from wal_proposer and send it to WAL receivers
//!
use anyhow::{Context, Result};
use postgres_backend::QueryError;
use std::{future, thread};
use tokio::net::TcpStream;
use tracing::*;
use utils::measured_stream::MeasuredStream;

use crate::SafeKeeperConf;
use crate::{handler::SafekeeperPostgresHandler, metrics::PG_IO_BYTES};
use postgres_backend::{AuthType, PostgresBackend};

/// Accept incoming TCP connections and spawn them into a background thread.
pub fn thread_main(conf: SafeKeeperConf, pg_listener: std::net::TcpListener) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create runtime")
        // todo catch error in main thread
        .expect("failed to create runtime");

    runtime
        .block_on(async move {
            // Tokio's from_std won't do this for us, per its comment.
            pg_listener.set_nonblocking(true)?;
            let listener = tokio::net::TcpListener::from_std(pg_listener)?;
            let mut connection_count: ConnectionCount = 0;

            loop {
                match listener.accept().await {
                    Ok((socket, peer_addr)) => {
                        debug!("accepted connection from {}", peer_addr);
                        let conf = conf.clone();
                        let conn_id = issue_connection_id(&mut connection_count);

                        let _ = thread::Builder::new()
                            .name("WAL service thread".into())
                            .spawn(move || {
                                if let Err(err) = handle_socket(socket, conf, conn_id) {
                                    error!("connection handler exited: {}", err);
                                }
                            })
                            .unwrap();
                    }
                    Err(e) => error!("Failed to accept connection: {}", e),
                }
            }
            #[allow(unreachable_code)] // hint compiler the closure return type
            Ok::<(), anyhow::Error>(())
        })
        .expect("listener failed")
}

/// This is run by `thread_main` above, inside a background thread.
///
fn handle_socket(
    socket: TcpStream,
    conf: SafeKeeperConf,
    conn_id: ConnectionId,
) -> Result<(), QueryError> {
    let _enter = info_span!("", cid = %conn_id).entered();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();

    let read_metrics = PG_IO_BYTES.with_label_values(&["read"]);
    let write_metrics = PG_IO_BYTES.with_label_values(&["write"]);

    socket.set_nodelay(true)?;
    let peer_addr = socket.peer_addr()?;

    // TODO: measure cross-az traffic
    let socket = MeasuredStream::new(
        socket,
        |cnt| {
            read_metrics.inc_by(cnt as u64);
        },
        |cnt| {
            write_metrics.inc_by(cnt as u64);
        },
    );

    let auth_type = match conf.auth {
        None => AuthType::Trust,
        Some(_) => AuthType::NeonJWT,
    };
    let mut conn_handler = SafekeeperPostgresHandler::new(conf, conn_id);
    let pgbackend = PostgresBackend::new_from_io(socket, peer_addr, auth_type, None)?;
    // libpq protocol between safekeeper and walproposer / pageserver
    // We don't use shutdown.
    local.block_on(
        &runtime,
        pgbackend.run(&mut conn_handler, future::pending::<()>),
    )?;

    Ok(())
}

/// Unique WAL service connection ids are logged in spans for observability.
pub type ConnectionId = u32;
pub type ConnectionCount = u32;

pub fn issue_connection_id(count: &mut ConnectionCount) -> ConnectionId {
    *count = count.wrapping_add(1);
    *count
}
