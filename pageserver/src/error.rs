//!
//! The `error` module provides a general [`PageServerError`] struct representing different types of
//! errors when running the page server.
//!

use thiserror::Error;

#[derive(Debug, Error)]
/// The general error type when running the page server.
pub enum PageServerError {
    #[error("Postgres IO error: {0:#}")]
    PostgresIO(#[from] PostgresIOError),
    #[error("Internal error: {0:?}")]
    Internal(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
/// The error type for I/O operations with the Postgres client.
pub enum PostgresIOError {
    #[error("Connection reset by peer")]
    ConnectionReset,
}
