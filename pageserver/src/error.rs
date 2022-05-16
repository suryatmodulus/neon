//!
//! The `error` module provides a general [`Error`] struct representing different types of
//! encountered errors when running the page server.
//!

use thiserror::Error;

#[derive(Error)]
pub enum Error {
    #[error("Postgres IO error: {0:?}")]
    PostgresIO(#[from] PostgresIOError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Error)]
pub enum PostgresIOError {
    #[error("Connection reset by peer")]
    ConnectionReset,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

// # Debug formatting for error structs
//
// We need to implement the `fmt::Debug` trait manually instead of deriving the trait
// because the `{error:?}` format is used when logging the application's errors and
// we want to have different formats for different types of encountered errors.
//
// - For internal/unexpected error, which is parameterized by `anyhow::Error`,
// print the source error in the debug format, which also includes the error's backtrace [1].
//
// - For expected error, print the error message (using the `{error:#}` format).
//
// [1]: Note this only applies when the "backtrace" feature is enabled for the `anyhow` crate.

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Internal(err) => {
                write!(f, "internal error {err:?}")
            }
            _ => {
                write!(f, "{self:#}")
            }
        }
    }
}

impl std::fmt::Debug for PostgresIOError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Internal(err) => {
                write!(f, "internal error {err:?}")
            }
            _ => {
                write!(f, "{self:#}")
            }
        }
    }
}
