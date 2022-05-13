//!
//! The `error` module provides a general [`Error`] struct representing different types of
//! encountered errors when running the page server.
//!

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Internal(#[from] anyhow::Error), // internal/unexpected errors are represented by `anyhow::Error`
}
