//! This module provides a wrapper around a real RemoteStorage implementation that
//! causes the first N attempts at each upload or download operatio to fail. For
//! testing purposes.
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::{Download, DownloadError, RemotePath, RemoteStorage, StorageMetadata};

pub struct UnreliableWrapper {
    inner: crate::GenericRemoteStorage,

    // This many attempts of each operation will fail, then we let it succeed.
    attempts_to_fail: u64,

    // Tracks how many failed attempts of each operation has been made.
    attempts: Mutex<HashMap<RemoteOp, u64>>,
}

/// Used to identify retries of different unique operation.
#[derive(Debug, Hash, Eq, PartialEq)]
enum RemoteOp {
    List,
    ListPrefixes(Option<RemotePath>),
    Upload(RemotePath),
    Download(RemotePath),
    Delete(RemotePath),
}

impl UnreliableWrapper {
    pub fn new(inner: crate::GenericRemoteStorage, attempts_to_fail: u64) -> Self {
        assert!(attempts_to_fail > 0);
        UnreliableWrapper {
            inner,
            attempts_to_fail,
            attempts: Mutex::new(HashMap::new()),
        }
    }

    ///
    /// Common functionality for all operations.
    ///
    /// On the first attempts of this operation, return an error. After 'attempts_to_fail'
    /// attempts, let the operation go ahead, and clear the counter.
    ///
    fn attempt(&self, op: RemoteOp) -> Result<u64, DownloadError> {
        let mut attempts = self.attempts.lock().unwrap();

        match attempts.entry(op) {
            Entry::Occupied(mut e) => {
                let attempts_before_this = {
                    let p = e.get_mut();
                    *p += 1;
                    *p
                };

                if attempts_before_this >= self.attempts_to_fail {
                    // let it succeed
                    e.remove();
                    Ok(attempts_before_this)
                } else {
                    let error =
                        anyhow::anyhow!("simulated failure of remote operation {:?}", e.key());
                    Err(DownloadError::Other(error))
                }
            }
            Entry::Vacant(e) => {
                let error = anyhow::anyhow!("simulated failure of remote operation {:?}", e.key());
                e.insert(1);
                Err(DownloadError::Other(error))
            }
        }
    }
}

#[async_trait::async_trait]
impl RemoteStorage for UnreliableWrapper {
    /// Lists all items the storage has right now.
    async fn list(&self) -> anyhow::Result<Vec<RemotePath>> {
        self.attempt(RemoteOp::List)?;
        self.inner.list().await
    }

    async fn list_prefixes(
        &self,
        prefix: Option<&RemotePath>,
    ) -> Result<Vec<RemotePath>, DownloadError> {
        self.attempt(RemoteOp::ListPrefixes(prefix.cloned()))?;
        self.inner.list_prefixes(prefix).await
    }

    async fn upload(
        &self,
        data: Box<(dyn tokio::io::AsyncRead + Unpin + Send + Sync + 'static)>,
        // S3 PUT request requires the content length to be specified,
        // otherwise it starts to fail with the concurrent connection count increasing.
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        self.attempt(RemoteOp::Upload(to.clone()))?;
        self.inner.upload(data, data_size_bytes, to, metadata).await
    }

    async fn download(&self, from: &RemotePath) -> Result<Download, DownloadError> {
        self.attempt(RemoteOp::Download(from.clone()))?;
        self.inner.download(from).await
    }

    async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError> {
        // Note: We treat any download_byte_range as an "attempt" of the same
        // operation. We don't pay attention to the ranges. That's good enough
        // for now.
        self.attempt(RemoteOp::Download(from.clone()))?;
        self.inner
            .download_byte_range(from, start_inclusive, end_exclusive)
            .await
    }

    async fn delete(&self, path: &RemotePath) -> anyhow::Result<()> {
        self.attempt(RemoteOp::Delete(path.clone()))?;
        self.inner.delete(path).await
    }
}
