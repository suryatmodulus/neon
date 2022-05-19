use anyhow::{bail, ensure, Context, Result};

use std::cmp::min;
use std::path::{Path, PathBuf};
use std::time::Duration;

use postgres_ffi::xlog_utils::{XLogFileName, XLogSegNo, XLogSegNoOffsetToRecPtr, PG_TLI};
use remote_storage::{GenericRemoteStorage, RemoteStorage};
use tokio::fs::File;
use tokio::runtime::{Builder, Runtime};

use tokio::select;
use tokio::sync::watch::{self, Receiver, Sender};
use tokio::time::sleep;
use tracing::*;

use utils::{lsn::Lsn, zid::ZTenantTimelineId};

use crate::broker::Election;
use crate::{broker, SafeKeeperConf};

use once_cell::sync::OnceCell;

static BACKUP_RUNTIME: OnceCell<Runtime> = OnceCell::new();
const DEFAULT_BACKUP_RUNTIME_SIZE: u32 = 16;

const MIN_WAL_SEGMENT_SIZE: usize = 1 << 20; // 1MB
const MAX_WAL_SEGMENT_SIZE: usize = 1 << 30; // 1GB

const BACKUP_ELECTION_NAME: &str = "WAL_BACKUP";

const BROKER_CONNECTION_RETRY_DELAY_MS: u64 = 1000;

const UPLOAD_FAILURE_RETRY_MIN_MS: u64 = 10;
const UPLOAD_FAILURE_RETRY_MAX_MS: u64 = 5000;

#[allow(clippy::too_many_arguments)]
async fn backup_task(
    backup_start: Lsn,
    timeline_id: ZTenantTimelineId,
    timeline_dir: PathBuf,
    mut segment_size_set: Receiver<u32>,
    mut lsn_durable: Receiver<Lsn>,
    mut shutdown: Receiver<bool>,
    lsn_backed_up: Sender<Lsn>,
    election: Election,
) -> Result<()> {
    info!("Starting backup task, backup_lsn {}", backup_start);

    segment_size_set
        .changed()
        .await
        .expect("Failed to recieve wal segment size");
    let wal_seg_size = *segment_size_set.borrow() as usize;
    ensure!(
        (MIN_WAL_SEGMENT_SIZE..=MAX_WAL_SEGMENT_SIZE).contains(&wal_seg_size),
        "Invalid wal seg size provided, should be between 1MiB and 1GiB per postgres"
    );

    let mut backup_lsn = backup_start;

    loop {
        let mut leader = None;
        let mut retry_attempt = 0u64;

        select! {
            result = broker::get_leader(&election) => {
                match result {
                    Ok(l) => { leader = Some(l);},
                    Err(e) => {
                        error!("Error during leader election {:?}", e);
                        sleep(Duration::from_millis(BROKER_CONNECTION_RETRY_DELAY_MS)).await;
                        continue;
                    },
                }
            }
            _ = shutdown.changed() => {}
        }

        let mut cancel = false;

        if *shutdown.borrow() {
            if let Some(l) = leader.as_ref() {
                l.give_up()
                    .await
                    .context("failed to drop election on shutdown")?;
            }

            break;
        }

        // TODO: antons - replace with backup start LSN discovery after leadership acquisition
        // backup_lsn = backup_start;

        loop {
            // If no changes to LSN we should retry failed upload anyway; No jitter for simplicity, this should be in S3 library anyway
            let retry_delay = if retry_attempt > 0 {
                min(
                    UPLOAD_FAILURE_RETRY_MIN_MS << retry_attempt,
                    UPLOAD_FAILURE_RETRY_MAX_MS,
                )
            } else {
                u64::MAX
            };

            select! {
                durable_changed  = lsn_durable.changed() => {
                    if let Err(e) = durable_changed {
                        error!("Channel closed shutting down wal backup {:?}", e);
                        cancel = true;
                        break;
                    }
                }
                _ = shutdown.changed() => {}
                _ = sleep(Duration::from_millis(retry_delay)) => {}
            }

            if *shutdown.borrow() {
                info!("Shutting down wal backup");
                cancel = true;
                break;
            }

            let commit_lsn = *lsn_durable.borrow();

            ensure!(
                commit_lsn >= backup_lsn,
                "backup lsn should never pass commit lsn"
            );

            if backup_lsn.segment_number(wal_seg_size) == commit_lsn.segment_number(wal_seg_size) {
                continue;
            }

            if let Some(l) = leader.as_ref() {
                // Optimization idea for later:
                //  Avoid checking election leader every time by returning current lease grant expiration time
                //  Re-check leadership only after expiration time,
                //  such approach woud reduce overhead on write-intensive workloads

                match l.check_am_i(election.election_name.clone(), election.candidate_name.clone()).await {
                    Ok(leader) => {
                        if !leader {
                            info!("Leader has changed");
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("Error validating leader, {:?}", e);
                        break;
                    }
                }
            }

            debug!(
                "Woken up for lsn {} committed, will back it up. backup lsn {}",
                commit_lsn, backup_lsn
            );

            match backup_lsn_range(
                backup_lsn,
                commit_lsn,
                wal_seg_size,
                timeline_id,
                timeline_dir.clone(),
            )
            .await
            {
                Ok(backup_lsn_result) => {
                    backup_lsn = backup_lsn_result;
                    lsn_backed_up.send(backup_lsn)?;
                    retry_attempt = 0;
                }
                Err(e) => {
                    error!(
                        "Failure in backup backup commit_lsn {} backup lsn {}, {:?}",
                        commit_lsn, backup_lsn, e
                    );

                    retry_attempt += 1;
                }
            }
        }

        if let Some(l) = leader {
            l.give_up()
                .await
                .context("failed to resign from election")?;
        }

        if cancel {
            break;
        }
    }

    Ok(())
}

pub async fn backup_lsn_range(
    start_lsn: Lsn,
    end_lsn: Lsn,
    wal_seg_size: usize,
    timeline_id: ZTenantTimelineId,
    timeline_dir: PathBuf,
) -> Result<Lsn> {
    let mut res = start_lsn;
    for s in get_segments(start_lsn, end_lsn, wal_seg_size) {
        let seg_backup = backup_single_segment(s, timeline_id, timeline_dir.clone()).await;

        // TODO: antons limit this only to Not Found errors
        if seg_backup.is_err() && start_lsn.is_valid() {
            error!("Segment {} not found in timeline {}", s.seg_no, timeline_id)
        }

        ensure!(
            start_lsn >= s.start_lsn || start_lsn.is_valid(),
            "Out of order segment upload detected"
        );

        if res == s.start_lsn {
            res = s.end_lsn;
        } else {
            warn!("Out of order Segment {} upload had been detected for timeline {}. Backup Lsn {}, Segment Start Lsn {}", s.seg_no, timeline_id, res, s.start_lsn)
        }
    }

    Ok(res)
}

async fn backup_single_segment(
    seg: Segment,
    timeline_id: ZTenantTimelineId,
    timeline_dir: PathBuf,
) -> Result<()> {
    let segment_file_name = seg.file_path(timeline_dir.as_path())?;
    let dest_name = PathBuf::from(format!(
        "{}/{}",
        timeline_id.tenant_id, timeline_id.timeline_id
    ))
    .with_file_name(seg.object_name());

    debug!("Backup of {} requested", segment_file_name.display());

    if !segment_file_name.exists() {
        // TODO: antons return a specific error
        bail!("Segment file is Missing");
    }

    // TODO: antons implement retry logic
    backup_object(&segment_file_name, seg.size(), &dest_name).await?;
    debug!("Backup of {} done", segment_file_name.display());

    Ok(())
}

pub struct WalBackup {
    wal_segment_size: Sender<u32>,
    lsn_committed: Sender<Lsn>,
    lsn_backed_up: Receiver<Lsn>,
    shutdown: Sender<bool>,
}

impl WalBackup {
    pub fn new(
        wal_segment_size: Sender<u32>,
        lsn_committed: Sender<Lsn>,
        lsn_backed_up: Receiver<Lsn>,
        shutdown: Sender<bool>,
    ) -> Self {
        Self {
            wal_segment_size,
            lsn_committed,
            lsn_backed_up,
            shutdown,
        }
    }

    pub fn set_wal_seg_size(&self, wal_seg_size: u32) -> Result<()> {
        self.wal_segment_size
            .send(wal_seg_size)
            .context("Failed to notify wal backup regarding wal segment size")?;
        Ok(())
    }

    pub fn notify_lsn_committed(&self, lsn: Lsn) -> Result<()> {
        self.lsn_committed
            .send(lsn)
            .context("Failed to notify wal backup regarding committed lsn")?;
        Ok(())
    }

    pub fn shutdown(&self) -> Result<()> {
        self.shutdown
            .send(true)
            .context("Failed to send shutdown signal to wal backup task")?;
        // TODO: antons we may need to await the task here
        Ok(())
    }

    pub fn get_backup_lsn(&self) -> Lsn {
        *self.lsn_backed_up.borrow()
    }
}

pub fn create(
    conf: &SafeKeeperConf,
    timeline_id: &ZTenantTimelineId,
    backup_start: Lsn,
) -> Result<WalBackup> {
    let rt_size = usize::try_from(
        conf.backup_runtime_threads
            .unwrap_or(DEFAULT_BACKUP_RUNTIME_SIZE),
    )
    .expect("Could not get configuration value for backup_runtime_threads");

    let runtime = BACKUP_RUNTIME.get_or_init(|| {
        info!("Initializing backup async runtime with {} threads", rt_size);

        Builder::new_multi_thread()
            .worker_threads(rt_size)
            .enable_all()
            .build()
            .expect("Failed to create wal backup runtime")
    });

    let (lsn_committed_sender, lsn_committed_receiver) = watch::channel(Lsn::INVALID);
    let (lsn_backed_up_sender, lsn_backed_up_receiver) = watch::channel(Lsn::INVALID);
    let (wal_seg_size_sender, wal_seg_size_receiver) = watch::channel::<u32>(0);
    let (shutdown_sender, shutdown_receiver) = watch::channel::<bool>(false);

    let _ = REMOTE_STORAGE.get_or_init(|| {
        let rs = conf.backup_storage.as_ref().map(|c| {
            GenericRemoteStorage::new(conf.timeline_dir(timeline_id), c)
                .expect("Failed to create remote storage")
        });

        Box::new(rs)
    });

    let election_name = broker::get_campaign_name(
        BACKUP_ELECTION_NAME.to_string(),
        conf.broker_etcd_prefix.clone(),
        timeline_id,
    );
    let my_candidate_name = broker::get_candiate_name(conf.my_id);
    let election = broker::Election::new(
        election_name,
        my_candidate_name,
        conf.broker_endpoints.clone(),
    );

    runtime.spawn(
        backup_task(
            backup_start,
            *timeline_id,
            conf.timeline_dir(timeline_id),
            wal_seg_size_receiver,
            lsn_committed_receiver,
            shutdown_receiver,
            lsn_backed_up_sender,
            election,
        )
        .instrument(info_span!("Wal Backup", timeline_id.timeline_id = %timeline_id)),
    );

    Ok(WalBackup::new(
        wal_seg_size_sender,
        lsn_committed_sender,
        lsn_backed_up_receiver,
        shutdown_sender,
    ))
}

pub fn create_noop() -> WalBackup {
    let (lsn_committed_sender, _lsn_committed_receiver) = watch::channel(Lsn::INVALID);
    let (_lsn_backed_up_sender, lsn_backed_up_receiver) = watch::channel(Lsn::INVALID);
    let (wal_seg_size_sender, _wal_seg_size_receiver) = watch::channel::<u32>(0);
    let (shutdown_sender, _shutdown_receiver) = watch::channel::<bool>(false);

    WalBackup::new(
        wal_seg_size_sender,
        lsn_committed_sender,
        lsn_backed_up_receiver,
        shutdown_sender,
    )
}

#[derive(Debug, Copy, Clone)]
pub struct Segment {
    seg_no: XLogSegNo,
    start_lsn: Lsn,
    end_lsn: Lsn,
}

impl Segment {
    pub fn new(seg_no: u64, start_lsn: Lsn, end_lsn: Lsn) -> Self {
        Self {
            seg_no,
            start_lsn,
            end_lsn,
        }
    }

    pub fn object_name(self) -> String {
        XLogFileName(PG_TLI, self.seg_no, self.size())
    }

    pub fn file_path(self, timeline_dir: &Path) -> Result<PathBuf> {
        Ok(timeline_dir.join(self.object_name()))
    }

    pub fn size(self) -> usize {
        (u64::from(self.end_lsn) - u64::from(self.start_lsn)) as usize
    }
}

fn get_segments(start: Lsn, end: Lsn, seg_size: usize) -> Vec<Segment> {
    let first_seg = start.segment_number(seg_size);
    let last_seg = end.segment_number(seg_size);

    let res: Vec<Segment> = (first_seg..last_seg)
        .map(|s| {
            let start_lsn = XLogSegNoOffsetToRecPtr(s, 0, seg_size);
            let end_lsn = XLogSegNoOffsetToRecPtr(s + 1, 0, seg_size);
            Segment::new(s, Lsn::from(start_lsn), Lsn::from(end_lsn))
        })
        .collect();

    res
}

static REMOTE_STORAGE: OnceCell<Box<Option<GenericRemoteStorage>>> = OnceCell::new();

async fn backup_object(source_file: &PathBuf, size: usize, destination: &Path) -> Result<()> {
    let storage = REMOTE_STORAGE.get().expect("failed to get remote storage");

    let file = File::open(&source_file).await?;

    match storage.as_ref() {
        Some(GenericRemoteStorage::Local(local_storage)) => {
            debug!(
                "local upload about to start from {} to {}",
                source_file.display(),
                destination.display()
            );
            local_storage
                .upload(file, size, &PathBuf::from(destination), None)
                .await
        }
        Some(GenericRemoteStorage::S3(s3_storage)) => {
            let s3key = s3_storage.remote_object_id(destination).with_context(|| {
                format!("Could not format remote path for {}", destination.display())
            })?;

            debug!(
                "S3 upload about to start from {} to {}",
                source_file.display(),
                destination.display()
            );
            s3_storage.upload(file, size, &s3key, None).await
        }
        None => {
            info!(
                "no backup storage configured, skipping backup {}",
                destination.display()
            );
            Ok(())
        }
    }
    .with_context(|| format!("Failed to backup {}", destination.display()))?;

    Ok(())
}
