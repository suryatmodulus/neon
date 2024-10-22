//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use std::collections::{hash_map, HashMap};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;

use anyhow::Context;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing::*;

use remote_storage::GenericRemoteStorage;
use utils::crashsafe;

use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext};
use crate::task_mgr::{self, TaskKind};
use crate::tenant::config::TenantConfOpt;
use crate::tenant::delete::DeleteTenantFlow;
use crate::tenant::{create_tenant_files, CreateTenantFilesMode, Tenant, TenantState};
use crate::{InitializationOrder, IGNORED_TENANT_FILE_NAME};

use utils::fs_ext::PathExt;
use utils::id::{TenantId, TimelineId};

use super::delete::DeleteTenantError;
use super::timeline::delete::DeleteTimelineFlow;
use super::TenantSharedResources;

/// The tenants known to the pageserver.
/// The enum variants are used to distinguish the different states that the pageserver can be in.
pub(crate) enum TenantsMap {
    /// [`init_tenant_mgr`] is not done yet.
    Initializing,
    /// [`init_tenant_mgr`] is done, all on-disk tenants have been loaded.
    /// New tenants can be added using [`tenant_map_insert`].
    Open(HashMap<TenantId, Arc<Tenant>>),
    /// The pageserver has entered shutdown mode via [`shutdown_all_tenants`].
    /// Existing tenants are still accessible, but no new tenants can be created.
    ShuttingDown(HashMap<TenantId, Arc<Tenant>>),
}

impl TenantsMap {
    pub(crate) fn get(&self, tenant_id: &TenantId) -> Option<&Arc<Tenant>> {
        match self {
            TenantsMap::Initializing => None,
            TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => m.get(tenant_id),
        }
    }
    pub(crate) fn remove(&mut self, tenant_id: &TenantId) -> Option<Arc<Tenant>> {
        match self {
            TenantsMap::Initializing => None,
            TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => m.remove(tenant_id),
        }
    }
}

static TENANTS: Lazy<RwLock<TenantsMap>> = Lazy::new(|| RwLock::new(TenantsMap::Initializing));

/// Initialize repositories with locally available timelines.
/// Timelines that are only partially available locally (remote storage has more data than this pageserver)
/// are scheduled for download and added to the tenant once download is completed.
#[instrument(skip_all)]
pub async fn init_tenant_mgr(
    conf: &'static PageServerConf,
    resources: TenantSharedResources,
    init_order: InitializationOrder,
) -> anyhow::Result<()> {
    // Scan local filesystem for attached tenants
    let tenants_dir = conf.tenants_path();

    let mut tenants = HashMap::new();

    let mut dir_entries = fs::read_dir(&tenants_dir)
        .await
        .with_context(|| format!("Failed to list tenants dir {tenants_dir:?}"))?;

    let ctx = RequestContext::todo_child(TaskKind::Startup, DownloadBehavior::Warn);

    loop {
        match dir_entries.next_entry().await {
            Ok(None) => break,
            Ok(Some(dir_entry)) => {
                let tenant_dir_path = dir_entry.path();
                if crate::is_temporary(&tenant_dir_path) {
                    info!(
                        "Found temporary tenant directory, removing: {}",
                        tenant_dir_path.display()
                    );
                    if let Err(e) = fs::remove_dir_all(&tenant_dir_path).await {
                        error!(
                            "Failed to remove temporary directory '{}': {:?}",
                            tenant_dir_path.display(),
                            e
                        );
                    }
                } else {
                    // This case happens if we:
                    // * crash during attach before creating the attach marker file
                    // * crash during tenant delete before removing tenant directory
                    let is_empty = tenant_dir_path.is_empty_dir().with_context(|| {
                        format!("Failed to check whether {tenant_dir_path:?} is an empty dir")
                    })?;
                    if is_empty {
                        info!("removing empty tenant directory {tenant_dir_path:?}");
                        if let Err(e) = fs::remove_dir(&tenant_dir_path).await {
                            error!(
                                "Failed to remove empty tenant directory '{}': {e:#}",
                                tenant_dir_path.display()
                            )
                        }
                        continue;
                    }

                    let tenant_ignore_mark_file = tenant_dir_path.join(IGNORED_TENANT_FILE_NAME);
                    if tenant_ignore_mark_file.exists() {
                        info!("Found an ignore mark file {tenant_ignore_mark_file:?}, skipping the tenant");
                        continue;
                    }

                    match schedule_local_tenant_processing(
                        conf,
                        &tenant_dir_path,
                        resources.clone(),
                        Some(init_order.clone()),
                        &TENANTS,
                        &ctx,
                    ) {
                        Ok(tenant) => {
                            tenants.insert(tenant.tenant_id(), tenant);
                        }
                        Err(e) => {
                            error!("Failed to collect tenant files from dir {tenants_dir:?} for entry {dir_entry:?}, reason: {e:#}");
                        }
                    }
                }
            }
            Err(e) => {
                // On error, print it, but continue with the other tenants. If we error out
                // here, the pageserver startup fails altogether, causing outage for *all*
                // tenants. That seems worse.
                error!(
                    "Failed to list tenants dir entry in directory {tenants_dir:?}, reason: {e:?}"
                );
            }
        }
    }

    info!("Processed {} local tenants at startup", tenants.len());

    let mut tenants_map = TENANTS.write().await;
    assert!(matches!(&*tenants_map, &TenantsMap::Initializing));
    *tenants_map = TenantsMap::Open(tenants);
    Ok(())
}

pub(crate) fn schedule_local_tenant_processing(
    conf: &'static PageServerConf,
    tenant_path: &Path,
    resources: TenantSharedResources,
    init_order: Option<InitializationOrder>,
    tenants: &'static tokio::sync::RwLock<TenantsMap>,
    ctx: &RequestContext,
) -> anyhow::Result<Arc<Tenant>> {
    anyhow::ensure!(
        tenant_path.is_dir(),
        "Cannot load tenant from path {tenant_path:?}, it either does not exist or not a directory"
    );
    anyhow::ensure!(
        !crate::is_temporary(tenant_path),
        "Cannot load tenant from temporary path {tenant_path:?}"
    );
    anyhow::ensure!(
        !tenant_path.is_empty_dir().with_context(|| {
            format!("Failed to check whether {tenant_path:?} is an empty dir")
        })?,
        "Cannot load tenant from empty directory {tenant_path:?}"
    );

    let tenant_id = tenant_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<TenantId>()
        .with_context(|| {
            format!("Could not parse tenant id out of the tenant dir name in path {tenant_path:?}")
        })?;

    let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(&tenant_id);
    anyhow::ensure!(
        !conf.tenant_ignore_mark_file_path(&tenant_id).exists(),
        "Cannot load tenant, ignore mark found at {tenant_ignore_mark:?}"
    );

    let tenant = if conf.tenant_attaching_mark_file_path(&tenant_id).exists() {
        info!("tenant {tenant_id} has attaching mark file, resuming its attach operation");
        if let Some(remote_storage) = resources.remote_storage {
            match Tenant::spawn_attach(
                conf,
                tenant_id,
                resources.broker_client,
                tenants,
                remote_storage,
                ctx,
            ) {
                Ok(tenant) => tenant,
                Err(e) => {
                    error!("Failed to spawn_attach tenant {tenant_id}, reason: {e:#}");
                    Tenant::create_broken_tenant(conf, tenant_id, format!("{e:#}"))
                }
            }
        } else {
            warn!("tenant {tenant_id} has attaching mark file, but pageserver has no remote storage configured");
            Tenant::create_broken_tenant(
                conf,
                tenant_id,
                "attaching mark file present but no remote storage configured".to_string(),
            )
        }
    } else {
        info!("tenant {tenant_id} is assumed to be loadable, starting load operation");
        // Start loading the tenant into memory. It will initially be in Loading state.
        Tenant::spawn_load(conf, tenant_id, resources, init_order, tenants, ctx)
    };
    Ok(tenant)
}

///
/// Shut down all tenants. This runs as part of pageserver shutdown.
///
/// NB: We leave the tenants in the map, so that they remain accessible through
/// the management API until we shut it down. If we removed the shut-down tenants
/// from the tenants map, the management API would return 404 for these tenants,
/// because TenantsMap::get() now returns `None`.
/// That could be easily misinterpreted by control plane, the consumer of the
/// management API. For example, it could attach the tenant on a different pageserver.
/// We would then be in split-brain once this pageserver restarts.
#[instrument(skip_all)]
pub async fn shutdown_all_tenants() {
    shutdown_all_tenants0(&TENANTS).await
}

async fn shutdown_all_tenants0(tenants: &tokio::sync::RwLock<TenantsMap>) {
    use utils::completion;

    // Prevent new tenants from being created.
    let tenants_to_shut_down = {
        let mut m = tenants.write().await;
        match &mut *m {
            TenantsMap::Initializing => {
                *m = TenantsMap::ShuttingDown(HashMap::default());
                info!("tenants map is empty");
                return;
            }
            TenantsMap::Open(tenants) => {
                let tenants_clone = tenants.clone();
                *m = TenantsMap::ShuttingDown(std::mem::take(tenants));
                tenants_clone
            }
            TenantsMap::ShuttingDown(_) => {
                // TODO: it is possible that detach and shutdown happen at the same time. as a
                // result, during shutdown we do not wait for detach.
                error!("already shutting down, this function isn't supposed to be called more than once");
                return;
            }
        }
    };

    let started_at = std::time::Instant::now();
    let mut join_set = JoinSet::new();
    for (tenant_id, tenant) in tenants_to_shut_down {
        join_set.spawn(
            async move {
                let freeze_and_flush = true;

                let res = {
                    let (_guard, shutdown_progress) = completion::channel();
                    tenant.shutdown(shutdown_progress, freeze_and_flush).await
                };

                if let Err(other_progress) = res {
                    // join the another shutdown in progress
                    other_progress.wait().await;
                }

                // we cannot afford per tenant logging here, because if s3 is degraded, we are
                // going to log too many lines

                debug!("tenant successfully stopped");
            }
            .instrument(info_span!("shutdown", %tenant_id)),
        );
    }

    let total = join_set.len();
    let mut panicked = 0;
    let mut buffering = true;
    const BUFFER_FOR: std::time::Duration = std::time::Duration::from_millis(500);
    let mut buffered = std::pin::pin!(tokio::time::sleep(BUFFER_FOR));

    while !join_set.is_empty() {
        tokio::select! {
            Some(joined) = join_set.join_next() => {
                match joined {
                    Ok(()) => {}
                    Err(join_error) if join_error.is_cancelled() => {
                        unreachable!("we are not cancelling any of the futures");
                    }
                    Err(join_error) if join_error.is_panic() => {
                        // cannot really do anything, as this panic is likely a bug
                        panicked += 1;
                    }
                    Err(join_error) => {
                        warn!("unknown kind of JoinError: {join_error}");
                    }
                }
                if !buffering {
                    // buffer so that every 500ms since the first update (or starting) we'll log
                    // how far away we are; this is because we will get SIGKILL'd at 10s, and we
                    // are not able to log *then*.
                    buffering = true;
                    buffered.as_mut().reset(tokio::time::Instant::now() + BUFFER_FOR);
                }
            },
            _ = &mut buffered, if buffering => {
                buffering = false;
                info!(remaining = join_set.len(), total, elapsed_ms = started_at.elapsed().as_millis(), "waiting for tenants to shutdown");
            }
        }
    }

    if panicked > 0 {
        warn!(
            panicked,
            total, "observed panicks while shutting down tenants"
        );
    }

    // caller will log how long we took
}

pub async fn create_tenant(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
    broker_client: storage_broker::BrokerClientChannel,
    remote_storage: Option<GenericRemoteStorage>,
    ctx: &RequestContext,
) -> Result<Arc<Tenant>, TenantMapInsertError> {
    tenant_map_insert(tenant_id, || {
        // We're holding the tenants lock in write mode while doing local IO.
        // If this section ever becomes contentious, introduce a new `TenantState::Creating`
        // and do the work in that state.
        let tenant_directory = super::create_tenant_files(conf, tenant_conf, &tenant_id, CreateTenantFilesMode::Create)?;
        // TODO: tenant directory remains on disk if we bail out from here on.
        //       See https://github.com/neondatabase/neon/issues/4233

        let tenant_resources = TenantSharedResources {
            broker_client,
            remote_storage,
        };
        let created_tenant =
            schedule_local_tenant_processing(conf, &tenant_directory, tenant_resources, None, &TENANTS, ctx)?;
        // TODO: tenant object & its background loops remain, untracked in tenant map, if we fail here.
        //      See https://github.com/neondatabase/neon/issues/4233

        let crated_tenant_id = created_tenant.tenant_id();
        anyhow::ensure!(
                tenant_id == crated_tenant_id,
                "loaded created tenant has unexpected tenant id (expect {tenant_id} != actual {crated_tenant_id})",
            );
        Ok(created_tenant)
    }).await
}

#[derive(Debug, thiserror::Error)]
pub enum SetNewTenantConfigError {
    #[error(transparent)]
    GetTenant(#[from] GetTenantError),
    #[error(transparent)]
    Persist(anyhow::Error),
}

pub async fn set_new_tenant_config(
    conf: &'static PageServerConf,
    new_tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
) -> Result<(), SetNewTenantConfigError> {
    info!("configuring tenant {tenant_id}");
    let tenant = get_tenant(tenant_id, true).await?;

    let tenant_config_path = conf.tenant_config_path(&tenant_id);
    Tenant::persist_tenant_config(&tenant_id, &tenant_config_path, new_tenant_conf, false)
        .map_err(SetNewTenantConfigError::Persist)?;
    tenant.set_new_tenant_config(new_tenant_conf);
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum GetTenantError {
    #[error("Tenant {0} not found")]
    NotFound(TenantId),
    #[error("Tenant {0} is not active")]
    NotActive(TenantId),
}

/// Gets the tenant from the in-memory data, erroring if it's absent or is not fitting to the query.
/// `active_only = true` allows to query only tenants that are ready for operations, erroring on other kinds of tenants.
pub async fn get_tenant(
    tenant_id: TenantId,
    active_only: bool,
) -> Result<Arc<Tenant>, GetTenantError> {
    let m = TENANTS.read().await;
    let tenant = m
        .get(&tenant_id)
        .ok_or(GetTenantError::NotFound(tenant_id))?;
    if active_only && !tenant.is_active() {
        Err(GetTenantError::NotActive(tenant_id))
    } else {
        Ok(Arc::clone(tenant))
    }
}

pub async fn delete_tenant(
    conf: &'static PageServerConf,
    remote_storage: Option<GenericRemoteStorage>,
    tenant_id: TenantId,
) -> Result<(), DeleteTenantError> {
    DeleteTenantFlow::run(conf, remote_storage, &TENANTS, tenant_id).await
}

#[derive(Debug, thiserror::Error)]
pub enum DeleteTimelineError {
    #[error("Tenant {0}")]
    Tenant(#[from] GetTenantError),

    #[error("Timeline {0}")]
    Timeline(#[from] crate::tenant::DeleteTimelineError),
}

pub async fn delete_timeline(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    _ctx: &RequestContext,
) -> Result<(), DeleteTimelineError> {
    let tenant = get_tenant(tenant_id, true).await?;
    DeleteTimelineFlow::run(&tenant, timeline_id, false).await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum TenantStateError {
    #[error("Tenant {0} not found")]
    NotFound(TenantId),
    #[error("Tenant {0} is stopping")]
    IsStopping(TenantId),
    #[error("Tenant {0} is not active")]
    NotActive(TenantId),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub async fn detach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    detach_ignored: bool,
) -> Result<(), TenantStateError> {
    detach_tenant0(conf, &TENANTS, tenant_id, detach_ignored).await
}

async fn detach_tenant0(
    conf: &'static PageServerConf,
    tenants: &tokio::sync::RwLock<TenantsMap>,
    tenant_id: TenantId,
    detach_ignored: bool,
) -> Result<(), TenantStateError> {
    let local_files_cleanup_operation = |tenant_id_to_clean| async move {
        let local_tenant_directory = conf.tenant_path(&tenant_id_to_clean);
        fs::remove_dir_all(&local_tenant_directory)
            .await
            .with_context(|| {
                format!("local tenant directory {local_tenant_directory:?} removal")
            })?;
        Ok(())
    };

    let removal_result =
        remove_tenant_from_memory(tenants, tenant_id, local_files_cleanup_operation(tenant_id))
            .await;

    // Ignored tenants are not present in memory and will bail the removal from memory operation.
    // Before returning the error, check for ignored tenant removal case — we only need to clean its local files then.
    if detach_ignored && matches!(removal_result, Err(TenantStateError::NotFound(_))) {
        let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(&tenant_id);
        if tenant_ignore_mark.exists() {
            info!("Detaching an ignored tenant");
            local_files_cleanup_operation(tenant_id)
                .await
                .with_context(|| format!("Ignored tenant {tenant_id} local files cleanup"))?;
            return Ok(());
        }
    }

    removal_result
}

pub async fn load_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    broker_client: storage_broker::BrokerClientChannel,
    remote_storage: Option<GenericRemoteStorage>,
    ctx: &RequestContext,
) -> Result<(), TenantMapInsertError> {
    tenant_map_insert(tenant_id, || {
        let tenant_path = conf.tenant_path(&tenant_id);
        let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(&tenant_id);
        if tenant_ignore_mark.exists() {
            std::fs::remove_file(&tenant_ignore_mark)
                .with_context(|| format!("Failed to remove tenant ignore mark {tenant_ignore_mark:?} during tenant loading"))?;
        }

        let resources = TenantSharedResources {
            broker_client,
            remote_storage,
        };
        let new_tenant = schedule_local_tenant_processing(conf, &tenant_path,  resources, None,  &TENANTS, ctx)
            .with_context(|| {
                format!("Failed to schedule tenant processing in path {tenant_path:?}")
            })?;

        Ok(new_tenant)
    }).await?;
    Ok(())
}

pub async fn ignore_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
) -> Result<(), TenantStateError> {
    ignore_tenant0(conf, &TENANTS, tenant_id).await
}

async fn ignore_tenant0(
    conf: &'static PageServerConf,
    tenants: &tokio::sync::RwLock<TenantsMap>,
    tenant_id: TenantId,
) -> Result<(), TenantStateError> {
    remove_tenant_from_memory(tenants, tenant_id, async {
        let ignore_mark_file = conf.tenant_ignore_mark_file_path(&tenant_id);
        fs::File::create(&ignore_mark_file)
            .await
            .context("Failed to create ignore mark file")
            .and_then(|_| {
                crashsafe::fsync_file_and_parent(&ignore_mark_file)
                    .context("Failed to fsync ignore mark file")
            })
            .with_context(|| format!("Failed to crate ignore mark for tenant {tenant_id}"))?;
        Ok(())
    })
    .await
}

#[derive(Debug, thiserror::Error)]
pub enum TenantMapListError {
    #[error("tenant map is still initiailizing")]
    Initializing,
}

///
/// Get list of tenants, for the mgmt API
///
pub async fn list_tenants() -> Result<Vec<(TenantId, TenantState)>, TenantMapListError> {
    let tenants = TENANTS.read().await;
    let m = match &*tenants {
        TenantsMap::Initializing => return Err(TenantMapListError::Initializing),
        TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => m,
    };
    Ok(m.iter()
        .map(|(id, tenant)| (*id, tenant.current_state()))
        .collect())
}

/// Execute Attach mgmt API command.
///
/// Downloading all the tenant data is performed in the background, this merely
/// spawns the background task and returns quickly.
pub async fn attach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    tenant_conf: TenantConfOpt,
    broker_client: storage_broker::BrokerClientChannel,
    remote_storage: GenericRemoteStorage,
    ctx: &RequestContext,
) -> Result<(), TenantMapInsertError> {
    tenant_map_insert(tenant_id, || {
        let tenant_dir = create_tenant_files(conf, tenant_conf, &tenant_id, CreateTenantFilesMode::Attach)?;
        // TODO: tenant directory remains on disk if we bail out from here on.
        //       See https://github.com/neondatabase/neon/issues/4233

        // Without the attach marker, schedule_local_tenant_processing will treat the attached tenant as fully attached
        let marker_file_exists = conf
            .tenant_attaching_mark_file_path(&tenant_id)
            .try_exists()
            .context("check for attach marker file existence")?;
        anyhow::ensure!(marker_file_exists, "create_tenant_files should have created the attach marker file");

        let resources = TenantSharedResources {
            broker_client,
            remote_storage: Some(remote_storage),
        };
        let attached_tenant = schedule_local_tenant_processing(conf, &tenant_dir, resources, None, &TENANTS, ctx)?;
        // TODO: tenant object & its background loops remain, untracked in tenant map, if we fail here.
        //      See https://github.com/neondatabase/neon/issues/4233

        let attached_tenant_id = attached_tenant.tenant_id();
        anyhow::ensure!(
            tenant_id == attached_tenant_id,
            "loaded created tenant has unexpected tenant id (expect {tenant_id} != actual {attached_tenant_id})",
        );
        Ok(attached_tenant)
    })
    .await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum TenantMapInsertError {
    #[error("tenant map is still initializing")]
    StillInitializing,
    #[error("tenant map is shutting down")]
    ShuttingDown,
    #[error("tenant {0} already exists, state: {1:?}")]
    TenantAlreadyExists(TenantId, TenantState),
    #[error(transparent)]
    Closure(#[from] anyhow::Error),
}

/// Give the given closure access to the tenants map entry for the given `tenant_id`, iff that
/// entry is vacant. The closure is responsible for creating the tenant object and inserting
/// it into the tenants map through the vacnt entry that it receives as argument.
///
/// NB: the closure should return quickly because the current implementation of tenants map
/// serializes access through an `RwLock`.
async fn tenant_map_insert<F>(
    tenant_id: TenantId,
    insert_fn: F,
) -> Result<Arc<Tenant>, TenantMapInsertError>
where
    F: FnOnce() -> anyhow::Result<Arc<Tenant>>,
{
    let mut guard = TENANTS.write().await;
    let m = match &mut *guard {
        TenantsMap::Initializing => return Err(TenantMapInsertError::StillInitializing),
        TenantsMap::ShuttingDown(_) => return Err(TenantMapInsertError::ShuttingDown),
        TenantsMap::Open(m) => m,
    };
    match m.entry(tenant_id) {
        hash_map::Entry::Occupied(e) => Err(TenantMapInsertError::TenantAlreadyExists(
            tenant_id,
            e.get().current_state(),
        )),
        hash_map::Entry::Vacant(v) => match insert_fn() {
            Ok(tenant) => {
                v.insert(tenant.clone());
                Ok(tenant)
            }
            Err(e) => Err(TenantMapInsertError::Closure(e)),
        },
    }
}

/// Stops and removes the tenant from memory, if it's not [`TenantState::Stopping`] already, bails otherwise.
/// Allows to remove other tenant resources manually, via `tenant_cleanup`.
/// If the cleanup fails, tenant will stay in memory in [`TenantState::Broken`] state, and another removal
/// operation would be needed to remove it.
async fn remove_tenant_from_memory<V, F>(
    tenants: &tokio::sync::RwLock<TenantsMap>,
    tenant_id: TenantId,
    tenant_cleanup: F,
) -> Result<V, TenantStateError>
where
    F: std::future::Future<Output = anyhow::Result<V>>,
{
    use utils::completion;

    // It's important to keep the tenant in memory after the final cleanup, to avoid cleanup races.
    // The exclusive lock here ensures we don't miss the tenant state updates before trying another removal.
    // tenant-wde cleanup operations may take some time (removing the entire tenant directory), we want to
    // avoid holding the lock for the entire process.
    let tenant = {
        tenants
            .write()
            .await
            .get(&tenant_id)
            .cloned()
            .ok_or(TenantStateError::NotFound(tenant_id))?
    };

    // allow pageserver shutdown to await for our completion
    let (_guard, progress) = completion::channel();

    // whenever we remove a tenant from memory, we don't want to flush and wait for upload
    let freeze_and_flush = false;

    // shutdown is sure to transition tenant to stopping, and wait for all tasks to complete, so
    // that we can continue safely to cleanup.
    match tenant.shutdown(progress, freeze_and_flush).await {
        Ok(()) => {}
        Err(_other) => {
            // if pageserver shutdown or other detach/ignore is already ongoing, we don't want to
            // wait for it but return an error right away because these are distinct requests.
            return Err(TenantStateError::IsStopping(tenant_id));
        }
    }

    match tenant_cleanup
        .await
        .with_context(|| format!("Failed to run cleanup for tenant {tenant_id}"))
    {
        Ok(hook_value) => {
            let mut tenants_accessor = tenants.write().await;
            if tenants_accessor.remove(&tenant_id).is_none() {
                warn!("Tenant {tenant_id} got removed from memory before operation finished");
            }
            Ok(hook_value)
        }
        Err(e) => {
            let tenants_accessor = tenants.read().await;
            match tenants_accessor.get(&tenant_id) {
                Some(tenant) => {
                    tenant.set_broken(e.to_string()).await;
                }
                None => {
                    warn!("Tenant {tenant_id} got removed from memory");
                    return Err(TenantStateError::NotFound(tenant_id));
                }
            }
            Err(TenantStateError::Other(e))
        }
    }
}

use {
    crate::repository::GcResult, pageserver_api::models::TimelineGcRequest,
    utils::http::error::ApiError,
};

pub async fn immediate_gc(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    gc_req: TimelineGcRequest,
    ctx: &RequestContext,
) -> Result<tokio::sync::oneshot::Receiver<Result<GcResult, anyhow::Error>>, ApiError> {
    let guard = TENANTS.read().await;
    let tenant = guard
        .get(&tenant_id)
        .map(Arc::clone)
        .with_context(|| format!("tenant {tenant_id}"))
        .map_err(|e| ApiError::NotFound(e.into()))?;

    let gc_horizon = gc_req.gc_horizon.unwrap_or_else(|| tenant.get_gc_horizon());
    // Use tenant's pitr setting
    let pitr = tenant.get_pitr_interval();

    // Run in task_mgr to avoid race with tenant_detach operation
    let ctx = ctx.detached_child(TaskKind::GarbageCollector, DownloadBehavior::Download);
    let (task_done, wait_task_done) = tokio::sync::oneshot::channel();
    task_mgr::spawn(
        &tokio::runtime::Handle::current(),
        TaskKind::GarbageCollector,
        Some(tenant_id),
        Some(timeline_id),
        &format!("timeline_gc_handler garbage collection run for tenant {tenant_id} timeline {timeline_id}"),
        false,
        async move {
            fail::fail_point!("immediate_gc_task_pre");
            let result = tenant
                .gc_iteration(Some(timeline_id), gc_horizon, pitr, &ctx)
                .instrument(info_span!("manual_gc", %tenant_id, %timeline_id))
                .await;
                // FIXME: `gc_iteration` can return an error for multiple reasons; we should handle it
                // better once the types support it.
            match task_done.send(result) {
                Ok(_) => (),
                Err(result) => error!("failed to send gc result: {result:?}"),
            }
            Ok(())
        }
    );

    // drop the guard until after we've spawned the task so that timeline shutdown will wait for the task
    drop(guard);

    Ok(wait_task_done)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tracing::{info_span, Instrument};

    use super::{super::harness::TenantHarness, TenantsMap};

    #[tokio::test(start_paused = true)]
    async fn shutdown_joins_remove_tenant_from_memory() {
        // the test is a bit ugly with the lockstep together with spawned tasks. the aim is to make
        // sure `shutdown_all_tenants0` per-tenant processing joins in any active
        // remove_tenant_from_memory calls, which is enforced by making the operation last until
        // we've ran `shutdown_all_tenants0` for a long time.

        let (t, _ctx) = TenantHarness::create("shutdown_joins_detach")
            .unwrap()
            .load()
            .await;

        // harness loads it to active, which is forced and nothing is running on the tenant

        let id = t.tenant_id();

        // tenant harness configures the logging and we cannot escape it
        let _e = info_span!("testing", tenant_id = %id).entered();

        let tenants = HashMap::from([(id, t.clone())]);
        let tenants = Arc::new(tokio::sync::RwLock::new(TenantsMap::Open(tenants)));

        let (until_cleanup_completed, can_complete_cleanup) = utils::completion::channel();
        let (until_cleanup_started, cleanup_started) = utils::completion::channel();

        // start a "detaching operation", which will take a while, until can_complete_cleanup
        let cleanup_task = {
            let jh = tokio::spawn({
                let tenants = tenants.clone();
                async move {
                    let cleanup = async move {
                        drop(until_cleanup_started);
                        can_complete_cleanup.wait().await;
                        anyhow::Ok(())
                    };
                    super::remove_tenant_from_memory(&tenants, id, cleanup).await
                }
                .instrument(info_span!("foobar", tenant_id = %id))
            });

            // now the long cleanup should be in place, with the stopping state
            cleanup_started.wait().await;
            jh
        };

        let mut cleanup_progress = std::pin::pin!(t
            .shutdown(utils::completion::Barrier::default(), false)
            .await
            .unwrap_err()
            .wait());

        let mut shutdown_task = {
            let (until_shutdown_started, shutdown_started) = utils::completion::channel();

            let shutdown_task = tokio::spawn(async move {
                drop(until_shutdown_started);
                super::shutdown_all_tenants0(&tenants).await;
            });

            shutdown_started.wait().await;
            shutdown_task
        };

        // if the joining in is removed from shutdown_all_tenants0, the shutdown_task should always
        // get to complete within timeout and fail the test. it is expected to continue awaiting
        // until completion or SIGKILL during normal shutdown.
        //
        // the timeout is long to cover anything that shutdown_task could be doing, but it is
        // handled instantly because we use tokio's time pausing in this test. 100s is much more than
        // what we get from systemd on shutdown (10s).
        let long_time = std::time::Duration::from_secs(100);
        tokio::select! {
            _ = &mut shutdown_task => unreachable!("shutdown must continue, until_cleanup_completed is not dropped"),
            _ = &mut cleanup_progress => unreachable!("cleanup progress must continue, until_cleanup_completed is not dropped"),
            _ = tokio::time::sleep(long_time) => {},
        }

        // allow the remove_tenant_from_memory and thus eventually the shutdown to continue
        drop(until_cleanup_completed);

        let (je, ()) = tokio::join!(shutdown_task, cleanup_progress);
        je.expect("Tenant::shutdown shutdown not have panicked");
        cleanup_task
            .await
            .expect("no panicking")
            .expect("remove_tenant_from_memory failed");

        futures::future::poll_immediate(
            t.shutdown(utils::completion::Barrier::default(), false)
                .await
                .unwrap_err()
                .wait(),
        )
        .await
        .expect("the stopping progress must still be complete");
    }
}
