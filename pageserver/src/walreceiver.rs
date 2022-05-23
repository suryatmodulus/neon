//! WAL receiver manages an open connection to safekeeper, to get the WAL it streams into.
//! To do so, a current implementation needs to do the following:
//!
//! * acknowledge the timelines that it needs to stream WAL into.
//! Pageserver is able to dynamically (un)load tenants on attach and detach,
//! hence WAL receiver needs to react on such events.
//!
//! * determine that a timeline needs WAL streaming.
//! For that, it watches specific keys in etcd broker and pulls the relevant data periodically.
//! The data is produced by safekeepers, that push it periodically and pull it to synchronize between each other.
//! Without this data, no WAL streaming is possible currently.
//!
//! Only one active WAL streaming connection is allowed at a time.
//! The connection is supposed to be updated periodically, based on safekeeper timeline data.
//!
//! * handle the actual connection and WAL streaming
//!
//! Handle happens dynamically, by portions of WAL being processed and registered in the server.
//! Along with the registration, certain metadata is written to show WAL streaming progress and rely on that when considering safekeepers for connection.
//!
//! ## Implementation details
//!
//! WAL receiver's implementation consists of 3 kinds of nested loops, separately handling the logic from the bullets above:
//!
//! * [`init_wal_receiver_main_thread`], a wal receiver main thread, containing the control async loop: timeline addition/removal and interruption of a whole thread handling.
//! The loop is infallible, always trying to continue with the new tasks, the only place where it can fail is its initialization.
//! All of the code inside the loop is either async or a spawn_blocking wrapper around the sync code.
//!
//! * [`start_timeline_wal_broker_loop`], a broker task, handling the etcd broker subscription and polling, safekeeper selection logic and [re]connects.
//! On every concequent broker/wal streamer connection attempt, the loop steps are forced to wait for some time before running,
//! increasing with the number of attempts (capped with some fixed value).
//! This is done endlessly, to ensure we don't miss the WAL streaming when it gets available on one of the safekeepers.
//!
//! Apart from the broker management, it keeps the wal streaming connection open, with the safekeeper having the most advanced timeline state.
//! The connection could be closed from safekeeper side (with error or not), could be cancelled from pageserver side from time to time.
//!
//! * [`connection_handler::handle_walreceiver_connection`], a wal streaming task, opening the libpq conneciton and reading the data out of it to the end.
//! Does periodic reporting of the progress, to share some of the data via external HTTP API and to ensure we're able to switch connections when needed.
//!
//! Every task is cancellable via its separate cancellation channel,
//! also every such task's dependency (broker subscription or the data source channel) cancellation/drop triggers the corresponding task cancellation either.

mod connection_handler;

use crate::config::PageServerConf;
use crate::http::models::WalReceiverEntry;
use crate::repository::Timeline;
use crate::tenant_mgr::{self, LocalTimelineUpdate, TenantState};
use crate::thread_mgr::ThreadKind;
use crate::{thread_mgr, DatadirTimelineImpl};
use anyhow::{ensure, Context};
use chrono::{NaiveDateTime, Utc};
use etcd_broker::{Client, SkTimelineInfo, SkTimelineSubscriptionKind};
use itertools::Itertools;
use once_cell::sync::Lazy;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::thread_local;
use std::time::Duration;
use tokio::select;
use tokio::{
    sync::{mpsc, watch, RwLock},
    task::JoinHandle,
};
use tracing::*;
use url::Url;
use utils::lsn::Lsn;
use utils::pq_proto::ZenithFeedback;
use utils::zid::{ZNodeId, ZTenantId, ZTenantTimelineId, ZTimelineId};

use self::connection_handler::{WalConnectionEvent, WalReceiverConnection};

thread_local! {
    // Boolean that is true only for WAL receiver threads
    //
    // This is used in `wait_lsn` to guard against usage that might lead to a deadlock.
    pub(crate) static IS_WAL_RECEIVER: Cell<bool> = Cell::new(false);
}

/// WAL receiver state for sharing with the outside world.
/// Only entries for timelines currently available in pageserver are stored.
static WAL_RECEIVER_ENTRIES: Lazy<RwLock<HashMap<ZTenantTimelineId, WalReceiverEntry>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Gets the public WAL streaming entry for a certain timeline.
pub async fn get_wal_receiver_entry(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) -> Option<WalReceiverEntry> {
    WAL_RECEIVER_ENTRIES
        .read()
        .await
        .get(&ZTenantTimelineId::new(tenant_id, timeline_id))
        .cloned()
}

/// Sets up the main WAL receiver thread that manages the rest of the subtasks inside of it, per timeline.
/// See comments in [`wal_receiver_main_thread_loop_step`] for more details on per timeline activities.
pub fn init_wal_receiver_main_thread(
    conf: &'static PageServerConf,
    mut timeline_updates_receiver: mpsc::UnboundedReceiver<LocalTimelineUpdate>,
) -> anyhow::Result<()> {
    let etcd_endpoints = conf.broker_endpoints.clone();
    ensure!(
        !etcd_endpoints.is_empty(),
        "Cannot start wal receiver: etcd endpoints are empty"
    );
    let broker_prefix = &conf.broker_etcd_prefix;
    info!(
        "Starting wal receiver main thread, etdc endpoints: {}",
        etcd_endpoints.iter().map(Url::to_string).join(", ")
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to create storage sync runtime")?;
    let mut etcd_client = runtime
        .block_on(etcd_broker::Client::connect(etcd_endpoints, None))
        .context("Failed to connect to etcd")?;

    thread_mgr::spawn(
        ThreadKind::WalReceiverManager,
        None,
        None,
        "WAL receiver manager main thread",
        true,
        move || {
            let mut local_timeline_wal_receivers = HashMap::new();
            loop {
                let should_break = runtime.block_on(async {
                    select! {
                        // check for shutdown first
                        biased;
                        _ = thread_mgr::shutdown_watcher() => {
                            info!("wal_receiver_main: Shutdown signal received");
                            shutdown_all_wal_connections(&mut local_timeline_wal_receivers).instrument(info_span!("wal_receiver_main")).await;
                            true
                        },
                        _ = wal_receiver_main_thread_loop_step(
                            broker_prefix,
                            &mut etcd_client,
                            &mut timeline_updates_receiver,
                            &mut local_timeline_wal_receivers,
                        ).instrument(info_span!("wal_receiver_main")) => false,
                    }
                });

                if should_break {
                    break;
                }
            }

            info!("Wal receiver main thread stopped");
            Ok(())
        },
    )
    .map(|_thread_id| ())
    .context("Failed to spawn wal receiver main thread")
}

/// A step to process timeline attach/detach events to enable/disable the corresponding WAL receiver machinery.
/// In addition to WAL streaming management, the step ensures that corresponding tenant has its service threads enabled or disabled.
/// This is done here, since only walreceiver knows when a certain tenant has no streaming enabled.
///
/// Cannot fail, should always try to process the next timeline event even if the other one was not processed properly.
async fn wal_receiver_main_thread_loop_step<'a>(
    broker_prefix: &str,
    etcd_client: &mut Client,
    timeline_updates_receiver: &'a mut mpsc::UnboundedReceiver<LocalTimelineUpdate>,
    local_timeline_wal_receivers: &'a mut HashMap<
        ZTenantId,
        HashMap<ZTimelineId, TimelineWalBrokerLoopHandles>,
    >,
) {
    // Only react on updates from [`tenant_mgr`] on local timeline attach/detach.
    match timeline_updates_receiver.recv().await {
        Some(update) => {
            info!("Processing timeline update: {update:?}");
            match update {
                // Timeline got detached, stop all related tasks and remove public timeline data.
                LocalTimelineUpdate::Detach(id) => {
                    match local_timeline_wal_receivers.get_mut(&id.tenant_id) {
                        Some(wal_receivers) => {
                            if let Some(handles) = wal_receivers.remove(&id.timeline_id) {
                                if let Err(e) = handles.shutdown(id).await {
                                    error!("Failed to shut down timeline {id} wal receiver handle: {e:#}");
                                    return;
                                }
                            };
                            if wal_receivers.is_empty() {
                                if let Err(e) = change_tenant_state(id.tenant_id, TenantState::Idle).await {
                                    error!("Failed to make tenant idle for id {id}: {e:#}");
                                }
                            }
                        }
                        None => warn!("Timeline {id} does not have a tenant entry in wal receiver main thread"),
                    };
                    {
                        WAL_RECEIVER_ENTRIES.write().await.remove(&id);
                    }
                }
                // Timeline got attached, retrieve all necessary information to start its broker loop and maintain this loop endlessly.
                LocalTimelineUpdate::Attach(new_id, new_timeline) => {
                    let timelines = local_timeline_wal_receivers
                        .entry(new_id.tenant_id)
                        .or_default();

                    if timelines.is_empty() {
                        if let Err(e) =
                            change_tenant_state(new_id.tenant_id, TenantState::Active).await
                        {
                            error!("Failed to make tenant active for id {new_id}: {e:#}");
                            return;
                        }
                    } else if let Some(current_entry_handles) =
                        timelines.remove(&new_id.timeline_id)
                    {
                        warn!(
                            "Readding to an existing timeline {new_id}, shutting the old wal receiver down"
                        );
                        if let Err(e) = current_entry_handles.shutdown(new_id).await {
                            error!(
                                "Failed to shut down timeline {new_id} wal receiver handle: {e:#}"
                            );
                            return;
                        }
                    }

                    let (wal_connect_timeout, lagging_wal_timeout, max_lsn_wal_lag) =
                        match fetch_tenant_settings(new_id.tenant_id).await {
                            Ok(settings) => settings,
                            Err(e) => {
                                error!("Failed to fetch tenant settings for id {new_id}: {e:#}");
                                return;
                            }
                        };

                    {
                        WAL_RECEIVER_ENTRIES.write().await.insert(
                            new_id,
                            WalReceiverEntry {
                                wal_producer_connstr: None,
                                last_received_msg_lsn: None,
                                last_received_msg_ts: None,
                            },
                        );
                    }

                    // Endlessly try to connect to subscribe for broker updates for a given timeline.
                    // If there are no safekeepers to maintain the lease, the timeline subscription will be inavailable in the broker and the operation will fail constantly.
                    // This is ok, pageservers should anyway try subscribing (with some backoff) since it's the only way they can get the timeline WAL anyway.
                    //
                    // When a task is spawned, it will loop until the subscription is cancelled, retrieving the timeline updates and deciding on the WAL connection state.
                    for attempt in 0.. {
                        exponential_backoff(attempt, 2.0, 60.0).await;
                        let wal_connection_manager = WalConnectionManager {
                            id: new_id,
                            timeline: Arc::clone(&new_timeline),
                            wal_connect_timeout,
                            lagging_wal_timeout,
                            max_lsn_wal_lag,
                            wal_connection_data: None,
                            wal_connection_attempt: 0,
                        };
                        match start_timeline_wal_broker_loop(
                            broker_prefix,
                            etcd_client,
                            wal_connection_manager,
                        )
                        .await
                        {
                            Ok(new_handles) => {
                                timelines.insert(new_id.timeline_id, new_handles);
                                break;
                            }
                            Err(e) => {
                                warn!("Attempt #{attempt}, failed to start timeline {new_id} WAL broker: {e:#}")
                            }
                        }
                    }
                }
            }
        }
        None => {
            info!("Local timeline update channel closed");
            shutdown_all_wal_connections(local_timeline_wal_receivers).await;
        }
    }
}

async fn fetch_tenant_settings(
    tenant_id: ZTenantId,
) -> anyhow::Result<(Duration, Duration, NonZeroU64)> {
    tokio::task::spawn_blocking(move || {
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)
            .with_context(|| format!("no repository found for tenant {tenant_id}"))?;
        Ok::<_, anyhow::Error>((
            repo.get_wal_receiver_connect_timeout(),
            repo.get_lagging_wal_timeout(),
            repo.get_max_lsn_wal_lag(),
        ))
    })
    .await
    .with_context(|| format!("Failed to join on tenant {tenant_id} settings fetch task"))?
}

async fn change_tenant_state(tenant_id: ZTenantId, new_state: TenantState) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || {
        tenant_mgr::set_tenant_state(tenant_id, new_state)
            .with_context(|| format!("Failed to activate tenant {tenant_id}"))
    })
    .await
    .with_context(|| format!("Failed to spawn activation task for tenant {tenant_id}"))?
}

async fn exponential_backoff(n: u32, base: f64, max_seconds: f64) {
    if n == 0 {
        return;
    }
    let seconds_to_wait = base.powf(f64::from(n) - 1.0).min(max_seconds);
    info!("Backpressure: waiting {seconds_to_wait} seconds before proceeding with the task");
    tokio::time::sleep(Duration::from_secs_f64(seconds_to_wait)).await;
}

async fn shutdown_all_wal_connections(
    local_timeline_wal_receivers: &mut HashMap<
        ZTenantId,
        HashMap<ZTimelineId, TimelineWalBrokerLoopHandles>,
    >,
) {
    info!("Shutting down all WAL connections");
    let mut broker_join_handles = Vec::new();
    for (tenant_id, timelines) in local_timeline_wal_receivers.drain() {
        for (timeline_id, handles) in timelines {
            handles.cancellation_sender.send(()).ok();
            broker_join_handles.push((
                ZTenantTimelineId::new(tenant_id, timeline_id),
                handles.broker_join_handle,
            ));
        }
    }

    let mut tenants = HashSet::with_capacity(broker_join_handles.len());
    for (id, broker_join_handle) in broker_join_handles {
        tenants.insert(id.tenant_id);
        debug!("Waiting for wal broker for timeline {id} to finish");
        if let Err(e) = broker_join_handle.await {
            error!("Failed to join on wal broker for timeline {id}: {e}");
        }
    }
    if let Err(e) = tokio::task::spawn_blocking(move || {
        for tenant_id in tenants {
            if let Err(e) = tenant_mgr::set_tenant_state(tenant_id, TenantState::Idle) {
                error!("Failed to make tenant {tenant_id} idle: {e:?}");
            }
        }
    })
    .await
    {
        error!("Failed to spawn a task to make all tenants idle: {e:?}");
    }
}

/// Broker WAL loop handles to cancel the loop safely when needed.
struct TimelineWalBrokerLoopHandles {
    broker_join_handle: JoinHandle<anyhow::Result<()>>,
    cancellation_sender: watch::Sender<()>,
}

impl TimelineWalBrokerLoopHandles {
    /// Stops the broker loop, waiting for its current task to finish.
    async fn shutdown(self, id: ZTenantTimelineId) -> anyhow::Result<()> {
        self.cancellation_sender.send(()).context(
            "Unexpected: cancellation sender is dropped before the receiver in the loop is",
        )?;
        debug!("Waiting for wal receiver for timeline {id} to finish");
        self.broker_join_handle
            .await
            .with_context(|| format!("Failed to join the wal reveiver broker for timeline {id}"))?
            .with_context(|| format!("Wal reveiver broker for timeline {id} failed to finish"))
    }
}

/// Attempts to subscribe for timeline updates, pushed by safekeepers into the broker.
/// Based on the updates, desides whether to start, keep or stop a WAL receiver task.
async fn start_timeline_wal_broker_loop(
    broker_prefix: &str,
    etcd_client: &mut Client,
    mut wal_connection_manager: WalConnectionManager,
) -> anyhow::Result<TimelineWalBrokerLoopHandles> {
    let id = wal_connection_manager.id;
    let (cancellation_sender, mut cancellation_receiver) = watch::channel(());

    let mut broker_subscription = etcd_broker::subscribe_to_safekeeper_timeline_updates(
        etcd_client,
        SkTimelineSubscriptionKind::timeline(broker_prefix.to_owned(), id),
    )
    .instrument(info_span!("etcd_subscription"))
    .await
    .with_context(|| format!("Failed to subscribe for timeline {id} updates in etcd"))?;

    let broker_join_handle = tokio::spawn(async move {
        info!("WAL receiver broker started");

        loop {
            select! {
                // the order of the polls is especially important here, since the first task to complete gets selected and the others get dropped (cancelled).
                // place more frequetly updated tasks below to ensure the "slow" tasks are also reacted to.
                biased;
                // first, the cancellations are checked, to ensure we exit eagerly
                _ = cancellation_receiver.changed() => {
                    break;
                }
                // then, we check for new events from the WAL connection: the existing connection should either return some progress data,
                // or block, allowing other tasks in this `select!` to run first.
                //
                // We set a "timebomb" in the polling method, that waits long enough and cancels the entire loop if nothing happens during the wait.
                // The wait is only initiated when no data (or a "channel closed" data) is received from the loop, ending with the break flow return.
                // While waiting, more broker events are expected to be retrieved from etcd (currently, every safekeeper posts ~1 message/second).
                // The timebomb ensures that we don't get stuck for too long on any of the WAL/etcd event polling, rather restarting the subscription entirely.
                //
                // We cannot return here eagerly on no WAL task data, since the result will get selected to early, not allowing etcd tasks to be polled properly.
                // We cannot move etcd tasks above this select, since they are very frequent to finish and WAL events might get ignored.
                // We need WAL events to periodically update the external data, so we cannot simply await the task result on the handler here.
                wal_receiver_poll_result = wal_connection_manager.poll_connection_event_or_cancel() => match wal_receiver_poll_result {
                    ControlFlow::Break(()) => break,
                    ControlFlow::Continue(()) => {},
                },
                // finally, if no other tasks are completed, get another broker update and possibly reconnect
                updates = broker_subscription.fetch_data() => match updates {
                    Some(mut all_timeline_updates) => {
                        if let Some(subscribed_timeline_updates) = all_timeline_updates.remove(&id) {
                            match wal_connection_manager.select_connection_candidate(subscribed_timeline_updates) {
                                Some(candidate) => {
                                    info!("Switching to different safekeeper {} for timeline {id}, reason: {:?}", candidate.safekeeper_id, candidate.reason);
                                    wal_connection_manager.change_connection(candidate.safekeeper_id, candidate.wal_producer_connstr).await;
                                },
                                None => {}
                            }
                        }
                    },
                    None => {
                        info!("Subscription source end was dropped, no more updates are possible, shutting down");
                        break;
                    },
                },
            }
        }

        info!("Loop ended, shutting down");
        wal_connection_manager.close_connection().await;
        broker_subscription
            .cancel()
            .await
            .with_context(|| format!("Failed to cancel timeline {id} subscription in etcd"))?;
        Ok(())
    }.instrument(info_span!("timeline", id = %id)));

    Ok(TimelineWalBrokerLoopHandles {
        broker_join_handle,
        cancellation_sender,
    })
}

/// All data that's needed to run endless broker loop and keep the WAL streaming conneciton alive, if possible.
struct WalConnectionManager {
    id: ZTenantTimelineId,
    timeline: Arc<DatadirTimelineImpl>,
    wal_connect_timeout: Duration,
    lagging_wal_timeout: Duration,
    max_lsn_wal_lag: NonZeroU64,
    wal_connection_attempt: u32,
    wal_connection_data: Option<WalConnectionData>,
}

#[derive(Debug)]
struct WalConnectionData {
    wal_producer_connstr: String,
    safekeeper_id: ZNodeId,
    connection: WalReceiverConnection,
    connection_init_time: NaiveDateTime,
    last_wal_receiver_data: Option<(ZenithFeedback, NaiveDateTime)>,
}

#[derive(Debug, PartialEq, Eq)]
struct NewWalConnectionCandidate {
    safekeeper_id: ZNodeId,
    wal_producer_connstr: String,
    reason: ReconnectReason,
}

/// Stores the reason why WAL connection was switched, for furter debugging purposes.
#[derive(Debug, PartialEq, Eq)]
enum ReconnectReason {
    NoExistingConnection,
    ConnectionStringUpdate {
        old: String,
    },
    LaggingWal {
        current_lsn: Lsn,
        new_lsn: Lsn,
        threshold: NonZeroU64,
    },
    NoWalTimeout {
        last_wal_interaction: NaiveDateTime,
        check_time: NaiveDateTime,
        threshold: Duration,
    },
}

impl WalConnectionManager {
    /// Tries to get more data from the WAL connection.
    /// If the WAL conneciton channel is dropped or no data is retrieved, a "timebomb" future is started to break the existing broker subscription.
    /// This future is intended to be used in the `select!` loop, so lengthy future normally gets dropped due to other futures completing.
    /// If not, it's better to cancel the entire "stuck" loop and start over.
    async fn poll_connection_event_or_cancel(&mut self) -> ControlFlow<(), ()> {
        let (connection_data, wal_receiver_event) = match self.wal_connection_data.as_mut() {
            Some(connection_data) => match connection_data.connection.next_event().await {
                Some(event) => (connection_data, event),
                None => {
                    warn!("WAL receiver event source stopped sending messages, waiting for other events to arrive");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    warn!("WAL receiver without a connection spent sleeping 30s without being interrupted, aborting the loop");
                    return ControlFlow::Break(());
                }
            },
            None => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                warn!("WAL receiver without a connection spent sleeping 30s without being interrupted, aborting the loop");
                return ControlFlow::Break(());
            }
        };

        match wal_receiver_event {
            WalConnectionEvent::Started => {
                self.wal_connection_attempt = 0;
            }
            WalConnectionEvent::NewWal(new_wal_data) => {
                self.wal_connection_attempt = 0;
                connection_data.last_wal_receiver_data =
                    Some((new_wal_data, Utc::now().naive_utc()));
            }
            WalConnectionEvent::End(wal_receiver_result) => {
                match wal_receiver_result {
                    Ok(()) => {
                        info!("WAL receiver task finished, reconnecting");
                        self.wal_connection_attempt = 0;
                    }
                    Err(e) => {
                        error!("WAL receiver task failed: {e:#}, reconnecting");
                        self.wal_connection_attempt += 1;
                    }
                }
                self.close_connection().await;
            }
        }

        ControlFlow::Continue(())
    }

    /// Shuts down current connection (if any), waiting for it to finish.
    async fn close_connection(&mut self) {
        if let Some(data) = self.wal_connection_data.take() {
            if let Err(e) = data.connection.shutdown().await {
                error!("Failed to shutdown wal receiver connection: {e:#}");
            }
        }
    }

    /// Shuts down the current connection (if any) and immediately starts another one with the given connection string.
    async fn change_connection(
        &mut self,
        new_safekeeper_id: ZNodeId,
        new_wal_producer_connstr: String,
    ) {
        self.close_connection().await;
        self.wal_connection_data = Some(WalConnectionData {
            wal_producer_connstr: new_wal_producer_connstr.clone(),
            safekeeper_id: new_safekeeper_id,
            connection: WalReceiverConnection::open(
                self.id,
                new_safekeeper_id,
                new_wal_producer_connstr,
                self.wal_connect_timeout,
            ),
            connection_init_time: Utc::now().naive_utc(),
            last_wal_receiver_data: None,
        });
    }

    /// Checks current state against every fetched safekeeper state of a given timeline.
    /// Returns a new candidate, if the current state is somewhat lagging, or `None` otherwise.
    /// The current rules for approving new candidates:
    /// * pick the safekeeper with biggest `commit_lsn` that's after than pageserver's latest Lsn for the timeline
    /// * if the leader is the current SK and it has a different connection url — reconnect
    /// * if the leader is a different SK and either
    ///     * no WAL updates happened after certain time (either none since the connection time or none since the last event after the connection) — reconnect
    ///     * same time amount had passed since the connection, WAL updates happened recently, but the new leader SK has timeline Lsn way ahead of the old one — reconnect
    ///
    /// This way we ensure to keep up with the most up-to-date safekeeper and don't try to jump from one safekeeper to another too frequently.
    /// Both thresholds are configured per tenant.
    fn select_connection_candidate(
        &self,
        safekeeper_timelines: HashMap<ZNodeId, SkTimelineInfo>,
    ) -> Option<NewWalConnectionCandidate> {
        let (&new_sk_id, new_sk_timeline, new_wal_producer_connstr) = safekeeper_timelines
            .iter()
            .filter(|(_, info)| {
                info.commit_lsn > Some(self.timeline.tline.get_last_record_lsn())
            })
            .filter_map(|(sk_id, info)| {
                match wal_stream_connection_string(
                    self.id,
                    info.safekeeper_connstr.as_deref()?,
                    info.pageserver_connstr.as_deref()?,
                ) {
                    Ok(connstr) => Some((sk_id, info, connstr)),
                    Err(e) => {
                        error!("Failed to create wal receiver connection string from broker data of safekeeper node {sk_id}: {e:#}");
                        None
                    }
                }
            })
            .max_by_key(|(_, info, _)| info.commit_lsn)?;

        match self.wal_connection_data.as_ref() {
            None => Some(NewWalConnectionCandidate {
                safekeeper_id: new_sk_id,
                wal_producer_connstr: new_wal_producer_connstr,
                reason: ReconnectReason::NoExistingConnection,
            }),
            Some(current_connection) => {
                let same_safekeeper = current_connection.safekeeper_id == new_sk_id;

                if same_safekeeper
                    && (current_connection.wal_producer_connstr != new_wal_producer_connstr)
                {
                    debug!("WAL producer connection string changed, old: '{}', new: '{new_wal_producer_connstr}', reconnecting",
                            current_connection.wal_producer_connstr);
                    return Some(NewWalConnectionCandidate {
                        safekeeper_id: new_sk_id,
                        wal_producer_connstr: new_wal_producer_connstr,
                        reason: ReconnectReason::ConnectionStringUpdate {
                            old: current_connection.wal_producer_connstr.clone(),
                        },
                    });
                } else if !same_safekeeper {
                    let candidate = self
                        .reason_to_reconnect(current_connection, new_sk_timeline)
                        .map(|reason| NewWalConnectionCandidate {
                            safekeeper_id: new_sk_id,
                            wal_producer_connstr: new_wal_producer_connstr,
                            reason,
                        });
                    if candidate.is_some() {
                        return candidate;
                    } else if let Some(wal_producer_connstr_from_broker) = safekeeper_timelines
                        .get(&current_connection.safekeeper_id)
                        .and_then(|info| {
                            wal_stream_connection_string(
                                self.id,
                                info.safekeeper_connstr.as_deref()?,
                                info.pageserver_connstr.as_deref()?,
                            )
                            .ok()
                        })
                    {
                        if current_connection.wal_producer_connstr
                            != wal_producer_connstr_from_broker
                        {
                            debug!("WAL producer connection string changed, old: '{}', new: '{wal_producer_connstr_from_broker}', reconnecting",
                                    current_connection.wal_producer_connstr);
                            return Some(NewWalConnectionCandidate {
                                safekeeper_id: current_connection.safekeeper_id,
                                wal_producer_connstr: wal_producer_connstr_from_broker,
                                reason: ReconnectReason::ConnectionStringUpdate {
                                    old: current_connection.wal_producer_connstr.clone(),
                                },
                            });
                        }
                    }
                }
                None
            }
        }
    }

    fn reason_to_reconnect(
        &self,
        current_connection: &WalConnectionData,
        new_sk_timeline: &SkTimelineInfo,
    ) -> Option<ReconnectReason> {
        let (last_sk_interaction_time, wal_advantage_reconnect_reason) = match current_connection
            .last_wal_receiver_data
            .as_ref()
        {
            Some((last_wal_receiver_data, data_submission_time)) => {
                let new_lsn = new_sk_timeline.commit_lsn?;
                let mut wal_lag_reason = None;
                match new_lsn.0.checked_sub(last_wal_receiver_data.ps_writelsn)
                {
                    Some(sk_lsn_advantage) => {
                        if sk_lsn_advantage >= self.max_lsn_wal_lag.get() {
                            wal_lag_reason = Some(ReconnectReason::LaggingWal { current_lsn: Lsn(last_wal_receiver_data.ps_writelsn), new_lsn, threshold: self.max_lsn_wal_lag });
                        }
                    }
                    None => debug!("Best SK candidate has its commit Lsn behind the current timeline's latest consistent Lsn"),
                }

                (*data_submission_time, wal_lag_reason)
            }
            None => (current_connection.connection_init_time, None),
        };

        let now = Utc::now().naive_utc();
        match (now - last_sk_interaction_time).to_std() {
            Ok(last_interaction) => {
                if last_interaction > self.lagging_wal_timeout {
                    return wal_advantage_reconnect_reason.or(Some(
                        ReconnectReason::NoWalTimeout {
                            last_wal_interaction: last_sk_interaction_time,
                            check_time: now,
                            threshold: self.lagging_wal_timeout,
                        },
                    ));
                }
            }
            Err(_e) => {
                warn!("Last interaction with safekeeper {} happened in the future, ignoring the candidate. Interaction time: {last_sk_interaction_time}, now: {now}",
                    current_connection.safekeeper_id);
            }
        }
        None
    }
}

fn wal_stream_connection_string(
    ZTenantTimelineId {
        tenant_id,
        timeline_id,
    }: ZTenantTimelineId,
    listen_pg_addr_str: &str,
    pageserver_connstr: &str,
) -> anyhow::Result<String> {
    let sk_connstr = format!("postgresql://no_user@{listen_pg_addr_str}/no_db");
    let me_conf = sk_connstr
        .parse::<postgres::config::Config>()
        .with_context(|| {
            format!("Failed to parse pageserver connection string '{sk_connstr}' as a postgres one")
        })?;
    let (host, port) = utils::connstring::connection_host_port(&me_conf);
    Ok(format!(
        "host={host} port={port} options='-c ztimelineid={timeline_id} ztenantid={tenant_id} pageserver_connstr={pageserver_connstr}'",
    ))
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::repository::{
        repo_harness::{RepoHarness, TIMELINE_ID},
        Repository,
    };

    use super::*;

    #[test]
    fn no_connection_no_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("no_connection_no_candidate")?;
        let mut data_manager_with_no_connection = dummy_wal_connection_manager(&harness);
        data_manager_with_no_connection.wal_connection_data = None;

        let no_candidate =
            data_manager_with_no_connection.select_connection_candidate(HashMap::from([
                (
                    ZNodeId(0),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(1)),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: None,
                        pageserver_connstr: Some("no safekeeper_connstr".to_string()),
                    },
                ),
                (
                    ZNodeId(1),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(1)),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("no pageserver_connstr".to_string()),
                        pageserver_connstr: None,
                    },
                ),
                (
                    ZNodeId(2),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: None,
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("no commit_lsn".to_string()),
                        pageserver_connstr: Some("no commit_lsn (p)".to_string()),
                    },
                ),
                (
                    ZNodeId(3),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: None,
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("no commit_lsn".to_string()),
                        pageserver_connstr: Some("no commit_lsn (p)".to_string()),
                    },
                ),
            ]));

        assert!(
            no_candidate.is_none(),
            "Expected no candidate selected out of non full data options, but got {no_candidate:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn connection_no_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("connection_no_candidate")?;

        let current_lsn = 100_000;
        let connected_sk_id = ZNodeId(0);
        let mut data_manager_with_connection = dummy_wal_connection_manager(&harness);
        let mut dummy_connection_data = dummy_connection_data(
            ZTenantTimelineId {
                tenant_id: harness.tenant_id,
                timeline_id: TIMELINE_ID,
            },
            connected_sk_id,
        )
        .await;
        let now = Utc::now().naive_utc();
        dummy_connection_data.last_wal_receiver_data = Some((
            ZenithFeedback {
                current_timeline_size: 1,
                ps_writelsn: 1,
                ps_applylsn: current_lsn,
                ps_flushlsn: 1,
                ps_replytime: SystemTime::now(),
            },
            now,
        ));
        dummy_connection_data.connection_init_time = now;
        data_manager_with_connection.wal_connection_data = Some(dummy_connection_data);

        let no_candidate =
            data_manager_with_connection.select_connection_candidate(HashMap::from([
                (
                    connected_sk_id,
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(
                            current_lsn + data_manager_with_connection.max_lsn_wal_lag.get() * 2
                        )),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                        pageserver_connstr: Some(DUMMY_PAGESERVER_CONNSTR.to_string()),
                    },
                ),
                (
                    ZNodeId(1),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(current_lsn)),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("not advanced Lsn".to_string()),
                        pageserver_connstr: Some("not advanced Lsn (p)".to_string()),
                    },
                ),
                (
                    ZNodeId(2),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(
                            current_lsn + data_manager_with_connection.max_lsn_wal_lag.get() / 2
                        )),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("not enough advanced Lsn".to_string()),
                        pageserver_connstr: Some("not enough advanced Lsn (p)".to_string()),
                    },
                ),
            ]));

        assert!(
            no_candidate.is_none(),
            "Expected no candidate selected out of valid options since candidate Lsn data is ignored and others' was not advanced enough, but got {no_candidate:?}"
        );

        Ok(())
    }

    #[test]
    fn no_connection_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("no_connection_candidate")?;
        let mut data_manager_with_no_connection = dummy_wal_connection_manager(&harness);
        data_manager_with_no_connection.wal_connection_data = None;

        let only_candidate = data_manager_with_no_connection
            .select_connection_candidate(HashMap::from([(
                ZNodeId(0),
                SkTimelineInfo {
                    last_log_term: None,
                    flush_lsn: None,
                    commit_lsn: Some(Lsn(1 + data_manager_with_no_connection
                        .max_lsn_wal_lag
                        .get())),
                    s3_wal_lsn: None,
                    remote_consistent_lsn: None,
                    peer_horizon_lsn: None,
                    safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                    pageserver_connstr: Some(DUMMY_PAGESERVER_CONNSTR.to_string()),
                },
            )]))
            .expect("Expected one candidate selected out of the only data option, but got none");
        assert_eq!(only_candidate.safekeeper_id, ZNodeId(0));
        assert_eq!(
            only_candidate.reason,
            ReconnectReason::NoExistingConnection,
            "Should select new safekeeper due to missing connection, even if there's also a lag in the wal over the threshold"
        );
        assert!(only_candidate
            .wal_producer_connstr
            .contains(DUMMY_SAFEKEEPER_CONNSTR));
        assert!(only_candidate
            .wal_producer_connstr
            .contains(DUMMY_PAGESERVER_CONNSTR));

        let selected_lsn = 100_000;
        let biggest_wal_candidate = data_manager_with_no_connection
            .select_connection_candidate(HashMap::from([
                (
                    ZNodeId(0),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(selected_lsn - 100)),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("smaller commit_lsn".to_string()),
                        pageserver_connstr: Some("smaller commit_lsn (p)".to_string()),
                    },
                ),
                (
                    ZNodeId(1),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(selected_lsn)),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                        pageserver_connstr: Some(DUMMY_PAGESERVER_CONNSTR.to_string()),
                    },
                ),
                (
                    ZNodeId(2),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(selected_lsn + 100)),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: None,
                        pageserver_connstr: Some(
                            "no safekeeper_connstr despite bigger commit_lsn".to_string(),
                        ),
                    },
                ),
            ]))
            .expect(
                "Expected one candidate selected out of multiple valid data options, but got none",
            );

        assert_eq!(biggest_wal_candidate.safekeeper_id, ZNodeId(1));
        assert_eq!(
            biggest_wal_candidate.reason,
            ReconnectReason::NoExistingConnection,
            "Should select new safekeeper due to missing connection, even if there's also a lag in the wal over the threshold"
        );
        assert!(biggest_wal_candidate
            .wal_producer_connstr
            .contains(DUMMY_SAFEKEEPER_CONNSTR));
        assert!(biggest_wal_candidate
            .wal_producer_connstr
            .contains(DUMMY_PAGESERVER_CONNSTR));

        Ok(())
    }

    #[tokio::test]
    async fn connection_url_switch() -> anyhow::Result<()> {
        let harness = RepoHarness::create("connection_url_switch")?;
        let connected_sk_id = ZNodeId(0);
        let current_lsn = 100_000;

        let mut data_manager_with_connection = dummy_wal_connection_manager(&harness);
        let mut dummy_connection_data = dummy_connection_data(
            ZTenantTimelineId {
                tenant_id: harness.tenant_id,
                timeline_id: TIMELINE_ID,
            },
            connected_sk_id,
        )
        .await;
        let current_wal_producer_connstr = dummy_connection_data.wal_producer_connstr.clone();
        let now = Utc::now().naive_utc();
        dummy_connection_data.last_wal_receiver_data = Some((
            ZenithFeedback {
                current_timeline_size: 1,
                ps_writelsn: 1,
                ps_applylsn: current_lsn,
                ps_flushlsn: 1,
                ps_replytime: SystemTime::now(),
            },
            now,
        ));
        dummy_connection_data.connection_init_time = now;
        data_manager_with_connection.wal_connection_data = Some(dummy_connection_data);

        let new_sk_connstr = "new_sk_connstr";
        let new_pg_connstr = "new_pg_connstr";
        let changed_connection_url_candidate = data_manager_with_connection
            .select_connection_candidate(HashMap::from([
                (
                    connected_sk_id,
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(current_lsn)),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some(new_sk_connstr.to_string()),
                        pageserver_connstr: Some(new_pg_connstr.to_string()),
                    },
                ),
                (
                    ZNodeId(1),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(
                            current_lsn + (data_manager_with_connection.max_lsn_wal_lag.get() / 2)
                        )),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some(
                            "advanced below threshold by Lsn safekeeper".to_string(),
                        ),
                        pageserver_connstr: Some(
                            "advanced below threshold by Lsn safekeeper".to_string(),
                        ),
                    },
                ),
            ]))
            .expect(
                "Expected one candidate selected out of multiple valid data options, but got none",
            );

        assert_eq!(
            changed_connection_url_candidate.safekeeper_id,
            connected_sk_id
        );
        assert_eq!(
            changed_connection_url_candidate.reason,
            ReconnectReason::ConnectionStringUpdate { old: current_wal_producer_connstr },
            "Should reconnect if the safekeeper currently connected to changes its conneciton url, even if the other safekeepers have more advanced WAL (but not over threshold)"
        );
        assert!(changed_connection_url_candidate
            .wal_producer_connstr
            .contains(new_sk_connstr));
        assert!(changed_connection_url_candidate
            .wal_producer_connstr
            .contains(new_pg_connstr));

        Ok(())
    }

    #[tokio::test]
    async fn lsn_wal_over_threshhold_current_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("lsn_wal_over_threshcurrent_candidate")?;
        let current_lsn = Lsn(100_000).align();

        let id = ZTenantTimelineId {
            tenant_id: harness.tenant_id,
            timeline_id: TIMELINE_ID,
        };

        let mut data_manager_with_connection = dummy_wal_connection_manager(&harness);
        let connected_sk_id = ZNodeId(0);
        let mut dummy_connection_data = dummy_connection_data(id, ZNodeId(0)).await;
        let lagging_wal_timeout =
            chrono::Duration::from_std(data_manager_with_connection.lagging_wal_timeout)?;
        let time_over_threshold =
            Utc::now().naive_utc() - lagging_wal_timeout - lagging_wal_timeout;
        dummy_connection_data.last_wal_receiver_data = Some((
            ZenithFeedback {
                current_timeline_size: 1,
                ps_writelsn: current_lsn.0,
                ps_applylsn: 1,
                ps_flushlsn: 1,
                ps_replytime: SystemTime::now(),
            },
            time_over_threshold,
        ));
        dummy_connection_data.connection_init_time = time_over_threshold;
        dummy_connection_data.wal_producer_connstr =
            wal_stream_connection_string(id, DUMMY_SAFEKEEPER_CONNSTR, DUMMY_PAGESERVER_CONNSTR)?;
        data_manager_with_connection.wal_connection_data = Some(dummy_connection_data);

        let new_lsn = Lsn(current_lsn.0 + data_manager_with_connection.max_lsn_wal_lag.get() + 1);
        let candidates = HashMap::from([
            (
                connected_sk_id,
                SkTimelineInfo {
                    last_log_term: None,
                    flush_lsn: None,
                    commit_lsn: Some(current_lsn),
                    s3_wal_lsn: None,
                    remote_consistent_lsn: None,
                    peer_horizon_lsn: None,
                    safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                    pageserver_connstr: Some(DUMMY_PAGESERVER_CONNSTR.to_string()),
                },
            ),
            (
                ZNodeId(1),
                SkTimelineInfo {
                    last_log_term: None,
                    flush_lsn: None,
                    commit_lsn: Some(new_lsn),
                    s3_wal_lsn: None,
                    remote_consistent_lsn: None,
                    peer_horizon_lsn: None,
                    safekeeper_connstr: Some("advanced by Lsn safekeeper".to_string()),
                    pageserver_connstr: Some("advanced by Lsn safekeeper (p)".to_string()),
                },
            ),
        ]);

        let over_threshcurrent_candidate = data_manager_with_connection
            .select_connection_candidate(candidates.clone())
            .expect(
                "Expected one candidate selected out of multiple valid data options, but got none",
            );

        assert_eq!(over_threshcurrent_candidate.safekeeper_id, ZNodeId(1));
        assert_eq!(
            over_threshcurrent_candidate.reason,
            ReconnectReason::LaggingWal {
                current_lsn,
                new_lsn,
                threshold: data_manager_with_connection.max_lsn_wal_lag
            },
            "Should select bigger WAL safekeeper if it starts to lag enough"
        );
        assert!(over_threshcurrent_candidate
            .wal_producer_connstr
            .contains("advanced by Lsn safekeeper"));
        assert!(over_threshcurrent_candidate
            .wal_producer_connstr
            .contains("advanced by Lsn safekeeper (p)"));

        // Reset the conneciton time to be below the threshold
        data_manager_with_connection
            .wal_connection_data
            .as_mut()
            .unwrap()
            .last_wal_receiver_data
            .as_mut()
            .unwrap()
            .1 = Utc::now().naive_utc();
        let no_candidate = data_manager_with_connection.select_connection_candidate(candidates);
        assert!(
            no_candidate.is_none(),
            "Should not select any candidate if the current connection is over Lsn WAL threshold, but not over time WAL threshold, but got: {no_candidate:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn timeout_wal_over_threshcurrent_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("timeout_wal_over_threshcurrent_candidate")?;
        let current_lsn = Lsn(100_000).align();

        let id = ZTenantTimelineId {
            tenant_id: harness.tenant_id,
            timeline_id: TIMELINE_ID,
        };

        let mut data_manager_with_connection = dummy_wal_connection_manager(&harness);
        let mut dummy_connection_data = dummy_connection_data(id, ZNodeId(1)).await;
        let lagging_wal_timeout =
            chrono::Duration::from_std(data_manager_with_connection.lagging_wal_timeout)?;
        let time_over_threshold =
            Utc::now().naive_utc() - lagging_wal_timeout - lagging_wal_timeout;
        dummy_connection_data.last_wal_receiver_data = None;
        dummy_connection_data.connection_init_time = time_over_threshold;
        dummy_connection_data.wal_producer_connstr =
            wal_stream_connection_string(id, DUMMY_SAFEKEEPER_CONNSTR, DUMMY_PAGESERVER_CONNSTR)?;
        data_manager_with_connection.wal_connection_data = Some(dummy_connection_data);

        let new_lsn = Lsn(current_lsn.0 + data_manager_with_connection.max_lsn_wal_lag.get() + 1);
        let over_threshcurrent_candidate = data_manager_with_connection
            .select_connection_candidate(HashMap::from([
                (
                    ZNodeId(0),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(new_lsn),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                        pageserver_connstr: Some(DUMMY_PAGESERVER_CONNSTR.to_string()),
                    },
                ),
                (
                    ZNodeId(1),
                    SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(current_lsn),
                        s3_wal_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("not advanced by Lsn safekeeper".to_string()),
                        pageserver_connstr: Some("not advanced by Lsn safekeeper".to_string()),
                    },
                ),
            ]))
            .expect(
                "Expected one candidate selected out of multiple valid data options, but got none",
            );

        assert_eq!(over_threshcurrent_candidate.safekeeper_id, ZNodeId(0));
        match over_threshcurrent_candidate.reason {
            ReconnectReason::NoWalTimeout {
                last_wal_interaction,
                threshold,
                ..
            } => {
                assert_eq!(last_wal_interaction, time_over_threshold);
                assert_eq!(threshold, data_manager_with_connection.lagging_wal_timeout);
            }
            unexpected => panic!("Unexpected reason: {unexpected:?}"),
        }
        assert!(over_threshcurrent_candidate
            .wal_producer_connstr
            .contains(DUMMY_SAFEKEEPER_CONNSTR));
        assert!(over_threshcurrent_candidate
            .wal_producer_connstr
            .contains(DUMMY_PAGESERVER_CONNSTR));

        Ok(())
    }

    fn dummy_wal_connection_manager(harness: &RepoHarness) -> WalConnectionManager {
        WalConnectionManager {
            id: ZTenantTimelineId {
                tenant_id: harness.tenant_id,
                timeline_id: TIMELINE_ID,
            },
            timeline: Arc::new(DatadirTimelineImpl::new(
                harness
                    .load()
                    .create_empty_timeline(TIMELINE_ID, Lsn(0))
                    .expect("Failed to create an empty timeline for dummy wal connection manager"),
                10_000,
            )),
            wal_connect_timeout: Duration::from_secs(1),
            lagging_wal_timeout: Duration::from_secs(10),
            max_lsn_wal_lag: NonZeroU64::new(300_000).unwrap(),
            wal_connection_attempt: 0,
            wal_connection_data: None,
        }
    }

    const DUMMY_SAFEKEEPER_CONNSTR: &str = "safekeeper_connstr";
    const DUMMY_PAGESERVER_CONNSTR: &str = "pageserver_connstr";

    // the function itself does not need async, but it spawns a tokio::task underneath hence neeed
    // a runtime to not to panic
    async fn dummy_connection_data(
        id: ZTenantTimelineId,
        safekeeper_id: ZNodeId,
    ) -> WalConnectionData {
        let dummy_connstr =
            wal_stream_connection_string(id, DUMMY_SAFEKEEPER_CONNSTR, DUMMY_PAGESERVER_CONNSTR)
                .expect("Failed to construct dummy wal producer connstr");
        WalConnectionData {
            wal_producer_connstr: dummy_connstr.clone(),
            safekeeper_id,
            connection: WalReceiverConnection::open(
                id,
                safekeeper_id,
                dummy_connstr,
                Duration::from_secs(1),
            ),
            connection_init_time: Utc::now().naive_utc(),
            last_wal_receiver_data: None,
        }
    }
}
