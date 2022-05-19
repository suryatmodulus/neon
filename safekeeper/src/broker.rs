//! Communication with etcd, providing safekeeper peers and pageserver coordination.

use anyhow::Context;
use anyhow::Error;
use anyhow::Result;
use etcd_broker::Client;
use etcd_broker::PutOptions;
use etcd_broker::SkTimelineSubscriptionKind;
use std::time::Duration;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio::{runtime, time::sleep};
use tracing::*;
use url::Url;

use crate::{timeline::GlobalTimelines, SafeKeeperConf};
use utils::zid::{ZNodeId, ZTenantTimelineId};

const RETRY_INTERVAL_MSEC: u64 = 1000;
const PUSH_INTERVAL_MSEC: u64 = 1000;
const LEASE_TTL_SEC: i64 = 5;

pub fn thread_main(conf: SafeKeeperConf) {
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let _enter = info_span!("broker").entered();
    info!("started, broker endpoints {:?}", conf.broker_endpoints);

    runtime.block_on(async {
        main_loop(conf).await;
    });
}

/// Key to per timeline per safekeeper data.
fn timeline_safekeeper_path(
    broker_etcd_prefix: String,
    zttid: ZTenantTimelineId,
    sk_id: ZNodeId,
) -> String {
    format!(
        "{}/{sk_id}",
        SkTimelineSubscriptionKind::timeline(broker_etcd_prefix, zttid).watch_key()
    )
}

pub struct Election {
    pub election_name: String,
    pub candidate_name: String,
    pub broker_endpoints: Vec<Url>,
}

impl Election {
    pub fn new(election_name: String, candidate_name: String, broker_endpoints: Vec<Url>) -> Self {
        Self {
            election_name,
            candidate_name,
            broker_endpoints,
        }
    }
}

pub struct ElectionLeader {
    client: Client,
    keep_alive: JoinHandle<Result<()>>,
}

impl ElectionLeader {
    pub async fn check_am_i(
        &self,
        election_name: String,
        candidate_name: String,
    ) -> Result<bool> {
        let mut c = self.client.clone();
        let resp = c.leader(election_name).await?;

        let kv = resp.kv().expect("failed to get leader response");
        let leader = kv.value_str().expect("failed to get campaign leader value");

        Ok(leader == candidate_name)
    }

    pub async fn give_up(&self) -> Result<()> {
        self.keep_alive.abort();
        // TODO: it'll be wise to resign here but it'll happen after lease expiration anyway
        // should we await for keep alive termination?
        // self.keep_alive.await;

        Ok(())
    }
}

pub async fn get_leader(req: &Election) -> Result<ElectionLeader> {
    let mut client = Client::connect(req.broker_endpoints.clone(), None)
        .await
        .context("Could not connect to etcd")?;

    let lease = client
        .lease_grant(LEASE_TTL_SEC, None)
        .await
        .context("Could not acquire a lease");

    let lease_id = lease.map(|l| l.id()).unwrap();

    let keep_alive = spawn::<_>(lease_keep_alive(client.clone(), lease_id));

    let resp = client
        .campaign(
            req.election_name.clone(),
            req.candidate_name.clone(),
            lease_id,
        )
        .await?;
    let _leader = resp.leader().unwrap();

    Ok(ElectionLeader { client, keep_alive })
}

pub async fn lease_keep_alive(mut client: Client, lease_id: i64) -> Result<()> {
    let (mut keeper, mut ka_stream) = client
        .lease_keep_alive(lease_id)
        .await
        .context("failed to create keepalive stream")?;

    loop {
        let push_interval = Duration::from_millis(PUSH_INTERVAL_MSEC);

        keeper
            .keep_alive()
            .await
            .context("failed to send LeaseKeepAliveRequest")?;

        ka_stream
            .message()
            .await
            .context("failed to receive LeaseKeepAliveResponse")?;

        sleep(push_interval).await;
    }
}

pub fn get_campaign_name(
    election_name: String,
    broker_prefix: String,
    timeline_id: &ZTenantTimelineId,
) -> String {
    return format!(
        "{}/{}",
        SkTimelineSubscriptionKind::timeline(broker_prefix, *timeline_id).watch_key(),
        election_name
    );
}

pub fn get_candiate_name(system_id: ZNodeId) -> String {
    format!("id_{}", system_id)
}

pub async fn is_leader(
    election_name: String,
    timeline_id: &ZTenantTimelineId,
    broker_etcd_prefix: String,
    broker_endpoints: &Vec<Url>,
    system_id: ZNodeId,
) -> Result<bool> {
    let mut client = Client::connect(broker_endpoints, None)
        .await
        .context("failed to get etcd client")?;

    let campaign_name = get_campaign_name(election_name, broker_etcd_prefix, timeline_id);
    let resp = client.leader(campaign_name).await?;

    let kv = resp.kv().expect("failed to get leader response");
    let leader = kv.value_str().expect("failed to get campaign leader value");

    let my_candidate_name = get_candiate_name(system_id);
    Ok(leader == my_candidate_name)
}

/// Push once in a while data about all active timelines to the broker.
async fn push_loop(conf: SafeKeeperConf) -> anyhow::Result<()> {
    let mut client = Client::connect(&conf.broker_endpoints, None).await?;

    // Get and maintain lease to automatically delete obsolete data
    let lease = client.lease_grant(LEASE_TTL_SEC, None).await?;
    let (mut keeper, mut ka_stream) = client.lease_keep_alive(lease.id()).await?;

    let push_interval = Duration::from_millis(PUSH_INTERVAL_MSEC);
    loop {
        // Note: we lock runtime here and in timeline methods as GlobalTimelines
        // is under plain mutex. That's ok, all this code is not performance
        // sensitive and there is no risk of deadlock as we don't await while
        // lock is held.
        for zttid in GlobalTimelines::get_active_timelines() {
            if let Ok(tli) = GlobalTimelines::get(&conf, zttid, false) {
                let sk_info = tli.get_public_info(&conf)?;
                let put_opts = PutOptions::new().with_lease(lease.id());
                client
                    .put(
                        timeline_safekeeper_path(
                            conf.broker_etcd_prefix.clone(),
                            zttid,
                            conf.my_id,
                        ),
                        serde_json::to_string(&sk_info)?,
                        Some(put_opts),
                    )
                    .await
                    .context("failed to push safekeeper info")?;
            }
        }
        // revive the lease
        keeper
            .keep_alive()
            .await
            .context("failed to send LeaseKeepAliveRequest")?;
        ka_stream
            .message()
            .await
            .context("failed to receive LeaseKeepAliveResponse")?;
        sleep(push_interval).await;
    }
}

/// Subscribe and fetch all the interesting data from the broker.
async fn pull_loop(conf: SafeKeeperConf) -> Result<()> {
    let mut client = Client::connect(&conf.broker_endpoints, None).await?;

    let mut subscription = etcd_broker::subscribe_to_safekeeper_timeline_updates(
        &mut client,
        SkTimelineSubscriptionKind::all(conf.broker_etcd_prefix.clone()),
    )
    .await
    .context("failed to subscribe for safekeeper info")?;
    loop {
        match subscription.fetch_data().await {
            Some(new_info) => {
                for (zttid, sk_info) in new_info {
                    // note: there are blocking operations below, but it's considered fine for now
                    if let Ok(tli) = GlobalTimelines::get(&conf, zttid, false) {
                        for (safekeeper_id, info) in sk_info {
                            tli.record_safekeeper_info(&info, safekeeper_id)?
                        }
                    }
                }
            }
            None => {
                debug!("timeline updates sender closed, aborting the pull loop");
                return Ok(());
            }
        }
    }
}

async fn main_loop(conf: SafeKeeperConf) {
    let mut ticker = tokio::time::interval(Duration::from_millis(RETRY_INTERVAL_MSEC));
    let mut push_handle: Option<JoinHandle<Result<(), Error>>> = None;
    let mut pull_handle: Option<JoinHandle<Result<(), Error>>> = None;
    // Selecting on JoinHandles requires some squats; is there a better way to
    // reap tasks individually?

    // Handling failures in task itself won't catch panic and in Tokio, task's
    // panic doesn't kill the whole executor, so it is better to do reaping
    // here.
    loop {
        tokio::select! {
                res = async { push_handle.as_mut().unwrap().await }, if push_handle.is_some() => {
                    // was it panic or normal error?
                    let err = match res {
                        Ok(res_internal) => res_internal.unwrap_err(),
                        Err(err_outer) => err_outer.into(),
                    };
                    warn!("push task failed: {:?}", err);
                    push_handle = None;
                },
                res = async { pull_handle.as_mut().unwrap().await }, if pull_handle.is_some() => {
                    // was it panic or normal error?
                    let err = match res {
                        Ok(res_internal) => res_internal.unwrap_err(),
                        Err(err_outer) => err_outer.into(),
                    };
                    warn!("pull task failed: {:?}", err);
                    pull_handle = None;
                },
                _ = ticker.tick() => {
                    if push_handle.is_none() {
                        push_handle = Some(tokio::spawn(push_loop(conf.clone())));
                    }
                    if pull_handle.is_none() {
                        pull_handle = Some(tokio::spawn(pull_loop(conf.clone())));
                    }
            }
        }
    }
}
