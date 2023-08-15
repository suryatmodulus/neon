//
// Main entry point for the safekeeper executable
//
use anyhow::{bail, Context, Result};
use clap::Parser;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use remote_storage::RemoteStorageConfig;
use tokio::runtime::Handle;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinError;
use toml_edit::Document;

use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use storage_broker::Uri;
use tokio::sync::mpsc;

use tracing::*;
use utils::pid_file;

use metrics::set_build_info_metric;
use safekeeper::defaults::{
    DEFAULT_HEARTBEAT_TIMEOUT, DEFAULT_HTTP_LISTEN_ADDR, DEFAULT_MAX_OFFLOADER_LAG_BYTES,
    DEFAULT_PG_LISTEN_ADDR,
};
use safekeeper::wal_service;
use safekeeper::GlobalTimelines;
use safekeeper::SafeKeeperConf;
use safekeeper::{broker, WAL_SERVICE_RUNTIME};
use safekeeper::{control_file, BROKER_RUNTIME};
use safekeeper::{http, WAL_REMOVER_RUNTIME};
use safekeeper::{remove_wal, WAL_BACKUP_RUNTIME};
use safekeeper::{wal_backup, HTTP_RUNTIME};
use storage_broker::DEFAULT_ENDPOINT;
use utils::auth::JwtAuth;
use utils::{
    id::NodeId,
    logging::{self, LogFormat},
    project_git_version,
    sentry_init::init_sentry,
    tcp_listener,
};

const PID_FILE_NAME: &str = "safekeeper.pid";
const ID_FILE_NAME: &str = "safekeeper.id";

project_git_version!(GIT_VERSION);

const ABOUT: &str = r#"
A fleet of safekeepers is responsible for reliably storing WAL received from
compute, passing it through consensus (mitigating potential computes brain
split), and serving the hardened part further downstream to pageserver(s).
"#;

#[derive(Parser)]
#[command(name = "Neon safekeeper", version = GIT_VERSION, about = ABOUT, long_about = None)]
struct Args {
    /// Path to the safekeeper data directory.
    #[arg(short = 'D', long, default_value = "./")]
    datadir: PathBuf,
    /// Safekeeper node id.
    #[arg(long)]
    id: Option<u64>,
    /// Initialize safekeeper with given id and exit.
    #[arg(long)]
    init: bool,
    /// Listen endpoint for receiving/sending WAL in the form host:port.
    #[arg(short, long, default_value = DEFAULT_PG_LISTEN_ADDR)]
    listen_pg: String,
    /// Listen http endpoint for management and metrics in the form host:port.
    #[arg(long, default_value = DEFAULT_HTTP_LISTEN_ADDR)]
    listen_http: String,
    /// Availability zone of the safekeeper.
    #[arg(long)]
    availability_zone: Option<String>,
    /// Do not wait for changes to be written safely to disk. Unsafe.
    #[arg(short, long)]
    no_sync: bool,
    /// Dump control file at path specified by this argument and exit.
    #[arg(long)]
    dump_control_file: Option<PathBuf>,
    /// Broker endpoint for storage nodes coordination in the form
    /// http[s]://host:port. In case of https schema TLS is connection is
    /// established; plaintext otherwise.
    #[arg(long, default_value = DEFAULT_ENDPOINT, verbatim_doc_comment)]
    broker_endpoint: Uri,
    /// Broker keepalive interval.
    #[arg(long, value_parser= humantime::parse_duration, default_value = storage_broker::DEFAULT_KEEPALIVE_INTERVAL)]
    broker_keepalive_interval: Duration,
    /// Peer safekeeper is considered dead after not receiving heartbeats from
    /// it during this period passed as a human readable duration.
    #[arg(long, value_parser= humantime::parse_duration, default_value = DEFAULT_HEARTBEAT_TIMEOUT)]
    heartbeat_timeout: Duration,
    /// Remote storage configuration for WAL backup (offloading to s3) as TOML
    /// inline table, e.g.
    ///   {"max_concurrent_syncs" = 17, "max_sync_errors": 13, "bucket_name": "<BUCKETNAME>", "bucket_region":"<REGION>", "concurrency_limit": 119}
    /// Safekeeper offloads WAL to
    ///   [prefix_in_bucket/]<tenant_id>/<timeline_id>/<segment_file>, mirroring
    /// structure on the file system.
    #[arg(long, value_parser = parse_remote_storage, verbatim_doc_comment)]
    remote_storage: Option<RemoteStorageConfig>,
    /// Safekeeper won't be elected for WAL offloading if it is lagging for more than this value in bytes
    #[arg(long, default_value_t = DEFAULT_MAX_OFFLOADER_LAG_BYTES)]
    max_offloader_lag: u64,
    /// Number of max parallel WAL segments to be offloaded to remote storage.
    #[arg(long, default_value = "5")]
    wal_backup_parallel_jobs: usize,
    /// Disable WAL backup to s3. When disabled, safekeeper removes WAL ignoring
    /// WAL backup horizon.
    #[arg(long)]
    disable_wal_backup: bool,
    /// Path to a .pem public key which is used to check JWT tokens.
    #[arg(long)]
    auth_validation_public_key_path: Option<PathBuf>,
    /// Format for logging, either 'plain' or 'json'.
    #[arg(long, default_value = "plain")]
    log_format: String,
    /// Run everything in single threaded current thread runtime, might be
    /// useful for debugging.
    #[arg(long)]
    current_thread_runtime: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if let Some(addr) = args.dump_control_file {
        let state = control_file::FileStorage::load_control_file(addr)?;
        let json = serde_json::to_string(&state)?;
        print!("{json}");
        return Ok(());
    }

    // important to keep the order of:
    // 1. init logging
    // 2. tracing panic hook
    // 3. sentry
    logging::init(
        LogFormat::from_config(&args.log_format)?,
        logging::TracingErrorLayerEnablement::Disabled,
    )?;
    logging::replace_panic_hook_with_tracing_panic_hook().forget();
    info!("version: {GIT_VERSION}");

    let args_workdir = &args.datadir;
    let workdir = args_workdir.canonicalize().with_context(|| {
        format!("Failed to get the absolute path for input workdir {args_workdir:?}")
    })?;

    // Change into the data directory.
    std::env::set_current_dir(&workdir)?;

    // Set or read our ID.
    let id = set_id(&workdir, args.id.map(NodeId))?;
    if args.init {
        return Ok(());
    }

    let auth = match args.auth_validation_public_key_path.as_ref() {
        None => {
            info!("auth is disabled");
            None
        }
        Some(path) => {
            info!("loading JWT auth key from {}", path.display());
            Some(Arc::new(
                JwtAuth::from_key_path(path).context("failed to load the auth key")?,
            ))
        }
    };

    let conf = SafeKeeperConf {
        workdir,
        my_id: id,
        listen_pg_addr: args.listen_pg,
        listen_http_addr: args.listen_http,
        availability_zone: args.availability_zone,
        no_sync: args.no_sync,
        broker_endpoint: args.broker_endpoint,
        broker_keepalive_interval: args.broker_keepalive_interval,
        heartbeat_timeout: args.heartbeat_timeout,
        remote_storage: args.remote_storage,
        max_offloader_lag_bytes: args.max_offloader_lag,
        wal_backup_enabled: !args.disable_wal_backup,
        backup_parallel_jobs: args.wal_backup_parallel_jobs,
        auth,
        current_thread_runtime: args.current_thread_runtime,
    };

    // initialize sentry if SENTRY_DSN is provided
    let _sentry_guard = init_sentry(
        Some(GIT_VERSION.into()),
        &[("node_id", &conf.my_id.to_string())],
    );
    start_safekeeper(conf).await
}

/// Result of joining any of main tasks: upper error means task failed to
/// complete, e.g. panicked, inner is error produced by task itself.
type JoinTaskRes = Result<anyhow::Result<()>, JoinError>;

async fn start_safekeeper(conf: SafeKeeperConf) -> Result<()> {
    // Prevent running multiple safekeepers on the same directory
    let lock_file_path = conf.workdir.join(PID_FILE_NAME);
    let lock_file =
        pid_file::claim_for_current_process(&lock_file_path).context("claim pid file")?;
    info!("claimed pid file at {lock_file_path:?}");

    // ensure that the lock file is held even if the main thread of the process is panics
    // we need to release the lock file only when the current process is gone
    std::mem::forget(lock_file);

    info!("starting safekeeper WAL service on {}", conf.listen_pg_addr);
    let pg_listener = tcp_listener::bind(conf.listen_pg_addr.clone()).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_pg_addr, e);
        e
    })?;

    info!(
        "starting safekeeper HTTP service on {}",
        conf.listen_http_addr
    );
    let http_listener = tcp_listener::bind(conf.listen_http_addr.clone()).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_http_addr, e);
        e
    })?;

    // Register metrics collector for active timelines. It's important to do this
    // after daemonizing, otherwise process collector will be upset.
    let timeline_collector = safekeeper::metrics::TimelineCollector::new();
    metrics::register_internal(Box::new(timeline_collector))?;

    let (wal_backup_launcher_tx, wal_backup_launcher_rx) = mpsc::channel(100);

    // Load all timelines from disk to memory.
    GlobalTimelines::init(conf.clone(), wal_backup_launcher_tx)?;

    // Keep handles to main tasks to die if any of them disappears.
    let mut tasks_handles: FuturesUnordered<BoxFuture<(String, JoinTaskRes)>> =
        FuturesUnordered::new();

    let conf_ = conf.clone();
    // Run everything in current thread rt, if asked.
    if conf.current_thread_runtime {
        info!("running in current thread runtime");
    }
    let current_thread_rt = conf
        .current_thread_runtime
        .then(|| Handle::try_current().expect("no runtime in main"));
    let wal_service_handle = current_thread_rt
        .as_ref()
        .unwrap_or_else(|| WAL_SERVICE_RUNTIME.handle())
        .spawn(wal_service::task_main(conf_, pg_listener))
        // wrap with task name for error reporting
        .map(|res| ("WAL service main".to_owned(), res));
    tasks_handles.push(Box::pin(wal_service_handle));

    let conf_ = conf.clone();
    let http_handle = current_thread_rt
        .as_ref()
        .unwrap_or_else(|| HTTP_RUNTIME.handle())
        .spawn(http::task_main(conf_, http_listener))
        .map(|res| ("HTTP service main".to_owned(), res));
    tasks_handles.push(Box::pin(http_handle));

    let conf_ = conf.clone();
    let broker_task_handle = current_thread_rt
        .as_ref()
        .unwrap_or_else(|| BROKER_RUNTIME.handle())
        .spawn(broker::task_main(conf_).instrument(info_span!("broker")))
        .map(|res| ("broker main".to_owned(), res));
    tasks_handles.push(Box::pin(broker_task_handle));

    let conf_ = conf.clone();
    let wal_remover_handle = current_thread_rt
        .as_ref()
        .unwrap_or_else(|| WAL_REMOVER_RUNTIME.handle())
        .spawn(remove_wal::task_main(conf_))
        .map(|res| ("WAL remover".to_owned(), res));
    tasks_handles.push(Box::pin(wal_remover_handle));

    let conf_ = conf.clone();
    let wal_backup_handle = current_thread_rt
        .as_ref()
        .unwrap_or_else(|| WAL_BACKUP_RUNTIME.handle())
        .spawn(wal_backup::wal_backup_launcher_task_main(
            conf_,
            wal_backup_launcher_rx,
        ))
        .map(|res| ("WAL backup launcher".to_owned(), res));
    tasks_handles.push(Box::pin(wal_backup_handle));

    set_build_info_metric(GIT_VERSION);

    // TODO: update tokio-stream, convert to real async Stream with
    // SignalStream, map it to obtain missing signal name, combine streams into
    // single stream we can easily sit on.
    let mut sigquit_stream = signal(SignalKind::quit())?;
    let mut sigint_stream = signal(SignalKind::interrupt())?;
    let mut sigterm_stream = signal(SignalKind::terminate())?;

    tokio::select! {
        Some((task_name, res)) = tasks_handles.next()=> {
            error!("{} task failed: {:?}, exiting", task_name, res);
            std::process::exit(1);
        }
        // On any shutdown signal, log receival and exit. Additionally, handling
        // SIGQUIT prevents coredump.
        _ = sigquit_stream.recv() => info!("received SIGQUIT, terminating"),
        _ = sigint_stream.recv() => info!("received SIGINT, terminating"),
        _ = sigterm_stream.recv() => info!("received SIGTERM, terminating")

    };
    std::process::exit(0);
}

/// Determine safekeeper id.
fn set_id(workdir: &Path, given_id: Option<NodeId>) -> Result<NodeId> {
    let id_file_path = workdir.join(ID_FILE_NAME);

    let my_id: NodeId;
    // If file with ID exists, read it in; otherwise set one passed.
    match fs::read(&id_file_path) {
        Ok(id_serialized) => {
            my_id = NodeId(
                std::str::from_utf8(&id_serialized)
                    .context("failed to parse safekeeper id")?
                    .parse()
                    .context("failed to parse safekeeper id")?,
            );
            if let Some(given_id) = given_id {
                if given_id != my_id {
                    bail!(
                        "safekeeper already initialized with id {}, can't set {}",
                        my_id,
                        given_id
                    );
                }
            }
            info!("safekeeper ID {}", my_id);
        }
        Err(error) => match error.kind() {
            ErrorKind::NotFound => {
                my_id = if let Some(given_id) = given_id {
                    given_id
                } else {
                    bail!("safekeeper id is not specified");
                };
                let mut f = File::create(&id_file_path)
                    .with_context(|| format!("Failed to create id file at {id_file_path:?}"))?;
                f.write_all(my_id.to_string().as_bytes())?;
                f.sync_all()?;
                info!("initialized safekeeper id {}", my_id);
            }
            _ => {
                return Err(error.into());
            }
        },
    }
    Ok(my_id)
}

// Parse RemoteStorage from TOML table.
fn parse_remote_storage(storage_conf: &str) -> anyhow::Result<RemoteStorageConfig> {
    // funny toml doesn't consider plain inline table as valid document, so wrap in a key to parse
    let storage_conf_toml = format!("remote_storage = {storage_conf}");
    let parsed_toml = storage_conf_toml.parse::<Document>()?; // parse
    let (_, storage_conf_parsed_toml) = parsed_toml.iter().next().unwrap(); // and strip key off again
    RemoteStorageConfig::from_toml(storage_conf_parsed_toml).and_then(|parsed_config| {
        // XXX: Don't print the original toml here, there might be some sensitive data
        parsed_config.context("Incorrectly parsed remote storage toml as no remote storage config")
    })
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Args::command().debug_assert()
}
