use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Write};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::{io, result};

use anyhow::{bail, Context};
use pageserver_api::models::{
    TenantConfigRequest, TenantCreateRequest, TenantInfo, TimelineCreateRequest, TimelineInfo,
};
use postgres_backend::AuthType;
use postgres_connection::{parse_host_port, PgConnectionConfig};
use reqwest::blocking::{Client, RequestBuilder, Response};
use reqwest::{IntoUrl, Method};
use thiserror::Error;
use utils::auth::{Claims, Scope};
use utils::{
    http::error::HttpErrorBody,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use crate::{background_process, local_env::LocalEnv};

#[derive(Error, Debug)]
pub enum PageserverHttpError {
    #[error("Reqwest error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("Error: {0}")]
    Response(String),
}

impl From<anyhow::Error> for PageserverHttpError {
    fn from(e: anyhow::Error) -> Self {
        Self::Response(e.to_string())
    }
}

type Result<T> = result::Result<T, PageserverHttpError>;

pub trait ResponseErrorMessageExt: Sized {
    fn error_from_body(self) -> Result<Self>;
}

impl ResponseErrorMessageExt for Response {
    fn error_from_body(self) -> Result<Self> {
        let status = self.status();
        if !(status.is_client_error() || status.is_server_error()) {
            return Ok(self);
        }

        // reqwest does not export its error construction utility functions, so let's craft the message ourselves
        let url = self.url().to_owned();
        Err(PageserverHttpError::Response(
            match self.json::<HttpErrorBody>() {
                Ok(err_body) => format!("Error: {}", err_body.msg),
                Err(_) => format!("Http error ({}) at {}.", status.as_u16(), url),
            },
        ))
    }
}

//
// Control routines for pageserver.
//
// Used in CLI and tests.
//
#[derive(Debug)]
pub struct PageServerNode {
    pub pg_connection_config: PgConnectionConfig,
    pub env: LocalEnv,
    pub http_client: Client,
    pub http_base_url: String,
}

impl PageServerNode {
    pub fn from_env(env: &LocalEnv) -> PageServerNode {
        let (host, port) = parse_host_port(&env.pageserver.listen_pg_addr)
            .expect("Unable to parse listen_pg_addr");
        let port = port.unwrap_or(5432);
        let password = if env.pageserver.auth_type == AuthType::NeonJWT {
            Some(env.pageserver.auth_token.clone())
        } else {
            None
        };

        Self {
            pg_connection_config: PgConnectionConfig::new_host_port(host, port)
                .set_password(password),
            env: env.clone(),
            http_client: Client::new(),
            http_base_url: format!("http://{}/v1", env.pageserver.listen_http_addr),
        }
    }

    // pageserver conf overrides defined by neon_local configuration.
    fn neon_local_overrides(&self) -> Vec<String> {
        let id = format!("id={}", self.env.pageserver.id);
        // FIXME: the paths should be shell-escaped to handle paths with spaces, quotas etc.
        let pg_distrib_dir_param = format!(
            "pg_distrib_dir='{}'",
            self.env.pg_distrib_dir_raw().display()
        );

        let authg_type_param = format!("auth_type='{}'", self.env.pageserver.auth_type);
        let listen_http_addr_param = format!(
            "listen_http_addr='{}'",
            self.env.pageserver.listen_http_addr
        );
        let listen_pg_addr_param =
            format!("listen_pg_addr='{}'", self.env.pageserver.listen_pg_addr);
        let broker_endpoint_param = format!("broker_endpoint='{}'", self.env.broker.client_url());

        let mut overrides = vec![
            id,
            pg_distrib_dir_param,
            authg_type_param,
            listen_http_addr_param,
            listen_pg_addr_param,
            broker_endpoint_param,
        ];

        if self.env.pageserver.auth_type != AuthType::Trust {
            overrides.push("auth_validation_public_key_path='auth_public_key.pem'".to_owned());
        }
        overrides
    }

    /// Initializes a pageserver node by creating its config with the overrides provided.
    pub fn initialize(&self, config_overrides: &[&str]) -> anyhow::Result<()> {
        // First, run `pageserver --init` and wait for it to write a config into FS and exit.
        self.pageserver_init(config_overrides).with_context(|| {
            format!(
                "Failed to run init for pageserver node {}",
                self.env.pageserver.id,
            )
        })
    }

    pub fn repo_path(&self) -> PathBuf {
        self.env.pageserver_data_dir()
    }

    /// The pid file is created by the pageserver process, with its pid stored inside.
    /// Other pageservers cannot lock the same file and overwrite it for as long as the current
    /// pageserver runs. (Unless someone removes the file manually; never do that!)
    fn pid_file(&self) -> PathBuf {
        self.repo_path().join("pageserver.pid")
    }

    pub fn start(&self, config_overrides: &[&str]) -> anyhow::Result<Child> {
        self.start_node(config_overrides, false)
    }

    fn pageserver_init(&self, config_overrides: &[&str]) -> anyhow::Result<()> {
        let datadir = self.repo_path();
        let node_id = self.env.pageserver.id;
        println!(
            "Initializing pageserver node {} at '{}' in {:?}",
            node_id,
            self.pg_connection_config.raw_address(),
            datadir
        );
        io::stdout().flush()?;

        let datadir_path_str = datadir.to_str().with_context(|| {
            format!("Cannot start pageserver node {node_id} in path that has no string representation: {datadir:?}")
        })?;
        let mut args = self.pageserver_basic_args(config_overrides, datadir_path_str);
        args.push(Cow::Borrowed("--init"));

        let init_output = Command::new(self.env.pageserver_bin())
            .args(args.iter().map(Cow::as_ref))
            .envs(self.pageserver_env_variables()?)
            .output()
            .with_context(|| format!("Failed to run pageserver init for node {node_id}"))?;

        anyhow::ensure!(
            init_output.status.success(),
            "Pageserver init for node {} did not finish successfully, stdout: {}, stderr: {}",
            node_id,
            String::from_utf8_lossy(&init_output.stdout),
            String::from_utf8_lossy(&init_output.stderr),
        );

        Ok(())
    }

    fn start_node(&self, config_overrides: &[&str], update_config: bool) -> anyhow::Result<Child> {
        let mut overrides = self.neon_local_overrides();
        overrides.extend(config_overrides.iter().map(|&c| c.to_owned()));

        let datadir = self.repo_path();
        print!(
            "Starting pageserver node {} at '{}' in {:?}",
            self.env.pageserver.id,
            self.pg_connection_config.raw_address(),
            datadir
        );
        io::stdout().flush()?;

        let datadir_path_str = datadir.to_str().with_context(|| {
            format!(
                "Cannot start pageserver node {} in path that has no string representation: {:?}",
                self.env.pageserver.id, datadir,
            )
        })?;
        let mut args = self.pageserver_basic_args(config_overrides, datadir_path_str);
        if update_config {
            args.push(Cow::Borrowed("--update-config"));
        }

        background_process::start_process(
            "pageserver",
            &datadir,
            &self.env.pageserver_bin(),
            args.iter().map(Cow::as_ref),
            self.pageserver_env_variables()?,
            background_process::InitialPidFile::Expect(&self.pid_file()),
            || match self.check_status() {
                Ok(()) => Ok(true),
                Err(PageserverHttpError::Transport(_)) => Ok(false),
                Err(e) => Err(anyhow::anyhow!("Failed to check node status: {e}")),
            },
        )
    }

    fn pageserver_basic_args<'a>(
        &self,
        config_overrides: &'a [&'a str],
        datadir_path_str: &'a str,
    ) -> Vec<Cow<'a, str>> {
        let mut args = vec![Cow::Borrowed("-D"), Cow::Borrowed(datadir_path_str)];

        let mut overrides = self.neon_local_overrides();
        overrides.extend(config_overrides.iter().map(|&c| c.to_owned()));
        for config_override in overrides {
            args.push(Cow::Borrowed("-c"));
            args.push(Cow::Owned(config_override));
        }

        args
    }

    fn pageserver_env_variables(&self) -> anyhow::Result<Vec<(String, String)>> {
        Ok(if self.env.pageserver.auth_type != AuthType::Trust {
            // Generate a token to connect from the pageserver to a safekeeper
            let token = self
                .env
                .generate_auth_token(&Claims::new(None, Scope::SafekeeperData))?;
            vec![("NEON_AUTH_TOKEN".to_owned(), token)]
        } else {
            Vec::new()
        })
    }

    ///
    /// Stop the server.
    ///
    /// If 'immediate' is true, we use SIGQUIT, killing the process immediately.
    /// Otherwise we use SIGTERM, triggering a clean shutdown
    ///
    /// If the server is not running, returns success
    ///
    pub fn stop(&self, immediate: bool) -> anyhow::Result<()> {
        background_process::stop_process(immediate, "pageserver", &self.pid_file())
    }

    pub fn page_server_psql(&self, sql: &str) -> Vec<postgres::SimpleQueryMessage> {
        let mut client = self.pg_connection_config.connect_no_tls().unwrap();

        println!("Pageserver query: '{sql}'");
        client.simple_query(sql).unwrap()
    }

    pub fn page_server_psql_client(&self) -> result::Result<postgres::Client, postgres::Error> {
        self.pg_connection_config.connect_no_tls()
    }

    fn http_request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        let mut builder = self.http_client.request(method, url);
        if self.env.pageserver.auth_type == AuthType::NeonJWT {
            builder = builder.bearer_auth(&self.env.pageserver.auth_token)
        }
        builder
    }

    pub fn check_status(&self) -> Result<()> {
        self.http_request(Method::GET, format!("{}/status", self.http_base_url))
            .send()?
            .error_from_body()?;
        Ok(())
    }

    pub fn tenant_list(&self) -> Result<Vec<TenantInfo>> {
        Ok(self
            .http_request(Method::GET, format!("{}/tenant", self.http_base_url))
            .send()?
            .error_from_body()?
            .json()?)
    }

    pub fn tenant_create(
        &self,
        new_tenant_id: Option<TenantId>,
        settings: HashMap<&str, &str>,
    ) -> anyhow::Result<TenantId> {
        let mut settings = settings.clone();
        let request = TenantCreateRequest {
            new_tenant_id,
            checkpoint_distance: settings
                .remove("checkpoint_distance")
                .map(|x| x.parse::<u64>())
                .transpose()?,
            checkpoint_timeout: settings.remove("checkpoint_timeout").map(|x| x.to_string()),
            compaction_target_size: settings
                .remove("compaction_target_size")
                .map(|x| x.parse::<u64>())
                .transpose()?,
            compaction_period: settings.remove("compaction_period").map(|x| x.to_string()),
            compaction_threshold: settings
                .remove("compaction_threshold")
                .map(|x| x.parse::<usize>())
                .transpose()?,
            gc_horizon: settings
                .remove("gc_horizon")
                .map(|x| x.parse::<u64>())
                .transpose()?,
            gc_period: settings.remove("gc_period").map(|x| x.to_string()),
            image_creation_threshold: settings
                .remove("image_creation_threshold")
                .map(|x| x.parse::<usize>())
                .transpose()?,
            pitr_interval: settings.remove("pitr_interval").map(|x| x.to_string()),
            walreceiver_connect_timeout: settings
                .remove("walreceiver_connect_timeout")
                .map(|x| x.to_string()),
            lagging_wal_timeout: settings
                .remove("lagging_wal_timeout")
                .map(|x| x.to_string()),
            max_lsn_wal_lag: settings
                .remove("max_lsn_wal_lag")
                .map(|x| x.parse::<NonZeroU64>())
                .transpose()
                .context("Failed to parse 'max_lsn_wal_lag' as non zero integer")?,
            trace_read_requests: settings
                .remove("trace_read_requests")
                .map(|x| x.parse::<bool>())
                .transpose()
                .context("Failed to parse 'trace_read_requests' as bool")?,
        };
        if !settings.is_empty() {
            bail!("Unrecognized tenant settings: {settings:?}")
        }
        self.http_request(Method::POST, format!("{}/tenant", self.http_base_url))
            .json(&request)
            .send()?
            .error_from_body()?
            .json::<Option<String>>()
            .with_context(|| {
                format!("Failed to parse tenant creation response for tenant id: {new_tenant_id:?}")
            })?
            .context("No tenant id was found in the tenant creation response")
            .and_then(|tenant_id_string| {
                tenant_id_string.parse().with_context(|| {
                    format!("Failed to parse response string as tenant id: '{tenant_id_string}'")
                })
            })
    }

    pub fn tenant_config(&self, tenant_id: TenantId, settings: HashMap<&str, &str>) -> Result<()> {
        self.http_request(Method::PUT, format!("{}/tenant/config", self.http_base_url))
            .json(&TenantConfigRequest {
                tenant_id,
                checkpoint_distance: settings
                    .get("checkpoint_distance")
                    .map(|x| x.parse::<u64>())
                    .transpose()
                    .context("Failed to parse 'checkpoint_distance' as an integer")?,
                checkpoint_timeout: settings.get("checkpoint_timeout").map(|x| x.to_string()),
                compaction_target_size: settings
                    .get("compaction_target_size")
                    .map(|x| x.parse::<u64>())
                    .transpose()
                    .context("Failed to parse 'compaction_target_size' as an integer")?,
                compaction_period: settings.get("compaction_period").map(|x| x.to_string()),
                compaction_threshold: settings
                    .get("compaction_threshold")
                    .map(|x| x.parse::<usize>())
                    .transpose()
                    .context("Failed to parse 'compaction_threshold' as an integer")?,
                gc_horizon: settings
                    .get("gc_horizon")
                    .map(|x| x.parse::<u64>())
                    .transpose()
                    .context("Failed to parse 'gc_horizon' as an integer")?,
                gc_period: settings.get("gc_period").map(|x| x.to_string()),
                image_creation_threshold: settings
                    .get("image_creation_threshold")
                    .map(|x| x.parse::<usize>())
                    .transpose()
                    .context("Failed to parse 'image_creation_threshold' as non zero integer")?,
                pitr_interval: settings.get("pitr_interval").map(|x| x.to_string()),
                walreceiver_connect_timeout: settings
                    .get("walreceiver_connect_timeout")
                    .map(|x| x.to_string()),
                lagging_wal_timeout: settings.get("lagging_wal_timeout").map(|x| x.to_string()),
                max_lsn_wal_lag: settings
                    .get("max_lsn_wal_lag")
                    .map(|x| x.parse::<NonZeroU64>())
                    .transpose()
                    .context("Failed to parse 'max_lsn_wal_lag' as non zero integer")?,
                trace_read_requests: settings
                    .get("trace_read_requests")
                    .map(|x| x.parse::<bool>())
                    .transpose()
                    .context("Failed to parse 'trace_read_requests' as bool")?,
                eviction_policy: settings
                    .get("eviction_policy")
                    .map(|x| serde_json::from_str(x))
                    .transpose()
                    .context("Failed to parse 'eviction_policy' json")?,
            })
            .send()?
            .error_from_body()?;

        Ok(())
    }

    pub fn timeline_list(&self, tenant_id: &TenantId) -> anyhow::Result<Vec<TimelineInfo>> {
        let timeline_infos: Vec<TimelineInfo> = self
            .http_request(
                Method::GET,
                format!("{}/tenant/{}/timeline", self.http_base_url, tenant_id),
            )
            .send()?
            .error_from_body()?
            .json()?;

        Ok(timeline_infos)
    }

    pub fn timeline_create(
        &self,
        tenant_id: TenantId,
        new_timeline_id: Option<TimelineId>,
        ancestor_start_lsn: Option<Lsn>,
        ancestor_timeline_id: Option<TimelineId>,
        pg_version: Option<u32>,
    ) -> anyhow::Result<TimelineInfo> {
        self.http_request(
            Method::POST,
            format!("{}/tenant/{}/timeline", self.http_base_url, tenant_id),
        )
        .json(&TimelineCreateRequest {
            new_timeline_id,
            ancestor_start_lsn,
            ancestor_timeline_id,
            pg_version,
        })
        .send()?
        .error_from_body()?
        .json::<Option<TimelineInfo>>()
        .with_context(|| {
            format!("Failed to parse timeline creation response for tenant id: {tenant_id}")
        })?
        .with_context(|| {
            format!(
                "No timeline id was found in the timeline creation response for tenant {tenant_id}"
            )
        })
    }

    /// Import a basebackup prepared using either:
    /// a) `pg_basebackup -F tar`, or
    /// b) The `fullbackup` pageserver endpoint
    ///
    /// # Arguments
    /// * `tenant_id` - tenant to import into. Created if not exists
    /// * `timeline_id` - id to assign to imported timeline
    /// * `base` - (start lsn of basebackup, path to `base.tar` file)
    /// * `pg_wal` - if there's any wal to import: (end lsn, path to `pg_wal.tar`)
    pub fn timeline_import(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        base: (Lsn, PathBuf),
        pg_wal: Option<(Lsn, PathBuf)>,
        pg_version: u32,
    ) -> anyhow::Result<()> {
        let mut client = self.pg_connection_config.connect_no_tls().unwrap();

        // Init base reader
        let (start_lsn, base_tarfile_path) = base;
        let base_tarfile = File::open(base_tarfile_path)?;
        let mut base_reader = BufReader::new(base_tarfile);

        // Init wal reader if necessary
        let (end_lsn, wal_reader) = if let Some((end_lsn, wal_tarfile_path)) = pg_wal {
            let wal_tarfile = File::open(wal_tarfile_path)?;
            let wal_reader = BufReader::new(wal_tarfile);
            (end_lsn, Some(wal_reader))
        } else {
            (start_lsn, None)
        };

        // Import base
        let import_cmd = format!(
            "import basebackup {tenant_id} {timeline_id} {start_lsn} {end_lsn} {pg_version}"
        );
        let mut writer = client.copy_in(&import_cmd)?;
        io::copy(&mut base_reader, &mut writer)?;
        writer.finish()?;

        // Import wal if necessary
        if let Some(mut wal_reader) = wal_reader {
            let import_cmd = format!("import wal {tenant_id} {timeline_id} {start_lsn} {end_lsn}");
            let mut writer = client.copy_in(&import_cmd)?;
            io::copy(&mut wal_reader, &mut writer)?;
            writer.finish()?;
        }

        Ok(())
    }
}
