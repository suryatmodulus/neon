#[cfg(test)]
mod tests;

pub(crate) mod connect_compute;
pub(crate) mod retry;
pub(crate) mod wake_compute;

use std::sync::Arc;

use futures::FutureExt;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use regex::Regex;
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, ToSmolStr, format_smolstr};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, warn};

use crate::cancellation::{self, CancellationHandler};
use crate::config::{ProxyConfig, ProxyProtocolV2, TlsConfig};
use crate::context::RequestContext;
use crate::error::{ReportableError, UserFacingError};
use crate::metrics::{Metrics, NumClientConnectionsGuard};
pub use crate::pglb::copy_bidirectional::{ErrorSource, copy_bidirectional_client_compute};
use crate::pglb::handshake::{HandshakeData, HandshakeError, handshake};
use crate::pglb::passthrough::ProxyPassthrough;
use crate::pqproto::{BeMessage, CancelKeyData, StartupMessageParams};
use crate::protocol2::{ConnectHeader, ConnectionInfo, ConnectionInfoExtra, read_proxy_protocol};
use crate::proxy::connect_compute::{TcpMechanism, connect_to_compute};
use crate::rate_limiter::EndpointRateLimiter;
use crate::stream::{PqStream, Stream};
use crate::types::EndpointCacheKey;
use crate::util::run_until_cancelled;
use crate::{auth, compute};

const ERR_INSECURE_CONNECTION: &str = "connection is insecure (try using `sslmode=require`)";

#[derive(Error, Debug)]
#[error("{ERR_INSECURE_CONNECTION}")]
pub struct TlsRequired;

impl ReportableError for TlsRequired {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        crate::error::ErrorKind::User
    }
}

impl UserFacingError for TlsRequired {}

pub async fn task_main(
    config: &'static ProxyConfig,
    auth_backend: &'static auth::Backend<'static, ()>,
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
    cancellation_handler: Arc<CancellationHandler>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("proxy has shut down");
    }

    // When set for the server socket, the keepalive setting
    // will be inherited by all accepted client sockets.
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let connections = tokio_util::task::task_tracker::TaskTracker::new();
    let cancellations = tokio_util::task::task_tracker::TaskTracker::new();

    while let Some(accept_result) =
        run_until_cancelled(listener.accept(), &cancellation_token).await
    {
        let (socket, peer_addr) = accept_result?;

        let conn_gauge = Metrics::get()
            .proxy
            .client_connections
            .guard(crate::metrics::Protocol::Tcp);

        let session_id = uuid::Uuid::new_v4();
        let cancellation_handler = Arc::clone(&cancellation_handler);
        let cancellations = cancellations.clone();

        debug!(protocol = "tcp", %session_id, "accepted new TCP connection");
        let endpoint_rate_limiter2 = endpoint_rate_limiter.clone();

        connections.spawn(async move {
            let (socket, conn_info) = match config.proxy_protocol_v2 {
                ProxyProtocolV2::Required => {
                    match read_proxy_protocol(socket).await {
                        Err(e) => {
                            warn!("per-client task finished with an error: {e:#}");
                            return;
                        }
                        // our load balancers will not send any more data. let's just exit immediately
                        Ok((_socket, ConnectHeader::Local)) => {
                            debug!("healthcheck received");
                            return;
                        }
                        Ok((socket, ConnectHeader::Proxy(info))) => (socket, info),
                    }
                }
                // ignore the header - it cannot be confused for a postgres or http connection so will
                // error later.
                ProxyProtocolV2::Rejected => (
                    socket,
                    ConnectionInfo {
                        addr: peer_addr,
                        extra: None,
                    },
                ),
            };

            match socket.set_nodelay(true) {
                Ok(()) => {}
                Err(e) => {
                    error!(
                        "per-client task finished with an error: failed to set socket option: {e:#}"
                    );
                    return;
                }
            }

            let ctx = RequestContext::new(session_id, conn_info, crate::metrics::Protocol::Tcp);

            let res = handle_client(
                config,
                auth_backend,
                &ctx,
                cancellation_handler,
                socket,
                ClientMode::Tcp,
                endpoint_rate_limiter2,
                conn_gauge,
                cancellations,
            )
            .instrument(ctx.span())
            .boxed()
            .await;

            match res {
                Err(e) => {
                    ctx.set_error_kind(e.get_error_kind());
                    warn!(parent: &ctx.span(), "per-client task finished with an error: {e:#}");
                }
                Ok(None) => {
                    ctx.set_success();
                }
                Ok(Some(p)) => {
                    ctx.set_success();
                    let _disconnect = ctx.log_connect();
                    match p.proxy_pass().await {
                        Ok(()) => {}
                        Err(ErrorSource::Client(e)) => {
                            warn!(
                                ?session_id,
                                "per-client task finished with an IO error from the client: {e:#}"
                            );
                        }
                        Err(ErrorSource::Compute(e)) => {
                            error!(
                                ?session_id,
                                "per-client task finished with an IO error from the compute: {e:#}"
                            );
                        }
                    }
                }
            }
        });
    }

    connections.close();
    cancellations.close();
    drop(listener);

    // Drain connections
    connections.wait().await;
    cancellations.wait().await;

    Ok(())
}

pub(crate) enum ClientMode {
    Tcp,
    Websockets { hostname: Option<String> },
}

/// Abstracts the logic of handling TCP vs WS clients
impl ClientMode {
    pub(crate) fn allow_cleartext(&self) -> bool {
        match self {
            ClientMode::Tcp => false,
            ClientMode::Websockets { .. } => true,
        }
    }

    fn hostname<'a, S>(&'a self, s: &'a Stream<S>) -> Option<&'a str> {
        match self {
            ClientMode::Tcp => s.sni_hostname(),
            ClientMode::Websockets { hostname } => hostname.as_deref(),
        }
    }

    fn handshake_tls<'a>(&self, tls: Option<&'a TlsConfig>) -> Option<&'a TlsConfig> {
        match self {
            ClientMode::Tcp => tls,
            // TLS is None here if using websockets, because the connection is already encrypted.
            ClientMode::Websockets { .. } => None,
        }
    }
}

#[derive(Debug, Error)]
// almost all errors should be reported to the user, but there's a few cases where we cannot
// 1. Cancellation: we are not allowed to tell the client any cancellation statuses for security reasons
// 2. Handshake: handshake reports errors if it can, otherwise if the handshake fails due to protocol violation,
//    we cannot be sure the client even understands our error message
// 3. PrepareClient: The client disconnected, so we can't tell them anyway...
pub(crate) enum ClientRequestError {
    #[error("{0}")]
    Cancellation(#[from] cancellation::CancelError),
    #[error("{0}")]
    Handshake(#[from] HandshakeError),
    #[error("{0}")]
    HandshakeTimeout(#[from] tokio::time::error::Elapsed),
    #[error("{0}")]
    PrepareClient(#[from] std::io::Error),
    #[error("{0}")]
    ReportedError(#[from] crate::stream::ReportedError),
}

impl ReportableError for ClientRequestError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ClientRequestError::Cancellation(e) => e.get_error_kind(),
            ClientRequestError::Handshake(e) => e.get_error_kind(),
            ClientRequestError::HandshakeTimeout(_) => crate::error::ErrorKind::RateLimit,
            ClientRequestError::ReportedError(e) => e.get_error_kind(),
            ClientRequestError::PrepareClient(_) => crate::error::ErrorKind::ClientDisconnect,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_client<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &'static ProxyConfig,
    auth_backend: &'static auth::Backend<'static, ()>,
    ctx: &RequestContext,
    cancellation_handler: Arc<CancellationHandler>,
    stream: S,
    mode: ClientMode,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    conn_gauge: NumClientConnectionsGuard<'static>,
    cancellations: tokio_util::task::task_tracker::TaskTracker,
) -> Result<Option<ProxyPassthrough<S>>, ClientRequestError> {
    debug!(
        protocol = %ctx.protocol(),
        "handling interactive connection from client"
    );

    let metrics = &Metrics::get().proxy;
    let proto = ctx.protocol();
    let request_gauge = metrics.connection_requests.guard(proto);

    let tls = config.tls_config.load();
    let tls = tls.as_deref();

    let record_handshake_error = !ctx.has_private_peer_addr();
    let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Client);
    let do_handshake = handshake(ctx, stream, mode.handshake_tls(tls), record_handshake_error);

    let (mut stream, params) = match tokio::time::timeout(config.handshake_timeout, do_handshake)
        .await??
    {
        HandshakeData::Startup(stream, params) => (stream, params),
        HandshakeData::Cancel(cancel_key_data) => {
            // spawn a task to cancel the session, but don't wait for it
            cancellations.spawn({
                let cancellation_handler_clone = Arc::clone(&cancellation_handler);
                let ctx = ctx.clone();
                let cancel_span = tracing::span!(parent: None, tracing::Level::INFO, "cancel_session", session_id = ?ctx.session_id());
                cancel_span.follows_from(tracing::Span::current());
                async move {
                    cancellation_handler_clone
                        .cancel_session(
                            cancel_key_data,
                            ctx,
                            config.authentication_config.ip_allowlist_check_enabled,
                            config.authentication_config.is_vpc_acccess_proxy,
                            auth_backend.get_api(),
                        )
                        .await
                        .inspect_err(|e | debug!(error = ?e, "cancel_session failed")).ok();
                }.instrument(cancel_span)
            });

            return Ok(None);
        }
    };
    drop(pause);

    ctx.set_db_options(params.clone());

    let hostname = mode.hostname(stream.get_ref());

    let common_names = tls.map(|tls| &tls.common_names);

    // Extract credentials which we're going to use for auth.
    let result = auth_backend
        .as_ref()
        .map(|()| auth::ComputeUserInfoMaybeEndpoint::parse(ctx, &params, hostname, common_names))
        .transpose();

    let user_info = match result {
        Ok(user_info) => user_info,
        Err(e) => Err(stream.throw_error(e, Some(ctx)).await)?,
    };

    let user = user_info.get_user().to_owned();
    let user_info = match user_info
        .authenticate(
            ctx,
            &mut stream,
            mode.allow_cleartext(),
            &config.authentication_config,
            endpoint_rate_limiter,
        )
        .await
    {
        Ok(auth_result) => auth_result,
        Err(e) => {
            let db = params.get("database");
            let app = params.get("application_name");
            let params_span = tracing::info_span!("", ?user, ?db, ?app);

            return Err(stream
                .throw_error(e, Some(ctx))
                .instrument(params_span)
                .await)?;
        }
    };

    let (cplane, creds) = match user_info {
        auth::Backend::ControlPlane(cplane, creds) => (cplane, creds),
        auth::Backend::Local(_) => unreachable!("local proxy does not run tcp proxy service"),
    };
    let params_compat = creds.info.options.get(NeonOptions::PARAMS_COMPAT).is_some();
    let mut auth_info = compute::AuthInfo::with_auth_keys(creds.keys);
    auth_info.set_startup_params(&params, params_compat);

    let res = connect_to_compute(
        ctx,
        &TcpMechanism {
            locks: &config.connect_compute_locks,
        },
        &auth::Backend::ControlPlane(cplane, creds.info.clone()),
        config.wake_compute_retry_config,
        &config.connect_to_compute,
    )
    .await;

    let mut node = match res {
        Ok(node) => node,
        Err(e) => Err(stream.throw_error(e, Some(ctx)).await)?,
    };

    let pg_settings = auth_info.authenticate(ctx, &mut node, creds.info).await;
    let pg_settings = match pg_settings {
        Ok(pg_settings) => pg_settings,
        Err(e) => Err(stream.throw_error(e, Some(ctx)).await)?,
    };

    let session = cancellation_handler.get_key();

    prepare_client_connection(&pg_settings, *session.key(), &mut stream);
    let stream = stream.flush_and_into_inner().await?;

    let session_id = ctx.session_id();
    let (cancel_on_shutdown, cancel) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        session
            .maintain_cancel_key(
                session_id,
                cancel,
                &pg_settings.cancel_closure,
                &config.connect_to_compute,
            )
            .await;
    });

    let private_link_id = match ctx.extra() {
        Some(ConnectionInfoExtra::Aws { vpce_id }) => Some(vpce_id.clone()),
        Some(ConnectionInfoExtra::Azure { link_id }) => Some(link_id.to_smolstr()),
        None => None,
    };

    Ok(Some(ProxyPassthrough {
        client: stream,
        compute: node.stream,

        aux: node.aux,
        private_link_id,

        _cancel_on_shutdown: cancel_on_shutdown,

        _req: request_gauge,
        _conn: conn_gauge,
        _db_conn: node.guage,
    }))
}

/// Finish client connection initialization: confirm auth success, send params, etc.
pub(crate) fn prepare_client_connection(
    settings: &compute::PostgresSettings,
    cancel_key_data: CancelKeyData,
    stream: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) {
    // Forward all deferred notices to the client.
    for notice in &settings.delayed_notice {
        stream.write_raw(notice.as_bytes().len(), b'N', |buf| {
            buf.extend_from_slice(notice.as_bytes());
        });
    }

    // Forward all postgres connection params to the client.
    for (name, value) in &settings.params {
        stream.write_message(BeMessage::ParameterStatus {
            name: name.as_bytes(),
            value: value.as_bytes(),
        });
    }

    stream.write_message(BeMessage::BackendKeyData(cancel_key_data));
    stream.write_message(BeMessage::ReadyForQuery);
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct NeonOptions(Vec<(SmolStr, SmolStr)>);

impl NeonOptions {
    // proxy options:

    /// `PARAMS_COMPAT` allows opting in to forwarding all startup parameters from client to compute.
    const PARAMS_COMPAT: &str = "proxy_params_compat";

    // cplane options:

    /// `LSN` allows provisioning an ephemeral compute with time-travel to the provided LSN.
    const LSN: &str = "lsn";

    /// `ENDPOINT_TYPE` allows configuring an ephemeral compute to be read_only or read_write.
    const ENDPOINT_TYPE: &str = "endpoint_type";

    pub(crate) fn parse_params(params: &StartupMessageParams) -> Self {
        params
            .options_raw()
            .map(Self::parse_from_iter)
            .unwrap_or_default()
    }

    pub(crate) fn parse_options_raw(options: &str) -> Self {
        Self::parse_from_iter(StartupMessageParams::parse_options_raw(options))
    }

    pub(crate) fn get(&self, key: &str) -> Option<SmolStr> {
        self.0
            .iter()
            .find_map(|(k, v)| (k == key).then_some(v))
            .cloned()
    }

    pub(crate) fn is_ephemeral(&self) -> bool {
        self.0.iter().any(|(k, _)| match &**k {
            // This is not a cplane option, we know it does not create ephemeral computes.
            Self::PARAMS_COMPAT => false,
            Self::LSN => true,
            Self::ENDPOINT_TYPE => true,
            // err on the side of caution. any cplane options we don't know about
            // might lead to ephemeral computes.
            _ => true,
        })
    }

    fn parse_from_iter<'a>(options: impl Iterator<Item = &'a str>) -> Self {
        let mut options = options
            .filter_map(neon_option)
            .map(|(k, v)| (k.into(), v.into()))
            .collect_vec();
        options.sort();
        Self(options)
    }

    pub(crate) fn get_cache_key(&self, prefix: &str) -> EndpointCacheKey {
        // prefix + format!(" {k}:{v}")
        // kinda jank because SmolStr is immutable
        std::iter::once(prefix)
            .chain(self.0.iter().flat_map(|(k, v)| [" ", &**k, ":", &**v]))
            .collect::<SmolStr>()
            .into()
    }

    /// <https://swagger.io/docs/specification/serialization/> DeepObject format
    /// `paramName[prop1]=value1&paramName[prop2]=value2&...`
    pub(crate) fn to_deep_object(&self) -> Vec<(SmolStr, SmolStr)> {
        self.0
            .iter()
            .map(|(k, v)| (format_smolstr!("options[{}]", k), v.clone()))
            .collect()
    }
}

pub(crate) fn neon_option(bytes: &str) -> Option<(&str, &str)> {
    static RE: OnceCell<Regex> = OnceCell::new();
    let re = RE.get_or_init(|| Regex::new(r"^neon_(\w+):(.+)").expect("regex should be correct"));

    let cap = re.captures(bytes)?;
    let (_, [k, v]) = cap.extract();
    Some((k, v))
}
