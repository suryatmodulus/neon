use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use metrics::launch_timestamp::LaunchTimestamp;
use pageserver_api::models::DownloadRemoteLayersTaskSpawnRequest;
use remote_storage::GenericRemoteStorage;
use tenant_size_model::{SizeResult, StorageModel};
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::http::endpoint::RequestSpan;
use utils::http::request::{get_request_param, must_get_query_param, parse_query_param};

use super::models::{
    StatusResponse, TenantConfigRequest, TenantCreateRequest, TenantCreateResponse, TenantInfo,
    TimelineCreateRequest, TimelineGcRequest, TimelineInfo,
};
use crate::context::{DownloadBehavior, RequestContext};
use crate::pgdatadir_mapping::LsnForTimestamp;
use crate::task_mgr::TaskKind;
use crate::tenant::config::TenantConfOpt;
use crate::tenant::mgr::TenantMapInsertError;
use crate::tenant::size::ModelInputs;
use crate::tenant::storage_layer::LayerAccessStatsReset;
use crate::tenant::{PageReconstructError, Timeline};
use crate::{config::PageServerConf, tenant::mgr};
use utils::{
    auth::JwtAuth,
    http::{
        endpoint::{self, attach_openapi_ui, auth_middleware, check_permission_with},
        error::{ApiError, HttpErrorBody},
        json::{json_request, json_response},
        request::parse_request_param,
        RequestExt, RouterBuilder,
    },
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

// Imports only used for testing APIs
#[cfg(feature = "testing")]
use super::models::ConfigureFailpointsRequest;

struct State {
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    allowlist_routes: Vec<Uri>,
    remote_storage: Option<GenericRemoteStorage>,
}

impl State {
    fn new(
        conf: &'static PageServerConf,
        auth: Option<Arc<JwtAuth>>,
        remote_storage: Option<GenericRemoteStorage>,
    ) -> anyhow::Result<Self> {
        let allowlist_routes = ["/v1/status", "/v1/doc", "/swagger.yml"]
            .iter()
            .map(|v| v.parse().unwrap())
            .collect::<Vec<_>>();
        Ok(Self {
            conf,
            auth,
            allowlist_routes,
            remote_storage,
        })
    }
}

#[inline(always)]
fn get_state(request: &Request<Body>) -> &State {
    request
        .data::<Arc<State>>()
        .expect("unknown state type")
        .as_ref()
}

#[inline(always)]
fn get_config(request: &Request<Body>) -> &'static PageServerConf {
    get_state(request).conf
}

fn check_permission(request: &Request<Body>, tenant_id: Option<TenantId>) -> Result<(), ApiError> {
    check_permission_with(request, |claims| {
        crate::auth::check_permission(claims, tenant_id)
    })
}

fn apierror_from_prerror(err: PageReconstructError) -> ApiError {
    match err {
        PageReconstructError::Other(err) => ApiError::InternalServerError(err),
        PageReconstructError::NeedsDownload(_, _) => {
            // This shouldn't happen, because we use a RequestContext that requests to
            // download any missing layer files on-demand.
            ApiError::InternalServerError(anyhow::anyhow!("need to download remote layer file"))
        }
        PageReconstructError::Cancelled => {
            ApiError::InternalServerError(anyhow::anyhow!("request was cancelled"))
        }
        PageReconstructError::WalRedo(err) => {
            ApiError::InternalServerError(anyhow::Error::new(err))
        }
    }
}

fn apierror_from_tenant_map_insert_error(e: TenantMapInsertError) -> ApiError {
    match e {
        TenantMapInsertError::StillInitializing | TenantMapInsertError::ShuttingDown => {
            ApiError::InternalServerError(anyhow::Error::new(e))
        }
        TenantMapInsertError::TenantAlreadyExists(id, state) => {
            ApiError::Conflict(format!("tenant {id} already exists, state: {state:?}"))
        }
        TenantMapInsertError::Closure(e) => ApiError::InternalServerError(e),
    }
}

// Helper function to construct a TimelineInfo struct for a timeline
async fn build_timeline_info(
    timeline: &Arc<Timeline>,
    include_non_incremental_logical_size: bool,
    ctx: &RequestContext,
) -> anyhow::Result<TimelineInfo> {
    let mut info = build_timeline_info_common(timeline, ctx)?;
    if include_non_incremental_logical_size {
        // XXX we should be using spawn_ondemand_logical_size_calculation here.
        // Otherwise, if someone deletes the timeline / detaches the tenant while
        // we're executing this function, we will outlive the timeline on-disk state.
        info.current_logical_size_non_incremental = Some(
            timeline
                .get_current_logical_size_non_incremental(
                    info.last_record_lsn,
                    CancellationToken::new(),
                    ctx,
                )
                .await?,
        );
    }
    Ok(info)
}

fn build_timeline_info_common(
    timeline: &Arc<Timeline>,
    ctx: &RequestContext,
) -> anyhow::Result<TimelineInfo> {
    let last_record_lsn = timeline.get_last_record_lsn();
    let (wal_source_connstr, last_received_msg_lsn, last_received_msg_ts) = {
        let guard = timeline.last_received_wal.lock().unwrap();
        if let Some(info) = guard.as_ref() {
            (
                Some(format!("{:?}", info.wal_source_connconf)), // Password is hidden, but it's for statistics only.
                Some(info.last_received_msg_lsn),
                Some(info.last_received_msg_ts),
            )
        } else {
            (None, None, None)
        }
    };

    let ancestor_timeline_id = timeline.get_ancestor_timeline_id();
    let ancestor_lsn = match timeline.get_ancestor_lsn() {
        Lsn(0) => None,
        lsn @ Lsn(_) => Some(lsn),
    };
    let current_logical_size = match timeline.get_current_logical_size(ctx) {
        Ok((size, _)) => Some(size),
        Err(err) => {
            error!("Timeline info creation failed to get current logical size: {err:?}");
            None
        }
    };
    let current_physical_size = Some(timeline.layer_size_sum().approximate_is_ok());
    let state = timeline.current_state();
    let remote_consistent_lsn = timeline.get_remote_consistent_lsn().unwrap_or(Lsn(0));

    let info = TimelineInfo {
        tenant_id: timeline.tenant_id,
        timeline_id: timeline.timeline_id,
        ancestor_timeline_id,
        ancestor_lsn,
        disk_consistent_lsn: timeline.get_disk_consistent_lsn(),
        remote_consistent_lsn,
        last_record_lsn,
        prev_record_lsn: Some(timeline.get_prev_record_lsn()),
        latest_gc_cutoff_lsn: *timeline.get_latest_gc_cutoff_lsn(),
        current_logical_size,
        current_physical_size,
        current_logical_size_non_incremental: None,
        timeline_dir_layer_file_size_sum: None,
        wal_source_connstr,
        last_received_msg_lsn,
        last_received_msg_ts,
        pg_version: timeline.pg_version,

        state,
    };
    Ok(info)
}

// healthcheck handler
async fn status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;
    let config = get_config(&request);
    json_response(StatusCode::OK, StatusResponse { id: config.id })
}

async fn timeline_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let request_data: TimelineCreateRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_id))?;

    let new_timeline_id = request_data
        .new_timeline_id
        .unwrap_or_else(TimelineId::generate);

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Error);

    let tenant = mgr::get_tenant(tenant_id, true)
        .await
        .map_err(ApiError::NotFound)?;
    match tenant.create_timeline(
        new_timeline_id,
        request_data.ancestor_timeline_id.map(TimelineId::from),
        request_data.ancestor_start_lsn,
        request_data.pg_version.unwrap_or(crate::DEFAULT_PG_VERSION),
        &ctx,
    )
    .instrument(info_span!("timeline_create", tenant = %tenant_id, new_timeline = ?request_data.new_timeline_id, timeline_id = %new_timeline_id, lsn=?request_data.ancestor_start_lsn, pg_version=?request_data.pg_version))
    .await {
        Ok(Some(new_timeline)) => {
            // Created. Construct a TimelineInfo for it.
            let timeline_info = build_timeline_info_common(&new_timeline, &ctx)
                .map_err(ApiError::InternalServerError)?;
            json_response(StatusCode::CREATED, timeline_info)
        }
        Ok(None) => json_response(StatusCode::CONFLICT, ()), // timeline already exists
        Err(err) => Err(ApiError::InternalServerError(err)),
    }
}

async fn timeline_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let include_non_incremental_logical_size: Option<bool> =
        parse_query_param(&request, "include-non-incremental-logical-size")?;
    check_permission(&request, Some(tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);

    let response_data = async {
        let tenant = mgr::get_tenant(tenant_id, true)
            .await
            .map_err(ApiError::NotFound)?;
        let timelines = tenant.list_timelines();

        let mut response_data = Vec::with_capacity(timelines.len());
        for timeline in timelines {
            let timeline_info = build_timeline_info(
                &timeline,
                include_non_incremental_logical_size.unwrap_or(false),
                &ctx,
            )
            .await
            .context("Failed to convert tenant timeline {timeline_id} into the local one: {e:?}")
            .map_err(ApiError::InternalServerError)?;

            response_data.push(timeline_info);
        }
        Ok(response_data)
    }
    .instrument(info_span!("timeline_list", tenant = %tenant_id))
    .await?;

    json_response(StatusCode::OK, response_data)
}

async fn timeline_detail_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let include_non_incremental_logical_size: Option<bool> =
        parse_query_param(&request, "include-non-incremental-logical-size")?;
    check_permission(&request, Some(tenant_id))?;

    // Logical size calculation needs downloading.
    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);

    let timeline_info = async {
        let tenant = mgr::get_tenant(tenant_id, true)
            .await
            .map_err(ApiError::NotFound)?;

        let timeline = tenant
            .get_timeline(timeline_id, false)
            .map_err(ApiError::NotFound)?;

        let timeline_info = build_timeline_info(
            &timeline,
            include_non_incremental_logical_size.unwrap_or(false),
            &ctx,
        )
        .await
        .context("get local timeline info")
        .map_err(ApiError::InternalServerError)?;

        Ok::<_, ApiError>(timeline_info)
    }
    .instrument(info_span!("timeline_detail", tenant = %tenant_id, timeline = %timeline_id))
    .await?;

    json_response(StatusCode::OK, timeline_info)
}

async fn get_lsn_by_timestamp_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let timestamp_raw = must_get_query_param(&request, "timestamp")?;
    let timestamp = humantime::parse_rfc3339(&timestamp_raw)
        .with_context(|| format!("Invalid time: {:?}", timestamp_raw))
        .map_err(ApiError::BadRequest)?;
    let timestamp_pg = postgres_ffi::to_pg_timestamp(timestamp);

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    let result = timeline
        .find_lsn_for_timestamp(timestamp_pg, &ctx)
        .await
        .map_err(apierror_from_prerror)?;

    let result = match result {
        LsnForTimestamp::Present(lsn) => format!("{lsn}"),
        LsnForTimestamp::Future(_lsn) => "future".into(),
        LsnForTimestamp::Past(_lsn) => "past".into(),
        LsnForTimestamp::NoData(_lsn) => "nodata".into(),
    };
    json_response(StatusCode::OK, result)
}

// TODO makes sense to provide tenant config right away the same way as it handled in tenant_create
async fn tenant_attach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    info!("Handling tenant attach {tenant_id}");

    let state = get_state(&request);

    if let Some(remote_storage) = &state.remote_storage {
        mgr::attach_tenant(state.conf, tenant_id, remote_storage.clone(), &ctx)
            .instrument(info_span!("tenant_attach", tenant = %tenant_id))
            .await
            .map_err(apierror_from_tenant_map_insert_error)?;
    } else {
        return Err(ApiError::BadRequest(anyhow!(
            "attach_tenant is not possible because pageserver was configured without remote storage"
        )));
    }

    json_response(StatusCode::ACCEPTED, ())
}

async fn timeline_delete_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    mgr::delete_timeline(tenant_id, timeline_id, &ctx)
        .instrument(info_span!("timeline_delete", tenant = %tenant_id, timeline = %timeline_id))
        .await
        // FIXME: Errors from `delete_timeline` can occur for a number of reasons, incuding both
        // user and internal errors. Replace this with better handling once the error type permits
        // it.
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

async fn tenant_detach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let state = get_state(&request);
    let conf = state.conf;
    mgr::detach_tenant(conf, tenant_id)
        .instrument(info_span!("tenant_detach", tenant = %tenant_id))
        .await
        // FIXME: Errors from `detach_tenant` can be caused by both both user and internal errors.
        // Replace this with better handling once the error type permits it.
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

async fn tenant_load_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    let state = get_state(&request);
    mgr::load_tenant(state.conf, tenant_id, state.remote_storage.clone(), &ctx)
        .instrument(info_span!("load", tenant = %tenant_id))
        .await
        .map_err(apierror_from_tenant_map_insert_error)?;

    json_response(StatusCode::ACCEPTED, ())
}

async fn tenant_ignore_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let state = get_state(&request);
    let conf = state.conf;
    mgr::ignore_tenant(conf, tenant_id)
        .instrument(info_span!("ignore_tenant", tenant = %tenant_id))
        .await
        // FIXME: Errors from `ignore_tenant` can be caused by both both user and internal errors.
        // Replace this with better handling once the error type permits it.
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

async fn tenant_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;

    let response_data = mgr::list_tenants()
        .instrument(info_span!("tenant_list"))
        .await
        .map_err(anyhow::Error::new)
        .map_err(ApiError::InternalServerError)?
        .iter()
        .map(|(id, state)| TenantInfo {
            id: *id,
            state: *state,
            current_physical_size: None,
            has_in_progress_downloads: Some(state.has_in_progress_downloads()),
        })
        .collect::<Vec<TenantInfo>>();

    json_response(StatusCode::OK, response_data)
}

async fn tenant_status(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let tenant_info = async {
        let tenant = mgr::get_tenant(tenant_id, false).await?;

        // Calculate total physical size of all timelines
        let mut current_physical_size = 0;
        for timeline in tenant.list_timelines().iter() {
            current_physical_size += timeline.layer_size_sum().approximate_is_ok();
        }

        let state = tenant.current_state();
        Ok(TenantInfo {
            id: tenant_id,
            state,
            current_physical_size: Some(current_physical_size),
            has_in_progress_downloads: Some(state.has_in_progress_downloads()),
        })
    }
    .instrument(info_span!("tenant_status_handler", tenant = %tenant_id))
    .await
    .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, tenant_info)
}

/// HTTP endpoint to query the current tenant_size of a tenant.
///
/// This is not used by consumption metrics under [`crate::consumption_metrics`], but can be used
/// to debug any of the calculations. Requires `tenant_id` request parameter, supports
/// `inputs_only=true|false` (default false) which supports debugging failure to calculate model
/// values.
///
/// 'retention_period' query parameter overrides the cutoff that is used to calculate the size
/// (only if it is shorter than the real cutoff).
///
/// Note: we don't update the cached size and prometheus metric here.
/// The retention period might be different, and it's nice to have a method to just calculate it
/// without modifying anything anyway.
async fn tenant_size_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    let inputs_only: Option<bool> = parse_query_param(&request, "inputs_only")?;
    let retention_period: Option<u64> = parse_query_param(&request, "retention_period")?;
    let headers = request.headers();

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let tenant = mgr::get_tenant(tenant_id, true)
        .await
        .map_err(ApiError::InternalServerError)?;

    // this can be long operation
    let inputs = tenant
        .gather_size_inputs(retention_period, &ctx)
        .await
        .map_err(ApiError::InternalServerError)?;

    let mut sizes = None;
    if !inputs_only.unwrap_or(false) {
        let storage_model = inputs
            .calculate_model()
            .map_err(ApiError::InternalServerError)?;
        let size = storage_model.calculate();

        // If request header expects html, return html
        if headers["Accept"] == "text/html" {
            return synthetic_size_html_response(inputs, storage_model, size);
        }
        sizes = Some(size);
    } else if headers["Accept"] == "text/html" {
        return Err(ApiError::BadRequest(anyhow!(
            "inputs_only parameter is incompatible with html output request"
        )));
    }

    /// The type resides in the pageserver not to expose `ModelInputs`.
    #[serde_with::serde_as]
    #[derive(serde::Serialize)]
    struct TenantHistorySize {
        #[serde_as(as = "serde_with::DisplayFromStr")]
        id: TenantId,
        /// Size is a mixture of WAL and logical size, so the unit is bytes.
        ///
        /// Will be none if `?inputs_only=true` was given.
        size: Option<u64>,
        /// Size of each segment used in the model.
        /// Will be null if `?inputs_only=true` was given.
        segment_sizes: Option<Vec<tenant_size_model::SegmentSizeResult>>,
        inputs: crate::tenant::size::ModelInputs,
    }

    json_response(
        StatusCode::OK,
        TenantHistorySize {
            id: tenant_id,
            size: sizes.as_ref().map(|x| x.total_size),
            segment_sizes: sizes.map(|x| x.segments),
            inputs,
        },
    )
}

async fn layer_map_info_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let reset: LayerAccessStatsReset =
        parse_query_param(&request, "reset")?.unwrap_or(LayerAccessStatsReset::NoReset);

    check_permission(&request, Some(tenant_id))?;

    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    let layer_map_info = timeline.layer_map_info(reset);

    json_response(StatusCode::OK, layer_map_info)
}

async fn layer_download_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let layer_file_name = get_request_param(&request, "layer_file_name")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    let downloaded = timeline
        .download_layer(layer_file_name)
        .await
        .map_err(ApiError::InternalServerError)?;

    match downloaded {
        Some(true) => json_response(StatusCode::OK, ()),
        Some(false) => json_response(StatusCode::NOT_MODIFIED, ()),
        None => json_response(
            StatusCode::BAD_REQUEST,
            format!("Layer {tenant_id}/{timeline_id}/{layer_file_name} not found"),
        ),
    }
}

async fn evict_timeline_layer_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let layer_file_name = get_request_param(&request, "layer_file_name")?;

    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    let evicted = timeline
        .evict_layer(layer_file_name)
        .await
        .map_err(ApiError::InternalServerError)?;

    match evicted {
        Some(true) => json_response(StatusCode::OK, ()),
        Some(false) => json_response(StatusCode::NOT_MODIFIED, ()),
        None => json_response(
            StatusCode::BAD_REQUEST,
            format!("Layer {tenant_id}/{timeline_id}/{layer_file_name} not found"),
        ),
    }
}

/// Get tenant_size SVG graph along with the JSON data.
fn synthetic_size_html_response(
    inputs: ModelInputs,
    storage_model: StorageModel,
    sizes: SizeResult,
) -> Result<Response<Body>, ApiError> {
    let mut timeline_ids: Vec<String> = Vec::new();
    let mut timeline_map: HashMap<TimelineId, usize> = HashMap::new();
    for (index, ti) in inputs.timeline_inputs.iter().enumerate() {
        timeline_map.insert(ti.timeline_id, index);
        timeline_ids.push(ti.timeline_id.to_string());
    }
    let seg_to_branch: Vec<usize> = inputs
        .segments
        .iter()
        .map(|seg| *timeline_map.get(&seg.timeline_id).unwrap())
        .collect();

    let svg =
        tenant_size_model::svg::draw_svg(&storage_model, &timeline_ids, &seg_to_branch, &sizes)
            .map_err(ApiError::InternalServerError)?;

    let mut response = String::new();

    use std::fmt::Write;
    write!(response, "<html>\n<body>\n").unwrap();
    write!(response, "<div>\n{svg}\n</div>").unwrap();
    writeln!(response, "Project size: {}", sizes.total_size).unwrap();
    writeln!(response, "<pre>").unwrap();
    writeln!(
        response,
        "{}",
        serde_json::to_string_pretty(&inputs).unwrap()
    )
    .unwrap();
    writeln!(
        response,
        "{}",
        serde_json::to_string_pretty(&sizes.segments).unwrap()
    )
    .unwrap();
    writeln!(response, "</pre>").unwrap();
    write!(response, "</body>\n</html>\n").unwrap();

    html_response(StatusCode::OK, response)
}

pub fn html_response(status: StatusCode, data: String) -> Result<Response<Body>, ApiError> {
    let response = Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/html")
        .body(Body::from(data.as_bytes().to_vec()))
        .map_err(|e| ApiError::InternalServerError(e.into()))?;
    Ok(response)
}

// Helper function to standardize the error messages we produce on bad durations
//
// Intended to be used with anyhow's `with_context`, e.g.:
//
//   let value = result.with_context(bad_duration("name", &value))?;
//
fn bad_duration<'a>(field_name: &'static str, value: &'a str) -> impl 'a + Fn() -> String {
    move || format!("Cannot parse `{field_name}` duration {value:?}")
}

async fn tenant_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    let request_data: TenantCreateRequest = json_request(&mut request).await?;

    let mut tenant_conf = TenantConfOpt::default();
    if let Some(gc_period) = request_data.gc_period {
        tenant_conf.gc_period = Some(
            humantime::parse_duration(&gc_period)
                .with_context(bad_duration("gc_period", &gc_period))
                .map_err(ApiError::BadRequest)?,
        );
    }
    tenant_conf.gc_horizon = request_data.gc_horizon;
    tenant_conf.image_creation_threshold = request_data.image_creation_threshold;

    if let Some(pitr_interval) = request_data.pitr_interval {
        tenant_conf.pitr_interval = Some(
            humantime::parse_duration(&pitr_interval)
                .with_context(bad_duration("pitr_interval", &pitr_interval))
                .map_err(ApiError::BadRequest)?,
        );
    }

    if let Some(walreceiver_connect_timeout) = request_data.walreceiver_connect_timeout {
        tenant_conf.walreceiver_connect_timeout = Some(
            humantime::parse_duration(&walreceiver_connect_timeout)
                .with_context(bad_duration(
                    "walreceiver_connect_timeout",
                    &walreceiver_connect_timeout,
                ))
                .map_err(ApiError::BadRequest)?,
        );
    }
    if let Some(lagging_wal_timeout) = request_data.lagging_wal_timeout {
        tenant_conf.lagging_wal_timeout = Some(
            humantime::parse_duration(&lagging_wal_timeout)
                .with_context(bad_duration("lagging_wal_timeout", &lagging_wal_timeout))
                .map_err(ApiError::BadRequest)?,
        );
    }
    if let Some(max_lsn_wal_lag) = request_data.max_lsn_wal_lag {
        tenant_conf.max_lsn_wal_lag = Some(max_lsn_wal_lag);
    }
    if let Some(trace_read_requests) = request_data.trace_read_requests {
        tenant_conf.trace_read_requests = Some(trace_read_requests);
    }

    tenant_conf.checkpoint_distance = request_data.checkpoint_distance;
    if let Some(checkpoint_timeout) = request_data.checkpoint_timeout {
        tenant_conf.checkpoint_timeout = Some(
            humantime::parse_duration(&checkpoint_timeout)
                .with_context(bad_duration("checkpoint_timeout", &checkpoint_timeout))
                .map_err(ApiError::BadRequest)?,
        );
    }

    tenant_conf.compaction_target_size = request_data.compaction_target_size;
    tenant_conf.compaction_threshold = request_data.compaction_threshold;

    if let Some(compaction_period) = request_data.compaction_period {
        tenant_conf.compaction_period = Some(
            humantime::parse_duration(&compaction_period)
                .with_context(bad_duration("compaction_period", &compaction_period))
                .map_err(ApiError::BadRequest)?,
        );
    }

    let target_tenant_id = request_data
        .new_tenant_id
        .map(TenantId::from)
        .unwrap_or_else(TenantId::generate);

    let state = get_state(&request);

    let new_tenant = mgr::create_tenant(
        state.conf,
        tenant_conf,
        target_tenant_id,
        state.remote_storage.clone(),
        &ctx,
    )
    .instrument(info_span!("tenant_create", tenant = ?target_tenant_id))
    .await
    .map_err(apierror_from_tenant_map_insert_error)?;

    // We created the tenant. Existing API semantics are that the tenant
    // is Active when this function returns.
    if let res @ Err(_) = new_tenant.wait_to_become_active().await {
        // This shouldn't happen because we just created the tenant directory
        // in tenant::mgr::create_tenant, and there aren't any remote timelines
        // to load, so, nothing can really fail during load.
        // Don't do cleanup because we don't know how we got here.
        // The tenant will likely be in `Broken` state and subsequent
        // calls will fail.
        res.context("created tenant failed to become active")
            .map_err(ApiError::InternalServerError)?;
    }
    json_response(
        StatusCode::CREATED,
        TenantCreateResponse(new_tenant.tenant_id()),
    )
}

async fn get_tenant_config_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let tenant = mgr::get_tenant(tenant_id, false)
        .await
        .map_err(ApiError::NotFound)?;

    let response = HashMap::from([
        (
            "tenant_specific_overrides",
            serde_json::to_value(tenant.tenant_specific_overrides())
                .context("serializing tenant specific overrides")
                .map_err(ApiError::InternalServerError)?,
        ),
        (
            "effective_config",
            serde_json::to_value(tenant.effective_config())
                .context("serializing effective config")
                .map_err(ApiError::InternalServerError)?,
        ),
    ]);

    json_response(StatusCode::OK, response)
}

async fn update_tenant_config_handler(
    mut request: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let request_data: TenantConfigRequest = json_request(&mut request).await?;
    let tenant_id = request_data.tenant_id;
    check_permission(&request, Some(tenant_id))?;

    let mut tenant_conf = TenantConfOpt::default();
    if let Some(gc_period) = request_data.gc_period {
        tenant_conf.gc_period = Some(
            humantime::parse_duration(&gc_period)
                .with_context(bad_duration("gc_period", &gc_period))
                .map_err(ApiError::BadRequest)?,
        );
    }
    tenant_conf.gc_horizon = request_data.gc_horizon;
    tenant_conf.image_creation_threshold = request_data.image_creation_threshold;

    if let Some(pitr_interval) = request_data.pitr_interval {
        tenant_conf.pitr_interval = Some(
            humantime::parse_duration(&pitr_interval)
                .with_context(bad_duration("pitr_interval", &pitr_interval))
                .map_err(ApiError::BadRequest)?,
        );
    }
    if let Some(walreceiver_connect_timeout) = request_data.walreceiver_connect_timeout {
        tenant_conf.walreceiver_connect_timeout = Some(
            humantime::parse_duration(&walreceiver_connect_timeout)
                .with_context(bad_duration(
                    "walreceiver_connect_timeout",
                    &walreceiver_connect_timeout,
                ))
                .map_err(ApiError::BadRequest)?,
        );
    }
    if let Some(lagging_wal_timeout) = request_data.lagging_wal_timeout {
        tenant_conf.lagging_wal_timeout = Some(
            humantime::parse_duration(&lagging_wal_timeout)
                .with_context(bad_duration("lagging_wal_timeout", &lagging_wal_timeout))
                .map_err(ApiError::BadRequest)?,
        );
    }
    tenant_conf.max_lsn_wal_lag = request_data.max_lsn_wal_lag;
    tenant_conf.trace_read_requests = request_data.trace_read_requests;

    tenant_conf.checkpoint_distance = request_data.checkpoint_distance;
    if let Some(checkpoint_timeout) = request_data.checkpoint_timeout {
        tenant_conf.checkpoint_timeout = Some(
            humantime::parse_duration(&checkpoint_timeout)
                .with_context(bad_duration("checkpoint_timeout", &checkpoint_timeout))
                .map_err(ApiError::BadRequest)?,
        );
    }
    tenant_conf.compaction_target_size = request_data.compaction_target_size;
    tenant_conf.compaction_threshold = request_data.compaction_threshold;

    if let Some(compaction_period) = request_data.compaction_period {
        tenant_conf.compaction_period = Some(
            humantime::parse_duration(&compaction_period)
                .with_context(bad_duration("compaction_period", &compaction_period))
                .map_err(ApiError::BadRequest)?,
        );
    }

    if let Some(eviction_policy) = request_data.eviction_policy {
        tenant_conf.eviction_policy = Some(
            serde_json::from_value(eviction_policy)
                .context("parse field `eviction_policy`")
                .map_err(ApiError::BadRequest)?,
        );
    }

    let state = get_state(&request);
    mgr::set_new_tenant_config(state.conf, tenant_conf, tenant_id)
        .instrument(info_span!("tenant_config", tenant = ?tenant_id))
        .await
        // FIXME: `update_tenant_config` can fail because of both user and internal errors.
        // Replace this `map_err` with better error handling once the type permits it
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

#[cfg(feature = "testing")]
async fn failpoints_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    if !fail::has_failpoints() {
        return Err(ApiError::BadRequest(anyhow!(
            "Cannot manage failpoints because pageserver was compiled without failpoints support"
        )));
    }

    let failpoints: ConfigureFailpointsRequest = json_request(&mut request).await?;
    for fp in failpoints {
        info!("cfg failpoint: {} {}", fp.name, fp.actions);

        // We recognize one extra "action" that's not natively recognized
        // by the failpoints crate: exit, to immediately kill the process
        let cfg_result = if fp.actions == "exit" {
            fail::cfg_callback(fp.name, || {
                info!("Exit requested by failpoint");
                std::process::exit(1);
            })
        } else {
            fail::cfg(fp.name, &fp.actions)
        };

        if let Err(err_msg) = cfg_result {
            return Err(ApiError::BadRequest(anyhow!(
                "Failed to configure failpoints: {err_msg}"
            )));
        }
    }

    json_response(StatusCode::OK, ())
}

// Run GC immediately on given timeline.
async fn timeline_gc_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let gc_req: TimelineGcRequest = json_request(&mut request).await?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let wait_task_done = mgr::immediate_gc(tenant_id, timeline_id, gc_req, &ctx).await?;
    let gc_result = wait_task_done
        .await
        .context("wait for gc task")
        .map_err(ApiError::InternalServerError)?
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, gc_result)
}

// Run compaction immediately on given timeline.
#[cfg(feature = "testing")]
async fn timeline_compact_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let result_receiver = mgr::immediate_compact(tenant_id, timeline_id, &ctx)
        .await
        .context("spawn compaction task")
        .map_err(ApiError::InternalServerError)?;

    let result: anyhow::Result<()> = result_receiver
        .await
        .context("receive compaction result")
        .map_err(ApiError::InternalServerError)?;
    result.map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

// Run checkpoint immediately on given timeline.
#[cfg(feature = "testing")]
async fn timeline_checkpoint_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;
    async {
        let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
        let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
        timeline
            .freeze_and_flush()
            .await
            .map_err(ApiError::InternalServerError)?;
        timeline
            .compact(&ctx)
            .await
            .map_err(ApiError::InternalServerError)?;

        json_response(StatusCode::OK, ())
    }
    .instrument(info_span!("manual_checkpoint", tenant_id = %tenant_id, timeline_id = %timeline_id))
    .await
}

async fn timeline_download_remote_layers_handler_post(
    mut request: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let body: DownloadRemoteLayersTaskSpawnRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_id))?;

    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    match timeline.spawn_download_all_remote_layers(body).await {
        Ok(st) => json_response(StatusCode::ACCEPTED, st),
        Err(st) => json_response(StatusCode::CONFLICT, st),
    }
}

async fn timeline_download_remote_layers_handler_get(
    request: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;

    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    let info = timeline
        .get_download_all_remote_layers_task_info()
        .context("task never started since last pageserver process start")
        .map_err(ApiError::NotFound)?;
    json_response(StatusCode::OK, info)
}

async fn active_timeline_of_active_tenant(
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Result<Arc<Timeline>, ApiError> {
    let tenant = mgr::get_tenant(tenant_id, true)
        .await
        .map_err(ApiError::NotFound)?;
    tenant
        .get_timeline(timeline_id, true)
        .map_err(ApiError::NotFound)
}

async fn always_panic_handler(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    // Deliberately cause a panic to exercise the panic hook registered via std::panic::set_hook().
    // For pageserver, the relevant panic hook is `tracing_panic_hook` , and the `sentry` crate's wrapper around it.
    // Use catch_unwind to ensure that tokio nor hyper are distracted by our panic.
    let query = req.uri().query();
    let _ = std::panic::catch_unwind(|| {
        panic!("unconditional panic for testing panic hook integration; request query: {query:?}")
    });
    json_response(StatusCode::NO_CONTENT, ())
}

async fn handler_404(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(
        StatusCode::NOT_FOUND,
        HttpErrorBody::from_msg("page not found".to_owned()),
    )
}

pub fn make_router(
    conf: &'static PageServerConf,
    launch_ts: &'static LaunchTimestamp,
    auth: Option<Arc<JwtAuth>>,
    remote_storage: Option<GenericRemoteStorage>,
) -> anyhow::Result<RouterBuilder<hyper::Body, ApiError>> {
    let spec = include_bytes!("openapi_spec.yml");
    let mut router = attach_openapi_ui(endpoint::make_router(), spec, "/swagger.yml", "/v1/doc");
    if auth.is_some() {
        router = router.middleware(auth_middleware(|request| {
            let state = get_state(request);
            if state.allowlist_routes.contains(request.uri()) {
                None
            } else {
                state.auth.as_deref()
            }
        }))
    }

    router = router.middleware(
        endpoint::add_response_header_middleware(
            "PAGESERVER_LAUNCH_TIMESTAMP",
            &launch_ts.to_string(),
        )
        .expect("construct launch timestamp header middleware"),
    );

    macro_rules! testing_api {
        ($handler_desc:literal, $handler:path $(,)?) => {{
            #[cfg(not(feature = "testing"))]
            async fn cfg_disabled(_req: Request<Body>) -> Result<Response<Body>, ApiError> {
                Err(ApiError::BadRequest(anyhow!(concat!(
                    "Cannot ",
                    $handler_desc,
                    " because pageserver was compiled without testing APIs",
                ))))
            }

            #[cfg(feature = "testing")]
            let handler = $handler;
            #[cfg(not(feature = "testing"))]
            let handler = cfg_disabled;

            move |r| RequestSpan(handler).handle(r)
        }};
    }

    Ok(router
        .data(Arc::new(
            State::new(conf, auth, remote_storage).context("Failed to initialize router state")?,
        ))
        .get("/v1/status", |r| RequestSpan(status_handler).handle(r))
        .put(
            "/v1/failpoints",
            testing_api!("manage failpoints", failpoints_handler),
        )
        .get("/v1/tenant", |r| RequestSpan(tenant_list_handler).handle(r))
        .post("/v1/tenant", |r| {
            RequestSpan(tenant_create_handler).handle(r)
        })
        .get("/v1/tenant/:tenant_id", |r| {
            RequestSpan(tenant_status).handle(r)
        })
        .get("/v1/tenant/:tenant_id/synthetic_size", |r| {
            RequestSpan(tenant_size_handler).handle(r)
        })
        .put("/v1/tenant/config", |r| {
            RequestSpan(update_tenant_config_handler).handle(r)
        })
        .get("/v1/tenant/:tenant_id/config", |r| {
            RequestSpan(get_tenant_config_handler).handle(r)
        })
        .get("/v1/tenant/:tenant_id/timeline", |r| {
            RequestSpan(timeline_list_handler).handle(r)
        })
        .post("/v1/tenant/:tenant_id/timeline", |r| {
            RequestSpan(timeline_create_handler).handle(r)
        })
        .post("/v1/tenant/:tenant_id/attach", |r| {
            RequestSpan(tenant_attach_handler).handle(r)
        })
        .post("/v1/tenant/:tenant_id/detach", |r| {
            RequestSpan(tenant_detach_handler).handle(r)
        })
        .post("/v1/tenant/:tenant_id/load", |r| {
            RequestSpan(tenant_load_handler).handle(r)
        })
        .post("/v1/tenant/:tenant_id/ignore", |r| {
            RequestSpan(tenant_ignore_handler).handle(r)
        })
        .get("/v1/tenant/:tenant_id/timeline/:timeline_id", |r| {
            RequestSpan(timeline_detail_handler).handle(r)
        })
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/get_lsn_by_timestamp",
            |r| RequestSpan(get_lsn_by_timestamp_handler).handle(r),
        )
        .put("/v1/tenant/:tenant_id/timeline/:timeline_id/do_gc", |r| {
            RequestSpan(timeline_gc_handler).handle(r)
        })
        .put(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/compact",
            testing_api!("run timeline compaction", timeline_compact_handler),
        )
        .put(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/checkpoint",
            testing_api!("run timeline checkpoint", timeline_checkpoint_handler),
        )
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/download_remote_layers",
            |r| RequestSpan(timeline_download_remote_layers_handler_post).handle(r),
        )
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/download_remote_layers",
            |r| RequestSpan(timeline_download_remote_layers_handler_get).handle(r),
        )
        .delete("/v1/tenant/:tenant_id/timeline/:timeline_id", |r| {
            RequestSpan(timeline_delete_handler).handle(r)
        })
        .get("/v1/tenant/:tenant_id/timeline/:timeline_id/layer", |r| {
            RequestSpan(layer_map_info_handler).handle(r)
        })
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/layer/:layer_file_name",
            |r| RequestSpan(layer_download_handler).handle(r),
        )
        .delete(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/layer/:layer_file_name",
            |r| RequestSpan(evict_timeline_layer_handler).handle(r),
        )
        .get("/v1/panic", |r| RequestSpan(always_panic_handler).handle(r))
        .any(handler_404))
}
