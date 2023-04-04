# It's possible to run any regular test with the local fs remote storage via
# env NEON_PAGESERVER_OVERRIDES="remote_storage={local_path='/tmp/neon_zzz/'}" poetry ......

import os
import shutil
import threading
import time
from pathlib import Path
from typing import Dict, List, Tuple

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PageserverApiException,
    RemoteStorageKind,
    available_remote_storages,
    wait_for_last_flush_lsn,
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_until_tenant_state,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import print_gc_result, query_scalar, wait_until


#
# Tests that a piece of data is backed up and restored correctly:
#
# 1. Initial pageserver
#   * starts a pageserver with remote storage, stores specific data in its tables
#   * triggers a checkpoint (which produces a local data scheduled for backup), gets the corresponding timeline id
#   * polls the timeline status to ensure it's copied remotely
#   * inserts more data in the pageserver and repeats the process, to check multiple checkpoints case
#   * stops the pageserver, clears all local directories
#
# 2. Second pageserver
#   * starts another pageserver, connected to the same remote storage
#   * timeline_attach is called for the same timeline id
#   * timeline status is polled until it's downloaded
#   * queries the specific data, ensuring that it matches the one stored before
#
# The tests are done for all types of remote storage pageserver supports.
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_remote_storage_backup_and_restore(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    # Use this test to check more realistic SK ids: some etcd key parsing bugs were related,
    # and this test needs SK to write data to pageserver, so it will be visible
    neon_env_builder.safekeepers_id_start = 12

    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_storage_backup_and_restore",
    )

    # Exercise retry code path by making all uploads and downloads fail for the
    # first time. The retries print INFO-messages to the log; we will check
    # that they are present after the test.
    neon_env_builder.pageserver_config_override = "test_remote_failures=1"

    data_id = 1
    data = "just some data"

    ##### First start, insert data and upload it to the remote storage
    env = neon_env_builder.init_start()

    # FIXME: Is this expected?
    env.pageserver.allowed_errors.append(
        ".*marking .* as locally complete, while it doesnt exist in remote index.*"
    )
    env.pageserver.allowed_errors.append(".*No timelines to attach received.*")

    env.pageserver.allowed_errors.append(".*Failed to get local tenant state.*")
    # FIXME retry downloads without throwing errors
    env.pageserver.allowed_errors.append(".*failed to load remote timeline.*")
    # we have a bunch of pytest.raises for these below
    env.pageserver.allowed_errors.append(".*tenant .*? already exists, state:.*")
    env.pageserver.allowed_errors.append(
        ".*Cannot attach tenant .*?, local tenant directory already exists.*"
    )
    env.pageserver.allowed_errors.append(".*simulated failure of remote operation.*")

    pageserver_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    checkpoint_numbers = range(1, 3)

    for checkpoint_number in checkpoint_numbers:
        with pg.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE t{checkpoint_number}(id int primary key, data text);
                INSERT INTO t{checkpoint_number} VALUES ({data_id}, '{data}|{checkpoint_number}');
            """
            )
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

        # wait until pageserver receives that data
        wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)

        # run checkpoint manually to be sure that data landed in remote storage
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

        # wait until pageserver successfully uploaded a checkpoint to remote storage
        log.info(f"waiting for checkpoint {checkpoint_number} upload")
        wait_for_upload(client, tenant_id, timeline_id, current_lsn)
        log.info(f"upload of checkpoint {checkpoint_number} is done")

    # Check that we had to retry the uploads
    assert env.pageserver.log_contains(
        ".*failed to perform remote task UploadLayer.*, will retry.*"
    )
    assert env.pageserver.log_contains(
        ".*failed to perform remote task UploadMetadata.*, will retry.*"
    )

    ##### Stop the first pageserver instance, erase all its data
    env.postgres.stop_all()
    env.pageserver.stop()

    dir_to_clear = Path(env.repo_dir) / "tenants"
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    # Introduce failpoint in list remote timelines code path to make tenant_attach fail.
    # This is before the failures injected by test_remote_failures, so it's a permanent error.
    pageserver_http.configure_failpoints(("storage-sync-list-remote-timelines", "return"))
    env.pageserver.allowed_errors.append(
        ".*error attaching tenant: storage-sync-list-remote-timelines",
    )
    # Attach it. This HTTP request will succeed and launch a
    # background task to load the tenant. In that background task,
    # listing the remote timelines will fail because of the failpoint,
    # and the tenant will be marked as Broken.
    client.tenant_attach(tenant_id)
    wait_until_tenant_state(pageserver_http, tenant_id, "Broken", 15)

    # Ensure that even though the tenant is broken, we can't attach it again.
    with pytest.raises(Exception, match=f"tenant {tenant_id} already exists, state: Broken"):
        client.tenant_attach(tenant_id)

    # Restart again, this implicitly clears the failpoint.
    # test_remote_failures=1 remains active, though, as it's in the pageserver config.
    # This means that any of the remote client operations after restart will exercise the
    # retry code path.
    #
    # The initiated attach operation should survive the restart, and continue from where it was.
    env.pageserver.stop()
    layer_download_failed_regex = (
        r"download.*[0-9A-F]+-[0-9A-F]+.*open a download stream for layer.*simulated failure"
    )
    assert not env.pageserver.log_contains(
        layer_download_failed_regex
    ), "we shouldn't have tried any layer downloads yet since list remote timelines has a failpoint"
    env.pageserver.start()

    # Ensure that the pageserver remembers that the tenant was attaching, by
    # trying to attach it again. It should fail.
    with pytest.raises(Exception, match=f"tenant {tenant_id} already exists, state:"):
        client.tenant_attach(tenant_id)
    log.info("waiting for tenant to become active. this should be quick with on-demand download")

    def tenant_active():
        all_states = client.tenant_list()
        [tenant] = [t for t in all_states if TenantId(t["id"]) == tenant_id]
        assert tenant["state"] == "Active"

    wait_until(
        number_of_iterations=5,
        interval=1,
        func=tenant_active,
    )

    detail = client.timeline_detail(tenant_id, timeline_id)
    log.info("Timeline detail after attach completed: %s", detail)
    assert (
        Lsn(detail["last_record_lsn"]) >= current_lsn
    ), "current db Lsn should should not be less than the one stored on remote storage"

    log.info("select some data, this will cause layers to be downloaded")
    pg = env.postgres.create_start("main")
    with pg.cursor() as cur:
        for checkpoint_number in checkpoint_numbers:
            assert (
                query_scalar(cur, f"SELECT data FROM t{checkpoint_number} WHERE id = {data_id};")
                == f"{data}|{checkpoint_number}"
            )

    log.info("ensure that we neede to retry downloads due to test_remote_failures=1")
    assert env.pageserver.log_contains(layer_download_failed_regex)


# Exercises the upload queue retry code paths.
# - Use failpoints to cause all storage ops to fail
# - Churn on database to create layer & index uploads, and layer deletions
# - Check that these operations are queued up, using the appropriate metrics
# - Disable failpoints
# - Wait for all uploads to finish
# - Verify that remote is consistent and up-to-date (=all retries were done and succeeded)
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_remote_storage_upload_queue_retries(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_storage_upload_queue_retries",
    )

    env = neon_env_builder.init_start()

    # create tenant with config that will determinstically allow
    # compaction and gc
    tenant_id, timeline_id = env.neon_cli.create_tenant(
        conf={
            # small checkpointing and compaction targets to ensure we generate many upload operations
            "checkpoint_distance": f"{128 * 1024}",
            "compaction_threshold": "1",
            "compaction_target_size": f"{128 * 1024}",
            # no PITR horizon, we specify the horizon when we request on-demand GC
            "pitr_interval": "0s",
            # disable background compaction and GC. We invoke it manually when we want it to happen.
            "gc_period": "0s",
            "compaction_period": "0s",
            # create image layers eagerly, so that GC can remove some layers
            "image_creation_threshold": "1",
        }
    )

    client = env.pageserver.http_client()

    pg = env.postgres.create_start("main", tenant_id=tenant_id)

    pg.safe_psql("CREATE TABLE foo (id INTEGER PRIMARY KEY, val text)")

    def configure_storage_sync_failpoints(action):
        client.configure_failpoints(
            [
                ("before-upload-layer", action),
                ("before-upload-index", action),
                ("before-delete-layer", action),
            ]
        )

    def overwrite_data_and_wait_for_it_to_arrive_at_pageserver(data):
        # create initial set of layers & upload them with failpoints configured
        pg.safe_psql_many(
            [
                f"""
               INSERT INTO foo (id, val)
               SELECT g, '{data}'
               FROM generate_series(1, 10000) g
               ON CONFLICT (id) DO UPDATE
               SET val = EXCLUDED.val
               """,
                # to ensure that GC can actually remove some layers
                "VACUUM foo",
            ]
        )
        wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)

    def get_queued_count(file_kind, op_kind):
        val = client.get_remote_timeline_client_metric(
            "pageserver_remote_timeline_client_calls_unfinished",
            tenant_id,
            timeline_id,
            file_kind,
            op_kind,
        )
        assert val is not None, "expecting metric to be present"
        return int(val)

    # create some layers & wait for uploads to finish
    overwrite_data_and_wait_for_it_to_arrive_at_pageserver("a")
    client.timeline_checkpoint(tenant_id, timeline_id)
    client.timeline_compact(tenant_id, timeline_id)
    overwrite_data_and_wait_for_it_to_arrive_at_pageserver("b")
    client.timeline_checkpoint(tenant_id, timeline_id)
    client.timeline_compact(tenant_id, timeline_id)
    gc_result = client.timeline_gc(tenant_id, timeline_id, 0)
    print_gc_result(gc_result)
    assert gc_result["layers_removed"] > 0

    wait_until(2, 1, lambda: get_queued_count(file_kind="layer", op_kind="upload") == 0)
    wait_until(2, 1, lambda: get_queued_count(file_kind="index", op_kind="upload") == 0)
    wait_until(2, 1, lambda: get_queued_count(file_kind="layer", op_kind="delete") == 0)

    # let all future operations queue up
    configure_storage_sync_failpoints("return")

    # Create more churn to generate all upload ops.
    # The checkpoint / compact / gc ops will block because they call remote_client.wait_completion().
    # So, run this in a different thread.
    churn_thread_result = [False]

    def churn_while_failpoints_active(result):
        overwrite_data_and_wait_for_it_to_arrive_at_pageserver("c")
        client.timeline_checkpoint(tenant_id, timeline_id)
        client.timeline_compact(tenant_id, timeline_id)
        overwrite_data_and_wait_for_it_to_arrive_at_pageserver("d")
        client.timeline_checkpoint(tenant_id, timeline_id)
        client.timeline_compact(tenant_id, timeline_id)
        gc_result = client.timeline_gc(tenant_id, timeline_id, 0)
        print_gc_result(gc_result)
        assert gc_result["layers_removed"] > 0
        result[0] = True

    churn_while_failpoints_active_thread = threading.Thread(
        target=churn_while_failpoints_active, args=[churn_thread_result]
    )
    churn_while_failpoints_active_thread.start()

    # wait for churn thread's data to get stuck in the upload queue
    wait_until(10, 0.1, lambda: get_queued_count(file_kind="layer", op_kind="upload") > 0)
    wait_until(10, 0.1, lambda: get_queued_count(file_kind="index", op_kind="upload") >= 2)
    wait_until(10, 0.1, lambda: get_queued_count(file_kind="layer", op_kind="delete") > 0)

    # unblock churn operations
    configure_storage_sync_failpoints("off")

    # ... and wait for them to finish. Exponential back-off in upload queue, so, gracious timeouts.
    wait_until(30, 1, lambda: get_queued_count(file_kind="layer", op_kind="upload") == 0)
    wait_until(30, 1, lambda: get_queued_count(file_kind="index", op_kind="upload") == 0)
    wait_until(30, 1, lambda: get_queued_count(file_kind="layer", op_kind="delete") == 0)

    # The churn thread doesn't make progress once it blocks on the first wait_completion() call,
    # so, give it some time to wrap up.
    churn_while_failpoints_active_thread.join(30)
    assert not churn_while_failpoints_active_thread.is_alive()
    assert churn_thread_result[0]

    # try a restore to verify that the uploads worked
    # XXX: should vary this test to selectively fail just layer uploads, index uploads, deletions
    #      but how do we validate the result after restore?

    env.pageserver.stop(immediate=True)
    env.postgres.stop_all()

    dir_to_clear = Path(env.repo_dir) / "tenants"
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    env.pageserver.start()
    client = env.pageserver.http_client()

    client.tenant_attach(tenant_id)

    def tenant_active():
        all_states = client.tenant_list()
        [tenant] = [t for t in all_states if TenantId(t["id"]) == tenant_id]
        assert tenant["state"] == "Active"

    wait_until(30, 1, tenant_active)

    log.info("restarting postgres to validate")
    pg = env.postgres.create_start("main", tenant_id=tenant_id)
    with pg.cursor() as cur:
        assert query_scalar(cur, "SELECT COUNT(*) FROM foo WHERE val = 'd'") == 10000


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_remote_timeline_client_calls_started_metric(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_timeline_client_metrics",
    )

    env = neon_env_builder.init_start()

    # create tenant with config that will determinstically allow
    # compaction and gc
    tenant_id, timeline_id = env.neon_cli.create_tenant(
        conf={
            # small checkpointing and compaction targets to ensure we generate many upload operations
            "checkpoint_distance": f"{128 * 1024}",
            "compaction_threshold": "1",
            "compaction_target_size": f"{128 * 1024}",
            # no PITR horizon, we specify the horizon when we request on-demand GC
            "pitr_interval": "0s",
            # disable background compaction and GC. We invoke it manually when we want it to happen.
            "gc_period": "0s",
            "compaction_period": "0s",
            # create image layers eagerly, so that GC can remove some layers
            "image_creation_threshold": "1",
        }
    )

    client = env.pageserver.http_client()

    pg = env.postgres.create_start("main", tenant_id=tenant_id)

    pg.safe_psql("CREATE TABLE foo (id INTEGER PRIMARY KEY, val text)")

    def overwrite_data_and_wait_for_it_to_arrive_at_pageserver(data):
        # create initial set of layers & upload them with failpoints configured
        pg.safe_psql_many(
            [
                f"""
               INSERT INTO foo (id, val)
               SELECT g, '{data}'
               FROM generate_series(1, 10000) g
               ON CONFLICT (id) DO UPDATE
               SET val = EXCLUDED.val
               """,
                # to ensure that GC can actually remove some layers
                "VACUUM foo",
            ]
        )
        wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)

    def get_queued_count(file_kind, op_kind):
        val = client.get_remote_timeline_client_metric(
            "pageserver_remote_timeline_client_calls_unfinished",
            tenant_id,
            timeline_id,
            file_kind,
            op_kind,
        )
        if val is None:
            return val
        return int(val)

    def wait_upload_queue_empty():
        wait_until(2, 1, lambda: get_queued_count(file_kind="layer", op_kind="upload") == 0)
        wait_until(2, 1, lambda: get_queued_count(file_kind="index", op_kind="upload") == 0)
        wait_until(2, 1, lambda: get_queued_count(file_kind="layer", op_kind="delete") == 0)

    calls_started: Dict[Tuple[str, str], List[int]] = {
        ("layer", "upload"): [0],
        ("index", "upload"): [0],
        ("layer", "delete"): [0],
    }

    def fetch_calls_started():
        for (file_kind, op_kind), observations in calls_started.items():
            val = client.get_remote_timeline_client_metric(
                "pageserver_remote_timeline_client_calls_started_count",
                tenant_id,
                timeline_id,
                file_kind,
                op_kind,
            )
            assert val is not None, f"expecting metric to be present: {file_kind} {op_kind}"
            val = int(val)
            observations.append(val)

    def ensure_calls_started_grew():
        for (file_kind, op_kind), observations in calls_started.items():
            log.info(f"ensure_calls_started_grew: {file_kind} {op_kind}: {observations}")
            assert all(
                x < y for x, y in zip(observations, observations[1:])
            ), f"observations for {file_kind} {op_kind} did not grow monotonically: {observations}"

    def churn(data_pass1, data_pass2):
        overwrite_data_and_wait_for_it_to_arrive_at_pageserver(data_pass1)
        client.timeline_checkpoint(tenant_id, timeline_id)
        client.timeline_compact(tenant_id, timeline_id)
        overwrite_data_and_wait_for_it_to_arrive_at_pageserver(data_pass2)
        client.timeline_checkpoint(tenant_id, timeline_id)
        client.timeline_compact(tenant_id, timeline_id)
        gc_result = client.timeline_gc(tenant_id, timeline_id, 0)
        print_gc_result(gc_result)
        assert gc_result["layers_removed"] > 0

    # create some layers & wait for uploads to finish
    churn("a", "b")

    wait_upload_queue_empty()

    # ensure that we updated the calls_started metric
    fetch_calls_started()
    ensure_calls_started_grew()

    # more churn to cause more operations
    churn("c", "d")

    # ensure that the calls_started metric continued to be updated
    fetch_calls_started()
    ensure_calls_started_grew()

    ### now we exercise the download path
    calls_started.clear()
    calls_started.update(
        {
            ("index", "download"): [0],
            ("layer", "download"): [0],
        }
    )

    env.pageserver.stop(immediate=True)
    env.postgres.stop_all()

    dir_to_clear = Path(env.repo_dir) / "tenants"
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    env.pageserver.start()
    client = env.pageserver.http_client()

    client.tenant_attach(tenant_id)

    def tenant_active():
        all_states = client.tenant_list()
        [tenant] = [t for t in all_states if TenantId(t["id"]) == tenant_id]
        assert tenant["state"] == "Active"

    wait_until(30, 1, tenant_active)

    log.info("restarting postgres to validate")
    pg = env.postgres.create_start("main", tenant_id=tenant_id)
    with pg.cursor() as cur:
        assert query_scalar(cur, "SELECT COUNT(*) FROM foo WHERE val = 'd'") == 10000

    # ensure that we updated the calls_started download metric
    fetch_calls_started()
    ensure_calls_started_grew()


# Test that we correctly handle timeline with layers stuck in upload queue
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_timeline_deletion_with_files_stuck_in_upload_queue(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_timeline_deletion_with_files_stuck_in_upload_queue",
    )

    env = neon_env_builder.init_start()

    # create tenant with config that will determinstically allow
    # compaction and gc
    tenant_id, timeline_id = env.neon_cli.create_tenant(
        conf={
            # small checkpointing and compaction targets to ensure we generate many operations
            "checkpoint_distance": f"{64 * 1024}",
            "compaction_threshold": "1",
            "compaction_target_size": f"{64 * 1024}",
            # large horizon to avoid automatic GC (our assert on gc_result below relies on that)
            "gc_horizon": f"{1024 ** 4}",
            "gc_period": "1h",
            # disable PITR so that GC considers just gc_horizon
            "pitr_interval": "0s",
        }
    )
    timeline_path = env.repo_dir / "tenants" / str(tenant_id) / "timelines" / str(timeline_id)

    client = env.pageserver.http_client()

    def get_queued_count(file_kind, op_kind):
        val = client.get_remote_timeline_client_metric(
            "pageserver_remote_timeline_client_calls_unfinished",
            tenant_id,
            timeline_id,
            file_kind,
            op_kind,
        )
        return int(val) if val is not None else val

    pg = env.postgres.create_start("main", tenant_id=tenant_id)

    client.configure_failpoints(("before-upload-layer", "return"))

    pg.safe_psql_many(
        [
            "CREATE TABLE foo (x INTEGER)",
            "INSERT INTO foo SELECT g FROM generate_series(1, 10000) g",
        ]
    )
    wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)

    # Kick off a checkpoint operation.
    # It will get stuck in remote_client.wait_completion(), since the select query will have
    # generated layer upload ops already.
    checkpoint_allowed_to_fail = threading.Event()

    def checkpoint_thread_fn():
        try:
            client.timeline_checkpoint(tenant_id, timeline_id)
        except PageserverApiException:
            assert (
                checkpoint_allowed_to_fail.is_set()
            ), "checkpoint op should only fail in response to timeline deletion"

    checkpoint_thread = threading.Thread(target=checkpoint_thread_fn)
    checkpoint_thread.start()

    # Wait for stuck uploads. NB: if there were earlier layer flushes initiated during `INSERT INTO`,
    # this will be their uploads. If there were none, it's the timeline_checkpoint()'s uploads.
    def assert_compacted_and_uploads_queued():
        assert timeline_path.exists()
        assert len(list(timeline_path.glob("*"))) >= 8
        assert get_queued_count(file_kind="index", op_kind="upload") > 0

    wait_until(20, 0.1, assert_compacted_and_uploads_queued)

    # Regardless, give checkpoint some time to block for good.
    # Not strictly necessary, but might help uncover failure modes in the future.
    time.sleep(2)

    # Now delete the timeline. It should take priority over ongoing
    # checkpoint operations. Hence, checkpoint is allowed to fail now.
    log.info("sending delete request")
    checkpoint_allowed_to_fail.set()
    env.pageserver.allowed_errors.append(
        ".* ERROR .*Error processing HTTP request: InternalServerError\\(timeline is Stopping"
    )
    client.timeline_delete(tenant_id, timeline_id)

    assert not timeline_path.exists()

    # timeline deletion should kill ongoing uploads, so, the metric will be gone
    assert get_queued_count(file_kind="index", op_kind="upload") is None

    # timeline deletion should be unblocking checkpoint ops
    checkpoint_thread.join(2.0)
    assert not checkpoint_thread.is_alive()

    # Just to be sure, unblock ongoing uploads. If the previous assert was incorrect, or the prometheus metric broken,
    # this would likely generate some ERROR level log entries that the NeonEnvBuilder would detect
    client.configure_failpoints(("before-upload-layer", "off"))
    # XXX force retry, currently we have to wait for exponential backoff
    time.sleep(10)


# TODO Test that we correctly handle GC of files that are stuck in upload queue.
