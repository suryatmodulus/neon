#
# Little stress test for the checkpointing and remote storage code.
#
# The test creates several tenants, and runs a simple workload on
# each tenant, in parallel. The test uses remote storage, and a tiny
# checkpoint_distance setting so that a lot of layer files are created.
#

import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import List, Tuple

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    LocalFsStorage,
    NeonEnv,
    NeonEnvBuilder,
    Postgres,
    RemoteStorageKind,
    assert_tenant_status,
    available_remote_storages,
    wait_for_last_record_lsn,
    wait_for_sk_commit_lsn_to_reach_remote_storage,
    wait_for_upload,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar, wait_until


async def tenant_workload(env: NeonEnv, pg: Postgres):
    await env.pageserver.connect_async()

    pg_conn = await pg.connect_async()

    await pg_conn.execute("CREATE TABLE t(key int primary key, value text)")
    for i in range(1, 100):
        await pg_conn.execute(
            f"INSERT INTO t SELECT {i}*1000 + g, 'payload' from generate_series(1,1000) g"
        )

        # we rely upon autocommit after each statement
        # as waiting for acceptors happens there
        res = await pg_conn.fetchval("SELECT count(*) FROM t")
        assert res == i * 1000


async def all_tenants_workload(env: NeonEnv, tenants_pgs):
    workers = []
    for _, pg in tenants_pgs:
        worker = tenant_workload(env, pg)
        workers.append(asyncio.create_task(worker))

    # await all workers
    await asyncio.gather(*workers)


@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_tenants_many(neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_tenants_many",
    )

    env = neon_env_builder.init_start()

    # FIXME: Is this expected?
    env.pageserver.allowed_errors.append(
        ".*init_tenant_mgr: marking .* as locally complete, while it doesnt exist in remote index.*"
    )

    tenants_pgs: List[Tuple[TenantId, Postgres]] = []

    for _ in range(1, 5):
        # Use a tiny checkpoint distance, to create a lot of layers quickly
        tenant, _ = env.neon_cli.create_tenant(
            conf={
                "checkpoint_distance": "5000000",
            }
        )
        env.neon_cli.create_timeline("test_tenants_many", tenant_id=tenant)

        pg = env.postgres.create_start(
            "test_tenants_many",
            tenant_id=tenant,
        )
        tenants_pgs.append((tenant, pg))

    asyncio.run(all_tenants_workload(env, tenants_pgs))

    # Wait for the remote storage uploads to finish
    pageserver_http = env.pageserver.http_client()
    for tenant, pg in tenants_pgs:
        res = pg.safe_psql_many(
            ["SHOW neon.tenant_id", "SHOW neon.timeline_id", "SELECT pg_current_wal_flush_lsn()"]
        )
        tenant_id = TenantId(res[0][0][0])
        timeline_id = TimelineId(res[1][0][0])
        current_lsn = Lsn(res[2][0][0])

        # wait until pageserver receives all the data
        wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)

        # run final checkpoint manually to flush all the data to remote storage
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
        wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_tenants_attached_after_download(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="remote_storage_kind",
    )

    data_id = 1
    data_secret = "very secret secret"

    # Exercise retry code path by making all uploads and downloads fail for the
    # first time. The retries print INFO-messages to the log; we will check
    # that they are present after the test.
    neon_env_builder.pageserver_config_override = "test_remote_failures=1"

    ##### First start, insert secret data and upload it to the remote storage
    env = neon_env_builder.init_start()

    # FIXME: Are these expected?
    env.pageserver.allowed_errors.append(".*No timelines to attach received.*")
    env.pageserver.allowed_errors.append(
        ".*marking .* as locally complete, while it doesnt exist in remote index.*"
    )

    pageserver_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    for checkpoint_number in range(1, 3):
        with pg.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE t{checkpoint_number}(id int primary key, secret text);
                INSERT INTO t{checkpoint_number} VALUES ({data_id}, '{data_secret}|{checkpoint_number}');
            """
            )
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

        # wait until pageserver receives that data
        wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)

        # run checkpoint manually to be sure that data landed in remote storage
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

        log.info(f"waiting for checkpoint {checkpoint_number} upload")
        # wait until pageserver successfully uploaded a checkpoint to remote storage
        wait_for_upload(client, tenant_id, timeline_id, current_lsn)
        log.info(f"upload of checkpoint {checkpoint_number} is done")

    # Check that we had to retry the uploads
    assert env.pageserver.log_contains(
        ".*failed to perform remote task UploadLayer.*, will retry.*"
    )
    assert env.pageserver.log_contains(
        ".*failed to perform remote task UploadMetadata.*, will retry.*"
    )

    ##### Stop the pageserver, erase its layer file to force it being downloaded from S3
    env.postgres.stop_all()

    wait_for_sk_commit_lsn_to_reach_remote_storage(
        tenant_id, timeline_id, env.safekeepers, env.pageserver
    )

    env.pageserver.stop()

    timeline_dir = Path(env.repo_dir) / "tenants" / str(tenant_id) / "timelines" / str(timeline_id)
    local_layer_deleted = False
    for path in Path.iterdir(timeline_dir):
        if path.name.startswith("00000"):
            # Looks like a layer file. Remove it
            os.remove(path)
            local_layer_deleted = True
            break
    assert local_layer_deleted, f"Found no local layer files to delete in directory {timeline_dir}"

    ##### Start the pageserver, forcing it to download the layer file and load the timeline into memory
    # FIXME: just starting the pageserver no longer downloads the
    # layer files. Do we want to force download, or maybe run some
    # queries, or is it enough that it starts up without layer files?
    env.pageserver.start()
    client = env.pageserver.http_client()

    wait_until(
        number_of_iterations=5,
        interval=1,
        func=lambda: assert_tenant_status(client, tenant_id, "Active"),
    )

    restored_timelines = client.timeline_list(tenant_id)
    assert (
        len(restored_timelines) == 1
    ), f"Tenant {tenant_id} should have its timeline reattached after its layer is downloaded from the remote storage"
    restored_timeline = restored_timelines[0]
    assert restored_timeline["timeline_id"] == str(
        timeline_id
    ), f"Tenant {tenant_id} should have its old timeline {timeline_id} restored from the remote storage"

    # Check that we had to retry the downloads
    assert env.pageserver.log_contains(".*download .* succeeded after 1 retries.*")


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_tenant_upgrades_index_json_from_v0(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    # the "image" for the v0 index_part.json. the fields themselves are
    # replaced with values read from the later version because of #2592 (initdb
    # lsn not reproducible).
    v0_skeleton = json.loads(
        """{
        "timeline_layers":[
            "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9"
        ],
        "missing_layers":["This should not fail as its not used anymore"],
        "disk_consistent_lsn":"0/16960E8",
        "metadata_bytes":[]
    }"""
    )

    # getting a too eager compaction happening for this test would not play
    # well with the strict assertions.
    neon_env_builder.pageserver_config_override = "tenant_config.compaction_period='1h'"

    neon_env_builder.enable_remote_storage(
        remote_storage_kind, "test_tenant_upgrades_index_json_from_v0"
    )

    # launch pageserver, populate the default tenants timeline, wait for it to be uploaded,
    # then go ahead and modify the "remote" version as if it was downgraded, needing upgrade
    env = neon_env_builder.init_start()

    pageserver_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    with pg.cursor() as cur:
        cur.execute("CREATE TABLE t0 AS VALUES (123, 'second column as text');")
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # flush, wait until in remote storage
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)
    env.postgres.stop_all()
    env.pageserver.stop()

    # remove all local data for the tenant to force redownloading and subsequent upgrade
    shutil.rmtree(Path(env.repo_dir) / "tenants" / str(tenant_id))

    # downgrade the remote file
    timeline_path = local_fs_index_part_path(env, tenant_id, timeline_id)
    with open(timeline_path, "r+") as timeline_file:
        # keep the deserialized for later inspection
        orig_index_part = json.load(timeline_file)

        v0_index_part = {
            key: orig_index_part[key]
            for key in v0_skeleton.keys() - ["missing_layers"]  # pgserver doesn't have it anymore
        }

        timeline_file.seek(0)
        json.dump(v0_index_part, timeline_file)
        timeline_file.truncate(timeline_file.tell())

    env.pageserver.start()
    pageserver_http = env.pageserver.http_client()
    pageserver_http.tenant_attach(tenant_id)

    wait_until(
        number_of_iterations=5,
        interval=1,
        func=lambda: assert_tenant_status(pageserver_http, tenant_id, "Active"),
    )

    pg = env.postgres.create_start("main")

    with pg.cursor() as cur:
        cur.execute("INSERT INTO t0 VALUES (234, 'test data');")
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)

    # not needed anymore
    env.postgres.stop_all()
    env.pageserver.stop()

    # make sure the file has been upgraded back to how it started
    index_part = local_fs_index_part(env, tenant_id, timeline_id)
    assert index_part["version"] == orig_index_part["version"]
    assert "missing_layers" not in index_part.keys()

    # expect one more layer because of the forced checkpoint
    assert len(index_part["timeline_layers"]) == len(orig_index_part["timeline_layers"]) + 1

    # all of the same layer files are there, but they might be shuffled around
    orig_layers = set(orig_index_part["timeline_layers"])
    later_layers = set(index_part["timeline_layers"])
    assert later_layers.issuperset(orig_layers)

    added_layers = later_layers - orig_layers
    assert len(added_layers) == 1

    # all of metadata has been regenerated (currently just layer file size)
    all_metadata_keys = set()
    for layer in orig_layers:
        orig_metadata = orig_index_part["layer_metadata"][layer]
        new_metadata = index_part["layer_metadata"][layer]
        assert (
            orig_metadata == new_metadata
        ), f"metadata for layer {layer} should not have changed {orig_metadata} vs. {new_metadata}"
        all_metadata_keys |= set(orig_metadata.keys())

    one_new_layer = next(iter(added_layers))
    assert one_new_layer in index_part["layer_metadata"], "new layer should have metadata"

    only_new_metadata = index_part["layer_metadata"][one_new_layer]

    assert (
        set(only_new_metadata.keys()).symmetric_difference(all_metadata_keys) == set()
    ), "new layer metadata has same metadata as others"


# FIXME: test index_part.json getting downgraded from imaginary new version


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_tenant_ignores_backup_file(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    # getting a too eager compaction happening for this test would not play
    # well with the strict assertions.
    neon_env_builder.pageserver_config_override = "tenant_config.compaction_period='1h'"

    neon_env_builder.enable_remote_storage(remote_storage_kind, "test_tenant_ignores_backup_file")

    # launch pageserver, populate the default tenants timeline, wait for it to be uploaded,
    # then go ahead and modify the "remote" version as if it was downgraded, needing upgrade
    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.append(".*got backup file on the remote storage, ignoring it.*")

    pageserver_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    with pg.cursor() as cur:
        cur.execute("CREATE TABLE t0 AS VALUES (123, 'second column as text');")
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # flush, wait until in remote storage
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)

    env.postgres.stop_all()
    env.pageserver.stop()

    # change the remote file to have entry with .0.old suffix
    timeline_path = local_fs_index_part_path(env, tenant_id, timeline_id)
    with open(timeline_path, "r+") as timeline_file:
        # keep the deserialized for later inspection
        orig_index_part = json.load(timeline_file)
        backup_layer_name = orig_index_part["timeline_layers"][0] + ".0.old"
        orig_index_part["timeline_layers"].append(backup_layer_name)

        timeline_file.seek(0)
        json.dump(orig_index_part, timeline_file)

    env.pageserver.start()
    pageserver_http = env.pageserver.http_client()

    wait_until(
        number_of_iterations=5,
        interval=1,
        func=lambda: assert_tenant_status(pageserver_http, tenant_id, "Active"),
    )

    pg = env.postgres.create_start("main")

    with pg.cursor() as cur:
        cur.execute("INSERT INTO t0 VALUES (234, 'test data');")
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)

    # not needed anymore
    env.postgres.stop_all()
    env.pageserver.stop()

    # the .old file is gone from newly serialized index_part
    new_index_part = local_fs_index_part(env, tenant_id, timeline_id)
    backup_layers = filter(lambda x: x.endswith(".old"), new_index_part["timeline_layers"])
    assert len(list(backup_layers)) == 0


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_tenant_redownloads_truncated_file_on_startup(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    # since we now store the layer file length metadata, we notice on startup that a layer file is of wrong size, and proceed to redownload it.
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_tenant_redownloads_truncated_file_on_startup",
    )

    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.append(
        ".*removing local file .* because it has unexpected length.*"
    )

    # FIXME: Are these expected?
    env.pageserver.allowed_errors.append(
        ".*init_tenant_mgr: marking .* as locally complete, while it doesnt exist in remote index.*"
    )
    env.pageserver.allowed_errors.append(".*No timelines to attach received.*")

    pageserver_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    with pg.cursor() as cur:
        cur.execute("CREATE TABLE t1 AS VALUES (123, 'foobar');")
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)

    env.postgres.stop_all()
    env.pageserver.stop()

    timeline_dir = Path(env.repo_dir) / "tenants" / str(tenant_id) / "timelines" / str(timeline_id)
    local_layer_truncated = None
    for path in Path.iterdir(timeline_dir):
        if path.name.startswith("00000"):
            correct_size = os.stat(path).st_size
            os.truncate(path, 0)
            local_layer_truncated = (path, correct_size)
            break
    assert (
        local_layer_truncated is not None
    ), f"Found no local layer files to delete in directory {timeline_dir}"

    (path, expected_size) = local_layer_truncated

    # ensure the same size is found from the index_part.json
    index_part = local_fs_index_part(env, tenant_id, timeline_id)
    assert index_part["layer_metadata"][path.name]["file_size"] == expected_size

    ## Start the pageserver. It will notice that the file size doesn't match, and
    ## rename away the local file. It will be re-downloaded when it's needed.
    env.pageserver.start()
    client = env.pageserver.http_client()

    wait_until(
        number_of_iterations=5,
        interval=1,
        func=lambda: assert_tenant_status(client, tenant_id, "Active"),
    )

    restored_timelines = client.timeline_list(tenant_id)
    assert (
        len(restored_timelines) == 1
    ), f"Tenant {tenant_id} should have its timeline reattached after its layer is downloaded from the remote storage"
    retored_timeline = restored_timelines[0]
    assert retored_timeline["timeline_id"] == str(
        timeline_id
    ), f"Tenant {tenant_id} should have its old timeline {timeline_id} restored from the remote storage"

    # Request non-incremental logical size. Calculating it needs the layer file that
    # we corrupted, forcing it to be redownloaded.
    client.timeline_detail(tenant_id, timeline_id, include_non_incremental_logical_size=True)

    assert os.stat(path).st_size == expected_size, "truncated layer should had been re-downloaded"

    # the remote side of local_layer_truncated
    remote_layer_path = local_fs_index_part_path(env, tenant_id, timeline_id).parent / path.name

    # if the upload ever was ongoing, this check would be racy, but at least one
    # extra http request has been made in between so assume it's enough delay
    assert (
        os.stat(remote_layer_path).st_size == expected_size
    ), "truncated file should not had been uploaded around re-download"

    pg = env.postgres.create_start("main")

    with pg.cursor() as cur:
        cur.execute("INSERT INTO t1 VALUES (234, 'test data');")
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)

    # now that the upload is complete, make sure the file hasn't been
    # re-uploaded truncated. this is a rather bogus check given the current
    # implementation, but it's critical it doesn't happen so wasting a few
    # lines of python to do this.
    assert (
        os.stat(remote_layer_path).st_size == expected_size
    ), "truncated file should not had been uploaded after next checkpoint"


def local_fs_index_part(env, tenant_id, timeline_id):
    """
    Return json.load parsed index_part.json of tenant and timeline from LOCAL_FS
    """
    timeline_path = local_fs_index_part_path(env, tenant_id, timeline_id)
    with open(timeline_path, "r") as timeline_file:
        return json.load(timeline_file)


def local_fs_index_part_path(env, tenant_id, timeline_id):
    """
    Return path to the LOCAL_FS index_part.json of the tenant and timeline.
    """
    assert isinstance(env.remote_storage, LocalFsStorage)
    return (
        env.remote_storage.root
        / "tenants"
        / str(tenant_id)
        / "timelines"
        / str(timeline_id)
        / "index_part.json"
    )
