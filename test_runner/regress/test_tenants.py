import os
import shutil
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import List

import pytest
from fixtures.log_helper import log
from fixtures.metrics import (
    PAGESERVER_GLOBAL_METRICS,
    PAGESERVER_PER_TENANT_METRICS,
    PAGESERVER_PER_TENANT_REMOTE_TIMELINE_CLIENT_METRICS,
    parse_metrics,
)
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    RemoteStorageKind,
    available_remote_storages,
)
from fixtures.types import Lsn, TenantId, TimelineId
from prometheus_client.samples import Sample


def test_tenant_creation_fails(neon_simple_env: NeonEnv):
    tenants_dir = Path(neon_simple_env.repo_dir) / "tenants"
    initial_tenants = sorted(
        map(lambda t: t.split()[0], neon_simple_env.neon_cli.list_tenants().stdout.splitlines())
    )
    initial_tenant_dirs = [d for d in tenants_dir.iterdir()]

    neon_simple_env.pageserver.allowed_errors.extend(
        [
            ".*Failed to create directory structure for tenant .*, cleaning tmp data.*",
            ".*Failed to fsync removed temporary tenant directory .*",
        ]
    )

    pageserver_http = neon_simple_env.pageserver.http_client()
    pageserver_http.configure_failpoints(("tenant-creation-before-tmp-rename", "return"))
    with pytest.raises(Exception, match="tenant-creation-before-tmp-rename"):
        _ = neon_simple_env.neon_cli.create_tenant()

    new_tenants = sorted(
        map(lambda t: t.split()[0], neon_simple_env.neon_cli.list_tenants().stdout.splitlines())
    )
    assert initial_tenants == new_tenants, "should not create new tenants"

    new_tenant_dirs = [d for d in tenants_dir.iterdir()]
    assert (
        new_tenant_dirs == initial_tenant_dirs
    ), "pageserver should clean its temp tenant dirs on tenant creation failure"


def test_tenants_normal_work(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    """Tests tenants with and without wal acceptors"""
    tenant_1, _ = env.neon_cli.create_tenant()
    tenant_2, _ = env.neon_cli.create_tenant()

    env.neon_cli.create_timeline("test_tenants_normal_work", tenant_id=tenant_1)
    env.neon_cli.create_timeline("test_tenants_normal_work", tenant_id=tenant_2)

    pg_tenant1 = env.postgres.create_start(
        "test_tenants_normal_work",
        tenant_id=tenant_1,
    )
    pg_tenant2 = env.postgres.create_start(
        "test_tenants_normal_work",
        tenant_id=tenant_2,
    )

    for pg in [pg_tenant1, pg_tenant2]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                # we rely upon autocommit after each statement
                # as waiting for acceptors happens there
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000,)


def test_metrics_normal_work(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    tenant_1, _ = env.neon_cli.create_tenant()
    tenant_2, _ = env.neon_cli.create_tenant()

    timeline_1 = env.neon_cli.create_timeline("test_metrics_normal_work", tenant_id=tenant_1)
    timeline_2 = env.neon_cli.create_timeline("test_metrics_normal_work", tenant_id=tenant_2)

    pg_tenant1 = env.postgres.create_start("test_metrics_normal_work", tenant_id=tenant_1)
    pg_tenant2 = env.postgres.create_start("test_metrics_normal_work", tenant_id=tenant_2)

    for pg in [pg_tenant1, pg_tenant2]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000,)

    collected_metrics = {
        "pageserver": env.pageserver.http_client().get_metrics_str(),
    }
    for sk in env.safekeepers:
        collected_metrics[f"safekeeper{sk.id}"] = sk.http_client().get_metrics_str()

    for name in collected_metrics:
        basepath = os.path.join(neon_env_builder.repo_dir, f"{name}.metrics")

        with open(basepath, "w") as stdout_f:
            print(collected_metrics[name], file=stdout_f, flush=True)

    all_metrics = [parse_metrics(m, name) for name, m in collected_metrics.items()]
    ps_metrics = all_metrics[0]
    sk_metrics = all_metrics[1:]

    ttids = [
        {"tenant_id": str(tenant_1), "timeline_id": str(timeline_1)},
        {"tenant_id": str(tenant_2), "timeline_id": str(timeline_2)},
    ]

    # Test metrics per timeline
    for tt in ttids:
        log.info(f"Checking metrics for {tt}")

        ps_lsn = Lsn(int(ps_metrics.query_one("pageserver_last_record_lsn", filter=tt).value))
        sk_lsns = [
            Lsn(int(sk.query_one("safekeeper_commit_lsn", filter=tt).value)) for sk in sk_metrics
        ]

        log.info(f"ps_lsn: {ps_lsn}")
        log.info(f"sk_lsns: {sk_lsns}")

        assert ps_lsn <= max(sk_lsns)
        assert ps_lsn > Lsn(0)

    # Test common metrics
    for metrics in all_metrics:
        log.info(f"Checking common metrics for {metrics.name}")

        log.info(
            f"process_cpu_seconds_total: {metrics.query_one('process_cpu_seconds_total').value}"
        )
        log.info(f"process_threads: {int(metrics.query_one('process_threads').value)}")
        log.info(
            f"process_resident_memory_bytes (MB): {metrics.query_one('process_resident_memory_bytes').value / 1024 / 1024}"
        )
        log.info(
            f"process_virtual_memory_bytes (MB): {metrics.query_one('process_virtual_memory_bytes').value / 1024 / 1024}"
        )
        log.info(f"process_open_fds: {int(metrics.query_one('process_open_fds').value)}")
        log.info(f"process_max_fds: {int(metrics.query_one('process_max_fds').value)}")
        log.info(
            f"process_start_time_seconds (UTC): {datetime.fromtimestamp(metrics.query_one('process_start_time_seconds').value)}"
        )

    # Test (a subset of) pageserver global metrics
    for metric in PAGESERVER_GLOBAL_METRICS:
        ps_samples = ps_metrics.query_all(metric, {})
        assert len(ps_samples) > 0
        for sample in ps_samples:
            labels = ",".join([f'{key}="{value}"' for key, value in sample.labels.items()])
            log.info(f"{sample.name}{{{labels}}} {sample.value}")


@pytest.mark.parametrize(
    "remote_storage_kind",
    # exercise both the code paths where remote_storage=None and remote_storage=Some(...)
    [RemoteStorageKind.NOOP, RemoteStorageKind.MOCK_S3],
)
def test_pageserver_metrics_removed_after_detach(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    """Tests that when a tenant is detached, the tenant specific metrics are not left behind"""

    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_pageserver_metrics_removed_after_detach",
    )

    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    tenant_1, _ = env.neon_cli.create_tenant()
    tenant_2, _ = env.neon_cli.create_tenant()

    env.neon_cli.create_timeline("test_metrics_removed_after_detach", tenant_id=tenant_1)
    env.neon_cli.create_timeline("test_metrics_removed_after_detach", tenant_id=tenant_2)

    pg_tenant1 = env.postgres.create_start("test_metrics_removed_after_detach", tenant_id=tenant_1)
    pg_tenant2 = env.postgres.create_start("test_metrics_removed_after_detach", tenant_id=tenant_2)

    for pg in [pg_tenant1, pg_tenant2]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000,)

    def get_ps_metric_samples_for_tenant(tenant_id: TenantId) -> List[Sample]:
        ps_metrics = env.pageserver.http_client().get_metrics()
        samples = []
        for metric_name in ps_metrics.metrics:
            for sample in ps_metrics.query_all(
                name=metric_name, filter={"tenant_id": str(tenant_id)}
            ):
                samples.append(sample)
        return samples

    for tenant in [tenant_1, tenant_2]:
        pre_detach_samples = set([x.name for x in get_ps_metric_samples_for_tenant(tenant)])
        expected = set(PAGESERVER_PER_TENANT_METRICS)
        if remote_storage_kind == RemoteStorageKind.NOOP:
            # if there's no remote storage configured, we don't expose the remote timeline client metrics
            expected -= set(PAGESERVER_PER_TENANT_REMOTE_TIMELINE_CLIENT_METRICS)
        assert pre_detach_samples == expected

        env.pageserver.http_client().tenant_detach(tenant)

        post_detach_samples = set([x.name for x in get_ps_metric_samples_for_tenant(tenant)])
        assert post_detach_samples == set()


# Check that empty tenants work with or without the remote storage
@pytest.mark.parametrize(
    "remote_storage_kind", available_remote_storages() + [RemoteStorageKind.NOOP]
)
def test_pageserver_with_empty_tenants(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_pageserver_with_empty_tenants",
    )

    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.append(
        ".*marking .* as locally complete, while it doesnt exist in remote index.*"
    )
    env.pageserver.allowed_errors.append(
        ".*could not load tenant.*Failed to list timelines directory.*"
    )

    client = env.pageserver.http_client()

    tenant_with_empty_timelines_dir = client.tenant_create()
    temp_timelines = client.timeline_list(tenant_with_empty_timelines_dir)
    for temp_timeline in temp_timelines:
        client.timeline_delete(
            tenant_with_empty_timelines_dir, TimelineId(temp_timeline["timeline_id"])
        )
    files_in_timelines_dir = sum(
        1
        for _p in Path.iterdir(
            Path(env.repo_dir) / "tenants" / str(tenant_with_empty_timelines_dir) / "timelines"
        )
    )
    assert (
        files_in_timelines_dir == 0
    ), f"Tenant {tenant_with_empty_timelines_dir} should have an empty timelines/ directory"

    # Trigger timeline re-initialization after pageserver restart
    env.postgres.stop_all()
    env.pageserver.stop()

    tenant_without_timelines_dir = env.initial_tenant
    shutil.rmtree(Path(env.repo_dir) / "tenants" / str(tenant_without_timelines_dir) / "timelines")

    env.pageserver.start()

    client = env.pageserver.http_client()
    tenants = client.tenant_list()

    assert len(tenants) == 2

    [broken_tenant] = [t for t in tenants if t["id"] == str(tenant_without_timelines_dir)]
    assert (
        broken_tenant["state"] == "Broken"
    ), f"Tenant {tenant_without_timelines_dir} without timelines dir should be broken"

    broken_tenant_status = client.tenant_status(tenant_without_timelines_dir)
    assert (
        broken_tenant_status["state"] == "Broken"
    ), f"Tenant {tenant_without_timelines_dir} without timelines dir should be broken"

    assert env.pageserver.log_contains(".*Setting tenant as Broken state, reason:.*")

    [loaded_tenant] = [t for t in tenants if t["id"] == str(tenant_with_empty_timelines_dir)]
    assert (
        loaded_tenant["state"] == "Active"
    ), "Tenant {tenant_with_empty_timelines_dir} with empty timelines dir should be active and ready for timeline creation"

    loaded_tenant_status = client.tenant_status(tenant_with_empty_timelines_dir)
    assert (
        loaded_tenant_status["state"] == "Active"
    ), f"Tenant {tenant_with_empty_timelines_dir} without timelines dir should be active"

    time.sleep(1)  # to allow metrics propagation

    ps_metrics = client.get_metrics()
    broken_tenants_metric_filter = {
        "tenant_id": str(tenant_without_timelines_dir),
        "state": "broken",
    }
    active_tenants_metric_filter = {
        "tenant_id": str(tenant_with_empty_timelines_dir),
        "state": "active",
    }

    tenant_active_count = int(
        ps_metrics.query_one(
            "pageserver_tenant_states_count", filter=active_tenants_metric_filter
        ).value
    )

    assert (
        tenant_active_count == 1
    ), f"Tenant {tenant_with_empty_timelines_dir} should have metric as active"

    tenant_broken_count = int(
        ps_metrics.query_one(
            "pageserver_tenant_states_count", filter=broken_tenants_metric_filter
        ).value
    )

    assert (
        tenant_broken_count == 1
    ), f"Tenant {tenant_without_timelines_dir} should have metric as broken"
