from typing import cast

import requests
from fixtures.neon_fixtures import (
    DEFAULT_BRANCH_NAME,
    NeonEnv,
    NeonEnvBuilder,
    PageserverHttpClient,
)
from fixtures.types import TenantId, TimelineId


def helper_compare_timeline_list(
    pageserver_http_client: PageserverHttpClient, env: NeonEnv, initial_tenant: TenantId
):
    """
    Compare timelines list returned by CLI and directly via API.
    Filters out timelines created by other tests.
    """

    timelines_api = sorted(
        map(
            lambda t: TimelineId(t["timeline_id"]),
            pageserver_http_client.timeline_list(initial_tenant),
        )
    )

    timelines_cli = env.neon_cli.list_timelines()
    assert timelines_cli == env.neon_cli.list_timelines(initial_tenant)

    cli_timeline_ids = sorted([timeline_id for (_, timeline_id) in timelines_cli])
    assert timelines_api == cli_timeline_ids


def test_cli_timeline_list(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http_client = env.pageserver.http_client()

    # Initial sanity check
    helper_compare_timeline_list(pageserver_http_client, env, env.initial_tenant)

    # Create a branch for us
    main_timeline_id = env.neon_cli.create_branch("test_cli_branch_list_main")
    helper_compare_timeline_list(pageserver_http_client, env, env.initial_tenant)

    # Create a nested branch
    nested_timeline_id = env.neon_cli.create_branch(
        "test_cli_branch_list_nested", "test_cli_branch_list_main"
    )
    helper_compare_timeline_list(pageserver_http_client, env, env.initial_tenant)

    # Check that all new branches are visible via CLI
    timelines_cli = [timeline_id for (_, timeline_id) in env.neon_cli.list_timelines()]

    assert main_timeline_id in timelines_cli
    assert nested_timeline_id in timelines_cli


def helper_compare_tenant_list(pageserver_http_client: PageserverHttpClient, env: NeonEnv):
    tenants = pageserver_http_client.tenant_list()
    tenants_api = sorted(map(lambda t: cast(str, t["id"]), tenants))

    res = env.neon_cli.list_tenants()
    tenants_cli = sorted(map(lambda t: t.split()[0], res.stdout.splitlines()))

    assert tenants_api == tenants_cli


def test_cli_tenant_list(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http_client = env.pageserver.http_client()
    # Initial sanity check
    helper_compare_tenant_list(pageserver_http_client, env)

    # Create new tenant
    tenant1, _ = env.neon_cli.create_tenant()

    # check tenant1 appeared
    helper_compare_tenant_list(pageserver_http_client, env)

    # Create new tenant
    tenant2, _ = env.neon_cli.create_tenant()

    # check tenant2 appeared
    helper_compare_tenant_list(pageserver_http_client, env)

    res = env.neon_cli.list_tenants()
    tenants = sorted(map(lambda t: TenantId(t.split()[0]), res.stdout.splitlines()))

    assert env.initial_tenant in tenants
    assert tenant1 in tenants
    assert tenant2 in tenants


def test_cli_tenant_create(neon_simple_env: NeonEnv):
    env = neon_simple_env
    tenant_id, _ = env.neon_cli.create_tenant()
    timelines = env.neon_cli.list_timelines(tenant_id)

    # an initial timeline should be created upon tenant creation
    assert len(timelines) == 1
    assert timelines[0][0] == DEFAULT_BRANCH_NAME


def test_cli_ipv4_listeners(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    # Connect to sk port on v4 loopback
    res = requests.get(f"http://127.0.0.1:{env.safekeepers[0].port.http}/v1/status")
    assert res.ok

    # FIXME Test setup is using localhost:xx in ps config.
    # Perhaps consider switching test suite to v4 loopback.

    # Connect to ps port on v4 loopback
    # res = requests.get(f'http://127.0.0.1:{env.pageserver.service_port.http}/v1/status')
    # assert res.ok


def test_cli_start_stop(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    # Stop default ps/sk
    env.neon_cli.pageserver_stop()
    env.neon_cli.safekeeper_stop()

    # Default start
    res = env.neon_cli.raw_cli(["start"])
    res.check_returncode()

    # Default stop
    res = env.neon_cli.raw_cli(["stop"])
    res.check_returncode()
