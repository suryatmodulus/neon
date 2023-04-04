import pytest
from fixtures.neon_fixtures import NeonEnv, PageserverApiException
from fixtures.types import TenantId, TimelineId
from fixtures.utils import wait_until


def test_timeline_delete(neon_simple_env: NeonEnv):
    env = neon_simple_env

    env.pageserver.allowed_errors.append(".*Timeline .* was not found.*")
    env.pageserver.allowed_errors.append(".*timeline not found.*")
    env.pageserver.allowed_errors.append(".*Cannot delete timeline which has child timelines.*")
    env.pageserver.allowed_errors.append(".*Tenant .* not found in the local state.*")

    ps_http = env.pageserver.http_client()

    # first try to delete non existing timeline
    # for existing tenant:
    invalid_timeline_id = TimelineId.generate()
    with pytest.raises(PageserverApiException, match="timeline not found"):
        ps_http.timeline_delete(tenant_id=env.initial_tenant, timeline_id=invalid_timeline_id)

    # for non existing tenant:
    invalid_tenant_id = TenantId.generate()
    with pytest.raises(
        PageserverApiException,
        match=f"Tenant {invalid_tenant_id} not found in the local state",
    ):
        ps_http.timeline_delete(tenant_id=invalid_tenant_id, timeline_id=invalid_timeline_id)

    # construct pair of branches to validate that pageserver prohibits
    # deletion of ancestor timelines when they have child branches
    parent_timeline_id = env.neon_cli.create_branch("test_ancestor_branch_delete_parent", "empty")

    leaf_timeline_id = env.neon_cli.create_branch(
        "test_ancestor_branch_delete_branch1", "test_ancestor_branch_delete_parent"
    )

    ps_http = env.pageserver.http_client()
    with pytest.raises(
        PageserverApiException, match="Cannot delete timeline which has child timelines"
    ):
        timeline_path = (
            env.repo_dir
            / "tenants"
            / str(env.initial_tenant)
            / "timelines"
            / str(parent_timeline_id)
        )
        assert timeline_path.exists()

        ps_http.timeline_delete(env.initial_tenant, parent_timeline_id)

        assert not timeline_path.exists()

    timeline_path = (
        env.repo_dir / "tenants" / str(env.initial_tenant) / "timelines" / str(leaf_timeline_id)
    )
    assert timeline_path.exists()

    # retry deletes when compaction or gc is running in pageserver
    wait_until(
        number_of_iterations=3,
        interval=0.2,
        func=lambda: ps_http.timeline_delete(env.initial_tenant, leaf_timeline_id),
    )

    assert not timeline_path.exists()

    # check 404
    with pytest.raises(
        PageserverApiException,
        match=f"Timeline {env.initial_tenant}/{leaf_timeline_id} was not found",
    ):
        ps_http.timeline_detail(env.initial_tenant, leaf_timeline_id)

        # FIXME leaves tenant without timelines, should we prevent deletion of root timeline?
        wait_until(
            number_of_iterations=3,
            interval=0.2,
            func=lambda: ps_http.timeline_delete(env.initial_tenant, parent_timeline_id),
        )
