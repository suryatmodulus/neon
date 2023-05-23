from fixtures.neon_fixtures import NeonEnvBuilder, PortDistributor


# Test that neon cli is able to start and stop all processes with the user defaults.
# Repeats the example from README.md as close as it can
def test_neon_cli_basics(neon_env_builder: NeonEnvBuilder, port_distributor: PortDistributor):
    env = neon_env_builder.init_configs()
    # Skipping the init step that creates a local tenant in Pytest tests
    try:
        env.neon_cli.start()
        env.neon_cli.create_tenant(tenant_id=env.initial_tenant, set_default=True)
        env.neon_cli.pg_start(node_name="main", port=port_distributor.get_port())

        env.neon_cli.create_branch(new_branch_name="migration_check")
        env.neon_cli.pg_start(node_name="migration_check", port=port_distributor.get_port())
    finally:
        env.neon_cli.stop()
