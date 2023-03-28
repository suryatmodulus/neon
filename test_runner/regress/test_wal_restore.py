from pathlib import Path

from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    PortDistributor,
    VanillaPostgres,
)
from fixtures.types import TenantId


def test_wal_restore(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    test_output_dir: Path,
    port_distributor: PortDistributor,
    base_dir: Path,
    pg_distrib_dir: Path,
):
    env = neon_env_builder.init_start()
    env.neon_cli.create_branch("test_wal_restore")
    pg = env.postgres.create_start("test_wal_restore")
    pg.safe_psql("create table t as select generate_series(1,300000)")
    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    env.neon_cli.pageserver_stop()
    port = port_distributor.get_port()
    data_dir = test_output_dir / "pgsql.restored"
    with VanillaPostgres(
        data_dir, PgBin(test_output_dir, env.pg_distrib_dir, env.pg_version), port
    ) as restored:
        pg_bin.run_capture(
            [
                str(base_dir / "libs/utils/scripts/restore_from_wal.sh"),
                str(pg_distrib_dir / f"v{env.pg_version}/bin"),
                str(test_output_dir / "repo" / "safekeepers" / "sk1" / str(tenant_id) / "*"),
                str(data_dir),
                str(port),
            ]
        )
        restored.start()
        assert restored.safe_psql("select count(*) from t", user="cloud_admin") == [(300000,)]
