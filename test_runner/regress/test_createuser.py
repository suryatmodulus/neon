from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import query_scalar


#
# Test CREATE USER to check shared catalog restore
#
def test_createuser(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_createuser", "empty")
    pg = env.postgres.create_start("test_createuser")
    log.info("postgres is running on 'test_createuser' branch")

    with pg.cursor() as cur:
        # Cause a 'relmapper' change in the original branch
        cur.execute("CREATE USER testuser with password %s", ("testpwd",))

        cur.execute("CHECKPOINT")

        lsn = query_scalar(cur, "SELECT pg_current_wal_insert_lsn()")

    # Create a branch
    env.neon_cli.create_branch("test_createuser2", "test_createuser", ancestor_start_lsn=lsn)
    pg2 = env.postgres.create_start("test_createuser2")

    # Test that you can connect to new branch as a new user
    assert pg2.safe_psql("select current_user", user="testuser") == [("testuser",)]
