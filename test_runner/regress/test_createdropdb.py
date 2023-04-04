import os
import pathlib

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, check_restored_datadir_content
from fixtures.utils import query_scalar


#
# Test CREATE DATABASE when there have been relmapper changes
#
def test_createdb(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_createdb", "empty")

    pg = env.postgres.create_start("test_createdb")
    log.info("postgres is running on 'test_createdb' branch")

    with pg.cursor() as cur:
        # Cause a 'relmapper' change in the original branch
        cur.execute("VACUUM FULL pg_class")

        cur.execute("CREATE DATABASE foodb")

        lsn = query_scalar(cur, "SELECT pg_current_wal_insert_lsn()")

    # Create a branch
    env.neon_cli.create_branch("test_createdb2", "test_createdb", ancestor_start_lsn=lsn)
    pg2 = env.postgres.create_start("test_createdb2")

    # Test that you can connect to the new database on both branches
    for db in (pg, pg2):
        with db.cursor(dbname="foodb") as cur:
            # Check database size in both branches
            cur.execute(
                """
                select pg_size_pretty(pg_database_size('foodb')),
                pg_size_pretty(
                sum(pg_relation_size(oid, 'main'))
                +sum(pg_relation_size(oid, 'vm'))
                +sum(pg_relation_size(oid, 'fsm'))
                ) FROM pg_class where relisshared is false
                """
            )
            res = cur.fetchone()
            assert res is not None
            # check that dbsize equals sum of all relation sizes, excluding shared ones
            # This is how we define dbsize in neon for now
            assert res[0] == res[1]


#
# Test DROP DATABASE
#
def test_dropdb(neon_simple_env: NeonEnv, test_output_dir):
    env = neon_simple_env
    env.neon_cli.create_branch("test_dropdb", "empty")
    pg = env.postgres.create_start("test_dropdb")
    log.info("postgres is running on 'test_dropdb' branch")

    with pg.cursor() as cur:
        cur.execute("CREATE DATABASE foodb")

        lsn_before_drop = query_scalar(cur, "SELECT pg_current_wal_insert_lsn()")

        dboid = query_scalar(cur, "SELECT oid FROM pg_database WHERE datname='foodb';")

    with pg.cursor() as cur:
        cur.execute("DROP DATABASE foodb")

        cur.execute("CHECKPOINT")

        lsn_after_drop = query_scalar(cur, "SELECT pg_current_wal_insert_lsn()")

    # Create two branches before and after database drop.
    env.neon_cli.create_branch(
        "test_before_dropdb", "test_dropdb", ancestor_start_lsn=lsn_before_drop
    )
    pg_before = env.postgres.create_start("test_before_dropdb")

    env.neon_cli.create_branch(
        "test_after_dropdb", "test_dropdb", ancestor_start_lsn=lsn_after_drop
    )
    pg_after = env.postgres.create_start("test_after_dropdb")

    # Test that database exists on the branch before drop
    pg_before.connect(dbname="foodb").close()

    # Test that database subdir exists on the branch before drop
    assert pg_before.pgdata_dir
    dbpath = pathlib.Path(pg_before.pgdata_dir) / "base" / str(dboid)
    log.info(dbpath)

    assert os.path.isdir(dbpath) is True

    # Test that database subdir doesn't exist on the branch after drop
    assert pg_after.pgdata_dir
    dbpath = pathlib.Path(pg_after.pgdata_dir) / "base" / str(dboid)
    log.info(dbpath)

    assert os.path.isdir(dbpath) is False

    # Check that we restore the content of the datadir correctly
    check_restored_datadir_content(test_output_dir, env, pg)
