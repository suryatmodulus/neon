import pytest
import random
import time
import os
import signal
import subprocess
import sys
import threading
import uuid

from contextlib import closing
from dataclasses import dataclass, field
from multiprocessing import Process, Value
from pathlib import Path
from fixtures.zenith_fixtures import PgBin, Etcd, Postgres, Safekeeper, ZenithEnv, ZenithEnvBuilder, PortDistributor, SafekeeperPort, zenith_binpath, PgProtocol
from fixtures.utils import get_dir_size, lsn_to_hex, mkdir_if_needed, lsn_from_hex
from fixtures.log_helper import log
from typing import List, Optional, Any


# basic test, write something in setup with wal acceptors, ensure that commits
# succeed and data is written
def test_normal_work(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('test_safekeepers_normal_work')
    pg = env.postgres.create_start('test_safekeepers_normal_work')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute('CREATE TABLE t(key int primary key, value text)')
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
            cur.execute('SELECT sum(key) FROM t')
            assert cur.fetchone() == (5000050000, )


@dataclass
class TimelineMetrics:
    timeline_id: str
    last_record_lsn: int
    # One entry per each Safekeeper, order is the same
    flush_lsns: List[int] = field(default_factory=list)
    commit_lsns: List[int] = field(default_factory=list)


# Run page server and multiple acceptors, and multiple compute nodes running
# against different timelines.
def test_many_timelines(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init_start()

    n_timelines = 3

    branch_names = [
        "test_safekeepers_many_timelines_{}".format(tlin) for tlin in range(n_timelines)
    ]
    # pageserver, safekeeper operate timelines via their ids (can be represented in hex as 'ad50847381e248feaac9876cc71ae418')
    # that's not really human readable, so the branch names are introduced in Zenith CLI.
    # Zenith CLI stores its branch <-> timeline mapping in its internals,
    # but we need this to collect metrics from other servers, related to the timeline.
    branch_names_to_timeline_ids = {}

    # start postgres on each timeline
    pgs = []
    for branch_name in branch_names:
        new_timeline_id = env.zenith_cli.create_branch(branch_name)
        pgs.append(env.postgres.create_start(branch_name))
        branch_names_to_timeline_ids[branch_name] = new_timeline_id

    tenant_id = env.initial_tenant

    def collect_metrics(message: str) -> List[TimelineMetrics]:
        with env.pageserver.http_client() as pageserver_http:
            timeline_details = [
                pageserver_http.timeline_detail(
                    tenant_id=tenant_id, timeline_id=branch_names_to_timeline_ids[branch_name])
                for branch_name in branch_names
            ]
        # All changes visible to pageserver (last_record_lsn) should be
        # confirmed by safekeepers first. As we cannot atomically get
        # state of both pageserver and safekeepers, we should start with
        # pageserver. Looking at outdated data from pageserver is ok.
        # Asking safekeepers first is not ok because new commits may arrive
        # to both safekeepers and pageserver after we've already obtained
        # safekeepers' state, it will look contradictory.
        sk_metrics = [sk.http_client().get_metrics() for sk in env.safekeepers]

        timeline_metrics = []
        for timeline_detail in timeline_details:
            timeline_id: str = timeline_detail["timeline_id"]

            local_timeline_detail = timeline_detail.get('local')
            if local_timeline_detail is None:
                log.debug(f"Timeline {timeline_id} is not present locally, skipping")
                continue

            m = TimelineMetrics(
                timeline_id=timeline_id,
                last_record_lsn=lsn_from_hex(local_timeline_detail['last_record_lsn']),
            )
            for sk_m in sk_metrics:
                m.flush_lsns.append(sk_m.flush_lsn_inexact[(tenant_id.hex, timeline_id)])
                m.commit_lsns.append(sk_m.commit_lsn_inexact[(tenant_id.hex, timeline_id)])

            for i, (flush_lsn, commit_lsn) in enumerate(zip(m.flush_lsns, m.commit_lsns)):
                if commit_lsn > flush_lsn:
                    log.info("something is very wrong!")
                    log.info(f"{env.safekeepers[i].http_client().timeline_status(tenant_id, timeline_id)}")

                # Invariant. May be < when transaction is in progress.
                assert commit_lsn <= flush_lsn, f"timeline_id={timeline_id}, timeline_detail={timeline_detail}, sk_metrics={sk_metrics}"
            # We only call collect_metrics() after a transaction is confirmed by
            # the compute node, which only happens after a consensus of safekeepers
            # has confirmed the transaction. We assume majority consensus here.
            assert (2 * sum(m.last_record_lsn <= lsn
                            for lsn in m.flush_lsns) > zenith_env_builder.num_safekeepers), f"timeline_id={timeline_id}, timeline_detail={timeline_detail}, sk_metrics={sk_metrics}"
            assert (2 * sum(m.last_record_lsn <= lsn
                            for lsn in m.commit_lsns) > zenith_env_builder.num_safekeepers), f"timeline_id={timeline_id}, timeline_detail={timeline_detail}, sk_metrics={sk_metrics}"
            timeline_metrics.append(m)
        log.info(f"{message}: {timeline_metrics}")
        return timeline_metrics

    # TODO: https://github.com/zenithdb/zenith/issues/809
    # collect_metrics("before CREATE TABLE")

    # Do everything in different loops to have actions on different timelines
    # interleaved.
    # create schema
    for pg in pgs:
        pg.safe_psql("CREATE TABLE t(key int primary key, value text)")
    init_m = collect_metrics("after CREATE TABLE")

    # Populate data for 2/3 timelines
    class MetricsChecker(threading.Thread):
        def __init__(self) -> None:
            super().__init__(daemon=True)
            self.should_stop = threading.Event()
            self.exception: Optional[BaseException] = None

        def run(self) -> None:
            try:
                while not self.should_stop.is_set():
                    collect_metrics("during INSERT INTO")
                    time.sleep(1)
            except:
                log.error("MetricsChecker's thread failed, the test will be failed on .stop() call",
                          exc_info=True)
                # We want to preserve traceback as well as the exception
                exc_type, exc_value, exc_tb = sys.exc_info()
                assert exc_type
                e = exc_type(exc_value)
                e.__traceback__ = exc_tb
                self.exception = e

        def stop(self) -> None:
            self.should_stop.set()
            self.join()
            if self.exception:
                raise self.exception

    metrics_checker = MetricsChecker()
    metrics_checker.start()

    for pg in pgs[:-1]:
        pg.safe_psql("INSERT INTO t SELECT generate_series(1,100000), 'payload'")

    metrics_checker.stop()

    collect_metrics("after INSERT INTO")

    # Check data for 2/3 timelines
    for pg in pgs[:-1]:
        res = pg.safe_psql("SELECT sum(key) FROM t")
        assert res[0] == (5000050000, )

    final_m = collect_metrics("after SELECT")
    # Assume that LSNs (a) behave similarly in all timelines; and (b) INSERT INTO alters LSN significantly.
    # Also assume that safekeepers will not be significantly out of sync in this test.
    middle_lsn = (init_m[0].last_record_lsn + final_m[0].last_record_lsn) // 2
    assert max(init_m[0].flush_lsns) < middle_lsn < min(final_m[0].flush_lsns)
    assert max(init_m[0].commit_lsns) < middle_lsn < min(final_m[0].commit_lsns)
    assert max(init_m[1].flush_lsns) < middle_lsn < min(final_m[1].flush_lsns)
    assert max(init_m[1].commit_lsns) < middle_lsn < min(final_m[1].commit_lsns)
    assert max(init_m[2].flush_lsns) <= min(final_m[2].flush_lsns) < middle_lsn
    assert max(init_m[2].commit_lsns) <= min(final_m[2].commit_lsns) < middle_lsn


# Check that dead minority doesn't prevent the commits: execute insert n_inserts
# times, with fault_probability chance of getting a wal acceptor down or up
# along the way. 2 of 3 are always alive, so the work keeps going.
def test_restarts(zenith_env_builder: ZenithEnvBuilder):
    fault_probability = 0.01
    n_inserts = 1000
    n_acceptors = 3

    zenith_env_builder.num_safekeepers = n_acceptors
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('test_safekeepers_restarts')
    pg = env.postgres.create_start('test_safekeepers_restarts')

    # we rely upon autocommit after each statement
    # as waiting for acceptors happens there
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    failed_node = None
    cur.execute('CREATE TABLE t(key int primary key, value text)')
    for i in range(n_inserts):
        cur.execute("INSERT INTO t values (%s, 'payload');", (i + 1, ))

        if random.random() <= fault_probability:
            if failed_node is None:
                failed_node = env.safekeepers[random.randrange(0, n_acceptors)]
                failed_node.stop()
            else:
                failed_node.start()
                failed_node = None
    cur.execute('SELECT sum(key) FROM t')
    assert cur.fetchone() == (500500, )


start_delay_sec = 2


def delayed_safekeeper_start(wa):
    time.sleep(start_delay_sec)
    wa.start()


# When majority of acceptors is offline, commits are expected to be frozen
def test_unavailability(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 2
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('test_safekeepers_unavailability')
    pg = env.postgres.create_start('test_safekeepers_unavailability')

    # we rely upon autocommit after each statement
    # as waiting for acceptors happens there
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    # check basic work with table
    cur.execute('CREATE TABLE t(key int primary key, value text)')
    cur.execute("INSERT INTO t values (1, 'payload')")

    # shutdown one of two acceptors, that is, majority
    env.safekeepers[0].stop()

    proc = Process(target=delayed_safekeeper_start, args=(env.safekeepers[0], ))
    proc.start()

    start = time.time()
    cur.execute("INSERT INTO t values (2, 'payload')")
    # ensure that the query above was hanging while acceptor was down
    assert (time.time() - start) >= start_delay_sec
    proc.join()

    # for the world's balance, do the same with second acceptor
    env.safekeepers[1].stop()

    proc = Process(target=delayed_safekeeper_start, args=(env.safekeepers[1], ))
    proc.start()

    start = time.time()
    cur.execute("INSERT INTO t values (3, 'payload')")
    # ensure that the query above was hanging while acceptor was down
    assert (time.time() - start) >= start_delay_sec
    proc.join()

    cur.execute("INSERT INTO t values (4, 'payload')")

    cur.execute('SELECT sum(key) FROM t')
    assert cur.fetchone() == (10, )


# shut down random subset of acceptors, sleep, wake them up, rinse, repeat
def xmas_garland(acceptors, stop):
    while not bool(stop.value):
        victims = []
        for wa in acceptors:
            if random.random() >= 0.5:
                victims.append(wa)
        for v in victims:
            v.stop()
        time.sleep(1)
        for v in victims:
            v.start()
        time.sleep(1)


# value which gets unset on exit
@pytest.fixture
def stop_value():
    stop = Value('i', 0)
    yield stop
    stop.value = 1


# do inserts while concurrently getting up/down subsets of acceptors
def test_race_conditions(zenith_env_builder: ZenithEnvBuilder, stop_value):

    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('test_safekeepers_race_conditions')
    pg = env.postgres.create_start('test_safekeepers_race_conditions')

    # we rely upon autocommit after each statement
    # as waiting for acceptors happens there
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    cur.execute('CREATE TABLE t(key int primary key, value text)')

    proc = Process(target=xmas_garland, args=(env.safekeepers, stop_value))
    proc.start()

    for i in range(1000):
        cur.execute("INSERT INTO t values (%s, 'payload');", (i + 1, ))

    cur.execute('SELECT sum(key) FROM t')
    assert cur.fetchone() == (500500, )

    stop_value.value = 1
    proc.join()


# Test that safekeepers push their info to the broker and learn peer status from it
def test_broker(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 3
    zenith_env_builder.enable_local_fs_remote_storage()
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch("test_broker", "main")
    pg = env.postgres.create_start('test_broker')
    pg.safe_psql("CREATE TABLE t(key int primary key, value text)")

    # learn zenith timeline from compute
    tenant_id = pg.safe_psql("show zenith.zenith_tenant")[0][0]
    timeline_id = pg.safe_psql("show zenith.zenith_timeline")[0][0]

    # wait until remote_consistent_lsn gets advanced on all safekeepers
    clients = [sk.http_client() for sk in env.safekeepers]
    stat_before = [cli.timeline_status(tenant_id, timeline_id) for cli in clients]
    log.info(f"statuses is {stat_before}")

    pg.safe_psql("INSERT INTO t SELECT generate_series(1,100), 'payload'")
    # force checkpoint to advance remote_consistent_lsn
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"checkpoint {tenant_id} {timeline_id}")
    # and wait till remote_consistent_lsn propagates to all safekeepers
    started_at = time.time()
    while True:
        stat_after = [cli.timeline_status(tenant_id, timeline_id) for cli in clients]
        if all(
                lsn_from_hex(s_after.remote_consistent_lsn) > lsn_from_hex(
                    s_before.remote_consistent_lsn) for s_after,
                s_before in zip(stat_after, stat_before)):
            break
        elapsed = time.time() - started_at
        if elapsed > 20:
            raise RuntimeError(
                f"timed out waiting {elapsed:.0f}s for remote_consistent_lsn propagation: status before {stat_before}, status current {stat_after}"
            )
        time.sleep(0.5)


# Test that old WAL consumed by peers and pageserver is removed from safekeepers.
def test_wal_removal(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 2
    # to advance remote_consistent_llsn
    zenith_env_builder.enable_local_fs_remote_storage()
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('test_safekeepers_wal_removal')
    pg = env.postgres.create_start('test_safekeepers_wal_removal')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute('CREATE TABLE t(key int primary key, value text)')
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")

    tenant_id = pg.safe_psql("show zenith.zenith_tenant")[0][0]
    timeline_id = pg.safe_psql("show zenith.zenith_timeline")[0][0]

    # force checkpoint to advance remote_consistent_lsn
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"checkpoint {tenant_id} {timeline_id}")

    # We will wait for first segment removal. Make sure they exist for starter.
    first_segments = [
        os.path.join(sk.data_dir(), tenant_id, timeline_id, '000000010000000000000001')
        for sk in env.safekeepers
    ]
    assert all(os.path.exists(p) for p in first_segments)

    http_cli = env.safekeepers[0].http_client()
    # Pretend WAL is offloaded to s3.
    http_cli.record_safekeeper_info(tenant_id, timeline_id, {'s3_wal_lsn': 'FFFFFFFF/FEFFFFFF'})

    # wait till first segment is removed on all safekeepers
    started_at = time.time()
    while True:
        if all(not os.path.exists(p) for p in first_segments):
            break
        elapsed = time.time() - started_at
        if elapsed > 20:
            raise RuntimeError(f"timed out waiting {elapsed:.0f}s for first segment get removed")
        time.sleep(0.5)


class ProposerPostgres(PgProtocol):
    """Object for running postgres without ZenithEnv"""
    def __init__(self,
                 pgdata_dir: str,
                 pg_bin,
                 timeline_id: uuid.UUID,
                 tenant_id: uuid.UUID,
                 listen_addr: str,
                 port: int):
        super().__init__(host=listen_addr, port=port, user='zenith_admin', dbname='postgres')

        self.pgdata_dir: str = pgdata_dir
        self.pg_bin: PgBin = pg_bin
        self.timeline_id: uuid.UUID = timeline_id
        self.tenant_id: uuid.UUID = tenant_id
        self.listen_addr: str = listen_addr
        self.port: int = port

    def pg_data_dir_path(self) -> str:
        """ Path to data directory """
        return self.pgdata_dir

    def config_file_path(self) -> str:
        """ Path to postgresql.conf """
        return os.path.join(self.pgdata_dir, 'postgresql.conf')

    def create_dir_config(self, safekeepers: str):
        """ Create dir and config for running --sync-safekeepers """

        mkdir_if_needed(self.pg_data_dir_path())
        with open(self.config_file_path(), "w") as f:
            cfg = [
                "synchronous_standby_names = 'walproposer'\n",
                "shared_preload_libraries = 'zenith'\n",
                f"zenith.zenith_timeline = '{self.timeline_id.hex}'\n",
                f"zenith.zenith_tenant = '{self.tenant_id.hex}'\n",
                f"zenith.page_server_connstring = ''\n",
                f"wal_acceptors = '{safekeepers}'\n",
                f"listen_addresses = '{self.listen_addr}'\n",
                f"port = '{self.port}'\n",
            ]

            f.writelines(cfg)

    def sync_safekeepers(self) -> str:
        """
        Run 'postgres --sync-safekeepers'.
        Returns execution result, which is commit_lsn after sync.
        """

        command = ["postgres", "--sync-safekeepers"]
        env = {
            "PGDATA": self.pg_data_dir_path(),
        }

        basepath = self.pg_bin.run_capture(command, env)
        stdout_filename = basepath + '.stdout'

        with open(stdout_filename, 'r') as stdout_f:
            stdout = stdout_f.read()
            return stdout.strip("\n ")

    def initdb(self):
        """ Run initdb """

        args = ["initdb", "-U", "zenith_admin", "-D", self.pg_data_dir_path()]
        self.pg_bin.run(args)

    def start(self):
        """ Start postgres with pg_ctl """

        log_path = os.path.join(self.pg_data_dir_path(), "pg.log")
        args = ["pg_ctl", "-D", self.pg_data_dir_path(), "-l", log_path, "-w", "start"]
        self.pg_bin.run(args)

    def stop(self):
        """ Stop postgres with pg_ctl """

        args = ["pg_ctl", "-D", self.pg_data_dir_path(), "-m", "immediate", "-w", "stop"]
        self.pg_bin.run(args)


# insert wal in all safekeepers and run sync on proposer
def test_sync_safekeepers(zenith_env_builder: ZenithEnvBuilder,
                          pg_bin: PgBin,
                          port_distributor: PortDistributor):

    # We don't really need the full environment for this test, just the
    # safekeepers would be enough.
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init_start()

    timeline_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    # write config for proposer
    pgdata_dir = os.path.join(env.repo_dir, "proposer_pgdata")
    pg = ProposerPostgres(pgdata_dir,
                          pg_bin,
                          timeline_id,
                          tenant_id,
                          '127.0.0.1',
                          port_distributor.get_port())
    pg.create_dir_config(env.get_safekeeper_connstrs())

    # valid lsn, which is not in the segment start, nor in zero segment
    epoch_start_lsn = 0x16B9188  # 0/16B9188
    begin_lsn = epoch_start_lsn

    # append and commit WAL
    lsn_after_append = []
    for i in range(3):
        res = env.safekeepers[i].append_logical_message(
            tenant_id,
            timeline_id,
            {
                "lm_prefix": "prefix",
                "lm_message": "message",
                "set_commit_lsn": True,
                "send_proposer_elected": True,
                "term": 2,
                "begin_lsn": begin_lsn,
                "epoch_start_lsn": epoch_start_lsn,
                "truncate_lsn": epoch_start_lsn,
            },
        )
        lsn_hex = lsn_to_hex(res["inserted_wal"]["end_lsn"])
        lsn_after_append.append(lsn_hex)
        log.info(f"safekeeper[{i}] lsn after append: {lsn_hex}")

    # run sync safekeepers
    lsn_after_sync = pg.sync_safekeepers()
    log.info(f"lsn after sync = {lsn_after_sync}")

    assert all(lsn_after_sync == lsn for lsn in lsn_after_append)


def test_timeline_status(zenith_env_builder: ZenithEnvBuilder):
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('test_timeline_status')
    pg = env.postgres.create_start('test_timeline_status')

    wa = env.safekeepers[0]
    wa_http_cli = wa.http_client()
    wa_http_cli.check_status()

    # learn zenith timeline from compute
    tenant_id = pg.safe_psql("show zenith.zenith_tenant")[0][0]
    timeline_id = pg.safe_psql("show zenith.zenith_timeline")[0][0]

    # fetch something sensible from status
    tli_status = wa_http_cli.timeline_status(tenant_id, timeline_id)
    epoch = tli_status.acceptor_epoch
    timeline_start_lsn = tli_status.timeline_start_lsn

    pg.safe_psql("create table t(i int)")

    # ensure epoch goes up after reboot
    pg.stop().start()
    pg.safe_psql("insert into t values(10)")

    tli_status = wa_http_cli.timeline_status(tenant_id, timeline_id)
    epoch_after_reboot = tli_status.acceptor_epoch
    assert epoch_after_reboot > epoch

    # and timeline_start_lsn stays the same
    assert tli_status.timeline_start_lsn == timeline_start_lsn


class SafekeeperEnv:
    def __init__(self,
                 repo_dir: Path,
                 port_distributor: PortDistributor,
                 pg_bin: PgBin,
                 num_safekeepers: int = 1):
        self.repo_dir = repo_dir
        self.port_distributor = port_distributor
        self.broker = Etcd(datadir=os.path.join(self.repo_dir, "etcd"),
                           port=self.port_distributor.get_port(),
                           peer_port=self.port_distributor.get_port())
        self.pg_bin = pg_bin
        self.num_safekeepers = num_safekeepers
        self.bin_safekeeper = os.path.join(str(zenith_binpath), 'safekeeper')
        self.safekeepers: Optional[List[subprocess.CompletedProcess[Any]]] = None
        self.postgres: Optional[ProposerPostgres] = None
        self.tenant_id: Optional[uuid.UUID] = None
        self.timeline_id: Optional[uuid.UUID] = None

    def init(self) -> "SafekeeperEnv":
        assert self.postgres is None, "postgres is already initialized"
        assert self.safekeepers is None, "safekeepers are already initialized"

        self.timeline_id = uuid.uuid4()
        self.tenant_id = uuid.uuid4()
        mkdir_if_needed(str(self.repo_dir))

        # Create config and a Safekeeper object for each safekeeper
        self.safekeepers = []
        for i in range(1, self.num_safekeepers + 1):
            self.safekeepers.append(self.start_safekeeper(i))

        # Create and start postgres
        self.postgres = self.create_postgres()
        self.postgres.start()

        return self

    def start_safekeeper(self, i):
        port = SafekeeperPort(
            pg=self.port_distributor.get_port(),
            http=self.port_distributor.get_port(),
        )

        safekeeper_dir = os.path.join(self.repo_dir, f"sk{i}")
        mkdir_if_needed(safekeeper_dir)

        args = [
            self.bin_safekeeper,
            "-l",
            f"127.0.0.1:{port.pg}",
            "--listen-http",
            f"127.0.0.1:{port.http}",
            "-D",
            safekeeper_dir,
            "--id",
            str(i),
            "--broker-endpoints",
            self.broker.client_url(),
            "--daemonize"
        ]

        log.info(f'Running command "{" ".join(args)}"')
        return subprocess.run(args, check=True)

    def get_safekeeper_connstrs(self):
        return ','.join([sk_proc.args[2] for sk_proc in self.safekeepers])

    def create_postgres(self):
        pgdata_dir = os.path.join(self.repo_dir, "proposer_pgdata")
        pg = ProposerPostgres(pgdata_dir,
                              self.pg_bin,
                              self.timeline_id,
                              self.tenant_id,
                              "127.0.0.1",
                              self.port_distributor.get_port())
        pg.initdb()
        pg.create_dir_config(self.get_safekeeper_connstrs())
        return pg

    def kill_safekeeper(self, sk_dir):
        """Read pid file and kill process"""
        pid_file = os.path.join(sk_dir, "safekeeper.pid")
        with open(pid_file, "r") as f:
            pid = int(f.read())
            log.info(f"Killing safekeeper with pid {pid}")
            os.kill(pid, signal.SIGKILL)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        log.info('Cleaning up all safekeeper and compute nodes')

        # Stop all the nodes
        if self.postgres is not None:
            self.postgres.stop()
        if self.safekeepers is not None:
            for sk_proc in self.safekeepers:
                self.kill_safekeeper(sk_proc.args[6])


def test_safekeeper_without_pageserver(test_output_dir: str,
                                       port_distributor: PortDistributor,
                                       pg_bin: PgBin):
    # Create the environment in the test-specific output dir
    repo_dir = Path(os.path.join(test_output_dir, "repo"))

    env = SafekeeperEnv(
        repo_dir,
        port_distributor,
        pg_bin,
    )

    with env:
        env.init()
        assert env.postgres is not None

        env.postgres.safe_psql("create table t(i int)")
        env.postgres.safe_psql("insert into t select generate_series(1, 100)")
        res = env.postgres.safe_psql("select sum(i) from t")[0][0]
        assert res == 5050


def test_replace_safekeeper(zenith_env_builder: ZenithEnvBuilder):
    def safekeepers_guc(env: ZenithEnv, sk_names: List[int]) -> str:
        return ','.join([f'localhost:{sk.port.pg}' for sk in env.safekeepers if sk.id in sk_names])

    def execute_payload(pg: Postgres):
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                # we rely upon autocommit after each statement
                # as waiting for acceptors happens there
                cur.execute('CREATE TABLE IF NOT EXISTS t(key int, value text)')
                cur.execute("INSERT INTO t VALUES (0, 'something')")
                cur.execute('SELECT SUM(key) FROM t')
                sum_before = cur.fetchone()[0]

                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute('SELECT SUM(key) FROM t')
                sum_after = cur.fetchone()[0]
                assert sum_after == sum_before + 5000050000

    def show_statuses(safekeepers: List[Safekeeper], tenant_id: str, timeline_id: str):
        for sk in safekeepers:
            http_cli = sk.http_client()
            try:
                status = http_cli.timeline_status(tenant_id, timeline_id)
                log.info(f"Safekeeper {sk.id} status: {status}")
            except Exception as e:
                log.info(f"Safekeeper {sk.id} status error: {e}")

    zenith_env_builder.num_safekeepers = 4
    env = zenith_env_builder.init_start()
    env.zenith_cli.create_branch('test_replace_safekeeper')

    log.info("Use only first 3 safekeepers")
    env.safekeepers[3].stop()
    active_safekeepers = [1, 2, 3]
    pg = env.postgres.create('test_replace_safekeeper')
    pg.adjust_for_safekeepers(safekeepers_guc(env, active_safekeepers))
    pg.start()

    # learn zenith timeline from compute
    tenant_id = pg.safe_psql("show zenith.zenith_tenant")[0][0]
    timeline_id = pg.safe_psql("show zenith.zenith_timeline")[0][0]

    execute_payload(pg)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Restart all safekeepers to flush everything")
    env.safekeepers[0].stop(immediate=True)
    execute_payload(pg)
    env.safekeepers[0].start()
    env.safekeepers[1].stop(immediate=True)
    execute_payload(pg)
    env.safekeepers[1].start()
    env.safekeepers[2].stop(immediate=True)
    execute_payload(pg)
    env.safekeepers[2].start()

    env.safekeepers[0].stop(immediate=True)
    env.safekeepers[1].stop(immediate=True)
    env.safekeepers[2].stop(immediate=True)
    env.safekeepers[0].start()
    env.safekeepers[1].start()
    env.safekeepers[2].start()

    execute_payload(pg)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Stop sk1 (simulate failure) and use only quorum of sk2 and sk3")
    env.safekeepers[0].stop(immediate=True)
    execute_payload(pg)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Recreate postgres to replace failed sk1 with new sk4")
    pg.stop_and_destroy().create('test_replace_safekeeper')
    active_safekeepers = [2, 3, 4]
    env.safekeepers[3].start()
    pg.adjust_for_safekeepers(safekeepers_guc(env, active_safekeepers))
    pg.start()

    execute_payload(pg)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Stop sk2 to require quorum of sk3 and sk4 for normal work")
    env.safekeepers[1].stop(immediate=True)
    execute_payload(pg)
    show_statuses(env.safekeepers, tenant_id, timeline_id)


# We have `wal_keep_size=0`, so postgres should trim WAL once it's broadcasted
# to all safekeepers. This test checks that compute WAL can fit into small number
# of WAL segments.
def test_wal_deleted_after_broadcast(zenith_env_builder: ZenithEnvBuilder):
    # used to calculate delta in collect_stats
    last_lsn = .0

    # returns LSN and pg_wal size, all in MB
    def collect_stats(pg: Postgres, cur, enable_logs=True):
        nonlocal last_lsn
        assert pg.pgdata_dir is not None

        log.info('executing INSERT to generate WAL')
        cur.execute("select pg_current_wal_lsn()")
        current_lsn = lsn_from_hex(cur.fetchone()[0]) / 1024 / 1024
        pg_wal_size = get_dir_size(os.path.join(pg.pgdata_dir, 'pg_wal')) / 1024 / 1024
        if enable_logs:
            log.info(f"LSN delta: {current_lsn - last_lsn} MB, current WAL size: {pg_wal_size} MB")
        last_lsn = current_lsn
        return current_lsn, pg_wal_size

    # generates about ~20MB of WAL, to create at least one new segment
    def generate_wal(cur):
        cur.execute("INSERT INTO t SELECT generate_series(1,300000), 'payload'")

    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('test_wal_deleted_after_broadcast')
    # Adjust checkpoint config to prevent keeping old WAL segments
    pg = env.postgres.create_start(
        'test_wal_deleted_after_broadcast',
        config_lines=['min_wal_size=32MB', 'max_wal_size=32MB', 'log_checkpoints=on'])

    pg_conn = pg.connect()
    cur = pg_conn.cursor()
    cur.execute('CREATE TABLE t(key int, value text)')

    collect_stats(pg, cur)

    # generate WAL to simulate normal workload
    for i in range(5):
        generate_wal(cur)
        collect_stats(pg, cur)

    log.info('executing checkpoint')
    cur.execute('CHECKPOINT')
    wal_size_after_checkpoint = collect_stats(pg, cur)[1]

    # there shouldn't be more than 2 WAL segments (but dir may have archive_status files)
    assert wal_size_after_checkpoint < 16 * 2.5


def test_delete_force(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init_start()

    # Create two tenants: one will be deleted, other should be preserved.
    tenant_id = env.initial_tenant.hex
    timeline_id_1 = env.zenith_cli.create_branch('br1').hex  # Acive, delete explicitly
    timeline_id_2 = env.zenith_cli.create_branch('br2').hex  # Inactive, delete explictly
    timeline_id_3 = env.zenith_cli.create_branch('br3').hex  # Active, delete with the tenant
    timeline_id_4 = env.zenith_cli.create_branch('br4').hex  # Inactive, delete with the tenant

    tenant_id_other_uuid, timeline_id_other_uuid = env.zenith_cli.create_tenant()
    tenant_id_other = tenant_id_other_uuid.hex
    timeline_id_other = timeline_id_other_uuid.hex

    # Populate branches
    pg_1 = env.postgres.create_start('br1')
    pg_2 = env.postgres.create_start('br2')
    pg_3 = env.postgres.create_start('br3')
    pg_4 = env.postgres.create_start('br4')
    pg_other = env.postgres.create_start('main', tenant_id=uuid.UUID(hex=tenant_id_other))
    for pg in [pg_1, pg_2, pg_3, pg_4, pg_other]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute('CREATE TABLE t(key int primary key)')
    sk = env.safekeepers[0]
    sk_data_dir = Path(sk.data_dir())
    sk_http = sk.http_client()
    assert (sk_data_dir / tenant_id / timeline_id_1).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_2).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_3).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_4).is_dir()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Stop branches which should be inactive and restart Safekeeper to drop its in-memory state.
    pg_2.stop_and_destroy()
    pg_4.stop_and_destroy()
    sk.stop()
    sk.start()

    # Ensure connections to Safekeeper are established
    for pg in [pg_1, pg_3, pg_other]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute('INSERT INTO t (key) VALUES (1)')

    # Remove initial tenant's br1 (active)
    assert sk_http.timeline_delete_force(tenant_id, timeline_id_1) == {
        "dir_existed": True,
        "was_active": True,
    }
    assert not (sk_data_dir / tenant_id / timeline_id_1).exists()
    assert (sk_data_dir / tenant_id / timeline_id_2).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_3).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_4).is_dir()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Ensure repeated deletion succeeds
    assert sk_http.timeline_delete_force(tenant_id, timeline_id_1) == {
        "dir_existed": False, "was_active": False
    }
    assert not (sk_data_dir / tenant_id / timeline_id_1).exists()
    assert (sk_data_dir / tenant_id / timeline_id_2).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_3).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_4).is_dir()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Remove initial tenant's br2 (inactive)
    assert sk_http.timeline_delete_force(tenant_id, timeline_id_2) == {
        "dir_existed": True,
        "was_active": False,
    }
    assert not (sk_data_dir / tenant_id / timeline_id_1).exists()
    assert not (sk_data_dir / tenant_id / timeline_id_2).exists()
    assert (sk_data_dir / tenant_id / timeline_id_3).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_4).is_dir()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Remove non-existing branch, should succeed
    assert sk_http.timeline_delete_force(tenant_id, '00' * 16) == {
        "dir_existed": False,
        "was_active": False,
    }
    assert not (sk_data_dir / tenant_id / timeline_id_1).exists()
    assert not (sk_data_dir / tenant_id / timeline_id_2).exists()
    assert (sk_data_dir / tenant_id / timeline_id_3).exists()
    assert (sk_data_dir / tenant_id / timeline_id_4).is_dir()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Remove initial tenant fully (two branches are active)
    response = sk_http.tenant_delete_force(tenant_id)
    assert response == {
        timeline_id_3: {
            "dir_existed": True,
            "was_active": True,
        }
    }
    assert not (sk_data_dir / tenant_id).exists()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Remove initial tenant again.
    response = sk_http.tenant_delete_force(tenant_id)
    assert response == {}
    assert not (sk_data_dir / tenant_id).exists()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Ensure the other tenant still works
    sk_http.timeline_status(tenant_id_other, timeline_id_other)
    with closing(pg_other.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('INSERT INTO t (key) VALUES (123)')
