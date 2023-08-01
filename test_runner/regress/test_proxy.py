import json
import subprocess
from typing import Any, List

import psycopg2
import pytest
import requests
from fixtures.neon_fixtures import PSQL, NeonProxy, VanillaPostgres


def test_proxy_select_1(static_proxy: NeonProxy):
    """
    A simplest smoke test: check proxy against a local postgres instance.
    """

    # no SNI, deprecated `options=project` syntax (before we had several endpoint in project)
    out = static_proxy.safe_psql("select 1", sslsni=0, options="project=generic-project-name")
    assert out[0][0] == 1

    # no SNI, new `options=endpoint` syntax
    out = static_proxy.safe_psql("select 1", sslsni=0, options="endpoint=generic-project-name")
    assert out[0][0] == 1

    # with SNI
    out = static_proxy.safe_psql("select 42", host="generic-project-name.localtest.me")
    assert out[0][0] == 42


def test_password_hack(static_proxy: NeonProxy):
    """
    Check the PasswordHack auth flow: an alternative to SCRAM auth for
    clients which can't provide the project/endpoint name via SNI or `options`.
    """

    user = "borat"
    password = "password"
    static_proxy.safe_psql(f"create role {user} with login password '{password}'")

    # Note the format of `magic`!
    magic = f"project=irrelevant;{password}"
    out = static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)
    assert out[0][0] == 1

    magic = f"endpoint=irrelevant;{password}"
    out = static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)
    assert out[0][0] == 1

    # Must also check that invalid magic won't be accepted.
    with pytest.raises(psycopg2.OperationalError):
        magic = "broken"
        static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)


@pytest.mark.asyncio
async def test_link_auth(vanilla_pg: VanillaPostgres, link_proxy: NeonProxy):
    """
    Check the Link auth flow: a lightweight auth method which delegates
    all necessary checks to the console by sending client an auth URL.
    """

    psql = await PSQL(host=link_proxy.host, port=link_proxy.proxy_port).run("select 42")

    base_uri = link_proxy.link_auth_uri
    link = await NeonProxy.find_auth_link(base_uri, psql)

    psql_session_id = NeonProxy.get_session_id(base_uri, link)
    await NeonProxy.activate_link_auth(vanilla_pg, link_proxy, psql_session_id)

    assert psql.stdout is not None
    out = (await psql.stdout.read()).decode("utf-8").strip()
    assert out == "42"


@pytest.mark.parametrize("option_name", ["project", "endpoint"])
def test_proxy_options(static_proxy: NeonProxy, option_name: str):
    """
    Check that we pass extra `options` to the PostgreSQL server:
    * `project=...` and `endpoint=...` shouldn't be passed at all
    * (otherwise postgres will raise an error).
    * everything else should be passed as-is.
    """

    options = f"{option_name}=irrelevant -cproxytest.option=value"
    out = static_proxy.safe_psql("show proxytest.option", options=options, sslsni=0)
    assert out[0][0] == "value"

    options = f"-c proxytest.foo=\\ str {option_name}=irrelevant"
    out = static_proxy.safe_psql("show proxytest.foo", options=options, sslsni=0)
    assert out[0][0] == " str"

    options = "-cproxytest.option=value"
    out = static_proxy.safe_psql("show proxytest.option", options=options)
    assert out[0][0] == "value"

    options = "-c proxytest.foo=\\ str"
    out = static_proxy.safe_psql("show proxytest.foo", options=options)
    assert out[0][0] == " str"


def test_auth_errors(static_proxy: NeonProxy):
    """
    Check that we throw very specific errors in some unsuccessful auth scenarios.
    """

    # User does not exist
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio")
    text = str(exprinfo.value).strip()
    assert text.find("password authentication failed for user 'pinocchio'") != -1

    static_proxy.safe_psql(
        "create role pinocchio with login password 'magic'",
    )

    # User exists, but password is missing
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio", password=None)
    text = str(exprinfo.value).strip()
    assert text.find("password authentication failed for user 'pinocchio'") != -1

    # User exists, but password is wrong
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio", password="bad")
    text = str(exprinfo.value).strip()
    assert text.find("password authentication failed for user 'pinocchio'") != -1

    # Finally, check that the user can connect
    with static_proxy.connect(user="pinocchio", password="magic"):
        pass


def test_forward_params_to_client(static_proxy: NeonProxy):
    """
    Check that we forward all necessary PostgreSQL server params to client.
    """

    # A subset of parameters (GUCs) which postgres
    # sends to the client during connection setup.
    # Unfortunately, `GUC_REPORT` can't be queried.
    # Proxy *should* forward them, otherwise client library
    # might misbehave (e.g. parse timestamps incorrectly).
    reported_params_subset = [
        "client_encoding",
        "integer_datetimes",
        "is_superuser",
        "server_encoding",
        "server_version",
        "session_authorization",
        "standard_conforming_strings",
    ]

    query = """
        select name, setting
        from pg_catalog.pg_settings
        where name = any(%s)
    """

    with static_proxy.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (reported_params_subset,))
            for name, value in cur.fetchall():
                # Check that proxy has forwarded this parameter.
                assert conn.get_parameter_status(name) == value


def test_close_on_connections_exit(static_proxy: NeonProxy):
    # Open two connections, send SIGTERM, then ensure that proxy doesn't exit
    # until after connections close.
    with static_proxy.connect(), static_proxy.connect():
        static_proxy.terminate()
        with pytest.raises(subprocess.TimeoutExpired):
            static_proxy.wait_for_exit(timeout=2)
        # Ensure we don't accept any more connections
        with pytest.raises(psycopg2.OperationalError):
            static_proxy.connect()
    static_proxy.wait_for_exit()


def test_sql_over_http(static_proxy: NeonProxy):
    static_proxy.safe_psql("create role http with login password 'http' superuser")

    def q(sql: str, params: List[Any] = []) -> Any:
        connstr = f"postgresql://http:http@{static_proxy.domain}:{static_proxy.proxy_port}/postgres"
        response = requests.post(
            f"https://{static_proxy.domain}:{static_proxy.external_http_port}/sql",
            data=json.dumps({"query": sql, "params": params}),
            headers={"Content-Type": "application/sql", "Neon-Connection-String": connstr},
            verify=str(static_proxy.test_output_dir / "proxy.crt"),
        )
        assert response.status_code == 200
        return response.json()

    rows = q("select 42 as answer")["rows"]
    assert rows == [{"answer": 42}]

    rows = q("select $1 as answer", [42])["rows"]
    assert rows == [{"answer": "42"}]

    rows = q("select $1 * 1 as answer", [42])["rows"]
    assert rows == [{"answer": 42}]

    rows = q("select $1::int[] as answer", [[1, 2, 3]])["rows"]
    assert rows == [{"answer": [1, 2, 3]}]

    rows = q("select $1::json->'a' as answer", [{"a": {"b": 42}}])["rows"]
    assert rows == [{"answer": {"b": 42}}]

    rows = q("select * from pg_class limit 1")["rows"]
    assert len(rows) == 1

    res = q("create table t(id serial primary key, val int)")
    assert res["command"] == "CREATE"
    assert res["rowCount"] is None

    res = q("insert into t(val) values (10), (20), (30) returning id")
    assert res["command"] == "INSERT"
    assert res["rowCount"] == 3
    assert res["rows"] == [{"id": 1}, {"id": 2}, {"id": 3}]

    res = q("select * from t")
    assert res["command"] == "SELECT"
    assert res["rowCount"] == 3

    res = q("drop table t")
    assert res["command"] == "DROP"
    assert res["rowCount"] is None


def test_sql_over_http_output_options(static_proxy: NeonProxy):
    static_proxy.safe_psql("create role http2 with login password 'http2' superuser")

    def q(sql: str, raw_text: bool, array_mode: bool, params: List[Any] = []) -> Any:
        connstr = (
            f"postgresql://http2:http2@{static_proxy.domain}:{static_proxy.proxy_port}/postgres"
        )
        response = requests.post(
            f"https://{static_proxy.domain}:{static_proxy.external_http_port}/sql",
            data=json.dumps({"query": sql, "params": params}),
            headers={
                "Content-Type": "application/sql",
                "Neon-Connection-String": connstr,
                "Neon-Raw-Text-Output": "true" if raw_text else "false",
                "Neon-Array-Mode": "true" if array_mode else "false",
            },
            verify=str(static_proxy.test_output_dir / "proxy.crt"),
        )
        assert response.status_code == 200
        return response.json()

    rows = q("select 1 as n, 'a' as s, '{1,2,3}'::int4[] as arr", False, False)["rows"]
    assert rows == [{"arr": [1, 2, 3], "n": 1, "s": "a"}]

    rows = q("select 1 as n, 'a' as s, '{1,2,3}'::int4[] as arr", False, True)["rows"]
    assert rows == [[1, "a", [1, 2, 3]]]

    rows = q("select 1 as n, 'a' as s, '{1,2,3}'::int4[] as arr", True, False)["rows"]
    assert rows == [{"arr": "{1,2,3}", "n": "1", "s": "a"}]

    rows = q("select 1 as n, 'a' as s, '{1,2,3}'::int4[] as arr", True, True)["rows"]
    assert rows == [["1", "a", "{1,2,3}"]]
