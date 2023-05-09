import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
from fixtures.neon_fixtures import RemotePostgres
from fixtures.utils import subprocess_capture


@pytest.mark.remote_cluster
@pytest.mark.parametrize(
    "client",
    [
        "csharp/npgsql",
        "java/jdbc",
        "rust/tokio-postgres",
        "python/asyncpg",
        "python/pg8000",
        pytest.param(
            "swift/PostgresClientKitExample",  # See https://github.com/neondatabase/neon/pull/2008#discussion_r911896592
            marks=pytest.mark.xfail(reason="Neither SNI nor parameters is supported"),
        ),
        "swift/PostgresNIOExample",
        "typescript/postgresql-client",
    ],
)
def test_pg_clients(test_output_dir: Path, remote_pg: RemotePostgres, client: str):
    conn_options = remote_pg.conn_options()

    env_file = None
    with NamedTemporaryFile(mode="w", delete=False) as f:
        env_file = f.name
        f.write(
            f"""
            NEON_HOST={conn_options["host"]}
            NEON_DATABASE={conn_options["dbname"]}
            NEON_USER={conn_options["user"]}
            NEON_PASSWORD={conn_options["password"]}
        """
        )

    image_tag = client.lower()
    docker_bin = shutil.which("docker")
    if docker_bin is None:
        raise RuntimeError("docker is required for running this test")

    build_cmd = [docker_bin, "build", "--tag", image_tag, f"{Path(__file__).parent / client}"]
    subprocess_capture(test_output_dir, build_cmd, check=True)

    run_cmd = [docker_bin, "run", "--rm", "--env-file", env_file, image_tag]
    basepath = subprocess_capture(test_output_dir, run_cmd, check=True)

    assert Path(f"{basepath}.stdout").read_text().strip() == "1"
