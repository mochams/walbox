"""Fixtures for the walbox integration test suite.

A session-scoped `testcontainers` Postgres container (plain and TLS-enabled
variants) plus per-test table/publication fixtures, introduced alongside
walbox/transport.py, the first module that needs a real replication
connection to test against.
"""

import re
from collections.abc import AsyncIterator
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from psycopg import AsyncConnection
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.container import ExecConfig
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

_POSTGRES_IMAGE = "postgres:18-alpine"
# Pinned, not "latest": an earlier version of this suite used
# `pgbouncer/pgbouncer:latest`, an abandoned third-party image stuck on
# pgbouncer 1.15.0 (from 2020), and that alone was enough to draw wrong
# conclusions about what pgbouncer can and can't do. edoburu/pgbouncer is
# actively maintained; pin an exact tag so this can't silently drift again.
_PGBOUNCER_IMAGE = "edoburu/pgbouncer:v1.25.2-p0"
_PGBOUNCER_MIN_VERSION = (1, 23, 0)

_REPLICATION_SETTINGS = (
    "postgres -c wal_level=logical -c max_replication_slots=50 -c max_wal_senders=50"
)

_OUTBOX_SQL = (Path(__file__).resolve().parent / "test-table.sql").read_text()


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[PostgresContainer]:
    container = PostgresContainer(_POSTGRES_IMAGE).with_command(_REPLICATION_SETTINGS)
    with container as running:
        yield running


@pytest.fixture
def postgres_dsn(postgres_container: PostgresContainer) -> str:
    return postgres_container.get_connection_url(driver=None)


@pytest.fixture
async def outbox_table(postgres_dsn: str) -> AsyncIterator[None]:
    """The real `outbox` table + `walbox_pub` publication from outbox-table.sql.

    Fresh per test; dropped afterward so tests don't leak state into each
    other despite sharing one session-scoped container.
    """
    async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
        await conn.execute(_OUTBOX_SQL)
        yield
        await conn.execute("DROP PUBLICATION IF EXISTS walbox_pub")
        await conn.execute("DROP TABLE IF EXISTS outbox")


def _generate_self_signed_cert(cert_dir: Path) -> tuple[Path, Path]:
    """Generate a self-signed cert/key pair for the TLS container fixture."""
    key_path = cert_dir / "server.key"
    cert_path = cert_dir / "server.crt"

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    cert = (
        x509
        .CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC))
        .not_valid_after(datetime.now(UTC) + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost")]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ),
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return cert_path, key_path


@pytest.fixture(scope="session")
def tls_postgres_container(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[PostgresContainer]:
    """A second, TLS-enabled, TLS-*required* container.

    Deliberately separate from postgres_container: enabling SSL is a
    per-server setting, not a per-connection one.

    The cert/key are installed *after* the container starts, rather than
    bind-mounted in: PostgreSQL refuses a private key file unless it's owned
    by the exact UID the server process runs as (70, for this image) *and*
    has no group/other permission bits. A host-generated file bind-mounted
    in keeps the host's UID, not 70, so the server can't read it on a real
    (Linux) Docker host. Copying the bytes in after start and `chown`ing them
    as the container's own root user sidesteps that entirely. The files land
    in `/tmp` rather than the data directory -- `copy_into_container` needs
    its destination directory to already exist, and the data directory's
    path is Postgres-version-specific (e.g. `/var/lib/postgresql/18/docker`
    for this image, not the `.../data` of older images), so a flat, always-
    present path avoids depending on that layout at all.

    `ssl`/`ssl_cert_file`/`ssl_key_file` are all `SIGHUP`-context settings,
    reloadable via `pg_reload_conf()` with no restart needed -- same for
    `pg_hba.conf`, which is rewritten here to `hostssl`-only so the server
    actually refuses plaintext connections rather than merely offering TLS.
    The client already sends `sslmode=require` (see tls_postgres_dsn below),
    which independently guarantees *this* test's own connection is
    encrypted or fails outright -- the hostssl rewrite is a second,
    server-side safety net against a future bug that silently dropped
    `sslmode=require` and would otherwise let such a test pass over
    plaintext undetected.
    """
    cert_dir = tmp_path_factory.mktemp("tls")
    cert_path, key_path = _generate_self_signed_cert(cert_dir)

    container = PostgresContainer(_POSTGRES_IMAGE).with_command(_REPLICATION_SETTINGS)
    with container as running:
        running.copy_into_container(
            cert_path.read_bytes(),
            "/tmp/server.crt",
            mode=0o644,
        )
        running.copy_into_container(
            key_path.read_bytes(),
            "/tmp/server.key",
            mode=0o600,
        )
        running.exec(
            ExecConfig(
                command=[
                    "chown",
                    "postgres:postgres",
                    "/tmp/server.crt",
                    "/tmp/server.key",
                ],
            ),
        )
        hba_file_result = running.exec(
            ExecConfig(
                command=[
                    "psql",
                    "-U",
                    running.username,
                    "-d",
                    running.dbname,
                    "-tAc",
                    "SHOW hba_file;",
                ],
                user="postgres",
            ),
        )
        hba_file = hba_file_result.output.decode().strip()
        running.exec(
            ExecConfig(
                command=["sh", "-c", f"sed -i 's/^host /hostssl /' {hba_file}"],
                user="postgres",
            ),
        )
        running.exec(
            ExecConfig(
                # Separate -c flags, not one `A; B; C;` string: psql sends a
                # multi-statement string as one implicit transaction block,
                # and ALTER SYSTEM refuses to run inside a transaction block.
                command=[
                    "psql",
                    "-U",
                    running.username,
                    "-d",
                    running.dbname,
                    "-c",
                    "ALTER SYSTEM SET ssl = on;",
                    "-c",
                    "ALTER SYSTEM SET ssl_cert_file = '/tmp/server.crt';",
                    "-c",
                    "ALTER SYSTEM SET ssl_key_file = '/tmp/server.key';",
                    "-c",
                    "SELECT pg_reload_conf();",
                ],
                user="postgres",
            ),
        )
        yield running


@pytest.fixture
def tls_postgres_dsn(tls_postgres_container: PostgresContainer) -> str:
    base = tls_postgres_container.get_connection_url(driver=None)
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}sslmode=require"


@pytest.fixture
async def tls_outbox_table(tls_postgres_dsn: str) -> AsyncIterator[None]:
    """The real `outbox` table + `walbox_pub` publication, over TLS."""
    async with await AsyncConnection.connect(tls_postgres_dsn, autocommit=True) as conn:
        await conn.execute(_OUTBOX_SQL)
        yield
        await conn.execute("DROP PUBLICATION IF EXISTS walbox_pub")
        await conn.execute("DROP TABLE IF EXISTS outbox")


# pgbouncer fixtures
#
# A real pgbouncer process (not a fake) fronting a dedicated Postgres
# container, one per `pool_mode`. Both containers share a Docker network so
# pgbouncer reaches Postgres by container alias rather than the host-mapped
# port. That's the point: to prove what happens when a client only ever
# talks to pgbouncer, never directly to Postgres.
#
# edoburu/pgbouncer's own entrypoint only auto-generates
# `/etc/pgbouncer/pgbouncer.ini` from environment variables when that file
# doesn't already exist, so placing a real ini via `with_copy_into_container`
# before the container starts is enough. No entrypoint override needed.

_PGBOUNCER_POSTGRES_ALIAS = "pgbouncer-postgres"
_PGBOUNCER_USERLIST = b'"test" "test"\n'
_PGBOUNCER_VERSION_RE = re.compile(r"PgBouncer (\d+)\.(\d+)\.(\d+)")


def _pgbouncer_ini(
    pool_mode: str,
    *,
    default_pool_size: int = 10,
    max_prepared_statements: int | None = None,
) -> bytes:
    lines = [
        "[databases]",
        f"test = host={_PGBOUNCER_POSTGRES_ALIAS} port=5432 dbname=test",
        "",
        "[pgbouncer]",
        "listen_addr = *",
        "listen_port = 6432",
        "auth_type = plain",
        "auth_file = /etc/pgbouncer/userlist.txt",
        f"pool_mode = {pool_mode}",
        "max_client_conn = 100",
        f"default_pool_size = {default_pool_size}",
        "admin_users = test",
        "logfile = /tmp/pgbouncer.log",
    ]
    if max_prepared_statements is not None:
        lines.append(f"max_prepared_statements = {max_prepared_statements}")
    return ("\n".join(lines) + "\n").encode()


def _build_pgbouncer_container(network: Network, ini: bytes) -> DockerContainer:
    return (
        DockerContainer(_PGBOUNCER_IMAGE)
        .with_network(network)
        .with_exposed_ports(6432)
        .with_copy_into_container(ini, "/etc/pgbouncer/pgbouncer.ini")
        .with_copy_into_container(_PGBOUNCER_USERLIST, "/etc/pgbouncer/userlist.txt")
        .waiting_for(LogMessageWaitStrategy("process up:"))
    )


def _check_pgbouncer_version(container: DockerContainer) -> None:
    """Fail loudly if `_PGBOUNCER_IMAGE` doesn't meet `_PGBOUNCER_MIN_VERSION`.

    An earlier version of this suite silently ran against pgbouncer 1.15.0
    for months (`pgbouncer/pgbouncer:latest`, an abandoned third-party
    image) and drew wrong conclusions about pgbouncer's capabilities as a
    result. This check exists so a future image swap that regresses the
    version fails a test immediately instead of quietly changing what these
    tests actually prove.
    """
    result = container.exec(["pgbouncer", "--version"])
    match = _PGBOUNCER_VERSION_RE.search(result.output.decode())
    if match is None:
        pytest.fail(f"could not parse pgbouncer version from: {result.output!r}")
    version = tuple(int(part) for part in match.groups())
    if version < _PGBOUNCER_MIN_VERSION:
        pytest.fail(
            f"pgbouncer {'.'.join(map(str, version))} is older than "
            f"{'.'.join(map(str, _PGBOUNCER_MIN_VERSION))}, the minimum this "
            "suite assumes (replication passthrough and "
            "max_prepared_statements both need it). Update _PGBOUNCER_IMAGE.",
        )


@pytest.fixture(scope="session")
def _pgbouncer_network() -> Iterator[Network]:
    with Network() as network:
        yield network


@pytest.fixture(scope="session")
def _pgbouncer_postgres_container(
    _pgbouncer_network: Network,
) -> Iterator[PostgresContainer]:
    """A Postgres container reachable from pgbouncer containers by alias.

    Separate from `postgres_container`: that one isn't attached to
    `_pgbouncer_network`, and pgbouncer must reach Postgres by container
    hostname, not the host-mapped port `postgres_dsn` uses.
    """
    container = (
        PostgresContainer(_POSTGRES_IMAGE)
        .with_command(_REPLICATION_SETTINGS)
        .with_network(_pgbouncer_network)
        .with_network_aliases(_PGBOUNCER_POSTGRES_ALIAS)
    )
    with container as running:
        yield running


@pytest.fixture
def pgbouncer_postgres_dsn(_pgbouncer_postgres_container: PostgresContainer) -> str:
    """A direct, non-pgbouncer DSN to the Postgres `pgbouncer_dsn` fronts.

    Used to set up the outbox table and insert rows from outside the thing
    under test, the same way `postgres_dsn` is used alongside
    `Client`/`Transport` tests elsewhere in this suite.
    """
    return _pgbouncer_postgres_container.get_connection_url(driver=None)


@pytest.fixture
async def pgbouncer_outbox_table(pgbouncer_postgres_dsn: str) -> AsyncIterator[None]:
    """The `outbox` table + `walbox_pub` publication, on the pgbouncer-fronted Postgres."""
    async with await AsyncConnection.connect(
        pgbouncer_postgres_dsn, autocommit=True
    ) as conn:
        await conn.execute(_OUTBOX_SQL)
        yield
        await conn.execute("DROP PUBLICATION IF EXISTS walbox_pub")
        await conn.execute("DROP TABLE IF EXISTS outbox")


@pytest.fixture(scope="session", params=["session", "transaction"])
def pool_mode(request: pytest.FixtureRequest) -> str:
    """The pgbouncer `pool_mode`s walbox supports; test bodies run once per value.

    `statement` isn't included: it can't run the checkpoint store at all
    (see docs/production/setup.md#pgbouncer for why), so it's left out of
    the matrix rather than tested and marked failing everywhere.
    """
    return request.param


@pytest.fixture(scope="session")
def pgbouncer_container(
    _pgbouncer_postgres_container: PostgresContainer,
    _pgbouncer_network: Network,
    pool_mode: str,
) -> Iterator[DockerContainer]:
    """A real pgbouncer process, configured for the current `pool_mode`.

    Session-scoped per `pool_mode` value: pytest re-runs this fixture for
    each parametrization of `pool_mode`, so each pool_mode gets its own
    pgbouncer with a fixed config for the life of that parametrization,
    rather than one pgbouncer reconfigured live between tests.
    """
    container = _build_pgbouncer_container(
        _pgbouncer_network, _pgbouncer_ini(pool_mode)
    )
    with container as running:
        _check_pgbouncer_version(running)
        yield running


@pytest.fixture
def pgbouncer_dsn(pgbouncer_container: DockerContainer) -> str:
    """A DSN that only ever reaches Postgres through pgbouncer."""
    host = pgbouncer_container.get_container_host_ip()
    port = pgbouncer_container.get_exposed_port(6432)
    return f"postgresql://test:test@{host}:{port}/test"


@pytest.fixture(scope="session")
def pgbouncer_no_prepared_statements_container(
    _pgbouncer_postgres_container: PostgresContainer,
    _pgbouncer_network: Network,
) -> Iterator[DockerContainer]:
    """A transaction-mode pgbouncer with `max_prepared_statements` off.

    `_PGBOUNCER_IMAGE` defaults `max_prepared_statements` to 200, which
    already re-prepares a client's named statements on whatever backend
    it's routed to, so plain `pgbouncer_dsn` is safe for psycopg's
    autoprepare under transaction pooling without any extra setup. This
    fixture turns that off on purpose, reproducing the failure mode this
    suite exists to catch for callers who explicitly disable it, or run an
    older pgbouncer that never had it.
    """
    # A backend pool this small forces concurrent clients to actually share
    # (and get handed off) backend connections, which is what makes the
    # collision this fixture exists to reproduce reliable.
    ini = _pgbouncer_ini("transaction", default_pool_size=1, max_prepared_statements=0)
    container = _build_pgbouncer_container(_pgbouncer_network, ini)
    with container as running:
        _check_pgbouncer_version(running)
        yield running


@pytest.fixture
def pgbouncer_no_prepared_statements_dsn(
    pgbouncer_no_prepared_statements_container: DockerContainer,
) -> str:
    """A DSN through a transaction-mode pgbouncer with prepared statements off."""
    host = pgbouncer_no_prepared_statements_container.get_container_host_ip()
    port = pgbouncer_no_prepared_statements_container.get_exposed_port(6432)
    return f"postgresql://test:test@{host}:{port}/test"
