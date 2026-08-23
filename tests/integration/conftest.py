"""Fixtures for the walbox integration test suite.

A session-scoped `testcontainers` Postgres container (plain and TLS-enabled
variants) plus per-test table/publication fixtures, introduced alongside
walbox/transport.py, the first module that needs a real replication
connection to test against.
"""

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
from testcontainers.core.container import ExecConfig

_POSTGRES_IMAGE = "postgres:18-alpine"

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
            x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False
        )
        .sign(key, hashes.SHA256())
    )

    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
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
            cert_path.read_bytes(), "/tmp/server.crt", mode=0o644
        )
        running.copy_into_container(
            key_path.read_bytes(), "/tmp/server.key", mode=0o600
        )
        running.exec(
            ExecConfig(
                command=[
                    "chown",
                    "postgres:postgres",
                    "/tmp/server.crt",
                    "/tmp/server.key",
                ],
            )
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
            )
        )
        hba_file = hba_file_result.output.decode().strip()
        running.exec(
            ExecConfig(
                command=["sh", "-c", f"sed -i 's/^host /hostssl /' {hba_file}"],
                user="postgres",
            )
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
            )
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
