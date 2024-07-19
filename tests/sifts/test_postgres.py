"""Unit tests for Postgres backend."""

import time

import pytest
import psycopg2
from sifts.core import SearchEnginePostgreSQL
from psycopg2 import OperationalError

TEST_DB_DSN = "postgresql://testuser:testpass@localhost:5432/testdb"


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return "docker-compose.yml"


@pytest.fixture(scope="session")
def postgres_service(docker_services):
    """Wait for the PostgreSQL service to be up and running."""

    def is_postgres_healthy():
        try:
            conn = psycopg2.connect(
                dbname="testdb",
                user="testuser",
                password="testpass",
                host="localhost",
                port=5432,
            )
            conn.close()
            return True
        except OperationalError:
            return False

    timeout = 30
    pause = 0.5
    start_time = time.time()

    while time.time() - start_time < timeout:
        if is_postgres_healthy():
            return
        time.sleep(pause)

    raise TimeoutError("PostgreSQL service did not become healthy in time")


@pytest.fixture
def search_engine():
    engine = SearchEnginePostgreSQL(dsn=TEST_DB_DSN)
    yield engine
    with engine.conn() as conn:
        conn.execute("TRUNCATE TABLE documents RESTART IDENTITY CASCADE;")


def test_add_document(postgres_service, search_engine):
    content = ["test content"]
    ids = search_engine.add(content)
    assert len(ids) == 1


def test_query_document(postgres_service, search_engine):
    content = ["test query content"]
    search_engine.add(content)
    results = search_engine.query("content")
    assert len(results) == 1


def test_update_document(postgres_service, search_engine):
    content = ["initial content"]
    ids = search_engine.add(content)
    updated_content = ["updated content"]
    search_engine.update(ids, updated_content)
    results = search_engine.query("updated")
    assert len(results) == 1


def test_delete_document(postgres_service, search_engine):
    content = ["content to delete"]
    ids = search_engine.add(content)
    search_engine.delete(ids)
    results = search_engine.query("delete")
    assert len(results) == 0
