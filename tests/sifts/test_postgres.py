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


@pytest.fixture
def search_engine_prefix():
    engine = SearchEnginePostgreSQL(dsn=TEST_DB_DSN, prefix="my_prefix")
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
    results = search_engine.query("content")
    assert len(results) == 1


def test_delete_document(postgres_service, search_engine):
    content = ["content to delete"]
    ids = search_engine.add(content)
    search_engine.delete(ids)
    results = search_engine.query("delete")
    assert len(results) == 0


def test_add(postgres_service, search_engine):
    assert search_engine.query("Lorem") == []
    ids1 = search_engine.add(["Lorem ipsum dolor"])
    ids2 = search_engine.add(["sit amet"])
    assert len(search_engine.query("Lorem")) == 1
    assert search_engine.query("Lorem")[0]["id"] == ids1[0]


def test_query_wildcard(postgres_service, search_engine):
    assert search_engine.query("Lorem") == []
    ids1 = search_engine.add(["Lorem ipsum dolor"])
    ids2 = search_engine.add(["sit amet"])
    assert len(search_engine.query("am*")) == 1
    assert search_engine.query("am*")[0]["id"] == ids2[0]
    assert len(search_engine.query("ame*")) == 1


def test_query_pr(postgres_service, search_engine):
    assert search_engine.query("Lorem") == []
    ids1 = search_engine.add(["Lorem ipsum dolor"])
    ids2 = search_engine.add(["sit amet"])
    assert len(search_engine.query("Lorem or amet")) == 2


def test_add_prefix(postgres_service, search_engine, search_engine_prefix):
    assert search_engine_prefix.query("Lorem") == []
    search_engine_prefix.add(["Lorem ipsum dolor"])
    assert len(search_engine_prefix.query("Lorem")) == 1
    assert len(search_engine.query("Lorem")) == 0


def test_add_id(postgres_service, search_engine):
    ids = search_engine.add(["x"])
    assert len(ids) == 1
    assert len(ids[0]) == 36  # is UUIDv4
    ids = search_engine.add(["y"], ids=["my_id"])
    assert ids == ["my_id"]
    res = search_engine.query("y")
    assert len(res) == 1
    assert res[0]["id"] == "my_id"
    # does not raise, but updates
    search_engine.add(["z"], ids=["my_id"])
    res = search_engine.query("y")
    assert len(res) == 0
    res = search_engine.query("z")
    assert len(res) == 1


def test_update(postgres_service, search_engine):
    ids = search_engine.add(["Lorem ipsum"])
    res = search_engine.query("Lorem")
    assert len(res) == 1
    assert res[0]["id"] == ids[0]
    search_engine.update(ids=ids, contents=["dolor sit"])
    res = search_engine.query("Lorem")
    assert len(res) == 0
    res = search_engine.query("sit")
    assert len(res) == 1
    assert res[0]["id"] == ids[0]


def test_delete(postgres_service, search_engine):
    ids = search_engine.add(["Lorem ipsum"])
    res = search_engine.query("Lorem")
    assert len(res) == 1
    search_engine.delete(ids)
    res = search_engine.query("Lorem")
    assert len(res) == 0
    search_engine.delete(ids)


def test_query_multiple(postgres_service, search_engine):
    search_engine.add(["Lorem ipsum dolor"])
    search_engine.add(["sit amet"])
    assert len(search_engine.query("Lorem ipsum")) == 1
    assert len(search_engine.query("sit amet")) == 1
    assert len(search_engine.query("Lorem sit")) == 0


def test_query_order(postgres_service, search_engine):
    search = search_engine
    search.add(["Lorem"], metadatas=[{"k1": "a", "k2": "c"}], ids=["i1"])
    search.add(["Lorem"], metadatas=[{"k1": "b", "k2": "c"}], ids=["i2"])
    search.add(["Lorem"], metadatas=[{"k1": "c", "k2": "c"}], ids=["i3"])
    search.add(["Lorem"], metadatas=[{"k1": "d", "k2": "b"}], ids=["i4"])
    search.add(["Lorem"], metadatas=[{"k1": "e", "k2": "b"}], ids=["i5"])
    search.add(["Lorem"], metadatas=[{"k1": "f", "k2": "b"}], ids=["i6"])
    search.add(["Lorem"], metadatas=[{"k1": "g", "k2": "a"}], ids=["i7"])
    search.add(["Lorem"], metadatas=[{"k1": "h", "k2": "a"}], ids=["i8"])
    search.add(["Lorem"], metadatas=[{"k1": "i", "k2": "a"}], ids=["i9"])
    search.add(["Lorem"], ids=["i0"])
    res = search.query("Lorem")
    assert len(res) == 10
    # k1
    res = search.query("Lorem", order_by="k1")
    assert len(res) == 10
    assert [r["id"][1:] for r in res] == list("1234567890")
    assert [(r["metadata"] or {}).get("k1", "0") for r in res] == list("abcdefghi0")
    # +k1
    res = search.query("Lorem", order_by="+k1")
    assert [r["id"][1:] for r in res] == list("1234567890")
    assert [(r["metadata"] or {}).get("k1", "0") for r in res] == list("abcdefghi0")
    # -k1
    res = search.query("Lorem", order_by="-k1")
    assert [r["id"][1:] for r in res] == list("0987654321")
    assert [(r["metadata"] or {}).get("k1", "0") for r in res] == list("0ihgfedcba")
    # k2,k1
    res = search.query("Lorem", order_by=["k2", "k1"])
    assert [r["id"][1:] for r in res] == list("7894561230")
    assert [(r["metadata"] or {}).get("k2", "0") for r in res] == list("aaabbbccc0")
    assert [(r["metadata"] or {}).get("k1", "0") for r in res] == list("ghidefabc0")
    # k2,-k1
    res = search.query("Lorem", order_by=["k2", "-k1"])
    assert [r["id"][1:] for r in res] == list("9876543210")
    assert [(r["metadata"] or {}).get("k2", "0") for r in res] == list("aaabbbccc0")
    assert [(r["metadata"] or {}).get("k1", "0") for r in res] == list("ihgfedcba0")


def test_query_limit_offset(postgres_service, search_engine):
    search = search_engine
    search.add(["Lorem"], metadatas=[{"k1": "a", "k2": "c"}], ids=["i1"])
    search.add(["Lorem"], metadatas=[{"k1": "b", "k2": "c"}], ids=["i2"])
    search.add(["Lorem"], metadatas=[{"k1": "c", "k2": "c"}], ids=["i3"])
    search.add(["Lorem"], metadatas=[{"k1": "d", "k2": "b"}], ids=["i4"])
    search.add(["Lorem"], metadatas=[{"k1": "e", "k2": "b"}], ids=["i5"])
    search.add(["Lorem"], metadatas=[{"k1": "f", "k2": "b"}], ids=["i6"])
    search.add(["Lorem"], metadatas=[{"k1": "g", "k2": "a"}], ids=["i7"])
    search.add(["Lorem"], metadatas=[{"k1": "h", "k2": "a"}], ids=["i8"])
    search.add(["Lorem"], metadatas=[{"k1": "i", "k2": "a"}], ids=["i9"])
    search.add(["Lorem"], ids=["i0"])
    res = search.query("Lorem", order_by="k1")
    assert len(res) == 10
    res = search.query("Lorem", order_by="k1", limit=0)
    assert len(res) == 10
    res = search.query("Lorem", order_by="k1", limit=3)
    assert len(res) == 3
    assert [r["id"][1:] for r in res] == list("123")
    res = search.query("Lorem", order_by="k1", limit=3, offset=3)
    assert len(res) == 3
    assert [r["id"][1:] for r in res] == list("456")
    res = search.query("Lorem", order_by="k1", limit=3, offset=8)
    assert len(res) == 2
    assert [r["id"][1:] for r in res] == list("90")


def test_query_where(postgres_service, search_engine):
    search = search_engine
    search.add(["Lorem"], metadatas=[{"k1": "a", "k2": "c"}], ids=["i1"])
    search.add(["Lorem"], metadatas=[{"k1": "b", "k2": "c"}], ids=["i2"])
    search.add(["Lorem"], metadatas=[{"k1": "c", "k2": "c"}], ids=["i3"])
    search.add(["Lorem"], metadatas=[{"k1": "d", "k2": "b"}], ids=["i4"])
    search.add(["Lorem"], metadatas=[{"k1": "e", "k2": "b"}], ids=["i5"])
    search.add(["Lorem"], metadatas=[{"k1": "f", "k2": "b"}], ids=["i6"])
    search.add(["Lorem"], metadatas=[{"k1": "g", "k2": "a"}], ids=["i7"])
    search.add(["Lorem"], metadatas=[{"k1": "h", "k2": "a"}], ids=["i8"])
    search.add(["Lorem"], metadatas=[{"k1": "i", "k2": "a"}], ids=["i9"])
    search.add(["Lorem"], ids=["i0"])
    res = search.query("Lorem", where={"k2": "a"}, order_by="k1")
    assert len(res) == 3


def test_all_docs(postgres_service, search_engine):
    search = search_engine
    search.add(["Lorem ipsum dolor"])
    search.add(["sit amet"])
