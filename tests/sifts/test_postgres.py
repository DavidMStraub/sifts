"""Unit tests for Postgres backend."""

import time

import pytest
import psycopg2
from sifts.core import CollectionPostgreSQL
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
    engine = CollectionPostgreSQL(dsn=TEST_DB_DSN, name="my_name")
    yield engine
    with engine.conn() as conn:
        # conn.execute("DROP TRIGGER tsvectorupdate ON documents CASCADE;")
        conn.execute("TRUNCATE TABLE documents RESTART IDENTITY CASCADE;")


def test_add_document(postgres_service, search_engine):
    content = ["test content"]
    assert search_engine.count() == 0
    ids = search_engine.add(content)
    assert len(ids) == 1
    assert search_engine.count() == 1


def test_query_document(postgres_service, search_engine):
    content = ["test query content"]
    search_engine.add(content)
    results = search_engine.query("content")
    assert results["total"] == 1
    assert len(results["results"]) == 1


def test_update_document(postgres_service, search_engine):
    content = ["initial content"]
    ids = search_engine.add(content)
    results = search_engine.query("initial")
    assert results["total"] == 1
    assert len(results["results"]) == 1
    updated_content = ["updated content"]
    search_engine.update(ids, updated_content)
    results = search_engine.query("updated")
    assert results["total"] == 1
    assert len(results["results"]) == 1
    results = search_engine.query("content")
    assert results["total"] == 1
    assert len(results["results"]) == 1
    results = search_engine.query("initial")
    assert results["total"] == 0
    assert len(results["results"]) == 0


def test_delete_document(postgres_service, search_engine):
    content = ["content to delete"]
    assert search_engine.count() == 0
    ids = search_engine.add(content)
    assert search_engine.count() == 1
    search_engine.delete(ids)
    assert search_engine.count() == 0
    results = search_engine.query("delete")
    assert results["total"] == 0
    assert len(results["results"]) == 0


def test_add(postgres_service, search_engine):
    assert search_engine.query("Lorem") == {"total": 0, "results": []}
    ids1 = search_engine.add(["Lorem ipsum dolor"])
    ids2 = search_engine.add(["sit amet"])
    assert len(search_engine.query("Lorem")["results"]) == 1
    assert search_engine.query("Lorem")["results"][0]["id"] == ids1[0]


def test_query_wildcard(postgres_service, search_engine):
    assert search_engine.query("Lorem") == {"total": 0, "results": []}
    ids1 = search_engine.add(["Lorem ipsum dolor"])
    ids2 = search_engine.add(["sit amet"])
    assert len(search_engine.query("am*")["results"]) == 1
    assert search_engine.query("am*")["results"][0]["id"] == ids2[0]
    assert len(search_engine.query("ame*")["results"]) == 1


def test_query_pr(postgres_service, search_engine):
    assert search_engine.query("Lorem") == {"total": 0, "results": []}
    ids1 = search_engine.add(["Lorem ipsum dolor"])
    ids2 = search_engine.add(["sit amet"])
    assert len(search_engine.query("Lorem or amet")["results"]) == 2


def test_add_name(postgres_service, search_engine):
    search_engine_2 = CollectionPostgreSQL(dsn=TEST_DB_DSN, name="my_other_name")
    assert search_engine_2.query("Lorem") == {"total": 0, "results": []}
    search_engine_2.add(["Lorem ipsum dolor"])
    assert len(search_engine_2.query("Lorem")["results"]) == 1
    assert len(search_engine.query("Lorem")["results"]) == 0


def test_add_id(postgres_service, search_engine):
    ids = search_engine.add(["x"])
    assert len(ids) == 1
    assert len(ids[0]) == 36  # is UUIDv4
    ids = search_engine.add(["y"], ids=["my_id"])
    assert ids == ["my_id"]
    res = search_engine.query("y")
    assert len(res["results"]) == 1
    res = res["results"]
    assert res[0]["id"] == "my_id"
    # does not raise, but updates
    search_engine.add(["z"], ids=["my_id"])
    res = search_engine.query("y")
    assert len(res["results"]) == 0
    res = search_engine.query("z")
    assert len(res["results"]) == 1


def test_update(postgres_service, search_engine):
    ids = search_engine.add(["Lorem ipsum"])
    res = search_engine.query("Lorem")
    assert len(res["results"]) == 1
    res = res["results"]
    assert res[0]["id"] == ids[0]
    search_engine.update(ids=ids, contents=["dolor sit"])
    res = search_engine.query("Lorem")
    assert len(res["results"]) == 0
    res = search_engine.query("sit")
    assert len(res["results"]) == 1
    res = res["results"]
    assert res[0]["id"] == ids[0]


def test_delete(postgres_service, search_engine):
    ids = search_engine.add(["Lorem ipsum"])
    res = search_engine.query("Lorem")
    assert len(res["results"]) == 1
    search_engine.delete(ids)
    res = search_engine.query("Lorem")
    assert len(res["results"]) == 0
    search_engine.delete(ids)


def test_query_multiple(postgres_service, search_engine):
    search_engine.add(["Lorem ipsum dolor"])
    search_engine.add(["sit amet"])
    assert len(search_engine.query("Lorem ipsum")["results"]) == 1
    assert len(search_engine.query("sit amet")["results"]) == 1
    assert len(search_engine.query("Lorem sit")["results"]) == 0


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
    assert len(res["results"]) == 10
    # k1
    res = search.query("Lorem", order_by="k1")
    res = res["results"]
    assert len(res) == 10
    assert [r["id"][1:] for r in res] == list("1234567890")
    assert [(r["metadata"] or {}).get("k1", "0") for r in res] == list("abcdefghi0")
    # +k1
    res = search.query("Lorem", order_by="+k1")["results"]
    assert [r["id"][1:] for r in res] == list("1234567890")
    assert [(r["metadata"] or {}).get("k1", "0") for r in res] == list("abcdefghi0")
    # -k1
    res = search.query("Lorem", order_by="-k1")["results"]
    assert [r["id"][1:] for r in res] == list("0987654321")
    assert [(r["metadata"] or {}).get("k1", "0") for r in res] == list("0ihgfedcba")
    # k2,k1
    res = search.query("Lorem", order_by=["k2", "k1"])["results"]
    assert [r["id"][1:] for r in res] == list("7894561230")
    assert [(r["metadata"] or {}).get("k2", "0") for r in res] == list("aaabbbccc0")
    assert [(r["metadata"] or {}).get("k1", "0") for r in res] == list("ghidefabc0")
    # k2,-k1
    res = search.query("Lorem", order_by=["k2", "-k1"])["results"]
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
    assert len(res["results"]) == 10
    res = search.query("Lorem", order_by="k1", limit=0)
    assert len(res["results"]) == 10
    res = search.query("Lorem", order_by="k1", limit=3)
    assert len(res["results"]) == 3
    res = res["results"]
    assert [r["id"][1:] for r in res] == list("123")
    res = search.query("Lorem", order_by="k1", limit=3, offset=3)
    assert len(res["results"]) == 3
    res = res["results"]
    assert [r["id"][1:] for r in res] == list("456")
    res = search.query("Lorem", order_by="k1", limit=3, offset=8)
    assert len(res["results"]) == 2
    res = res["results"]
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
    assert len(res["results"]) == 3
    res = search.query("Lorem", where={"k2": {"$eq": "a"}}, order_by="k1")
    assert res["total"] == 3
    res = res["results"]
    assert len(res) == 3
    res = search.query("Lorem", where={"k2": {"$gt": "a"}}, order_by="k1")
    assert res["total"] == 6
    res = res["results"]
    assert len(res) == 6
    res = search.query("Lorem", where={"k2": {"$lt": "a"}}, order_by="k1")
    assert res["total"] == 0
    res = res["results"]
    assert len(res) == 0


def test_query_where_num(postgres_service, search_engine):
    search = search_engine
    search.add(["Lorem"], metadatas=[{"k1": 1, "k2": 3}], ids=["i1"])
    search.add(["Lorem"], metadatas=[{"k1": 2, "k2": 3}], ids=["i2"])
    search.add(["Lorem"], metadatas=[{"k1": 3, "k2": 3}], ids=["i3"])
    search.add(["Lorem"], metadatas=[{"k1": 4, "k2": 2}], ids=["i4"])
    search.add(["Lorem"], metadatas=[{"k1": 5, "k2": 2}], ids=["i5"])
    search.add(["Lorem"], metadatas=[{"k1": 6, "k2": 2}], ids=["i6"])
    search.add(["Lorem"], metadatas=[{"k1": 7, "k2": 1}], ids=["i7"])
    search.add(["Lorem"], metadatas=[{"k1": 8, "k2": 1}], ids=["i8"])
    search.add(["Lorem"], metadatas=[{"k1": 9, "k2": 1}], ids=["i9"])
    search.add(["Lorem"], ids=["i0"])
    res = search.query("Lorem", where={"k2": 1}, order_by="k1")
    assert res["total"] == 3
    res = res["results"]
    assert len(res) == 3
    res = search.query("Lorem", where={"k2": {"$eq": 1}}, order_by="k1")
    assert res["total"] == 3
    res = res["results"]
    assert len(res) == 3
    res = search.query("Lorem", where={"k2": {"$gt": 1}}, order_by="k1")
    assert res["total"] == 6
    res = res["results"]
    assert len(res) == 6
    res = search.query("Lorem", where={"k2": {"$lt": 1}}, order_by="k1")
    assert res["total"] == 0
    res = res["results"]
    assert len(res) == 0


def test_query_where_in(postgres_service, search_engine):
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
    with pytest.raises(ValueError):
        # wrong operator
        search.query("Lorem", where={"k1": {"in": "a"}})
    res = search.query(
        "Lorem", where={"k1": {"$in": ["a", "b", "c", "d"]}}, order_by="k1"
    )
    assert res["total"] == 4
    res = res["results"]
    assert len(res) == 4
    assert [r["id"][1:] for r in res] == list("1234")
    res = search.query(
        "Lorem", where={"k1": {"$nin": ["a", "b", "c", "d"]}}, order_by="k1"
    )
    assert res["total"] == 5
    res = res["results"]
    assert len(res) == 5
    assert [r["id"][1:] for r in res] == list("56789")


def test_all_docs(postgres_service, search_engine):
    search = search_engine
    search.add(["Lorem ipsum dolor"])
    search.add(["sit amet"])
    res = search.get()
    assert len(res["results"]) == 2
    assert res["total"] == 2


def test_instantiate_twice(postgres_service, search_engine):
    search = search_engine
    search.add(["Lorem ipsum dolor"])
    res = search.get()
    assert len(res["results"]) == 1
    search = CollectionPostgreSQL(dsn=TEST_DB_DSN, name="my_name")
    res = search.get()
    assert len(res["results"]) == 1


def test_vector(postgres_service, search_engine):
    vectors = {"Lorem ipsum dolor": [0.0, 0.0, 0.0], "sit amet": [0, 0.5, 0]}

    def f(documents):
        return [vectors[doc] for doc in documents]

    search = CollectionPostgreSQL(dsn=TEST_DB_DSN, name="vector", embedding_function=f)
    search.add(["Lorem ipsum dolor", "sit amet"])
    with search.conn() as conn:
        conn.execute("SELECT embedding FROM documents")
        vectors = conn.fetchall()
        vectors = [v[0] for v in vectors]
        assert len(vectors) == 2
        assert isinstance(vectors[0], str)
        assert vectors[0] == "[0,0,0]"
        assert vectors[1] == "[0,0.5,0]"


def test_vector_query(postgres_service, search_engine):
    vectors = {
        "Lorem ipsum dolor": [1, 1, 1],
        "sit amet": [1, -1, 1],
        "consectetur": [-1, -1, 1],
        "adipiscing": [-1, -1, -1],
    }

    def f(documents):
        return [vectors[doc] for doc in documents]

    search = CollectionPostgreSQL(dsn=TEST_DB_DSN, name="vector", embedding_function=f)
    search.add(["Lorem ipsum dolor", "sit amet"])
    res = search.query("consectetur", vector_search=True)
    assert res["total"] == 2
    assert res["results"][0]["content"] == "sit amet"
    assert res["results"][0]["rank"] == pytest.approx(1 / 3)
    assert res["results"][1]["content"] == "Lorem ipsum dolor"
    assert res["results"][1]["rank"] == pytest.approx(-1 / 3)
    # limit & offset
    res = search.query("consectetur", vector_search=True, offset=0, limit=1)
    assert res["total"] == 2
    assert len(res["results"]) == 1
    assert res["results"][0]["content"] == "sit amet"
    res = search.query("consectetur", vector_search=True, offset=1, limit=1)
    assert res["total"] == 2
    assert len(res["results"]) == 1
    assert res["results"][0]["content"] == "Lorem ipsum dolor"
    res = search.query("consectetur", vector_search=True, offset=2)
    assert res["total"] == 0
    assert len(res["results"]) == 0


def test_vector_query_fts(postgres_service, search_engine):
    vectors = {
        "Lorem ipsum dolor": [1, 1, 1],
        "sit amet": [1, -1, 1],
        "consectetur": [-1, -1, 1],
        "adipiscing": [-1, -1, -1],
    }

    def f(documents):
        return [vectors[doc] for doc in documents]

    search = CollectionPostgreSQL(dsn=TEST_DB_DSN, name="vector", embedding_function=f)
    search.add(["Lorem ipsum dolor", "sit amet"])
    res = search.query("Lorem", vector_search=False)
    assert res["total"] == 1
    assert res["results"][0]["content"] == "Lorem ipsum dolor"


def test_vector_update(postgres_service, search_engine):
    vectors = {
        "Lorem ipsum dolor": [1, 1, 1],
        "sit amet": [1, -1, 1],
        "consectetur": [-1, -1, 1],
        "adipiscing": [-1, -1, -1],
    }

    def f(documents):
        return [vectors[doc] for doc in documents]

    search = CollectionPostgreSQL(dsn=TEST_DB_DSN, name="vector", embedding_function=f)
    ids = search.add(["Lorem ipsum dolor", "sit amet"])
    res = search.query("Lorem", vector_search=False)
    assert res["total"] == 1
    res = search.query("consectetur", vector_search=False)
    assert res["total"] == 0
    res = search.query("consectetur", vector_search=True)
    assert res["total"] == 2
    assert res["results"][0]["content"] == "sit amet"
    assert res["results"][0]["content"] == "sit amet"
    assert res["results"][0]["rank"] == pytest.approx(1 / 3)
    assert res["results"][0]["id"] == ids[1]
    assert res["results"][1]["content"] == "Lorem ipsum dolor"
    assert res["results"][1]["rank"] == pytest.approx(-1 / 3)
    assert res["results"][1]["id"] == ids[0]
    # update: switch order
    search.update(ids=ids, contents=["sit amet", "Lorem ipsum dolor"])
    res = search.query("Lorem", vector_search=False)
    assert res["total"] == 1
    res = search.query("consectetur", vector_search=False)
    assert res["total"] == 0
    res = search.query("consectetur", vector_search=True)
    assert res["total"] == 2
    assert res["results"][0]["content"] == "sit amet"
    assert res["results"][0]["content"] == "sit amet"
    assert res["results"][0]["rank"] == pytest.approx(1 / 3)
    assert res["results"][0]["id"] == ids[0]
    assert res["results"][1]["content"] == "Lorem ipsum dolor"
    assert res["results"][1]["rank"] == pytest.approx(-1 / 3)
    assert res["results"][1]["id"] == ids[1]


def test_vector_update_nofts(postgres_service, search_engine):
    vectors = {
        "Lorem ipsum dolor": [1, 1, 1],
        "sit amet": [1, -1, 1],
        "consectetur": [-1, -1, 1],
        "adipiscing": [-1, -1, -1],
    }

    def f(documents):
        return [vectors[doc] for doc in documents]

    search = CollectionPostgreSQL(
        dsn=TEST_DB_DSN, name="vector", embedding_function=f, use_fts=False
    )
    ids = search.add(["Lorem ipsum dolor", "sit amet"])
    with pytest.raises(ValueError):
        res = search.query("Lorem", vector_search=False)
    res = search.query("consectetur", vector_search=True)
    assert res["total"] == 2
    assert res["results"][0]["content"] == "sit amet"
    assert res["results"][0]["content"] == "sit amet"
    assert res["results"][0]["rank"] == pytest.approx(1 / 3)
    assert res["results"][0]["id"] == ids[1]
    assert res["results"][1]["content"] == "Lorem ipsum dolor"
    assert res["results"][1]["rank"] == pytest.approx(-1 / 3)
    assert res["results"][1]["id"] == ids[0]
    # update: switch order
    search.update(ids=ids, contents=["sit amet", "Lorem ipsum dolor"])
    res = search.query("consectetur", vector_search=True)
    assert res["total"] == 2
    assert res["results"][0]["content"] == "sit amet"
    assert res["results"][0]["content"] == "sit amet"
    assert res["results"][0]["rank"] == pytest.approx(1 / 3)
    assert res["results"][0]["id"] == ids[0]
    assert res["results"][1]["content"] == "Lorem ipsum dolor"
    assert res["results"][1]["rank"] == pytest.approx(-1 / 3)
    assert res["results"][1]["id"] == ids[1]
