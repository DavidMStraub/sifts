"""Unit tests."""

import os
import sqlite3
import threading

import numpy as np
import pytest
from sifts.core import CollectionSQLite


def test_init(tmp_path):
    path = tmp_path / "search_engine.db"
    CollectionSQLite(path, name="123")
    assert os.path.isfile(path)
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='documents';"
    )
    assert cursor.fetchone() is not None
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='documents_fts';"
    )
    assert cursor.fetchone() is not None


def test_collection_names(tmp_path):
    path = tmp_path / "search_engine.db"
    with pytest.raises(ValueError):
        CollectionSQLite(path, name="1 2")
    with pytest.raises(ValueError):
        CollectionSQLite(path, name=" abc")
    CollectionSQLite(path, name="1+2")
    CollectionSQLite(path, name="1-2")
    CollectionSQLite(path, name="ab/c")
    CollectionSQLite(path, name="abc")


def test_add(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    assert search.query("Lorem") == {"results": [], "total": 0}
    # assert search.count() == 0
    search.count()
    ids1 = search.add(["Lorem ipsum dolor"])
    ids2 = search.add(["sit amet"])
    assert len(search.query("Lorem")["results"]) == 1
    assert search.query("Lorem")["results"][0]["id"] == ids1[0]
    assert len(search.query("am*")["results"]) == 1
    assert search.query("am*")["results"][0]["id"] == ids2[0]
    assert len(search.query("Lorem or amet")["results"]) == 2
    # assert search.count() == 2
    search.count()


def test_query_multiple(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["Lorem ipsum dolor"])
    search.add(["sit amet"])
    assert len(search.query("Lorem ipsum")["results"]) == 1
    assert len(search.query("sit amet")["results"]) == 1
    assert len(search.query("Lorem sit")["results"]) == 0


def test_add_name(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="my_name")
    assert search.query("Lorem")["results"] == []
    search.add(["Lorem ipsum dolor"])
    assert len(search.query("Lorem")["results"]) == 1
    search = CollectionSQLite(path, name="123")
    assert len(search.query("Lorem")["results"]) == 0
    search = CollectionSQLite(path, name="my_name")
    assert len(search.query("Lorem")["results"]) == 1


def test_add_id(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    ids = search.add(["x"])
    assert len(ids) == 1
    assert len(ids[0]) == 36  # is UUIDv4
    ids = search.add(["y"], ids=["my_id"])
    assert ids == ["my_id"]
    res = search.query("y")
    assert len(res["results"]) == 1
    res = res["results"]
    assert res[0]["id"] == "my_id"
    # does not raise, but updates
    search.add(["z"], ids=["my_id"])
    res = search.query("y")
    assert len(res["results"]) == 0
    res = search.query("z")
    assert len(res["results"]) == 1


def test_update(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    ids = search.add(["Lorem ipsum"])
    res = search.query("Lorem")
    res = res["results"]
    assert len(res) == 1
    assert res[0]["id"] == ids[0]
    search.update(ids=ids, contents=["dolor sit"])
    res = search.query("Lorem")
    assert len(res["results"]) == 0
    res = search.query("sit")
    res = res["results"]
    assert len(res) == 1
    assert res[0]["id"] == ids[0]


def test_delete(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.count()
    ids = search.add(["Lorem ipsum", "Lorem dolor"])
    res = search.query("Lorem")
    assert len(res["results"]) == 2
    search.count()
    search.delete(ids)
    res = search.query("Lorem")
    assert len(res["results"]) == 0
    search.count()
    search.delete(ids)


def test_query_metadata(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["Lorem ipsum dolor"], metadatas=[{"foo": "bar"}])
    search.add(["sit amet"])
    res = search.query("Lorem")
    res = res["results"]
    assert len(res) == 1
    assert res[0]["metadata"] == {"foo": "bar"}
    res = search.query("sit")
    assert res["total"] == 1
    res = res["results"]
    assert len(res) == 1
    assert res[0]["metadata"] is None


def test_query_metadata_filter_and_order_with_dqs_disabled(tmp_path, monkeypatch):
    if (
        not hasattr(sqlite3.Connection, "setconfig")
        or not hasattr(sqlite3, "SQLITE_DBCONFIG_DQS_DML")
        or not hasattr(sqlite3, "SQLITE_DBCONFIG_DQS_DDL")
    ):
        pytest.skip("SQLite DQS runtime config not available")

    def disable_dqs(conn):
        conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DML, 0)
        conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DDL, 0)

    probe = sqlite3.connect(":memory:")
    try:
        disable_dqs(probe)
    except sqlite3.Error:
        pytest.skip("SQLite DQS runtime config not supported")
    finally:
        probe.close()

    original_connect = sqlite3.connect
    dqs_settings = []

    def connect_with_dqs_disabled(*args, **kwargs):
        conn = original_connect(*args, **kwargs)
        disable_dqs(conn)
        if hasattr(conn, "getconfig"):
            dqs_settings.append(
                (
                    conn.getconfig(sqlite3.SQLITE_DBCONFIG_DQS_DML),
                    conn.getconfig(sqlite3.SQLITE_DBCONFIG_DQS_DDL),
                )
            )
        return conn

    monkeypatch.setattr("sifts.core.sqlite3.connect", connect_with_dqs_disabled)

    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["Lorem"], metadatas=[{"k1": "a", "k2": "b"}], ids=["i1"])
    search.add(["Lorem"], metadatas=[{"k1": "b", "k2": "a"}], ids=["i2"])
    search.add(["Lorem"], metadatas=[{"k1": "c", "k2": "a"}], ids=["i3"])

    if dqs_settings:
        assert all(setting == (0, 0) for setting in dqs_settings)

    res = search.query("Lorem", where={"k2": "a"}, order_by="k1")["results"]
    assert [r["id"] for r in res] == ["i2", "i3"]

    res = search.query(
        "Lorem", where={"k1": {"$in": ["b", "c"]}}, order_by="k1"
    )["results"]
    assert [r["id"] for r in res] == ["i2", "i3"]

    res = search.query(
        "Lorem", where={"k1": {"$nin": ["a"]}}, order_by="k1"
    )["results"]
    assert [r["id"] for r in res] == ["i2", "i3"]


def test_query_order(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
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
    assert res["total"] == 10
    assert len(res["results"]) == 10
    # k1
    res = search.query("Lorem", order_by="k1")
    assert res["total"] == 10
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


def test_query_limit_offset(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
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
    assert res["total"] == 10
    assert len(res["results"]) == 10
    res = search.query("Lorem", order_by="k1", limit=0)
    assert res["total"] == 10
    assert len(res["results"]) == 10
    res = search.query("Lorem", order_by="k1", limit=3)
    assert res["total"] == 10
    res = res["results"]
    assert len(res) == 3
    assert [r["id"][1:] for r in res] == list("123")
    res = search.query("Lorem", order_by="k1", limit=3, offset=3)
    assert res["total"] == 10
    res = res["results"]
    assert len(res) == 3
    assert [r["id"][1:] for r in res] == list("456")
    res = search.query("Lorem", order_by="k1", limit=3, offset=8)
    assert res["total"] == 10
    res = res["results"]
    assert len(res) == 2
    assert [r["id"][1:] for r in res] == list("90")


def test_query_where(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
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
    assert res["total"] == 3
    res = res["results"]
    assert len(res) == 3
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


def test_query_where_num(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
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


def test_query_where_in(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
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


def test_all_docs(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["Lorem ipsum dolor"])
    search.add(["sit amet"])
    res = search.get()
    assert len(res["results"]) == 2
    assert res["total"] == 2


def test_vector_add(tmp_path):
    path = tmp_path / "search_engine.db"
    vectors = {"Lorem ipsum dolor": [0, 0, 0], "sit amet": [0, 0.5, 0]}

    def f(documents):
        return [vectors[doc] for doc in documents]

    search = CollectionSQLite(path, name="vector", embedding_function=f)
    search.add(["Lorem ipsum dolor", "sit amet"])
    with search.conn() as conn:
        res = conn.execute("SELECT embedding FROM documents")
        vectors = res.fetchall()
        vectors = [np.frombuffer(v[0], dtype=np.float32) for v in vectors]
        assert len(vectors) == 2
        assert isinstance(vectors[0], np.ndarray)
        assert vectors[0].tolist() == [0, 0, 0]
        assert vectors[1].tolist() == [0, 0.5, 0]


def test_vector_query(tmp_path):
    path = tmp_path / "search_engine.db"
    vectors = {
        "Lorem ipsum dolor": [1, 1, 1],
        "sit amet": [1, -1, 1],
        "consectetur": [-1, -1, 1],
        "adipiscing": [-1, -1, -1],
    }

    def f(documents):
        return [vectors[doc] for doc in documents]

    search = CollectionSQLite(path, name="vector", embedding_function=f)
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
    assert res["total"] == 2
    assert len(res["results"]) == 0


def test_vector_query_fts(tmp_path):
    path = tmp_path / "search_engine.db"
    vectors = {
        "Lorem ipsum dolor": [1, 1, 1],
        "sit amet": [1, -1, 1],
        "consectetur": [-1, -1, 1],
        "adipiscing": [-1, -1, -1],
    }

    def f(documents):
        return [vectors[doc] for doc in documents]

    search = CollectionSQLite(path, name="vector", embedding_function=f)
    search.add(["Lorem ipsum dolor", "sit amet"])
    res = search.query("Lorem", vector_search=False)
    assert res["total"] == 1
    assert res["results"][0]["content"] == "Lorem ipsum dolor"


def test_vector_update(tmp_path):
    path = tmp_path / "search_engine.db"
    vectors = {
        "Lorem ipsum dolor": [1, 1, 1],
        "sit amet": [1, -1, 1],
        "consectetur": [-1, -1, 1],
        "adipiscing": [-1, -1, -1],
    }

    def f(documents):
        return [vectors[doc] for doc in documents]

    search = CollectionSQLite(path, name="vector", embedding_function=f)
    ids = search.add(["Lorem ipsum dolor", "sit amet"])
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


def test_vector_update_nofts(tmp_path):
    path = tmp_path / "search_engine.db"
    vectors = {
        "Lorem ipsum dolor": [1, 1, 1],
        "sit amet": [1, -1, 1],
        "consectetur": [-1, -1, 1],
        "adipiscing": [-1, -1, -1],
    }

    def f(documents):
        return [vectors[doc] for doc in documents]

    search = CollectionSQLite(path, name="vector", embedding_function=f, use_fts=False)
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


def test_query_hyphenated_word(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["This is a test-word example"])
    search.add(["Another document without it"])
    res = search.query("test-word")
    assert res["total"] == 1
    assert len(res["results"]) == 1
    assert res["results"][0]["content"] == "This is a test-word example"


def test_query_hyphenated_wildcard(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["This is a test-word example"])
    search.add(["This is a test-word-extended version"])
    search.add(["Another document without it"])
    res = search.query("test-word*")
    assert res["total"] == 2
    assert len(res["results"]) == 2


def test_query_wildcard_end(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["testing document"])
    search.add(["tester document"])
    search.add(["tests document"])
    search.add(["another document"])
    res = search.query("test*")
    assert res["total"] == 3
    assert len(res["results"]) == 3


def test_query_wildcard_middle(tmp_path):
    # Note: SQLite FTS5 does not support wildcards in the middle of words
    # Only trailing wildcards are supported
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["testing document"])
    search.add(["tester document"])
    search.add(["another document"])
    res = search.query("te*ing")
    # This won't match anything because mid-word wildcards aren't supported
    assert res["total"] == 0


def test_query_wildcard_beginning(tmp_path):
    # Note: SQLite FTS5 does not support leading wildcards
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["testing document"])
    search.add(["interesting document"])
    search.add(["another document"])
    res = search.query("*sting")
    # This won't match anything because leading wildcards aren't supported
    assert res["total"] == 0


def test_query_apostrophe(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["It's a test document"])
    search.add(["Another document"])
    res = search.query("it's")
    assert res["total"] == 1
    assert res["results"][0]["content"] == "It's a test document"


def test_query_comma_special_char(tmp_path):
    """Integration test: Search for text containing comma (FTS5 special char)"""
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["Bydgoszcz, Poland is a city"])
    search.add(["Another document without special chars"])
    # This should not raise FTS5 syntax error and should find the document
    res = search.query("Bydgoszcz, Poland")
    assert res["total"] == 1
    assert "Bydgoszcz, Poland" in res["results"][0]["content"]


def test_query_colon_special_char(tmp_path):
    """Integration test: Search for text containing colon (FTS5 special char)"""
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["The time:12:00 format is common"])
    search.add(["Regular document"])
    # Should not raise FTS5 syntax error
    res = search.query("time:12:00")
    assert res["total"] == 1
    assert "time:12:00" in res["results"][0]["content"]


def test_query_parentheses_special_char(tmp_path):
    """Integration test: Search for text with parentheses (FTS5 special char)"""
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["This is test (example) text"])
    search.add(["Regular document"])
    # Should not raise FTS5 syntax error
    res = search.query("test (example)")
    assert res["total"] == 1
    assert "test (example)" in res["results"][0]["content"]


def test_query_brackets_special_char(tmp_path):
    """Integration test: Search for text with brackets (FTS5 special char)"""
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["Array access test[0] example"])
    search.add(["Regular document"])
    # Should not raise FTS5 syntax error
    res = search.query("test[0]")
    assert res["total"] == 1
    assert "test[0]" in res["results"][0]["content"]


def test_query_curly_braces_special_char(tmp_path):
    """Integration test: Search for text with curly braces (FTS5 special char)"""
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["Template {value} syntax"])
    search.add(["Regular document"])
    # Should not raise FTS5 syntax error
    res = search.query("template{value}")
    assert res["total"] == 1
    assert "{value}" in res["results"][0]["content"]


def test_query_colon_wildcard_special_char(tmp_path):
    """Integration test: Special char with wildcard"""
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["User profile user:john123"])
    search.add(["User profile user:jane456"])
    search.add(["Regular document"])
    # Should find both user: documents
    res = search.query("user:*")
    assert res["total"] == 2


def test_query_multiple_special_chars(tmp_path):
    """Integration test: Multiple special char types in one query"""
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["Complex: (test) with {data}, and [values]"])
    search.add(["Regular document"])
    # Should handle multiple special characters correctly
    res = search.query("(test) and {data}")
    assert res["total"] == 1
    assert "(test)" in res["results"][0]["content"]
    assert "{data}" in res["results"][0]["content"]


@pytest.mark.parametrize(
    "query, expected",
    [
        ("Lübbenau/Spreewald", 1),
        ("Lübbenau/Spree*", 1),
        ("C:\\Users\\test", 1),
        ("1.2.1900", 1),
        ("mail@example.org", 1),
        ("-foo", 1),
        ("+foo", 1),
        ("foo.", 1),
        ("/", 0),
    ],
)
def test_query_non_bareword_chars(tmp_path, query, expected):
    """Any ASCII punctuation outside an FTS5 bareword must not cause a syntax error."""
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(
        [
            "Lübbenau/Spreewald",
            "C:\\Users\\test",
            "born 1.2.1900",
            "mail@example.org",
            "foo",
        ]
    )
    assert search.query(query)["total"] == expected


@pytest.mark.parametrize("operation", ["delete", "delete_all"])
def test_write_waits_for_concurrent_writer(tmp_path, operation):
    """A write must wait for another process's write lock, not fail at once.

    Regression test: ``delete`` and ``delete_all`` start with a statement on
    the FTS5 virtual table.  Preparing it makes FTS5 read its ``*_config``
    shadow table, which under a deferred ``BEGIN`` opens a read transaction
    before the write lock is requested.  SQLite then refuses to invoke the
    busy handler (deadlock avoidance) and raises ``database is locked``
    immediately, even though the other writer only holds the lock briefly.
    """
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["Lorem ipsum", "dolor sit"], ids=["a", "b"])

    # Another process (e.g. a background reindex worker) holds the write
    # lock for a moment while we write.
    other = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    other.execute("BEGIN IMMEDIATE")
    other.execute("UPDATE documents SET metadata = NULL WHERE id = 'b'")
    release = threading.Timer(0.3, other.commit)
    release.start()
    try:
        if operation == "delete":
            search.delete(["a"])
            assert [r["id"] for r in search.get()["results"]] == ["b"]
        else:
            search.delete_all()
            assert search.get()["total"] == 0
    finally:
        release.join()
        other.close()


def test_embeddings_computed_outside_write_transaction(tmp_path):
    """Embedding functions can be slow (model inference, remote API calls);
    running them while holding the database write lock blocks every other
    writer for that long, so they must run before the transaction begins."""
    path = tmp_path / "search_engine.db"

    def embed(contents):
        # Fails with "database is locked" if add() already holds the write lock.
        probe = sqlite3.connect(path, timeout=0, isolation_level=None)
        try:
            probe.execute("BEGIN IMMEDIATE")
            probe.rollback()
        finally:
            probe.close()
        return [np.ones(4) for _ in contents]

    search = CollectionSQLite(path, name="123", embedding_function=embed)
    search.add(["Lorem ipsum", "dolor sit"], ids=["a", "b"])
    assert search.query("Lorem ipsum", vector_search=True)["total"] == 2


def test_vector_query_embedding_function(tmp_path):
    path = tmp_path / "search_engine.db"
    doc_vectors = {"Lorem ipsum dolor": [1, 1, 1], "sit amet": [1, -1, 1]}
    query_vectors = {"consectetur": [-1, -1, 1]}
    embedded_docs = []

    def f_docs(documents):
        embedded_docs.extend(documents)
        return [doc_vectors[doc] for doc in documents]

    def f_query(queries):
        return [query_vectors[q] for q in queries]

    search = CollectionSQLite(
        path,
        name="vector",
        embedding_function=f_docs,
        query_embedding_function=f_query,
    )
    search.add(["Lorem ipsum dolor", "sit amet"])
    res = search.query("consectetur", vector_search=True)
    assert embedded_docs == ["Lorem ipsum dolor", "sit amet"]
    assert res["total"] == 2
    assert res["results"][0]["content"] == "sit amet"
    assert res["results"][0]["rank"] == pytest.approx(1 / 3)
    assert res["results"][1]["content"] == "Lorem ipsum dolor"
    assert res["results"][1]["rank"] == pytest.approx(-1 / 3)


def test_vector_add_precomputed_embeddings(tmp_path):
    path = tmp_path / "search_engine.db"

    def f(documents):
        vectors = {"consectetur": [-1, -1, 1]}
        return [vectors[doc] for doc in documents]

    search = CollectionSQLite(path, name="vector", embedding_function=f)
    ids = search.add(
        ["Lorem ipsum dolor", "sit amet"],
        embeddings=[[1, 1, 1], [1, -1, 1]],
    )
    res = search.query("consectetur", vector_search=True)
    assert res["results"][0]["id"] == ids[1]
    assert res["results"][0]["rank"] == pytest.approx(1 / 3)
    # update with precomputed embeddings: switch order
    search.update(
        ids=ids,
        contents=["Lorem ipsum dolor", "sit amet"],
        embeddings=np.array([[1, -1, 1], [1, 1, 1]]),
    )
    res = search.query("consectetur", vector_search=True)
    assert res["results"][0]["id"] == ids[0]
    assert res["results"][0]["rank"] == pytest.approx(1 / 3)
    with pytest.raises(ValueError):
        search.add(["Lorem ipsum dolor"], embeddings=[[1, 1, 1], [1, -1, 1]])


def test_precomputed_embeddings_without_embedding_function(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="vector")
    with pytest.raises(ValueError):
        search.add(["Lorem ipsum dolor"], embeddings=[[1, 1, 1]])


def _fts_ids(path):
    conn = sqlite3.connect(path)
    try:
        return sorted(row[0] for row in conn.execute("SELECT id FROM documents_fts"))
    finally:
        conn.close()


def test_update_numeric_looking_ids(tmp_path):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["Lorem", "ipsum", "dolor"], ids=["7", "007", "7.0"])
    search.update(ids=["7"], contents=["sit"])
    assert _fts_ids(path) == ["007", "7", "7.0"]
    assert search.query("ipsum")["results"][0]["id"] == "007"
    assert search.query("dolor")["results"][0]["id"] == "7.0"
    assert search.query("sit")["results"][0]["id"] == "7"
    assert search.query("Lorem")["total"] == 0


@pytest.mark.parametrize(
    "ids",
    [
        ["a-b", "a b", "a_b", 'a"b'],
        ["---", "***", "Ünï", "ünï"],
        ["OR", "NEAR", "a*", "^a"],
    ],
)
def test_update_delete_ids_special_chars(tmp_path, ids):
    path = tmp_path / "search_engine.db"
    search = CollectionSQLite(path, name="123")
    search.add(["Lorem"] * len(ids), ids=ids)
    for i, did in enumerate(ids):
        search.update(ids=[did], contents=[f"ipsum{i}"])
        assert _fts_ids(path) == sorted(ids)
        assert search.query(f"ipsum{i}")["results"][0]["id"] == did
    assert search.query("Lorem")["total"] == 0
    for i, did in enumerate(ids):
        search.delete([did])
        assert _fts_ids(path) == sorted(ids[i + 1 :])
        assert search.query(f"ipsum{i}")["total"] == 0
