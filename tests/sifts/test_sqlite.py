"""Unit tests."""

import os
import sqlite3

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
