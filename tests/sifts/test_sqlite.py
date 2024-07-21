"""Unit tests."""

import os
import sqlite3

import pytest
from sifts.core import SearchEngineSQLite


def test_init(tmp_path):
    path = tmp_path / "search_engine.db"
    SearchEngineSQLite(path)
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


def test_add(tmp_path):
    path = tmp_path / "search_engine.db"
    search = SearchEngineSQLite(path)
    assert search.query("Lorem") == []
    ids1 = search.add(["Lorem ipsum dolor"])
    ids2 = search.add(["sit amet"])
    assert len(search.query("Lorem")) == 1
    assert search.query("Lorem")[0]["id"] == ids1[0]
    assert len(search.query("am*")) == 1
    assert search.query("am*")[0]["id"] == ids2[0]
    assert len(search.query("Lorem or amet")) == 2


def test_query_multiple(tmp_path):
    path = tmp_path / "search_engine.db"
    search = SearchEngineSQLite(path)
    search.add(["Lorem ipsum dolor"])
    search.add(["sit amet"])
    assert len(search.query("Lorem ipsum")) == 1
    assert len(search.query("sit amet")) == 1
    assert len(search.query("Lorem sit")) == 0


def test_add_prefix(tmp_path):
    path = tmp_path / "search_engine.db"
    search = SearchEngineSQLite(path, prefix="my_prefix")
    assert search.query("Lorem") == []
    search.add(["Lorem ipsum dolor"])
    assert len(search.query("Lorem")) == 1
    search = SearchEngineSQLite(path)
    assert len(search.query("Lorem")) == 0
    search = SearchEngineSQLite(path, prefix="my_prefix")
    assert len(search.query("Lorem")) == 1


def test_add_id(tmp_path):
    path = tmp_path / "search_engine.db"
    search = SearchEngineSQLite(path)
    ids = search.add(["x"])
    assert len(ids) == 1
    assert len(ids[0]) == 36  # is UUIDv4
    ids = search.add(["y"], ids=["my_id"])
    assert ids == ["my_id"]
    res = search.query("y")
    assert len(res) == 1
    assert res[0]["id"] == "my_id"
    # does not raise, but updates
    search.add(["z"], ids=["my_id"])
    res = search.query("y")
    assert len(res) == 0
    res = search.query("z")
    assert len(res) == 1


def test_update(tmp_path):
    path = tmp_path / "search_engine.db"
    search = SearchEngineSQLite(path)
    ids = search.add(["Lorem ipsum"])
    res = search.query("Lorem")
    assert len(res) == 1
    assert res[0]["id"] == ids[0]
    search.update(ids=ids, contents=["dolor sit"])
    res = search.query("Lorem")
    assert len(res) == 0
    res = search.query("sit")
    assert len(res) == 1
    assert res[0]["id"] == ids[0]


def test_delete(tmp_path):
    path = tmp_path / "search_engine.db"
    search = SearchEngineSQLite(path)
    ids = search.add(["Lorem ipsum", "Lorem dolor"])
    res = search.query("Lorem")
    assert len(res) == 2
    search.delete(ids)
    res = search.query("Lorem")
    assert len(res) == 0
    search.delete(ids)


def test_query_metadata(tmp_path):
    path = tmp_path / "search_engine.db"
    search = SearchEngineSQLite(path)
    search.add(["Lorem ipsum dolor"], metadatas=[{"foo": "bar"}])
    search.add(["sit amet"])
    res = search.query("Lorem")
    assert len(res) == 1
    assert res[0]["metadata"] == {"foo": "bar"}
    res = search.query("sit")
    assert len(res) == 1
    assert res[0]["metadata"] is None


def test_query_order(tmp_path):
    path = tmp_path / "search_engine.db"
    search = SearchEngineSQLite(path)
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


def test_query_limit_offset(tmp_path):
    path = tmp_path / "search_engine.db"
    search = SearchEngineSQLite(path)
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


def test_query_where(tmp_path):
    path = tmp_path / "search_engine.db"
    search = SearchEngineSQLite(path)
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


def test_all_docs(tmp_path):
    path = tmp_path / "search_engine.db"
    search = SearchEngineSQLite(path)
    search.add(["Lorem ipsum dolor"])
    search.add(["sit amet"])
