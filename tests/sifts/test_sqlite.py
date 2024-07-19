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
    with pytest.raises(sqlite3.IntegrityError):
        # ID must be unique
        search.add(["z"], ids=["my_id"])


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
    ids = search.add(["Lorem ipsum"])
    res = search.query("Lorem")
    assert len(res) == 1
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
