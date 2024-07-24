"""Core classes for Sifts."""

from __future__ import annotations
import json
import re
import sqlite3
import uuid
from typing import Any, TypedDict

import psycopg2
import psycopg2.extras
from urllib.parse import urlparse
from contextlib import contextmanager


def make_id():
    return str(uuid.uuid4())


class QueryResult(TypedDict):
    total: int
    results: list[dict[str, Any]]


class QueryParser:
    """Parser for search queries."""

    def __init__(self, query: str, backend: str = "sqlite") -> None:
        """Initialize given a query."""
        self.query = query.strip()
        self.backend = backend

    def _to_sqlite(self) -> str:
        query = self.query
        query = re.sub(r"\band\b", "AND", query, flags=re.IGNORECASE)
        query = re.sub(r"\bor\b", "OR", query, flags=re.IGNORECASE)
        return query

    def _to_pg(self) -> str:
        query = self.query

        operators = {"&", "|", "and", "or"}
        words = query.split()
        query_list = []
        i = 0
        while i < len(words):
            if words[i] in operators:
                query_list.append(words[i])
                i += 1
            else:
                query_list.append(words[i])
                if i + 1 < len(words) and words[i + 1] not in operators:
                    query_list.append("&")
                i += 1
        query = " ".join(query_list)
        query = re.sub(r"\band\b", "&", query, flags=re.IGNORECASE)
        query = re.sub(r"\bor\b", "|", query, flags=re.IGNORECASE)
        query = re.sub(r"\b(\w+)\*(?=\s|$|[^\w])", r"\1:*", query)
        return query

    def __str__(self) -> str:
        """Return the right string representation for the backend."""
        if self.backend == "sqlite":
            return self._to_sqlite()
        return self._to_pg()


class CollectionBase:

    IS_POSTGRES = False
    QUERY_CREATE_INDEX = ""
    QUERY_CREATE_DOC = ""
    QUERY_INSERT_DOC = ""
    QUERY_INSERT_INDEX = ""
    QUERY_SEARCH = ""
    QUERY_FILTER_META = ""
    QUERY_FILTER_META_IN = ""
    QUERY_FILTER_META_NOT_IN = ""
    QUERY_ORDER_META = ""
    QUERY_LIMIT = ""
    QUERY_OFFSET = ""
    QUERY_DELETE_INDEX = ""
    QUERY_DELETE_DOC = ""
    QUERY_SELECT = ""

    def __init__(self, name: str) -> None:
        if not name:
            raise ValueError("Collection name is required!")
        self.name = name
        self.create_tables()

    @contextmanager
    def conn(self):
        """Provide a transactional scope around a series of operations."""
        raise NotImplementedError

    def create_tables(self) -> None:
        with self.conn() as conn:
            conn.execute(self.QUERY_CREATE_INDEX)
            if self.QUERY_CREATE_DOC:
                conn.execute(self.QUERY_CREATE_DOC)
            conn.execute("CREATE INDEX IF NOT EXISTS name_idx ON documents (name)")

    def add(
        self,
        contents: list[str],
        ids: list[str | None] | None = None,
        metadatas: list[dict[str, str] | None] | None = None,
    ) -> list[str]:
        if ids is None:
            ids = [make_id() for _ in contents]
        else:
            ids = [i or make_id() for i in ids]
        if metadatas is None:
            metadatas = [None for _ in contents]
        else:
            metadatas = [json.dumps(m) if m else None for m in metadatas]
        namees = [self.name for _ in contents]
        return self._add(contents, ids, metadatas, namees)

    def _add(
        self,
        contents: list[str],
        ids: list[str | None],
        metadatas: list[str | None],
        namees: list[str | None],
    ) -> list[str]:
        raise NotImplementedError

    def update(
        self,
        ids: list[str],
        contents: list[str],
        metadatas: list[dict[str, str] | None] | None = None,
    ) -> list[str]:
        if ids is None or any([i is None for i in ids]):
            raise ValueError("ids must be specified for update")
        return self.add(contents=contents, ids=ids, metadatas=metadatas)

    def delete(self, ids: list[str]) -> None:
        with self.conn() as conn:
            conn.executemany(self.QUERY_DELETE_INDEX, [(did,) for did in ids])
            conn.executemany(self.QUERY_DELETE_DOC, [(did,) for did in ids])

    def query(
        self,
        query_string: str,
        limit: int = 0,
        offset: int = 0,
        where: dict | None = None,
        order_by: str | None = None,
    ) -> QueryResult:
        with self.conn() as conn:
            try:
                fts_query = self.QUERY_SEARCH
                fts_query += f" AND name = '{self.name}'"

                backend = "postgresql" if self.IS_POSTGRES else "sqlite"
                query_string = str(QueryParser(query_string, backend=backend))
                params = [query_string]

                if where:
                    for key, value in where.items():
                        if isinstance(value, dict):
                            if "$in" not in value and "$nin" not in value:
                                raise ValueError("Invalid where condition")
                            if "$in" in value:
                                values = [str(val) for val in value["$in"]]
                                placeholders = ",".join("?" for _ in values)
                                fts_query += " AND " + self.QUERY_FILTER_META_IN.format(
                                    key, placeholders
                                )
                            else:
                                values = [str(val) for val in value["$nin"]]
                                placeholders = ",".join("?" for _ in values)
                                fts_query += (
                                    " AND "
                                    + self.QUERY_FILTER_META_NOT_IN.format(
                                        key, placeholders
                                    )
                                )
                            params += values

                        else:
                            fts_query += " AND " + self.QUERY_FILTER_META.format(key)
                            params.append(value)

                if order_by:
                    fts_query += " ORDER BY "
                    if isinstance(order_by, str):
                        order_by = [order_by]
                    for field in order_by:
                        if field.startswith("-"):
                            descending = True
                        else:
                            descending = False
                        fts_query += self.QUERY_ORDER_META.format(field.lstrip("+-"))
                        if descending:
                            fts_query += " DESC NULLS FIRST"
                        else:
                            fts_query += " ASC NULLS LAST"
                        fts_query += ","
                    fts_query = fts_query.rstrip(",")  # remove last trailing comma

                if limit:
                    fts_query += self.QUERY_LIMIT
                    params.append(str(int(limit)))
                if offset:
                    fts_query += self.QUERY_OFFSET
                    params.append(str(int(offset)))
                result = conn.execute(fts_query, params) or []
                if self.IS_POSTGRES:
                    result = conn.fetchall()
                else:
                    result = list(result)
                if not result:
                    n_tot = 0
                else:
                    n_tot = result[0][4]

                result = [
                    {
                        "id": match[0],
                        "rank": match[1],
                        "content": match[2],
                        "metadata": (
                            match[3]
                            if self.IS_POSTGRES
                            else json.loads(match[3] or "null")
                        ),
                    }
                    for match in result
                ]
            except (sqlite3.OperationalError, psycopg2.OperationalError):
                return {"total": 0, "results": []}
        return {"total": n_tot, "results": result}

    def all_documents(self, content: bool = False):
        """Return all documents."""
        with self.conn() as conn:
            if content:
                query = self.QUERY_SELECT
            else:
                query = "SELECT id, metadata FROM documents"
            query += f" WHERE name = '{self.name}'"
            result = conn.execute(query)
            if self.IS_POSTGRES:
                result = conn.fetchall()

            result = [
                {
                    "id": match[0],
                    "metadata": (
                        match[1] if self.IS_POSTGRES else json.loads(match[1] or "null")
                    ),
                    "content": match[2] if len(match) > 2 else None,
                }
                for match in result
            ]
        return result

    def delete_all(self) -> None:
        """Delete all documents."""
        where = f"WHERE doc.name = '{self.name}'"
        with self.conn() as conn:
            if not self.IS_POSTGRES:
                conn.execute(
                    f"""
                    DELETE FROM documents_fts
                    WHERE id IN (
                        SELECT doc.id
                        FROM documents doc
                        {where}
                    );"""
                )
            conn.execute(f"DELETE FROM documents AS doc {where}")


class CollectionSQLite(CollectionBase):

    QUERY_CREATE_INDEX = (
        "CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(id, content)"
    )
    QUERY_CREATE_DOC = """
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            name TEXT,
            metadata JSON
        );
        """
    QUERY_INSERT_INDEX = "INSERT INTO documents_fts (content, id) VALUES (?, ?)"
    QUERY_INSERT_DOC = """INSERT INTO documents
            (id, metadata, name) VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                metadata = excluded.metadata,
                name = excluded.name
            """
    QUERY_DELETE_INDEX = "DELETE FROM documents_fts WHERE id = (?)"
    QUERY_DELETE_DOC = "DELETE FROM documents WHERE id = (?)"
    QUERY_SEARCH = """SELECT doc.id, fts.rank, fts.content, doc.metadata,
                count(*) OVER() AS full_count
                FROM documents_fts fts
                JOIN documents doc ON doc.id = fts.id
                WHERE fts.content MATCH (?)
                """
    QUERY_FILTER_META = 'json_extract(doc.metadata, "$.{}") = (?)'
    QUERY_FILTER_META_IN = 'json_extract(doc.metadata, "$.{}") IN ({})'
    QUERY_FILTER_META_NOT_IN = 'json_extract(doc.metadata, "$.{}") NOT IN ({})'
    QUERY_ORDER_META = 'json_extract(doc.metadata, "$.{}")'
    QUERY_LIMIT = " LIMIT (?)"
    QUERY_OFFSET = " OFFSET (?)"
    QUERY_SELECT = """
                    SELECT doc.id, doc.metadata, fts.content
                    FROM documents doc
                    INNER JOIN documents_fts fts
                    ON doc.id = fts.id
                """

    def __init__(self, db_path="search_engine.db", name: str | None = None) -> None:
        self.db_path = db_path
        super().__init__(name=name)

    @contextmanager
    def conn(self):
        """Provide a transactional scope around a series of operations."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("begin")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _add(
        self,
        contents: list[str],
        ids: list[str | None],
        metadatas: list[str | None],
        namees: list[str | None],
    ) -> list[str]:
        with self.conn() as conn:
            conn.executemany(self.QUERY_INSERT_DOC, list(zip(ids, metadatas, namees)))

            conn.execute("CREATE TEMPORARY TABLE temp_ids (id INTEGER)")
            conn.executemany(
                "INSERT INTO temp_ids (id) VALUES (?)", [(did,) for did in ids]
            )
            conn.execute(
                "DELETE FROM documents_fts WHERE id IN (SELECT id FROM temp_ids)"
            )
            conn.execute("DROP TABLE temp_ids")
            conn.executemany(self.QUERY_INSERT_INDEX, list(zip(contents, ids)))
        return ids


class CollectionPostgreSQL(CollectionBase):

    IS_POSTGRES = True
    QUERY_CREATE_INDEX = """
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            content TEXT,
            name TEXT,
            metadata JSONB,
            tsvector TSVECTOR
        );
        CREATE INDEX IF NOT EXISTS documents_tsvector_idx ON documents USING GIN (tsvector);
        CREATE INDEX IF NOT EXISTS name_idx ON documents (name);

        CREATE OR REPLACE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE
        ON documents FOR EACH ROW EXECUTE FUNCTION
        tsvector_update_trigger(tsvector, 'pg_catalog.simple', content);
        """
    QUERY_CREATE_DOC = ""
    QUERY_INSERT_INDEX = ""
    QUERY_INSERT_DOC = """INSERT INTO documents
        (content, id, metadata, name) VALUES %s
        ON CONFLICT(id) DO UPDATE SET
            content = EXCLUDED.content,
            metadata = EXCLUDED.metadata,
            name = EXCLUDED.name
    """

    QUERY_DELETE_INDEX = "UPDATE documents SET tsvector = NULL WHERE id = %s"
    QUERY_DELETE_DOC = "DELETE FROM documents WHERE id = %s"

    QUERY_SEARCH = """
    SELECT id, ts_rank(tsvector, query) AS rank, content, metadata,
    count(*) OVER() AS full_count
    FROM documents, to_tsquery('simple', %s) query
    WHERE tsvector @@ query
    """

    QUERY_FILTER_META = "metadata->>'{}' = %s"
    QUERY_FILTER_META_IN = "metadata->>'{}' IN ({})"
    QUERY_FILTER_META_NOT_IN = "metadata->>'{}' NOT IN ({})"
    QUERY_ORDER_META = "metadata->>'{}'"
    QUERY_LIMIT = " LIMIT %s"
    QUERY_OFFSET = " OFFSET %s"
    QUERY_SELECT = "SELECT id, metadata, content FROM documents"

    def __init__(
        self,
        dsn,
        name: str | None = None,
    ) -> None:
        self.dsn = dsn
        super().__init__(name=name)

    @contextmanager
    def conn(self):
        """Provide a transactional scope around a series of operations."""
        conn = psycopg2.connect(dsn=self.dsn)
        try:
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        finally:
            conn.close()

    def _add(
        self,
        contents: list[str],
        ids: list[str | None],
        metadatas: list[str | None],
        namees: list[str | None],
    ) -> list[str]:
        with self.conn() as conn:
            psycopg2.extras.execute_values(
                conn,
                self.QUERY_INSERT_DOC,
                list(zip(contents, ids, metadatas, namees)),
            )
        return ids


def db_url_to_dsn(db_url: str) -> str:
    """Convert a database URL to a DSN"""
    url = urlparse(db_url)
    dbname = url.path[1:]
    user = url.username
    password = url.password
    host = url.hostname
    port = url.port
    dsn = f"dbname={dbname} user={user} password={password} host={host} port={port}"
    return dsn


def Collection(db_url: str, name: str) -> CollectionBase:
    """Constructor for search engine instance."""
    if not db_url:
        return CollectionSQLite(name=name)
    if db_url.startswith("sqlite:///"):
        return CollectionSQLite(db_path=db_url[10:], name=name)
    return CollectionPostgreSQL(dsn=db_url_to_dsn(db_url), name=name)
