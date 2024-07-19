"""Core classes for Sifts."""

from __future__ import annotations
import json
import re
import sqlite3
import uuid

import psycopg2
from urllib.parse import urlparse
from contextlib import contextmanager


def make_id():
    return str(uuid.uuid4())


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


class SearchEngineBase:

    IS_POSTGRES = False
    QUERY_CREATE_INDEX = ""
    QUERY_CREATE_DOCUMENT = ""
    QUERY_INSERT_DOC = ""
    QUERY_INSERT_INDEX = ""
    QUERY_UPDATE_DOC = ""
    QUERY_UPDATE_INDEX = ""
    QUERY_DELETE = ""
    QUERY_SEARCH = ""
    QUERY_FILTER_META = ""
    QUERY_ORDER_META = ""

    def __init__(self, prefix: str | None = None) -> None:
        self.prefix = prefix
        self.create_tables()

    @contextmanager
    def conn(self):
        """Provide a transactional scope around a series of operations."""
        raise NotImplementedError

    def create_tables(self) -> None:
        with self.conn() as conn:
            conn.execute(self.QUERY_CREATE_INDEX)
            if self.QUERY_CREATE_DOCUMENT:
                conn.execute(self.QUERY_CREATE_DOCUMENT)
            conn.execute("CREATE INDEX IF NOT EXISTS prefix_idx ON documents (prefix)")

    def add(
        self,
        contents: list[str],
        ids: list[str | None] | None = None,
        metadatas: list[dict[str, str] | None] | None = None,
    ) -> int:
        if ids is None:
            ids = [make_id() for _ in contents]
        else:
            ids = [i or make_id() for i in ids]
        if metadatas is None:
            metadatas = [None for _ in contents]
        else:
            metadatas = [json.dumps(m) if m else None for m in metadatas]
        prefixes = [self.prefix for _ in contents]
        with self.conn() as conn:
            conn.executemany(
                self.QUERY_INSERT_DOC, list(zip(contents, ids, metadatas, prefixes))
            )
            if self.IS_POSTGRES:
                conn.executemany(self.QUERY_INSERT_INDEX, [(did,) for did in ids])
            else:
                conn.executemany(self.QUERY_INSERT_INDEX, list(zip(contents, ids)))
        return ids

    def update(
        self,
        ids: list[str],
        contents: list[str],
        metadatas: list[dict[str, str] | None] | None = None,
    ) -> None:
        if metadatas is None:
            metadatas = [None for _ in contents]
        else:
            metadatas = [json.dumps(m) if m else None for m in metadatas]
        with self.conn() as conn:
            if self.IS_POSTGRES:
                conn.executemany(
                    self.QUERY_UPDATE_INDEX, list(zip(contents, contents, ids))
                )
            else:
                conn.executemany(self.QUERY_UPDATE_INDEX, list(zip(contents, ids)))
            if metadatas and any(metadatas):
                metadatas = [json.dumps(m) if m else None for m in metadatas]
                conn.executemany(
                    self.QUERY_UPDATE_DOC,
                    list(zip(metadatas, ids)),
                )

    def delete(self, ids: list[str]) -> None:
        with self.conn() as conn:
            conn.executemany(self.QUERY_DELETE_INDEX, (ids,))
            conn.executemany(self.QUERY_DELETE_DOC, (ids,))

    def query(
        self,
        query_string: str,
        limit: int = 0,
        where: dict | None = None,
        order_by: str | None = None,
        descending: bool = False,
    ) -> list:
        with self.conn() as conn:
            try:
                fts_query = self.QUERY_SEARCH
                if self.prefix is None:
                    fts_query += " AND prefix IS NULL"
                else:
                    fts_query += f" AND prefix = '{self.prefix}'"

                backend = "postgresql" if self.IS_POSTGRES else "sqlite"
                query_string = str(QueryParser(query_string, backend=backend))
                params = [query_string]

                if where:
                    for key, value in where.items():
                        fts_query += " AND " + self.QUERY_FILTER_META.format(key)
                        params.append(value)

                if limit:
                    fts_query += " LIMIT (?)"

                if order_by:
                    fts_query += " ORDER BY " + self.QUERY_ORDER_META.format(order_by)
                    if descending:
                        fts_query += " DESC"
                result = conn.execute(fts_query, params) or []
                if self.IS_POSTGRES:
                    result = conn.fetchall()
                result = [
                    {
                        "id": match[0],
                        "rank": match[1],
                        "content": match[2],
                        "metadata": json.loads(match[3] or "null"),
                    }
                    for match in result
                ]
            except (sqlite3.OperationalError, psycopg2.OperationalError):
                return []
        return result


class SearchEngineSQLite(SearchEngineBase):

    QUERY_CREATE_INDEX = (
        "CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(id, content)"
    )
    QUERY_CREATE_DOCUMENT = """CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            content TEXT,
            prefix TEXT,
            metadata JSON
        )"""
    QUERY_INSERT_INDEX = "INSERT INTO documents_fts (content, id) VALUES (?, ?)"
    QUERY_INSERT_DOC = (
        "INSERT INTO documents (content, id, metadata, prefix) VALUES (?, ?, ?, ?)"
    )
    QUERY_UPDATE_INDEX = "UPDATE documents_fts SET content = (?) WHERE id = (?)"
    QUERY_UPDATE_DOC = "UPDATE documents SET metadata = (?) WHERE id = (?)"
    QUERY_DELETE_INDEX = "DELETE FROM documents_fts WHERE id = (?)"
    QUERY_DELETE_DOC = "DELETE FROM documents WHERE id = (?)"
    QUERY_SEARCH = """SELECT doc.id, fts.rank, doc.content, doc.metadata
                FROM documents_fts fts
                JOIN documents doc ON doc.id = fts.id
                WHERE fts.content MATCH (?)
                """
    QUERY_FILTER_META = 'json_extract(doc.metadata, "$.{}") = (?)'
    QUERY_ORDER_META = 'json_extract(doc.metadata, "$.{}")'

    def __init__(self, db_path="search_engine.db", prefix: str | None = None) -> None:
        self.db_path = db_path
        super().__init__(prefix=prefix)

    @contextmanager
    def conn(self):
        """Provide a transactional scope around a series of operations."""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


class SearchEnginePostgreSQL(SearchEngineBase):

    IS_POSTGRES = True
    QUERY_CREATE_INDEX = """
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            content TEXT,
            prefix TEXT,
            metadata JSONB,
            tsvector TSVECTOR
        );
        CREATE INDEX IF NOT EXISTS documents_tsvector_idx ON documents USING GIN (tsvector);
        CREATE INDEX IF NOT EXISTS prefix_idx ON documents (prefix);
        """

    QUERY_CREATE_DOCUMENT = ""

    QUERY_INSERT_INDEX = (
        "UPDATE documents SET tsvector = to_tsvector(content) WHERE id = %s"
    )
    QUERY_INSERT_DOC = (
        "INSERT INTO documents (content, id, metadata, prefix) VALUES (%s, %s, %s, %s)"
    )

    QUERY_UPDATE_INDEX = "UPDATE documents SET content = %s, tsvector = to_tsvector('simple', %s) WHERE id = %s"
    QUERY_UPDATE_DOC = "UPDATE documents SET metadata = %s WHERE id = %s"

    QUERY_DELETE_INDEX = "UPDATE documents SET tsvector = NULL WHERE id = %s"
    QUERY_DELETE_DOC = "DELETE FROM documents WHERE id = %s"

    QUERY_SEARCH = """
    SELECT id, ts_rank(tsvector, query) AS rank, content, metadata
    FROM documents, to_tsquery('simple', %s) query
    WHERE tsvector @@ query
    """

    QUERY_FILTER_META = "metadata->>%s = %s"
    QUERY_ORDER_META = "metadata->>%s"

    def __init__(
        self,
        dsn,
        prefix: str | None = None,
    ) -> None:
        self.dsn = dsn
        super().__init__(prefix=prefix)

    @contextmanager
    def conn(self):
        """Provide a transactional scope around a series of operations."""
        conn = psycopg2.connect(dsn=self.dsn)
        try:
            yield conn.cursor()
            conn.commit()
        finally:
            conn.close()


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


def SearchEngine(db_url: str, prefix: str | None = None) -> SearchEngineBase:
    """Constructor for search engine instance."""
    if not db_url:
        return SearchEngineSQLite(prefix=prefix)
    if db_url.startswith("sqlite:///"):
        return SearchEngineSQLite(db_path=db_url[10:], prefix=prefix)
    return SearchEnginePostgreSQL(dsn=db_url_to_dsn(db_url))
