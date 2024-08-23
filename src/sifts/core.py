"""Core classes for Sifts."""

from __future__ import annotations
import json
import re
import sqlite3
import uuid
from typing import Any, Callable, TypedDict

import numpy as np
import psycopg2
import psycopg2.errors
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
            if words[i].lower() in operators:
                query_list.append(words[i])
                i += 1
            else:
                query_list.append(words[i])
                if i + 1 < len(words) and words[i + 1].lower() not in operators:
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
    QUERY_INSERT_INDEX = ""
    QUERY_SEARCH = ""
    QUERY_GET = ""
    QUERY_FILTER_META = ""
    QUERY_FILTER_META_FLOAT = ""
    QUERY_FILTER_META_IN = ""
    QUERY_FILTER_META_NOT_IN = ""
    QUERY_ORDER_META = ""
    QUERY_LIMIT = ""
    QUERY_OFFSET = ""
    QUERY_DELETE_INDEX = ""
    QUERY_DELETE_DOC = ""
    PLACEHOLDER = "(?)"

    def __init__(
        self,
        name: str,
        embedding_function: Callable | None = None,
        use_fts: bool = True,
    ) -> None:
        """Initialize collection given a name (cumpulsory)."""
        if not name:
            raise ValueError("Collection name is required!")
        if not re.fullmatch(r"[-a-zA-Z0-9_\\+~#=/]+", name):
            raise ValueError("Invalid collection name!")
        self.name = name
        self.embedding_function = embedding_function
        self.use_fts = use_fts
        self.create_tables()

    @contextmanager
    def conn(self):
        """Provide a transactional scope around a series of operations."""
        raise NotImplementedError

    def create_tables(self) -> None:
        """Create the database tables if they don't exist yet."""
        with self.conn() as conn:
            self._create_document_tables(conn)
            conn.execute("CREATE INDEX IF NOT EXISTS name_idx ON documents (name)")
        if self.embedding_function:
            with self.conn() as conn:
                self._create_embedding_column(conn)

    def _create_document_tables(self, conn) -> None:
        """Create the database tables if they don't exist yet."""
        raise NotImplementedError

    def _create_embedding_column(self, conn) -> None:
        """Create the embedding column if it doesn't exist yet."""
        raise NotImplementedError

    def count(self) -> int:
        """Return the number of items in the collection."""
        with self.conn() as conn:
            cursor = conn.execute(
                f"SELECT count(*) FROM documents WHERE name = {self.PLACEHOLDER}",
                (self.name,),
            )
            if self.IS_POSTGRES:
                result = conn.fetchone()
            else:
                result = cursor.fetchone()
            if not result:
                return 0
            return result[0]

    def add(
        self,
        contents: list[str],
        ids: list[str | None] | None = None,
        metadatas: list[dict[str, str] | None] | None = None,
    ) -> list[str]:
        """Add one or more documents to the collection."""
        if ids is None:
            ids = [make_id() for _ in contents]
        else:
            ids = [i or make_id() for i in ids]
        if metadatas is None:
            metadatas = [None for _ in contents]
        else:
            metadatas = [json.dumps(m) if m else None for m in metadatas]
        names = [self.name for _ in contents]
        ids = self._add(contents, ids, metadatas, names)
        return ids

    def _add(
        self,
        contents: list[str],
        ids: list[str | None],
        metadatas: list[str | None],
        names: list[str | None],
    ) -> list[str]:
        """Add one or more documents to the collection."""
        raise NotImplementedError

    def _format_vectors(self, vectors):
        """Format the vectors so they can be inserted in the table."""
        return [np.asarray(v, dtype=np.float32).tobytes() for v in vectors]

    def update(
        self,
        ids: list[str],
        contents: list[str],
        metadatas: list[dict[str, str] | None] | None = None,
    ) -> list[str]:
        """Update one or more documents."""
        if ids is None or any([i is None for i in ids]):
            raise ValueError("ids must be specified for update")
        return self.add(contents=contents, ids=ids, metadatas=metadatas)

    def delete(self, ids: list[str]) -> None:
        """Delete one or more documents."""
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
        vector_search: bool = False,
    ) -> QueryResult:
        """Query the collection."""
        if order_by and vector_search:
            raise ValueError("order_by is not allowed for vector search.")
        if vector_search and not self.embedding_function:
            raise ValueError("vector search not possible without embedding_function.")
        if query_string and not vector_search and not self.use_fts:
            raise ValueError("Full-text search not enabled for this collection.")
        with self.conn() as conn:
            try:
                params = []
                if query_string:
                    if vector_search:
                        vector = self.embedding_function([query_string])[0]
                        fts_query = self.QUERY_VECTOR_SEARCH
                        if self.IS_POSTGRES:
                            vector = self._format_vectors([vector])[0]
                            params += [vector]
                    else:
                        fts_query = self.QUERY_SEARCH
                        backend = "postgresql" if self.IS_POSTGRES else "sqlite"
                        query_string = str(QueryParser(query_string, backend=backend))
                        params += [query_string]

                else:
                    fts_query = self.QUERY_GET
                    params = []

                fts_query += f" AND name = '{self.name}'"

                if where:
                    for key, value in where.items():
                        if isinstance(value, dict):
                            if not set(value.keys()) & {
                                "$in",
                                "$nin",
                                "$gt",
                                "$lt",
                                "$gte",
                                "$lte",
                                "$eq",
                            }:
                                raise ValueError("Invalid where condition")
                            if "$in" in value:
                                values = [str(val) for val in value["$in"]]
                                placeholders = ",".join(
                                    "%s" if self.IS_POSTGRES else "?" for _ in values
                                )
                                fts_query += " AND " + self.QUERY_FILTER_META_IN.format(
                                    key, placeholders
                                )
                                params += values
                            if "$nin" in value:
                                values = [str(val) for val in value["$nin"]]
                                placeholders = ",".join(
                                    "%s" if self.IS_POSTGRES else "?" for _ in values
                                )
                                fts_query += (
                                    " AND "
                                    + self.QUERY_FILTER_META_NOT_IN.format(
                                        key, placeholders
                                    )
                                )
                                params += values

                            ops = {
                                "$gt": ">",
                                "$lt": "<",
                                "$gte": ">=",
                                "$lte": "<=",
                                "$eq": "=",
                            }
                            for op in value:
                                if op in ops:
                                    if isinstance(value[op], (float, int)):
                                        fts_query += (
                                            " AND "
                                            + self.QUERY_FILTER_META_FLOAT.format(
                                                key, ops[op]
                                            )
                                        )
                                        params.append(value[op])
                                    else:
                                        fts_query += (
                                            " AND "
                                            + self.QUERY_FILTER_META.format(
                                                key, ops[op]
                                            )
                                        )
                                        params.append(str(value[op]))

                        else:
                            if isinstance(value, (float, int)):
                                fts_query += (
                                    " AND "
                                    + self.QUERY_FILTER_META_FLOAT.format(key, "=")
                                )
                                params.append(value)
                            else:
                                fts_query += " AND " + self.QUERY_FILTER_META.format(
                                    key, "="
                                )
                                params.append(str(value))

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

                if vector_search and self.IS_POSTGRES:
                    fts_query += " ORDER BY embedding <=> %s"
                    params += [vector]

                if vector_search and not self.IS_POSTGRES:
                    # we can't directly apply limit and offset in SQLite vector search
                    # since we need to do it manually
                    pass
                else:
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
                    n_tot = result[0][0]

                result = [
                    {
                        "id": match[1],
                        "content": match[2],
                        "metadata": (
                            match[3]
                            if self.IS_POSTGRES
                            else json.loads(match[3] or "null")
                        ),
                        **({"rank": match[4]} if len(match) == 5 else {}),
                    }
                    for match in result
                ]
            except (sqlite3.OperationalError, psycopg2.OperationalError):
                raise
                return {"total": 0, "results": []}
            if vector_search and not self.IS_POSTGRES:
                result = self._order_result(result, vector, limit, offset)
        return {"total": n_tot, "results": result}

    def _order_result(self, result, vector, limit, offset):
        """Order the result by vector similarity."""
        return result

    def get(
        self,
        limit: int = 0,
        offset: int = 0,
        where: dict | None = None,
        order_by: str | None = None,
    ) -> QueryResult:
        """Get documents from the collection without searching."""
        return self.query(
            query_string="",
            limit=limit,
            offset=offset,
            where=where,
            order_by=order_by,
        )

    def delete_all(self) -> None:
        """Delete all documents."""
        where = f"WHERE doc.name = '{self.name}'"
        with self.conn() as conn:
            if self.use_fts and not self.IS_POSTGRES:
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

    QUERY_INSERT_INDEX = "INSERT INTO documents_fts (content, id) VALUES (?, ?)"
    QUERY_DELETE_INDEX = "DELETE FROM documents_fts WHERE id = (?)"
    QUERY_DELETE_DOC = "DELETE FROM documents WHERE id = (?)"
    QUERY_SEARCH = """SELECT count(*) OVER() AS full_count,
                doc.id, fts.content, doc.metadata,
                fts.rank
                FROM documents_fts fts
                JOIN documents doc ON doc.id = fts.id
                WHERE fts.content MATCH (?)
                """
    QUERY_VECTOR_SEARCH = """SELECT count(*) OVER() AS full_count,
                doc.id, doc.content, doc.metadata, doc.embedding
                FROM documents doc
                WHERE TRUE
                """
    QUERY_GET = """SELECT count(*) OVER() AS full_count,
                doc.id, fts.content, doc.metadata
                FROM documents_fts fts
                JOIN documents doc ON doc.id = fts.id
                WHERE TRUE
                """
    QUERY_FILTER_META = 'json_extract(doc.metadata, "$.{}") {} (?)'
    QUERY_FILTER_META_FLOAT = QUERY_FILTER_META
    QUERY_FILTER_META_IN = 'json_extract(doc.metadata, "$.{}") IN ({})'
    QUERY_FILTER_META_NOT_IN = 'json_extract(doc.metadata, "$.{}") NOT IN ({})'
    QUERY_ORDER_META = 'json_extract(doc.metadata, "$.{}")'
    QUERY_LIMIT = " LIMIT (?)"
    QUERY_OFFSET = " OFFSET (?)"

    def __init__(
        self,
        db_path="search_engine.db",
        name: str | None = None,
        embedding_function: Callable | None = None,
        use_fts: bool = True,
    ) -> None:
        self.db_path = db_path
        super().__init__(
            name=name, embedding_function=embedding_function, use_fts=use_fts
        )

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

    def _create_document_tables(self, conn) -> None:
        """Create the database tables if they don't exist yet."""
        if self.use_fts:
            conn.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(id, content)"
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                name TEXT,
                metadata JSON
            );
            """
        )
        columns = conn.execute("PRAGMA table_info(documents);")
        column_exists = any(column[1] == "content" for column in columns)
        if not column_exists:
            conn.execute("ALTER TABLE documents ADD COLUMN content TEXT;")

    def _create_embedding_column(self, conn) -> None:
        """Create the embedding column if it doesn't exist yet."""
        columns = conn.execute("PRAGMA table_info(documents);")
        column_exists = any(column[1] == "embedding" for column in columns)
        if not column_exists:
            conn.execute("ALTER TABLE documents ADD COLUMN embedding BLOB;")

    def _add(
        self,
        contents: list[str],
        ids: list[str | None],
        metadatas: list[str | None],
        names: list[str | None],
    ) -> list[str]:
        """Add one or more documents to the collection."""
        with self.conn() as conn:
            conn.executemany(
                """INSERT INTO documents
            (id, metadata, name, content) VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                metadata = excluded.metadata,
                name = excluded.name,
                content = excluded.content
            """,
                list(zip(ids, metadatas, names, contents)),
            )

            # add/update full-text search index
            if self.use_fts:
                conn.execute("CREATE TEMPORARY TABLE temp_ids (id INTEGER)")
                conn.executemany(
                    "INSERT INTO temp_ids (id) VALUES (?)", [(did,) for did in ids]
                )
                conn.execute(
                    "DELETE FROM documents_fts WHERE id IN (SELECT id FROM temp_ids)"
                )
                conn.execute("DROP TABLE temp_ids")
                conn.executemany(self.QUERY_INSERT_INDEX, list(zip(contents, ids)))

            # add/update embeddings
            if self.embedding_function:
                vectors = self.embedding_function(contents)
                embeddings = self._format_vectors(vectors)
                conn.executemany(
                    f"UPDATE documents SET embedding = {self.PLACEHOLDER} WHERE id = {self.PLACEHOLDER}",
                    list(zip(embeddings, ids)),
                )

        return ids

    def _order_result(self, result, vector, limit, offset):
        """Order the result by vector similarity."""
        vectors = np.array(
            [np.frombuffer(res.pop("rank"), dtype=np.float32) for res in result],
            dtype=np.float32,
        )
        vector_norm = np.linalg.norm(vector)
        vectors_norm = np.linalg.norm(vectors, axis=1)
        similarities = vector @ vectors.T / vectors_norm / vector_norm
        pos = np.argsort(-similarities)
        result = [{**result[i], "rank": similarities[i]} for i in pos]
        if offset:
            result = result[offset:]
        if limit:
            result = result[:limit]
        return result


class CollectionPostgreSQL(CollectionBase):

    IS_POSTGRES = True
    QUERY_INSERT_INDEX = ""
    QUERY_DELETE_INDEX = "UPDATE documents SET tsvector = NULL WHERE id = %s"
    QUERY_DELETE_DOC = "DELETE FROM documents WHERE id = %s"
    QUERY_SEARCH = """
    SELECT count(*) OVER() AS full_count,
    id, content, metadata,
    ts_rank(tsvector, query) AS rank
    FROM documents, to_tsquery('simple', %s) query
    WHERE tsvector @@ query
    """
    QUERY_VECTOR_SEARCH = """
    SELECT count(*) OVER() AS full_count,
    id, content, metadata,
    1 - (embedding <=> %s)
    FROM documents
    WHERE TRUE
    """
    QUERY_GET = """
    SELECT count(*) OVER() AS full_count,
    id, content, metadata    
    FROM documents
    WHERE TRUE
    """
    QUERY_FILTER_META = "metadata->>'{}' {} %s"
    QUERY_FILTER_META_FLOAT = "(metadata->>'{}')::double precision {} %s"
    QUERY_FILTER_META_IN = "metadata->>'{}' IN ({})"
    QUERY_FILTER_META_NOT_IN = "metadata->>'{}' NOT IN ({})"
    QUERY_ORDER_META = "metadata->>'{}'"
    QUERY_LIMIT = " LIMIT %s"
    QUERY_OFFSET = " OFFSET %s"
    PLACEHOLDER = "%s"

    def __init__(
        self,
        dsn,
        name: str | None = None,
        embedding_function: Callable | None = None,
        use_fts: bool = True,
    ) -> None:
        self.dsn = dsn
        super().__init__(
            name=name, embedding_function=embedding_function, use_fts=use_fts
        )

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

    def _create_document_tables(self, conn) -> None:
        """Create the database tables if they don't exist yet."""
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                content TEXT,
                name TEXT,
                metadata JSONB,
                tsvector TSVECTOR
            );
            CREATE INDEX IF NOT EXISTS documents_tsvector_idx ON documents USING GIN (tsvector);
            CREATE INDEX IF NOT EXISTS name_idx ON documents (name);
        """
        )

    def _create_embedding_column(self, conn) -> None:
        """Create the embedding column if it doesn't exist yet."""

        # before attempting "CREATE EXTENSION", check whether it exists.
        # otherwise it might fail due to permission error even if it exists
        conn.execute("SELECT extname FROM pg_extension WHERE extname = 'vector'")
        extension_exists = conn.fetchone()
        if not extension_exists:
            try:
                conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            except psycopg2.errors.InsufficientPrivilege as exc:
                raise exc

        conn.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS embedding vector;")

    def _add(
        self,
        contents: list[str],
        ids: list[str | None],
        metadatas: list[str | None],
        names: list[str | None],
    ) -> list[str]:
        """Add one or more documents to the collection."""
        with self.conn() as conn:
            if self.embedding_function:
                vectors = self.embedding_function(contents)
                embeddings = self._format_vectors(vectors)
                if self.use_fts:
                    psycopg2.extras.execute_values(
                        conn,
                        """INSERT INTO documents
                            (content, id, metadata, name, tsvector, embedding) VALUES %s
                            ON CONFLICT(id) DO UPDATE SET
                                content = EXCLUDED.content,
                                metadata = EXCLUDED.metadata,
                                name = EXCLUDED.name,
                                tsvector = to_tsvector('simple', EXCLUDED.content),
                                embedding = EXCLUDED.embedding;
                        """,
                        list(
                            zip(contents, ids, metadatas, names, contents, embeddings)
                        ),
                        template="(%s, %s, %s, %s, to_tsvector('simple', %s), %s)",
                    )
                else:
                    psycopg2.extras.execute_values(
                        conn,
                        """INSERT INTO documents
                            (content, id, metadata, name, embedding) VALUES %s
                            ON CONFLICT(id) DO UPDATE SET
                                content = EXCLUDED.content,
                                metadata = EXCLUDED.metadata,
                                name = EXCLUDED.name,
                                embedding = EXCLUDED.embedding;
                        """,
                        list(zip(contents, ids, metadatas, names, embeddings)),
                        template="(%s, %s, %s, %s, %s)",
                    )
            else:
                psycopg2.extras.execute_values(
                    conn,
                    """INSERT INTO documents
                        (content, id, metadata, name, tsvector) VALUES %s
                        ON CONFLICT(id) DO UPDATE SET
                            content = EXCLUDED.content,
                            metadata = EXCLUDED.metadata,
                            name = EXCLUDED.name,
                            tsvector = to_tsvector('simple', EXCLUDED.content);
                    """,
                    list(zip(contents, ids, metadatas, names, contents)),
                    template="(%s, %s, %s, %s, to_tsvector('simple', %s))",
                )
        return ids

    def _format_vectors(self, vectors):
        """Format the vectors so they can be inserted in the table."""

        def format_vector(v):
            return "[" + ",".join([f"{float(x):.8f}" for x in v]) + "]"

        return [format_vector(v) for v in vectors]


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


def Collection(
    db_url: str,
    name: str,
    embedding_function: Callable | None = None,
    use_fts: bool = True,
) -> CollectionBase:
    """Constructor for search engine instance."""
    if not db_url:
        return CollectionSQLite(
            name=name, embedding_function=embedding_function, use_fts=use_fts
        )
    if db_url.startswith("sqlite:///"):
        return CollectionSQLite(
            db_path=db_url[10:],
            name=name,
            embedding_function=embedding_function,
            use_fts=use_fts,
        )
    return CollectionPostgreSQL(
        dsn=db_url_to_dsn(db_url),
        name=name,
        embedding_function=embedding_function,
        use_fts=use_fts,
    )
