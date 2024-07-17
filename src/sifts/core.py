from __future__ import annotations
import json
import sqlite3
import uuid


def make_id():
    return str(uuid.uuid4())


class SearchEngineSQLite:
    def __init__(self, db_path="search_engine.db") -> None:
        self.con = sqlite3.connect(db_path)
        self.create_tables()

    def create_tables(self) -> None:
        with self.con:
            self.con.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(id, content)"
            )
            self.con.execute(
                """CREATE TABLE IF NOT EXISTS documents (
                    id TEXT PRIMARY KEY,
                    content TEXT,
                    metadata JSON
                )"""
            )

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
        with self.con:
            self.con.executemany(
                "INSERT INTO documents (content, id, metadata) VALUES (?, ?, ?)",
                list(zip(contents, ids, metadatas)),
            )
            self.con.executemany(
                "INSERT INTO documents_fts (content, id) VALUES (?, ?)",
                list(zip(contents, ids)),
            )
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
        with self.con:
            self.con.executemany(
                "UPDATE documents_fts SET content = (?) WHERE id = (?)",
                list(zip(contents, ids)),
            )
            if metadatas and any(metadatas):
                metadatas = [json.dumps(m) if m else None for m in metadatas]
                self.con.executemany(
                    "UPDATE documents SET metadata = (?) WHERE id = (?)",
                    list(zip(metadatas, ids)),
                )

    def delete(self, ids: list[str]) -> None:
        with self.con:
            self.con.executemany(
                "DELETE FROM documents_fts WHERE id = (?)",
                (ids,),
            )
            self.con.executemany(
                "DELETE FROM documents WHERE id = (?)",
                (ids,),
            )

    def query(
        self,
        query_string: str,
        limit: int = 0,
        where: dict | None = None,
        order_by: str | None = None,
        descending: bool = False,
    ) -> list:
        with self.con:
            try:
                fts_query = """SELECT doc.id, fts.rank FROM documents_fts fts
                JOIN documents doc ON doc.id = fts.id
                WHERE fts.content MATCH (?)"""
                params = [query_string]

                if where:
                    for key, value in where.items():
                        fts_query += f' AND json_extract(doc.metadata, "$.{key}") = (?)'
                        params.append(value)

                if limit:
                    fts_query += "LIMIT (?)"

                if order_by:
                    fts_query += f' ORDER BY json_extract(doc.metadata, "$.{order_by}")'
                    if descending:
                        fts_query += " DESC"
                result = self.con.execute(fts_query, params)
                matching_ids = [match for match in result]
            except sqlite3.OperationalError:
                return []
        return matching_ids
