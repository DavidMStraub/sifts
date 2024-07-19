# Sifts &ndash; Simple Full Text Search

Sifts is a simple Python full text search engine library with an SQLite or PostgreSQL backend.

It is meant to be used with Python libraries that need full-text search support, but where deploying something like ElasticSearch is overkill.

It supports both **SQLite** FTS5 and **PostgreSQL** FTS as backends.

## Installation

```bash
pip install sifts
```

## Usage

```python
import sifts

# by default, creates a new SQLite database in the working directory
search = sifts.SearchEngine()

# Add docs to the index. Can also update and delete.
search.add(
    documents=["Lorem ipsum dolor", "sit amet"],
    metadatas=[{"foo": "bar"}, {"foo": "baz"}], # otpional, can filter on these
    ids=["doc1", "doc2"], # unique for each doc. Uses UUIDs if omitted
)

results = search.query(
    "Lorem",
    # limit=2,  # optionally limit the number of results
    # where={"foo": "bar"},  # optional filter
    # order_by="foo",  # sort by metadata key (rather than rank)
)
```

The API is inspired by [chroma](https://github.com/chroma-core/chroma).

## Search Syntax

Sifts supports the following search syntax:

- Search for individual words
- Search for multiple words (will match documents where all words are present)
- `and` operator
- `or` operator
- `*` wildcard (in SQLite, supported anywhere in the search term, in PostgreSQL only at the end of the search term)

The search syntax is the same regardless of backend.