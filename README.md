# Sifts &ndash; Simple Full Text & Semantic Search

🔎 Sifts is a simple but powerful Python package for managing and querying document collections with support for both SQLite and PostgreSQL databases.

It is designed to efficiently handle full-text search and vector search, making it ideal for applications that involve large-scale text data retrieval.



## Features

- **Dual Database Support**: Sifts works with both SQLite and PostgreSQL, offering the simplicity of SQLite for lightweight applications and the scalability of PostgreSQL for larger, production environments.
- **Full-Text Search (FTS)**: Perform advanced text search queries with full-text search support.
- **Vector Search**: Integrate with embedding models to perform vector-based similarity searches, perfect for applications involving natural language processing.
- **Flexible Querying**: Supports complex queries with filtering, ordering, and pagination.

## Background

The main idea of Sifts is to leverage the built-in full-text search capabilities in SQLite and PostgreSQL and to make them available via a unified, Pythonic API. You can use SQLite for small projects or development and trivially switch to PostgreSQL to scale your application.

For vector search, cosine similarity is computed in PostgreSQL via the pgvector extension, while with SQLite similarity is calculated in memory.

Sifts does not come with a server mode as it's meant as a library to be imported by other apps. The original motivation for its development was to replace whoosh as search backend in [Gramps Web](https://www.grampsweb.org/), which is based on Flask.


## Installation

You can install Sifts via pip:

```bash
pip install sifts
```

## Usage

### Full-text search

```python
import sifts

# by default, creates a new SQLite database in the working directory
collection = sifts.Collection(name="my_collection")

# Add docs to the index. Can also update and delete.
collection.add(
    documents=["Lorem ipsum dolor", "sit amet"],
    metadatas=[{"foo": "bar"}, {"foo": "baz"}], # otpional, can filter on these
    ids=["doc1", "doc2"], # unique for each doc. Uses UUIDs if omitted
)

results = collection.query(
    "Lorem",
    # limit=2,  # optionally limit the number of results
    # where={"foo": "bar"},  # optional filter
    # order_by="foo",  # sort by metadata key (rather than rank)
)
```

The API is inspired by [chroma](https://github.com/chroma-core/chroma).


### Full-text search syntax

Sifts supports the following search syntax:

- Search for individual words
- Search for multiple words (will match documents where all words are present)
- `and` operator
- `or` operator
- `*` wildcard (in SQLite, supported anywhere in the search term, in PostgreSQL only at the end of the search term)

The search syntax is the same regardless of backend.

### Vector search (semantic search)

Sifts can also be used as vector store, used for semantic search engines or retrieval-augmented generation (RAG) with large language models (LLMs).

Simply pass the `embedding_function` to the `Collection` factory to enable vector storage and set `vector_search=True` in the query method. For instance, using the [Sentence Transformers](https://sbert.net/) library,

```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("intfloat/multilingual-e5-small")

def embedding_function(queries: list[str]):
    return model.encode(queries)

collection = sifts.Collection(
    db_url="sqlite:///vector_store.db",
    name="my_vector_store",
    embedding_function=embedding_function
)

# Adding vector data to the collection
collection.add(["This is a test sentence.", "Another example query."])

# Querying the collection with semantic search
results = collection.query("Find similar sentences.", vector_search=True)
```

PostgreSQL collections require installing and enabling the `pgvector` extension.


### Updating and Deleting Documents

Documents can be updated or deleted using their IDs.

```python
# Update a document
collection.update(ids=["document_id"], contents=["Updated content"])

# Delete a document
collection.delete(ids=["document_id"])
```

## Contributing

Contributions are welcome! Feel free to create an [issue](https://github.com/DavidMStraub/sifts/issues) if you encounter problems or have an improvement suggestion, and even better submit a PR along with it!

## License

Sifts is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.


---

Happy Sifting! 🚀



