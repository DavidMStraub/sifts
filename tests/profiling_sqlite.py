"""Script to assess the performance of the SQLite backend."""

import shutil
import tempfile
import time
import random
import uuid
import numpy as np
from contextlib import contextmanager
from pathlib import Path

from sifts import Collection


@contextmanager
def timer(title: str):
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time
        if elapsed_time < 1e-9:
            print(f"{title:<25} Took < 1 ns")
        elif elapsed_time < 1e-6:
            print(f"{title:<25} Took {elapsed_time / 1e-9:.0f} ns")
        elif elapsed_time < 1e-3:
            print(f"{title:<25} Took {elapsed_time / 1e-6:.0f} µs")
        elif elapsed_time < 0.5:
            print(f"{title:<25} Took {elapsed_time / 1e-3:.0f} ms")
        else:
            print(f"{title:<25} Took {elapsed_time:.2f} s")


word_list = [
    "Lorem",
    "ipsum",
    "dolor",
    "sit",
    "amet",
    "consectetur",
    "adipiscing",
    "elit",
    "Sed",
    "do",
    "eiusmod",
    "tempor",
    "incididunt",
    "ut",
    "labore",
    "et",
    "dolore",
    "magna",
    "aliqua",
    "Ut",
    "enim",
    "ad",
    "minim",
    "veniam",
    "quis",
    "nostrud",
    "exercitation",
    "ullamco",
    "laboris",
    "nisi",
    "ut",
    "aliquip",
    "ex",
    "ea",
    "commodo",
    "consequat",
    "Duis",
    "aute",
    "irure",
    "dolor",
    "in",
    "reprehenderit",
    "in",
    "voluptate",
    "velit",
    "esse",
    "cillum",
    "dolore",
    "eu",
    "fugiat",
    "nulla",
    "pariatur",
    "Excepteur",
    "sint",
    "occaecat",
    "cupidatat",
    "non",
    "proident",
    "sunt",
    "in",
    "culpa",
    "qui",
    "officia",
    "deserunt",
    "mollit",
    "anim",
    "id",
    "est",
    "laborum",
]


def get_document(n: int = 20) -> str:
    """Generate a string of n words."""
    return " ".join(random.choices(word_list, k=n))


def get_word() -> str:
    """Get a random word."""
    return random.choice(word_list)


def run_timing():
    """Run timing."""
    tmp_dir = Path(tempfile.mkdtemp())
    path = tmp_dir / "search_engine.db"

    with timer("Create database table"):
        Collection(f"sqlite:///{path}", name="123")

    with timer("Instantiate again"):
        engine = Collection(f"sqlite:///{path}", name="123")

    print("-- Full-text search --")
    run_add_update_delete(engine)

    def f(documents):
        return [np.random.rand(384) for _ in documents]

    engine = Collection(
        f"sqlite:///{path}", name="456", embedding_function=f, use_fts=False
    )

    print("-- Vector search --")
    run_add_update_delete(engine)

    engine = Collection(f"sqlite:///{path}", name="789", embedding_function=f)

    print("-- Both --")
    run_add_update_delete(engine)

    shutil.rmtree(tmp_dir)


def run_add_update_delete(engine, n=100000):
    """Run some long running operations."""

    N = n
    random.seed(711)

    with timer("Count on empty"):
        engine.count()

    with timer("Create random documents"):
        contents = [get_document() for _ in range(N)]

    with timer("Create random IDs"):
        ids = [str(uuid.uuid4()) for _ in range(N)]

    with timer("Create random metadata"):
        metadatas = [{"k1": get_word(), "k2": get_word()} for _ in range(N)]

    if engine.embedding_function:
        with timer("Embeddings"):
            vectors = engine.embedding_function(contents)
        with timer("Format vectors"):
            engine._format_vectors(vectors)

    with timer("Add documents"):
        engine.add(contents, ids, metadatas)

    with timer("Retrieve metadata"):
        engine.get()

    with timer("Get all"):
        engine.get()

    with timer("Count on full"):
        engine.count()

    with timer("Delete documents"):
        engine.delete_all()

    with timer("Add documents"):
        engine.add(contents, ids, metadatas)

    with timer("Update documents"):
        engine.add(contents, ids, metadatas)

    with timer("Query"):
        engine.query("reprehenderit", vector_search=bool(engine.embedding_function))


if __name__ == "__main__":
    run_timing()
