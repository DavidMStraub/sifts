"""Script to assess the performance of the Postgres backend."""

import numpy as np
import subprocess
import time

import psycopg2
from psycopg2 import OperationalError
from sifts import Collection
from profiling_sqlite import run_add_update_delete, timer


def is_postgres_healthy():
    try:
        conn = psycopg2.connect(
            dbname="testdb",
            user="testuser",
            password="testpass",
            host="localhost",
            port=5432,
        )
        conn.close()
        return True
    except OperationalError:
        return False


def run_timing():
    """Run timing."""
    subprocess.run(["docker", "compose", "up", "-d"])

    timeout = 30
    pause = 0.5
    start_time = time.time()

    while time.time() - start_time < timeout:
        if is_postgres_healthy():
            break
        else:
            time.sleep(pause)

    try:

        with timer("Create database table"):
            Collection(
                "postgresql://testuser:testpass@localhost:5432/testdb", name="123"
            )

        with timer("Instantiate again"):
            engine = Collection(
                "postgresql://testuser:testpass@localhost:5432/testdb", name="123"
            )

        print("-- Full-text search --")
        run_add_update_delete(engine)

        def f(documents):
            return [np.random.rand(384) for _ in documents]

        engine = Collection(
            "postgresql://testuser:testpass@localhost:5432/testdb",
            name="456",
            embedding_function=f,
            use_fts=False,
        )

        print("-- Vector search --")
        run_add_update_delete(engine)

        engine = Collection(
            "postgresql://testuser:testpass@localhost:5432/testdb",
            name="456",
            embedding_function=f,
        )

        print("-- Both --")
        run_add_update_delete(engine)

    except:
        subprocess.run(["docker", "compose", "down"])
        raise
    finally:
        subprocess.run(["docker", "compose", "down"])


if __name__ == "__main__":
    run_timing()
