from sifts.core import QueryParser


def test_trim_sqlite():
    query = " Lorem\t"
    assert str(QueryParser(query)) == "Lorem"


def test_and_sqlite():
    query = "Lorem and ipsum"
    assert str(QueryParser(query)) == "Lorem AND ipsum"


def test_or_sqlite():
    query = "Lorem or ipsum"
    assert str(QueryParser(query)) == "Lorem OR ipsum"


def test_wildcard_sqlite():
    query = "Lor*"
    assert str(QueryParser(query)) == "Lor*"


def test_wildcard_and_sqlite():
    query = "Lor* and ips*"
    assert str(QueryParser(query)) == "Lor* AND ips*"


def test_trim_postgres():
    query = " Lorem\t"
    assert str(QueryParser(query, backend="postgresql")) == "Lorem"


def test_and_postgres():
    query = "Lorem and ipsum"
    assert str(QueryParser(query, backend="postgresql")) == "Lorem & ipsum"


def test_and_postgres_upper():
    query = "Lorem AND ipsum"
    assert str(QueryParser(query, backend="postgresql")) == "Lorem & ipsum"


def test_or_postgres():
    query = "Lorem or ipsum"
    assert str(QueryParser(query, backend="postgresql")) == "Lorem | ipsum"


def test_wildcard_postgres():
    query = "Lor*"
    assert str(QueryParser(query, backend="postgresql")) == "Lor:*"


def test_wildcard_and_postgres():
    query = "Lor* and ips*"
    assert str(QueryParser(query, backend="postgresql")) == "Lor:* & ips:*"


def test_hyphen_sqlite():
    query = "test-word"
    assert str(QueryParser(query)) == '"test-word"'


def test_hyphen_postgres():
    query = "test-word"
    assert str(QueryParser(query, backend="postgresql")) == '"test-word"'


def test_hyphen_multiple_sqlite():
    query = "test-word and another-word"
    assert str(QueryParser(query)) == '"test-word" AND "another-word"'


def test_hyphen_multiple_postgres():
    query = "test-word and another-word"
    assert str(QueryParser(query, backend="postgresql")) == '"test-word" & "another-word"'


def test_hyphen_wildcard_end_sqlite():
    query = "test-word*"
    assert str(QueryParser(query)) == '"test-word"*'


def test_hyphen_wildcard_end_postgres():
    query = "test-word*"
    assert str(QueryParser(query, backend="postgresql")) == '"test-word":*'


def test_hyphen_wildcard_middle_sqlite():
    # Mid-word wildcards are not supported, left as-is
    query = "test-wo*rd"
    assert str(QueryParser(query)) == '"test-wo"*rd'


def test_hyphen_wildcard_middle_postgres():
    # Mid-word wildcards are not supported, left as-is
    query = "test-wo*rd"
    assert str(QueryParser(query, backend="postgresql")) == '"test-wo"*rd'


def test_hyphen_wildcard_beginning_sqlite():
    query = "*test-word"
    assert str(QueryParser(query)) == '*"test-word"'


def test_hyphen_wildcard_beginning_postgres():
    query = "*test-word"
    assert str(QueryParser(query, backend="postgresql")) == '*"test-word"'


def test_wildcard_end_only_sqlite():
    query = "testword*"
    assert str(QueryParser(query)) == "testword*"


def test_wildcard_end_only_postgres():
    query = "testword*"
    assert str(QueryParser(query, backend="postgresql")) == "testword:*"


def test_wildcard_middle_only_sqlite():
    # Mid-word wildcards are not supported by SQLite FTS5, left as-is
    query = "test*word"
    assert str(QueryParser(query)) == "test*word"


def test_wildcard_middle_only_postgres():
    # Mid-word wildcards are not supported by PostgreSQL tsquery, left as-is
    query = "test*word"
    assert str(QueryParser(query, backend="postgresql")) == "test*word"


def test_wildcard_beginning_only_sqlite():
    # Leading wildcards are not supported by SQLite FTS5, left as-is
    query = "*testword"
    assert str(QueryParser(query)) == "*testword"


def test_wildcard_beginning_only_postgres():
    # Leading wildcards are not supported by PostgreSQL tsquery, left as-is
    query = "*testword"
    assert str(QueryParser(query, backend="postgresql")) == "*testword"
