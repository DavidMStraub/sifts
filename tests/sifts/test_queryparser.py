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


def test_or_postgres():
    query = "Lorem or ipsum"
    assert str(QueryParser(query, backend="postgresql")) == "Lorem | ipsum"


def test_wildcard_postgres():
    query = "Lor*"
    assert str(QueryParser(query, backend="postgresql")) == "Lor:*"


def test_wildcard_and_postgres():
    query = "Lor* and ips*"
    assert str(QueryParser(query, backend="postgresql")) == "Lor:* & ips:*"
