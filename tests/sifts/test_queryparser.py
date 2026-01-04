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
    assert (
        str(QueryParser(query, backend="postgresql")) == '"test-word" & "another-word"'
    )


def test_hyphen_wildcard_end_sqlite():
    query = "test-word*"
    assert str(QueryParser(query)) == '"test-word"*'


def test_hyphen_wildcard_end_postgres():
    query = "test-word*"
    assert str(QueryParser(query, backend="postgresql")) == '"test-word":*'


def test_hyphen_wildcard_middle_sqlite():
    # Mid-word wildcards are not supported by FTS5
    # Quote the entire token to make it clear it's treated as a literal string
    query = "test-wo*rd"
    assert str(QueryParser(query)) == '"test-wo*rd"'


def test_hyphen_wildcard_middle_postgres():
    # Mid-word wildcards are not supported by PostgreSQL tsquery
    # The entire token is quoted to make it clear it's treated as a literal
    # Note: This is a breaking change from previous behavior for consistency
    query = "test-wo*rd"
    assert str(QueryParser(query, backend="postgresql")) == '"test-wo"*rd'


def test_hyphen_wildcard_beginning_sqlite():
    # Leading wildcards are stripped, then hyphenated word is quoted
    query = "*test-word"
    assert str(QueryParser(query)) == '"test-word"'


def test_hyphen_wildcard_beginning_postgres():
    # Leading wildcards are stripped, then hyphenated word is quoted
    query = "*test-word"
    assert str(QueryParser(query, backend="postgresql")) == '"test-word"'


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
    # Leading wildcards are not supported by SQLite FTS5, stripped out
    query = "*testword"
    assert str(QueryParser(query)) == "testword"


def test_wildcard_beginning_only_postgres():
    # Leading wildcards are not supported by PostgreSQL tsquery, stripped out
    query = "*testword"
    assert str(QueryParser(query, backend="postgresql")) == "testword"


def test_apostrophe_sqlite():
    query = "it's"
    assert str(QueryParser(query)) == '"it\'s"'


def test_apostrophe_postgres():
    query = "it's"
    assert str(QueryParser(query, backend="postgresql")) == '"it\'s"'


def test_comma_sqlite():
    """Test FTS5 special character: comma"""
    query = "Bydgoszcz, Poland"
    assert str(QueryParser(query)) == '"Bydgoszcz," Poland'


def test_comma_with_wildcard_sqlite():
    """Test FTS5 special character: comma with wildcard"""
    query = "Bydgoszcz, Poland*"
    assert str(QueryParser(query)) == '"Bydgoszcz," Poland*'


def test_parentheses_sqlite():
    """Test FTS5 special character: parentheses"""
    query = "test (example)"
    assert str(QueryParser(query)) == 'test "(example)"'


def test_colon_sqlite():
    """Test FTS5 special character: colon"""
    query = "time:12:00"
    assert str(QueryParser(query)) == '"time:12:00"'


def test_brackets_sqlite():
    """Test FTS5 special character: brackets"""
    query = "test[bracket]"
    assert str(QueryParser(query)) == '"test[bracket]"'


def test_multiple_special_chars_sqlite():
    """Test multiple FTS5 special characters in one query"""
    query = "test, (data) and value:123"
    assert str(QueryParser(query)) == '"test," "(data)" AND "value:123"'


def test_mixed_special_and_normal_sqlite():
    """Test mix of normal words and special character words"""
    query = "normal word, special"
    assert str(QueryParser(query)) == 'normal "word," special'


def test_curly_braces_sqlite():
    """Test FTS5 special character: curly braces"""
    query = "template{value}"
    assert str(QueryParser(query)) == '"template{value}"'


def test_combined_hyphen_and_comma_sqlite():
    """Test token with both hyphen and comma (combined special chars)"""
    query = "test-word, another"
    assert str(QueryParser(query)) == '"test-word," another'


def test_combined_hyphen_and_colon_sqlite():
    """Test token with both hyphen and colon"""
    query = "test-word:value"
    assert str(QueryParser(query)) == '"test-word:value"'


def test_special_char_with_trailing_wildcard_sqlite():
    """Test special character with trailing wildcard (critical edge case)"""
    query = "test:value*"
    assert str(QueryParser(query)) == '"test:value"*'


def test_comma_with_trailing_wildcard_sqlite():
    """Test comma with trailing wildcard"""
    query = "city, country*"
    assert str(QueryParser(query)) == '"city," country*'


def test_parentheses_with_wildcard_sqlite():
    """Test parentheses with wildcard"""
    query = "(example)*"
    assert str(QueryParser(query)) == '"(example)"*'


def test_quote_in_token_sqlite():
    """Test token containing quote character"""
    query = 'test"value'
    assert str(QueryParser(query)) == '"test""value"'


def test_quote_with_comma_sqlite():
    """Test token with both quote and comma"""
    query = 'test"value,'
    assert str(QueryParser(query)) == '"test""value,"'


def test_complex_special_chars_combination_sqlite():
    """Test complex combination of multiple special character types"""
    query = "test-word, (data:123) and value{x}"
    assert str(QueryParser(query)) == '"test-word," "(data:123)" AND "value{x}"'


def test_multiple_wildcards_with_special_chars_sqlite():
    """Test multiple tokens with wildcards and special chars"""
    query = "city:* and country:*"
    assert str(QueryParser(query)) == '"city:"* AND "country:"*'


def test_already_quoted_string_sqlite():
    """Test that already-quoted strings are kept as-is"""
    query = '"test value"'
    assert str(QueryParser(query)) == '"test value"'


def test_already_quoted_with_escaped_quotes_sqlite():
    """Test already-quoted string with FTS5 escaped quotes (should stay unchanged)"""
    query = '"test""value"'
    assert str(QueryParser(query)) == '"test""value"'


def test_already_quoted_with_special_chars_sqlite():
    """Test already-quoted string containing special characters"""
    query = '"test, (value:123)"'
    assert str(QueryParser(query)) == '"test, (value:123)"'


def test_mixed_quoted_and_unquoted_sqlite():
    """Test mix of already-quoted and unquoted tokens"""
    query = '"quoted part" and unquoted, word'
    assert str(QueryParser(query)) == '"quoted part" AND "unquoted," word'
