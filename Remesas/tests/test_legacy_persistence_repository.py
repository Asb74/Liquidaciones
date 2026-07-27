import sqlite3

from data.legacy_persistence_repository import LegacyPersistenceRepository


def _repository(rows):
    conn = sqlite3.connect(":memory:")
    conn.execute("ATTACH DATABASE ':memory:' AS eepp")
    conn.execute("CREATE TABLE eepp.MVariedad(CULTIVO TEXT, Variedad TEXT, ARTICULO)")
    conn.executemany("INSERT INTO eepp.MVariedad VALUES(?,?,?)", rows)
    return LegacyPersistenceRepository(conn)


def test_article_code_accepts_numeric_and_alphanumeric_identifiers():
    repository = _repository((
        ("CITRICOS", "VALENCIA", 3984),
        ("MANDARINA", "TANGO", "B391"),
    ))

    assert repository.article_code("CITRICOS", "VALENCIA") == "3984"
    assert repository.article_code("MANDARINA", "TANGO") == "B391"


def test_article_code_normalizes_whitespace_and_preserves_missing_values():
    repository = _repository((
        ("MANDARINA", "NADORCOTT", " B391 "),
        ("MANDARINA", "VACIA", "  "),
    ))

    assert repository.article_code(" mandarina ", " nadorcott ") == "B391"
    assert repository.article_code("MANDARINA", "VACIA") is None
    assert repository.article_code("MANDARINA", "INEXISTENTE") is None
