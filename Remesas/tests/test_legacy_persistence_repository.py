import sqlite3

from data.legacy_persistence_repository import LegacyPersistenceRepository


def _repository(rows):
    conn = sqlite3.connect(":memory:")
    conn.execute("ATTACH DATABASE ':memory:' AS eepp")
    conn.execute("CREATE TABLE eepp.MVariedad(CULTIVO TEXT, Variedad TEXT, ARTICULO)")
    conn.executemany("INSERT INTO eepp.MVariedad VALUES(?,?,?)", rows)
    return LegacyPersistenceRepository(conn)


def test_article_code_accepts_numeric_alphanumeric_and_shared_identifiers():
    repository = _repository((
        ("CITRICOS", "NAVELINA", 3970),
        ("MANDARINA", "TANGO", "B391"),
        ("MANDARINA", "NADORCOTT", "B391"),
        ("CITRICOS", "CARA CARA", "D294"),
        ("CITRICOS", "CODIGO CERO", "0012"),
    ))

    assert repository.article_code("CITRICOS", "NAVELINA") == "3970"
    assert repository.article_code("MANDARINA", "TANGO") == "B391"
    assert repository.article_code("MANDARINA", "NADORCOTT") == "B391"
    assert repository.article_code("CITRICOS", "CARA CARA") == "D294"
    assert repository.article_code("CITRICOS", "CODIGO CERO") == "0012"


def test_article_code_normalizes_whitespace_and_preserves_missing_values():
    repository = _repository((
        ("MANDARINA", "NADORCOTT", " B391 "),
        ("MANDARINA", "VACIA", "  "),
    ))

    assert repository.article_code(" mandarina ", " nadorcott ") == "B391"
    assert repository.article_code("MANDARINA", "VACIA") is None
    assert repository.article_code("MANDARINA", "INEXISTENTE") is None


def test_article_code_normalizes_aliases_and_logs_resolution(caplog):
    repository = _repository((("MANDARINA", "TANGO", "B391"),))

    with caplog.at_level("INFO"):
        assert repository.article_code(" citricos ", " Tango ", {" citricos ": " mandarina "}) == "B391"

    assert "[MVariedadArticulo]" in caplog.text
    assert "article=B391" in caplog.text
    assert "status=resolved" in caplog.text
