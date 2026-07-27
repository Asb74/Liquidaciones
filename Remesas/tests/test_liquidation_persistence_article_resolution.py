import sqlite3

from data.legacy_persistence_repository import LegacyPersistenceRepository
from data.variety_repository import VarietyRepository
from services.liquidation_persistence_service import LiquidationPersistenceService
from services.variety_selection_resolver import VarietySelectionResolver


def _service(rows):
    connection = sqlite3.connect(":memory:")
    connection.execute("ATTACH DATABASE ':memory:' AS eepp")
    connection.execute(
        """CREATE TABLE eepp.MVariedad(
        Id INTEGER, CULTIVO TEXT, Variedad TEXT, GRUPO TEXT, SUBGRUPO TEXT,
        CODROPA TEXT, ARTICULO TEXT, PRODUCTO TEXT, COLOR TEXT)"""
    )
    connection.executemany("INSERT INTO eepp.MVariedad VALUES(?,?,?,?,?,?,?,?,?)", rows)

    service = LiquidationPersistenceService.__new__(LiquidationPersistenceService)
    service.legacy = LegacyPersistenceRepository(connection)
    service.aliases = {"DIRECTO": "CITRICOS"}
    service.variety_resolver = VarietySelectionResolver(VarietyRepository(connection))
    return service


def test_article_code_uses_the_master_crop_resolved_for_each_mixed_output_variety():
    service = _service(
        (
            (1, "CITRICOS", "NAVELINA", "NAVEL", "TEMPRANA", None, "3970", None, None),
            (2, "MANDARINA", "TANGO", "MANDARINA", "MEDIA", None, "B391", None, None),
        )
    )

    assert service._article_code("DIRECTO", "NAVELINA") == "3970"
    assert service._article_code("DIRECTO", "TANGO") == "B391"


def test_article_code_does_not_choose_a_master_when_the_variety_is_ambiguous():
    service = _service(
        (
            (1, "CITRICOS", "COMPARTIDA", "NAVEL", "MEDIA", None, "C100", None, None),
            (2, "MANDARINA", "COMPARTIDA", "MANDARINA", "MEDIA", None, "M100", None, None),
        )
    )

    assert service._article_code("DIRECTO", "COMPARTIDA") is None
