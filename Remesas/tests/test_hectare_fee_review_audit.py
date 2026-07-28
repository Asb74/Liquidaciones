from decimal import Decimal
import sqlite3

from data.hectare_repository import HectareRepository


def test_review_audits_partial_young_surface_and_inconsistent_years():
    conn = sqlite3.connect(":memory:")
    conn.execute("ATTACH DATABASE ':memory:' AS eepp")
    conn.execute("CREATE TABLE eepp.DEEPP (IdSocio INTEGER, Boleta TEXT, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, CHA INTEGER, SupCul REAL, BAJA TEXT)")
    conn.execute("CREATE TABLE eepp.DParcela (Boleta TEXT, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, IdPM TEXT, Pol TEXT, Par TEXT, Rec TEXT, SupCul REAL, SupApor REAL, BAJA TEXT, Año INTEGER)")
    conn.execute("INSERT INTO eepp.DEEPP VALUES (1, '10', '2026', '1', 'CITRICOS', 1, 3, '')")
    conn.executemany("INSERT INTO eepp.DParcela VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
        ("10", "2026", "1", "CITRICOS", "", "1", "2", "3", 2, 0, "", 2018),
        ("10", "2026", "1", "CITRICOS", "", "1", "3", "3", 1, 0, "", 2023),
        ("10", "2026", "1", "CITRICOS", "", "1", "4", "3", 0, 0, "", 2018),
    ])

    audit = HectareRepository(conn).get_boleta_surface_audit(1, "10", "2026", "1", ("CITRICOS",))

    assert audit["estado_boleta"] == "APLICADA_CON_INCIDENCIAS"
    assert audit["superficie_total"] == Decimal("3")
    assert audit["superficie_valida"] == Decimal("2")
    assert audit["superficie_excluida"] == Decimal("1")
    assert audit["numero_parcelas"] == 3
    assert audit["numero_parcelas_validas"] == 1
    assert "PLANTACION_MENOR_CINCO_ANOS" in audit["motivos_exclusion"]
    assert set(audit["incidencias"]) == {"PARCELA_SUPERFICIE_CERO", "ANOS_PLANTACION_INCOHERENTES"}
