import sqlite3

import pytest

from data.variety_repository import VarietyRepository
from services.individual_pdf_refresh_service import _document_group
from services.persisted_variety_benchmark_service import variety_group_code
from services.variety_group_service import VarietyGroupService, VarietyGroupResolutionError


def master_service():
    conn=sqlite3.connect(":memory:")
    conn.execute("ATTACH DATABASE ':memory:' AS eepp")
    conn.execute("CREATE TABLE eepp.MVariedad(CULTIVO TEXT,Variedad TEXT,GRUPO TEXT,SUBGRUPO TEXT)")
    conn.executemany("INSERT INTO eepp.MVariedad VALUES(?,?,?,?)",(
        ("CITRICOS","NAVELINA","NAVEL","TEMPRANA"),
        ("CITRICOS","LANE LATE","NAVEL","TARDIA"),
        ("CITRICOS","SALUSTIANA","BLANCA","TEMPRANA"),
    ))
    return conn,VarietyGroupService(VarietyRepository(conn))


@pytest.mark.parametrize("crop,variety,expected",(
    ("CITRICOS","NAVELINA","NAVEL_TEMPRANA"),
    ("INDUSTRIA","LANE LATE","NAVEL_TARDIA"),
    ("DIRECTOCHF","SALUSTIANA","BLANCA_TEMPRANA"),
))
def test_persisted_concrete_variety_resolves_master_group(crop,variety,expected):
    conn,service=master_service()
    try:
        group,resolution=service.resolve_variety_group(crop,variety)
        assert variety_group_code(group.group,group.subgroup)==expected
        assert resolution.selected_varieties==(variety,)
    finally:
        conn.close()


def test_remittance_text_is_never_an_input_to_group_resolution():
    _conn,service=master_service()
    with pytest.raises(VarietyGroupResolutionError,match="REMESA NAVELINA"):
        service.resolve_variety_group("CITRICOS","REMESA NAVELINA SEMANA 3")


def test_document_accepts_many_lines_in_same_group():
    lines=({"variety_group_code":"NAVEL_TARDIA","variety_group_name":"NAVEL TARDÍA","variety":"LANE LATE","variety_name":"LANE LATE"},
           {"variety_group_code":"NAVEL_TARDIA","variety_group_name":"NAVEL TARDÍA","variety":"CHISLETT","variety_name":"CHISLETT"})
    assert _document_group(lines)==("NAVEL_TARDIA","NAVEL TARDÍA")


def test_document_rejects_multiple_groups_explicitly():
    lines=({"variety_group_code":"NAVEL_TARDIA","variety_group_name":"NAVEL TARDÍA","variety":"LANE LATE","variety_name":"LANE LATE"},
           {"variety_group_code":"BLANCA_TEMPRANA","variety_group_name":"BLANCA TEMPRANA","variety":"SALUSTIANA","variety_name":"SALUSTIANA"})
    with pytest.raises(ValueError,match="más de un grupo varietal"):
        _document_group(lines)


def test_document_reports_unresolved_variety_name():
    with pytest.raises(ValueError,match="DESCONOCIDA"):
        _document_group(({"variety_group_code":None,"variety_group_name":None,"variety":"DESCONOCIDA","variety_name":None},))
