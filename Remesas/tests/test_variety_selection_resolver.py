import sqlite3
from pathlib import Path

from data.deliveries_repository import DeliveriesRepository
from data.variety_repository import VarietyRepository
from domain.models import DeliveryFilter, Period, WorkContext
from services.variety_group_service import VarietyGroupService
from services.variety_selection_resolver import VarietySelectionKind, VarietySelectionResolver, normalize_variety_token
from datetime import date


def make_conn():
    conn = sqlite3.connect(':memory:')
    conn.execute("ATTACH DATABASE ':memory:' AS eepp")
    conn.execute('CREATE TABLE eepp.MVariedad(Id INTEGER, CULTIVO TEXT, Variedad TEXT, GRUPO TEXT, SUBGRUPO TEXT, CODROPA TEXT, ARTICULO TEXT, PRODUCTO TEXT, COLOR TEXT)')
    conn.executemany('INSERT INTO eepp.MVariedad VALUES(?,?,?,?,?,?,?,?,?)', [
        (1,'CITRICOS','LANE LATE','NAVEL','TARDIA',None,None,None,None),
        (2,'CITRICOS','NAVELINA','NAVEL','TARDIA',None,None,None,None),
        (3,'CITRICOS','CHISLETT SUMER','NAVEL','TARDIA',None,None,None,None),
        (4,'CITRICOS','SALUSTIANA','BLANCA','TEMPRANA',None,None,None,None),
        (5,'CITRICOS','BLANCA TEMPRANA','BLANCA','TEMPRANA',None,None,None,None),
    ])
    return conn


def resolver(conn, tmp_path):
    aliases = tmp_path / 'aliases.json'
    aliases.write_text('{"DIRECTO":"CITRICOS","DIRECTOCHF":"CITRICOS","INDUSTRIA":"CITRICOS"}', encoding='utf-8')
    return VarietySelectionResolver(VarietyRepository(conn), aliases_path=aliases, log_path=tmp_path / 'variety_resolution.log')


def test_normalize_variety_token_collapses_case_and_spaces():
    assert normalize_variety_token(' lane  late ') == 'LANE LATE'


def test_lane_late_directo_resolves_as_exact_variety_not_group(tmp_path):
    conn = make_conn()
    res = resolver(conn, tmp_path).resolve('DIRECTO', 'LANE LATE')
    assert res.kind == VarietySelectionKind.VARIETY
    assert res.selected_varieties == ('LANE LATE',)
    assert 'NAVELINA' not in res.selected_varieties
    assert 'CHISLETT SUMER' not in res.selected_varieties
    assert res.warnings == ()


def test_group_resolves_all_group_varieties(tmp_path):
    conn = make_conn()
    res = resolver(conn, tmp_path).resolve('CITRICOS', 'BLANCA TEMPRANA')
    assert res.kind == VarietySelectionKind.VARIETY  # exact variety priority on ambiguity
    assert res.selected_varieties == ('BLANCA TEMPRANA',)
    assert 'Ambiguous value resolved as exact variety.' in res.warnings


def test_group_when_no_exact_variety(tmp_path):
    conn = make_conn()
    conn.execute("DELETE FROM eepp.MVariedad WHERE Variedad='BLANCA TEMPRANA'")
    res = resolver(conn, tmp_path).resolve('CITRICOS', 'BLANCA TEMPRANA')
    assert res.kind == VarietySelectionKind.GROUP
    assert res.selected_varieties == ('SALUSTIANA',)


def test_not_found_has_single_warning(tmp_path):
    conn = make_conn()
    res = resolver(conn, tmp_path).resolve('CITRICOS', 'VARIEDAD INEXISTENTE')
    assert res.kind == VarietySelectionKind.NOT_FOUND
    assert len(res.warnings) == 1


def test_delivery_filter_for_lane_late_only_includes_lane_late(tmp_path):
    conn = make_conn()
    conn.execute('CREATE TABLE PesosFres(CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, Fcarga TEXT, Reg INTEGER, IdSocio INTEGER, Variedad TEXT, Categoria TEXT, Neto REAL, NetoPartida REAL, Albaran TEXT, Boleta TEXT, Plataforma TEXT, Liquidado INTEGER, Coste_Recoleccion TEXT, SSocialRecoleccion TEXT, Manijeria TEXT, Coste_Trans TEXT)')
    conn.execute('CREATE TABLE eepp.DSocio(IdSocio INTEGER, Nombre TEXT)')
    rows = [('2026','1','DIRECTO','2026-01-01',i,1544,v,'A',10,0,'','','',0,'0','0','0','0') for i,v in enumerate(['NAVELINA','LANE LATE','CHISLETT SUMER'], start=1)]
    conn.executemany('INSERT INTO PesosFres VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', rows)
    res = resolver(conn, tmp_path).resolve('DIRECTO', 'LANE LATE')
    filters = DeliveryFilter(WorkContext('2026','1','DIRECTO'), Period(date(2026,1,1), date(2026,1,2)), list(res.selected_varieties), '1544')
    deliveries, *_ = DeliveriesRepository(conn).fetch(filters)
    assert [d.variedad for d in deliveries] == ['LANE LATE']


def test_batch_uses_same_variety_group_service_resolution(tmp_path):
    conn = make_conn()
    conn.execute("DELETE FROM eepp.MVariedad WHERE Variedad='BLANCA TEMPRANA'")
    service = VarietyGroupService(VarietyRepository(conn))
    one = service.resolve_selection('DIRECTO', 'LANE LATE')
    two = service.resolve_selection('DIRECTO', 'BLANCA TEMPRANA')
    assert one.varieties == ('LANE LATE',)
    assert two.varieties == ('SALUSTIANA',)


def test_mixed_output_crops_resolve_varieties_in_their_matching_master(tmp_path):
    conn = make_conn()
    conn.executemany('INSERT INTO eepp.MVariedad VALUES(?,?,?,?,?,?,?,?,?)', [
        (6, 'MANDARINA', 'TANGO', 'MANDARINA', 'MEDIA', None, None, None, None),
        (7, 'MANDARINA', 'NADORCOTT', 'MANDARINA', 'TARDIA', None, None, None, None),
    ])
    selection_resolver = resolver(conn, tmp_path)
    for crop, variety in [('DIRECTO', 'TANGO'), ('DIRECTOCHF', 'TANGO'), ('INDUSTRIA', 'TANGO'), ('DIRECTO', 'NADORCOTT')]:
        result = selection_resolver.resolve(crop, variety)
        assert result.kind == VarietySelectionKind.VARIETY
        assert result.resolved_master_crop == 'MANDARINA'
        assert result.candidate_master_crops == ('CITRICOS', 'MANDARINA')


def test_citricos_does_not_resolve_mandarin_variety(tmp_path):
    conn = make_conn()
    conn.execute('INSERT INTO eepp.MVariedad VALUES(?,?,?,?,?,?,?,?,?)', (6, 'MANDARINA', 'TANGO', 'MANDARINA', 'MEDIA', None, None, None, None))
    assert resolver(conn, tmp_path).resolve('CITRICOS', 'TANGO').kind == VarietySelectionKind.NOT_FOUND


def test_mixed_match_in_multiple_masters_is_ambiguous(tmp_path):
    conn = make_conn()
    conn.execute('INSERT INTO eepp.MVariedad VALUES(?,?,?,?,?,?,?,?,?)', (6, 'MANDARINA', 'COMPARTIDA', 'MANDARINA', 'MEDIA', None, None, None, None))
    conn.execute('INSERT INTO eepp.MVariedad VALUES(?,?,?,?,?,?,?,?,?)', (7, 'CITRICOS', 'COMPARTIDA', 'NAVEL', 'MEDIA', None, None, None, None))
    result = resolver(conn, tmp_path).resolve('DIRECTO', 'COMPARTIDA')
    assert result.kind == VarietySelectionKind.AMBIGUOUS
    assert 'CITRICOS, MANDARINA' in result.warnings[0]


def test_mixed_options_are_combined_without_duplicates(tmp_path):
    conn = make_conn()
    conn.execute('INSERT INTO eepp.MVariedad VALUES(?,?,?,?,?,?,?,?,?)', (6, 'MANDARINA', 'TANGO', 'MANDARINA', 'MEDIA', None, None, None, None))
    service = VarietyGroupService(VarietyRepository(conn))
    options = service.list_selection_options('DIRECTO')
    assert 'TANGO' in options
    assert len(options) == len({normalize_variety_token(option) for option in options})


def test_resolution_log_records_candidates_and_resolved_master(tmp_path):
    conn = make_conn()
    conn.execute('INSERT INTO eepp.MVariedad VALUES(?,?,?,?,?,?,?,?,?)', (6, 'MANDARINA', 'TANGO', 'MANDARINA', 'MEDIA', None, None, None, None))
    selection_resolver = resolver(conn, tmp_path)
    selection_resolver.resolve('DIRECTO', 'TANGO')
    log = (tmp_path / 'variety_resolution.log').read_text(encoding='utf-8')
    assert 'candidate_master_crops=CITRICOS,MANDARINA' in log
    assert 'resolved_master_crop=MANDARINA' in log
