from decimal import Decimal
import sqlite3
import re

from data.group_benchmark_repository import ProductiveSurfaceResult, VarietalGroup
from domain.calculation_models import LiquidationHeader, MemberLiquidation
from services.group_benchmark_service import GroupBenchmarkService


def header():
    return LiquidationHeader(1,"REM","2026","1","CITRICOS","","","","Normal","Primera","",["NAVELINA"],{}, {})


def member(mid, variety, kg, amount, price=None):
    return MemberLiquidation(mid, f"Socio {mid}", variety, 1, Decimal(kg), Decimal(kg), Decimal("0"), Decimal("0"), (), Decimal("0"), effective_net_kg=Decimal(kg), total_amount=Decimal(amount) if amount is not None else None, final_average_price=Decimal(price) if price is not None else (Decimal(amount)/Decimal(kg) if Decimal(kg) != 0 and amount is not None else None))


class FakeRepo:
    group = VarietalGroup("CITRICOS", "NAVEL", "TEMPRANA", "NAVEL TEMPRANA", ("FUKUMOTO", "NAVELINA", "NEWHALL"))
    hectares = {1: Decimal("2"), 2: Decimal("10"), 3: Decimal("0")}
    def get_varietal_group(self, crop, variety):
        if variety == "NOEXISTE":
            return None
        return self.group
    def get_productive_hectares(self, member_id, campaign, company, crop, varieties):
        return ProductiveSurfaceResult(self.hectares.get(member_id, Decimal("0")), 1, 0, (), ())


def service():
    return GroupBenchmarkService(FakeRepo(), log_path="/tmp/group_benchmark_test.log")


def test_group_with_several_varieties_aggregates_member_kg_and_weighted_price():
    benchmarks = service().build_benchmarks(header(), (
        member(1, "NAVELINA", "40000", "16000"),
        member(1, "NEWHALL", "20000", "8000"),
        member(2, "FUKUMOTO", "100000", "35000"),
    ))
    b = benchmarks[(1, "NAVEL TEMPRANA", "2026", "1", "CITRICOS", "Normal", "Primera")]
    assert b.varieties == ("FUKUMOTO", "NAVELINA", "NEWHALL")
    assert b.price_per_kg.average_value == Decimal("0.37500")  # media simple de precio medio final por socio válido
    assert b.kilograms_per_hectare.own_value == Decimal("30000.00000")
    assert b.kilograms_per_hectare.average_value == Decimal("20000.00000")  # media de kg/ha por socio válido
    assert b.euros_per_hectare.own_value == Decimal("12000.00000")
    assert b.euros_per_hectare.average_value == Decimal("7750.00000")  # media de €/ha por socio válido


def test_without_surface_keeps_price_available_and_surface_metrics_unavailable():
    benchmarks = service().build_benchmarks(header(), (member(3, "NAVELINA", "10000", "4000"),))
    b = benchmarks[(3, "NAVEL TEMPRANA", "2026", "1", "CITRICOS", "Normal", "Primera")]
    assert b.price_per_kg.own_value == Decimal("0.40000")
    assert b.kilograms_per_hectare.own_value is None
    assert b.euros_per_hectare.own_value is None
    assert "superficie productiva válida" in b.euros_per_hectare.warning


def test_group_not_found_omits_benchmark():
    assert service().build_benchmarks(header(), (member(1, "NOEXISTE", "1", "1"),)) == {}


def test_statistical_values_use_same_collection_for_price():
    benchmarks = service().build_benchmarks(header(), (member(1,'NAVELINA','1','0.20'), member(2,'NAVELINA','1','0.30'), member(4,'NAVELINA','1','0.40')))
    b=benchmarks[(1,'NAVEL TEMPRANA','2026','1','CITRICOS','Normal','Primera')]
    assert b.price_per_kg.minimum_value == Decimal('0.20000')
    assert b.price_per_kg.average_value == Decimal('0.30000')
    assert b.price_per_kg.maximum_value == Decimal('0.40000')


def test_production_and_amount_exclude_zero_values():
    repo=FakeRepo(); repo.hectares={1:Decimal('1'),2:Decimal('1'),4:Decimal('1')}
    svc=GroupBenchmarkService(repo, log_path='/tmp/group_benchmark_test.log')
    benchmarks=svc.build_benchmarks(header(), (member(1,'NAVELINA','0','0', price='0'), member(2,'NAVELINA','5000','4000'), member(4,'NAVELINA','10000','8000')))
    b=benchmarks[(2,'NAVEL TEMPRANA','2026','1','CITRICOS','Normal','Primera')]
    assert b.kilograms_per_hectare.minimum_value == Decimal('5000.00000')
    assert b.kilograms_per_hectare.average_value == Decimal('7500.00000')
    assert b.euros_per_hectare.minimum_value == Decimal('4000.00000')
    assert b.euros_per_hectare.average_value == Decimal('6000.00000')


def test_only_zero_metric_unavailable():
    repo=FakeRepo(); repo.hectares={1:Decimal('1'),2:Decimal('1')}
    svc=GroupBenchmarkService(repo, log_path='/tmp/group_benchmark_test.log')
    b=svc.build_benchmarks(header(), (member(1,'NAVELINA','0','0', price='0'), member(2,'NAVELINA','0','0', price='0')))[(1,'NAVEL TEMPRANA','2026','1','CITRICOS','Normal','Primera')]
    assert b.kilograms_per_hectare.status == 'unavailable'
    assert b.euros_per_hectare.status == 'unavailable'


def test_final_price_excludes_zero_null_negative_and_invalid_values():
    repo=FakeRepo(); repo.hectares={1:Decimal('1'),2:Decimal('1'),3:Decimal('1'),4:Decimal('1'),5:Decimal('1'),6:Decimal('1')}
    svc=GroupBenchmarkService(repo, log_path='/tmp/group_benchmark_test.log')
    benchmarks=svc.build_benchmarks(header(), (
        member(1,'NAVELINA','100','50'),
        member(2,'NAVELINA','0','50', price='0'),
        member(3,'NAVELINA','100','0', price='0'),
        member(4,'NAVELINA','0','0', price='0'),
        member(5,'NAVELINA','100',None, price='0'),
        member(6,'NAVELINA','-100','50', price='0'),
    ))
    b=benchmarks[(1,'NAVEL TEMPRANA','2026','1','CITRICOS','Normal','Primera')]
    assert b.price_per_kg.minimum_value == Decimal('0.50000')
    assert b.price_per_kg.average_value == Decimal('0.50000')
    assert b.price_per_kg.maximum_value == Decimal('0.50000')
    assert b.price_per_kg.valid_member_count == 1
    assert b.price_per_kg.excluded_member_count == 5


def test_final_price_multiple_valid_records_min_average_max():
    benchmarks = service().build_benchmarks(header(), (member(1,'NAVELINA','100','20'), member(2,'NAVELINA','100','30'), member(4,'NAVELINA','100','40')))
    b=benchmarks[(1,'NAVEL TEMPRANA','2026','1','CITRICOS','Normal','Primera')]
    assert b.price_per_kg.minimum_value == Decimal('0.20000')
    assert b.price_per_kg.average_value == Decimal('0.30000')
    assert b.price_per_kg.maximum_value == Decimal('0.40000')


def test_final_price_without_valid_records_does_not_return_zero_minimum():
    repo=FakeRepo(); repo.hectares={1:Decimal('1'),2:Decimal('1')}
    svc=GroupBenchmarkService(repo, log_path='/tmp/group_benchmark_test.log')
    b=svc.build_benchmarks(header(), (member(1,'NAVELINA','0','0', price='0'), member(2,'NAVELINA','100','0', price='0')))[(1,'NAVEL TEMPRANA','2026','1','CITRICOS','Normal','Primera')]
    assert b.price_per_kg.status == 'unavailable'
    assert b.price_per_kg.minimum_value is None
    assert b.price_per_kg.warning == 'Sin datos comparables suficientes'


def test_surface_audit_sums_distinct_physical_parcel_rows(tmp_path):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("ATTACH DATABASE ':memory:' AS eepp")
    conn.execute('CREATE TABLE eepp.DEEPP (IdSocio, Boleta, "CAMPAÑA", EMPRESA, CULTIVO, Variedad)')
    conn.execute('CREATE TABLE eepp.DParcela (Boleta, "CAMPAÑA", EMPRESA, CULTIVO, IdPM, Pol, Par, Rec, SupCul, BAJA)')
    conn.executemany('INSERT INTO eepp.DEEPP VALUES (?,?,?,?,?,?)', (
        (7, 659, "2026", "1", "CITRICOS", " navelina "),
        (7, 660, "2026", "1", "CITRICOS", "OTRA"),
        (7, 698, "2026", "1", "CITRICOS", "NEWHALL"),
        (7, 699, "2026", "1", "CITRICOS", "NAVELINA"),
    ))
    conn.executemany('INSERT INTO eepp.DParcela VALUES (?,?,?,?,?,?,?,?,?,?)', (
        (659, "2026", "1", "CITRICOS", 1, 2, 3, 4, "0.6500", None),
        (659, "2026", "1", "CITRICOS", 1, 2, 3, 4, "0.6500", None),
        (699, "2026", "1", "CITRICOS", 5, 6, 7, 8, "1.2", None),
        (699, "2026", "1", "CITRICOS", 5, 6, 7, 8, "1.3", None),
    ))
    log = tmp_path / "surface.log"
    from data.group_benchmark_repository import GroupBenchmarkRepository
    result = GroupBenchmarkRepository(conn, log).get_productive_hectares(
        7, "2026", "1", "CITRICOS", ("NAVELINA", "NEWHALL")
    )

    assert result.hectares == Decimal("3.8000")
    assert result.parcel_count == 4
    assert result.parcel_row_count == 4
    assert result.excluded_count == 0
    assert result.status == "PARTIAL_SURFACE"
    assert result.candidate_boletas == ("659", "660", "698", "699")
    assert result.matched_boletas == ("659", "698", "699")
    assert result.included_boletas == ("659", "699")
    assert any(r.get("incident_type") == "VARIEDAD_NO_COINCIDE" and r["boleta"] == "660" for r in result.audit_rows)
    assert any(r.get("incident_type") == "BOLETA_SIN_PARCELAS" and r["boleta"] == "698" for r in result.audit_rows)
    text = log.read_text(encoding="utf-8")
    for section in (
        "ProductiveSurfaceQuery",
        "ProductiveSurfaceDeeppCandidate",
        "ProductiveSurfaceJoinRow",
        "ProductiveSurfaceRowDecision",
        "ProductiveSurfaceBoletaCalculation",
        "ProductiveSurfaceBoletaSummary",
        "ProductiveSurfaceResult",
    ):
        assert f"[{section}]" in text
    assert "varieties_original=NAVELINA|NEWHALL" in text
    assert "varieties_normalized=NAVELINA|NEWHALL" in text
    assert "boleta=660\nvariety_original=OTRA\nvariety_normalized=OTRA\nmatches_group=no" in text
    assert "excluded_conflicting_surfaces" not in text
    assert "hectares=3.8000" in text


def test_member_production_audit_contains_kilos_hectares_and_ratio(tmp_path):
    log = tmp_path / "surface.log"
    svc = GroupBenchmarkService(FakeRepo(), log_path=tmp_path / "metrics.log", audit_log_path=log)
    svc.build_benchmarks(header(), (member(1, "NAVELINA", "112745", "1"),))
    text = log.read_text(encoding="utf-8")
    assert "[GroupBenchmarkMemberAggregation]" in text
    assert "total_net_kg=112745" in text
    assert "[GroupBenchmarkMemberProduction]" in text
    assert "surface_hectares=2" in text
    assert "production_kg_ha=56372.5" in text


def test_real_case_production_formula_reference():
    production = Decimal("112745") / Decimal("7.2337")
    assert production.quantize(Decimal("0.00001")) == Decimal("15586.07628")


def test_audit_configuration_is_absolute_and_independent_of_cwd(tmp_path, monkeypatch):
    from group_benchmark_surface_audit import AUDIT_LOG_PATH, record_surface_audit_config

    original = AUDIT_LOG_PATH
    monkeypatch.chdir(tmp_path)
    assert AUDIT_LOG_PATH == original
    assert AUDIT_LOG_PATH.is_absolute()
    log = tmp_path / "nested" / "group_benchmark_surface_audit.log"
    record_surface_audit_config(log)
    text = log.read_text(encoding="utf-8")
    assert "[SurfaceAuditConfig]" in text
    assert f"resolved_path={log.resolve()}" in text
    assert f"working_directory={tmp_path}" in text


def test_two_benchmark_executions_have_distinct_run_ids_and_completion(tmp_path):
    log = tmp_path / "group_benchmark_surface_audit.log"
    svc = GroupBenchmarkService(FakeRepo(), log_path=tmp_path / "metrics.log", audit_log_path=log)
    rows = (member(1, "NAVELINA", "112745", "1"),)
    svc.build_benchmarks(header(), rows)
    svc.build_benchmarks(header(), rows)

    text = log.read_text(encoding="utf-8")
    run_ids = re.findall(r"\[GroupBenchmarkAuditRun\]\nrun_id=([^\n]+)", text)
    assert len(run_ids) == 2
    assert run_ids[0] != run_ids[1]
    assert text.count("[GroupBenchmarkAuditRunCompleted]") == 2
    assert "member_variety=NAVELINA" in text
    assert "group_label=NAVEL TEMPRANA" in text


def _surface_repo(tmp_path, deepp_rows, parcel_rows):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("ATTACH DATABASE ':memory:' AS eepp")
    conn.execute('CREATE TABLE eepp.DEEPP (IdSocio, Boleta, "CAMPAÑA", EMPRESA, CULTIVO, Variedad)')
    conn.execute('CREATE TABLE eepp.DParcela (Boleta, "CAMPAÑA", EMPRESA, CULTIVO, IdPM, Pol, Par, Rec, SupCul, BAJA)')
    conn.executemany('INSERT INTO eepp.DEEPP VALUES (?,?,?,?,?,?)', deepp_rows)
    conn.executemany('INSERT INTO eepp.DParcela VALUES (?,?,?,?,?,?,?,?,?,?)', parcel_rows)
    from data.group_benchmark_repository import GroupBenchmarkRepository
    return GroupBenchmarkRepository(conn, tmp_path / "surface.log").get_productive_hectares(
        7, "2026", "1", "CITRICOS", ("NAVELINA",)
    )


def _deepp(boleta):
    return (7, boleta, "2026", "1", "CITRICOS", "NAVELINA")


def _parcel(boleta, surface, polygon=1, parcel=1, enclosure=1):
    return (boleta, "2026", "1", "CITRICOS", 1, polygon, parcel, enclosure, surface, None)


def test_one_boleta_with_two_cadastral_rows_sums_both_surfaces(tmp_path):
    result = _surface_repo(tmp_path, [_deepp(47)], [
        _parcel(47, "1.01", parcel=1), _parcel(47, "1.18", parcel=2),
    ])
    assert result.hectares == Decimal("2.19")
    assert result.parcel_row_count == 2
    assert result.excluded_count == 0
    assert result.status == "OK"
    assert not any(r.get("decision") == "excluded_conflicting_surfaces" for r in result.audit_rows)


def test_duplicate_join_rows_are_counted_once_by_parcela_rowid(tmp_path):
    result = _surface_repo(tmp_path, [_deepp(47), _deepp(47)], [_parcel(47, "1.01")])
    assert result.hectares == Decimal("1.01")
    assert result.parcel_row_count == 1
    assert sum(r.get("decision") == "DUPLICATE_JOIN_ROW" for r in result.audit_rows) == 1


def test_three_parcels_and_enclosures_on_one_boleta_are_added(tmp_path):
    result = _surface_repo(tmp_path, [_deepp(100)], [
        _parcel(100, "1.20", parcel=1), _parcel(100, "0.35", parcel=2),
        _parcel(100, "0.80", parcel=2, enclosure=2),
    ])
    assert result.hectares == Decimal("2.35")
    calculation = next(r for r in result.audit_rows if r.get("audit_type") == "row_decision")
    assert calculation["decision"] == "INCLUDED"


def test_missing_surfaces_return_explicit_missing_data_status(tmp_path):
    result = _surface_repo(tmp_path, [_deepp(200)], [
        _parcel(200, None, parcel=1), _parcel(200, "0", parcel=2),
    ])
    assert result.hectares == 0
    assert result.status == "MISSING_SURFACE_DATA"
    assert result.invalid_row_count == 2
    assert "deben ser completados por el usuario" in result.warnings[0]
    production_kg_ha = Decimal("1000") / result.hectares if result.hectares > 0 else None
    assert production_kg_ha is None


def test_valid_and_missing_boletas_return_partial_surface(tmp_path):
    result = _surface_repo(tmp_path, [_deepp(300), _deepp(301)], [_parcel(300, "2.10")])
    assert result.hectares == Decimal("2.10")
    assert result.status == "PARTIAL_SURFACE"
    assert result.missing_surface_boletas == ("301",)
    assert result.included_boletas == ("300",)
