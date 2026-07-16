from decimal import Decimal

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
