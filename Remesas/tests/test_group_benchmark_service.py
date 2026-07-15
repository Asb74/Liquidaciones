from decimal import Decimal

from data.group_benchmark_repository import ProductiveSurfaceResult, VarietalGroup
from domain.calculation_models import LiquidationHeader, MemberLiquidation
from services.group_benchmark_service import GroupBenchmarkService


def header():
    return LiquidationHeader(1,"REM","2026","1","CITRICOS","","","","Normal","Primera","",["NAVELINA"],{}, {})


def member(mid, variety, kg, amount, price=None):
    return MemberLiquidation(mid, f"Socio {mid}", variety, 1, Decimal(kg), Decimal(kg), Decimal("0"), Decimal("0"), (), Decimal("0"), effective_net_kg=Decimal(kg), total_amount=Decimal(amount), final_average_price=Decimal(price) if price else Decimal(amount)/Decimal(kg))


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
    assert b.price_per_kg.average_value == Decimal("0.36875")  # 59000 / 160000
    assert b.kilograms_per_hectare.own_value == Decimal("30000.00000")
    assert b.kilograms_per_hectare.average_value == Decimal("13333.33333")  # SUM kg / SUM ha
    assert b.euros_per_hectare.own_value == Decimal("12000.00000")
    assert b.euros_per_hectare.average_value == Decimal("4916.66667")  # SUM final amount / SUM ha


def test_without_surface_keeps_price_available_and_surface_metrics_unavailable():
    benchmarks = service().build_benchmarks(header(), (member(3, "NAVELINA", "10000", "4000"),))
    b = benchmarks[(3, "NAVEL TEMPRANA", "2026", "1", "CITRICOS", "Normal", "Primera")]
    assert b.price_per_kg.own_value == Decimal("0.40000")
    assert b.kilograms_per_hectare.own_value is None
    assert b.euros_per_hectare.own_value is None
    assert "superficie productiva válida" in b.euros_per_hectare.warning


def test_group_not_found_omits_benchmark():
    assert service().build_benchmarks(header(), (member(1, "NOEXISTE", "1", "1"),)) == {}
