from decimal import Decimal

from services.group_benchmark_population import (
    PopulationValue, benchmark_for_entry, group_benchmark_key, normalize_group_label,
)


def test_group_identity_is_normalized_and_never_contains_crop_or_variety():
    assert normalize_group_label(" Nável  Temprana ")=="NAVEL TEMPRANA"
    assert group_benchmark_key("2026","1","Navel Temprana")==group_benchmark_key("2026","1","NAVEL  TEMPRANA")


def test_mixed_crop_population_selects_real_extrema_for_both_metrics():
    # Entries represent CITRICOS, DIRECTO and INDUSTRIA respectively.  Crop is
    # deliberately absent from PopulationValue because it cannot partition stats.
    citricos=PopulationValue(1,101,Decimal("100"),Decimal("900"),Decimal("1"))
    directo=PopulationValue(2,102,Decimal("300"),Decimal("500"),Decimal("1"))
    industria=PopulationValue(3,103,Decimal("50"),Decimal("700"),Decimal("1"))
    population=(citricos,directo,industria)
    result=benchmark_for_entry(directo,population,template=None,group_label="NAVEL TEMPRANA",campaign="2026",company="1")
    assert result.kilograms_per_hectare.own_value==Decimal("300")
    assert result.kilograms_per_hectare.maximum_value==Decimal("300")  # DIRECTO
    assert result.kilograms_per_hectare.minimum_value==Decimal("50")   # INDUSTRIA
    assert result.euros_per_hectare.maximum_value==Decimal("900")     # CITRICOS


def test_same_member_multiple_liquidations_have_distinct_own_values_and_shared_stats():
    first=PopulationValue(10,1623,Decimal("15182.67"),Decimal("1000"),Decimal("1"))
    second=PopulationValue(11,1623,Decimal("9000"),Decimal("800"),Decimal("1"))
    population=(first,second)
    one=benchmark_for_entry(first,population,template=None,group_label="NAVEL TEMPRANA",campaign="2026",company="1")
    two=benchmark_for_entry(second,population,template=None,group_label="NAVEL TEMPRANA",campaign="2026",company="1")
    assert one.kilograms_per_hectare.own_value==Decimal("15182.67")
    assert two.kilograms_per_hectare.own_value==Decimal("9000")
    assert (one.kilograms_per_hectare.maximum_value,one.kilograms_per_hectare.minimum_value,one.kilograms_per_hectare.average_value)==(two.kilograms_per_hectare.maximum_value,two.kilograms_per_hectare.minimum_value,two.kilograms_per_hectare.average_value)
