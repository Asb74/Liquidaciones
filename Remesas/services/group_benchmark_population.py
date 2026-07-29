"""Shared identity and statistics rules for varietal-group benchmarks."""
from __future__ import annotations

from dataclasses import dataclass, is_dataclass, replace
from decimal import Decimal
import unicodedata

from services.benchmark_calculation import metric_statistics
from services.group_benchmark_service import BenchmarkMetric, PremiumGroupBenchmark


def normalize_group_label(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return " ".join(text.encode("ascii", "ignore").decode().upper().split())


def group_benchmark_key(campaign: object, company: object, group_label: object) -> tuple[str, str, str]:
    """Return the population boundary; crop is intentionally not part of it."""
    return str(campaign), str(company), normalize_group_label(group_label)


@dataclass(frozen=True)
class PopulationValue:
    document_id: object
    member_id: int
    kg_ha: Decimal | None
    euros_ha: Decimal | None
    price: Decimal | None


def benchmark_for_entry(entry: PopulationValue, population: tuple[PopulationValue, ...], *, template,
                        group_label: str, campaign: str, company: str) -> PremiumGroupBenchmark:
    """Create one document DTO with statistics shared by the whole population."""
    def metric(field, name):
        stats = metric_statistics(getattr(item, field) for item in population)
        return BenchmarkMetric(getattr(entry, field), stats.maximum, stats.minimum, stats.average,
                               stats.count, len(population)-stats.count,
                               "ok" if stats.count else "unavailable",
                               "" if stats.count else "Sin datos comparables suficientes", name)
    values = dict(price_per_kg=metric("price", "FINAL_PRICE"),
                  kilograms_per_hectare=metric("kg_ha", "PRODUCTION_KG_HA"),
                  euros_per_hectare=metric("euros_ha", "FINAL_AMOUNT_EUR_HA"))
    if template is not None and is_dataclass(template):
        return replace(template, group_label=group_label, campaign=str(campaign), company=str(company), **values)
    parts = group_label.split(maxsplit=1)
    return PremiumGroupBenchmark(group_label, "", parts[0], parts[1] if len(parts)>1 else "", (),
                                 str(campaign), str(company), "", "", warnings=(), **values)
