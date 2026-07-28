from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

@dataclass(frozen=True, order=True)
class BenchmarkScope:
    campaign: str; company: str; variety_group_code: str

@dataclass(frozen=True)
class PersistedMemberBenchmark:
    recipient_member_id: int; member_name: str; commercial_kg: Decimal; final_amount: Decimal
    surface_hectares: Decimal | None; final_average_price: Decimal | None
    production_kg_ha: Decimal | None; final_amount_eur_ha: Decimal | None

@dataclass(frozen=True)
class PersistedBenchmarkMetric:
    current: Decimal | None; maximum: Decimal | None; average: Decimal | None; minimum: Decimal | None
    percentage_difference: Decimal | None; comparable_count: int

@dataclass(frozen=True)
class VarietyGroupBenchmark:
    scope: BenchmarkScope; comparable_members: tuple[PersistedMemberBenchmark, ...]
    price_metric: PersistedBenchmarkMetric; production_metric: PersistedBenchmarkMetric
    final_amount_metric: PersistedBenchmarkMetric; generated_at: datetime; source_fingerprint: str
