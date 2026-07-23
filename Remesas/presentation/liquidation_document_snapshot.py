"""Versioned JSON snapshot for the immutable liquidation PDF model."""
from __future__ import annotations

from dataclasses import fields
from decimal import Decimal
import json

from data.persistence.json_serialization import to_json_compatible
from presentation.premium_liquidation_view_model import CommercialBreakdownRow, PremiumLiquidationViewModel
from services.group_benchmark_service import BenchmarkMetric, PremiumGroupBenchmark

SCHEMA_VERSION = 1


def _decimal_fields(model_type):
    return {field.name for field in fields(model_type) if "Decimal" in str(field.type)}


def _restore_decimal_fields(payload, model_type):
    decimal_fields = _decimal_fields(model_type)
    return {
        name: (None if value is None else Decimal(value)) if name in decimal_fields else value
        for name, value in payload.items()
    }


def dump(vm: PremiumLiquidationViewModel) -> str:
    payload = to_json_compatible(vm)
    return json.dumps({"schema_version": SCHEMA_VERSION, "model": payload}, ensure_ascii=False,
                      sort_keys=True, separators=(",", ":"))


def load(payload_json: str) -> PremiumLiquidationViewModel:
    raw=json.loads(payload_json)
    if raw.get("schema_version") != SCHEMA_VERSION: raise ValueError("Versión de snapshot documental no compatible")
    payload=dict(raw["model"])
    payload["commercial_breakdown"]=tuple(
        CommercialBreakdownRow(**_restore_decimal_fields(row, CommercialBreakdownRow))
        for row in payload["commercial_breakdown"]
    )
    benchmark=payload.get("group_benchmark")
    if benchmark:
        for metric in ("price_per_kg", "kilograms_per_hectare", "euros_per_hectare"):
            benchmark[metric]=BenchmarkMetric(**_restore_decimal_fields(benchmark[metric], BenchmarkMetric))
        benchmark["varieties"] = tuple(benchmark["varieties"])
        benchmark["warnings"] = tuple(benchmark["warnings"])
        payload["group_benchmark"]=PremiumGroupBenchmark(**benchmark)
    return PremiumLiquidationViewModel(**_restore_decimal_fields(payload, PremiumLiquidationViewModel))
