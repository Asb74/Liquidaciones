"""Versioned JSON snapshot for the immutable liquidation PDF model."""
from __future__ import annotations

from dataclasses import fields
from decimal import Decimal
import json

from presentation.premium_liquidation_view_model import CommercialBreakdownRow, PremiumLiquidationViewModel
from services.group_benchmark_service import BenchmarkMetric, PremiumGroupBenchmark

SCHEMA_VERSION = 1


def _encode(value):
    if isinstance(value, Decimal): return {"$decimal": str(value)}
    if isinstance(value, tuple): return [_encode(item) for item in value]
    if isinstance(value, list): return [_encode(item) for item in value]
    if isinstance(value, dict): return {key: _encode(item) for key, item in value.items()}
    return value


def _decode(value):
    if isinstance(value, list): return tuple(_decode(item) for item in value)
    if isinstance(value, dict):
        if set(value) == {"$decimal"}: return Decimal(value["$decimal"])
        return {key: _decode(item) for key, item in value.items()}
    return value


def dump(vm: PremiumLiquidationViewModel) -> str:
    payload={name: _encode(getattr(vm, name)) for name in (field.name for field in fields(vm)) if name not in {"group_benchmark", "commercial_breakdown"}}
    payload["commercial_breakdown"]=[_encode({field.name: getattr(row, field.name) for field in fields(row)}) for row in vm.commercial_breakdown]
    benchmark=vm.group_benchmark
    if benchmark:
        payload["group_benchmark"]={name: _encode(({field.name: getattr(getattr(benchmark, name), field.name) for field in fields(getattr(benchmark, name))} if name in {"price", "production", "income"} else getattr(benchmark, name))) for name in (field.name for field in fields(benchmark))}
    return json.dumps({"schema_version": SCHEMA_VERSION, "model": payload}, ensure_ascii=False, sort_keys=True)


def load(payload_json: str) -> PremiumLiquidationViewModel:
    raw=json.loads(payload_json)
    if raw.get("schema_version") != SCHEMA_VERSION: raise ValueError("Versión de snapshot documental no compatible")
    payload=_decode(raw["model"])
    payload["commercial_breakdown"]=tuple(CommercialBreakdownRow(**row) if isinstance(row, dict) else row for row in payload["commercial_breakdown"])
    benchmark=payload.get("group_benchmark")
    if benchmark:
        for metric in ("price", "production", "income"):
            benchmark[metric]=BenchmarkMetric(**benchmark[metric]) if isinstance(benchmark[metric], dict) else benchmark[metric]
        payload["group_benchmark"]=PremiumGroupBenchmark(**benchmark)
    return PremiumLiquidationViewModel(**payload)
