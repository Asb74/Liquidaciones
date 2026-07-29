"""Versioned JSON snapshot for the immutable liquidation PDF model."""
from __future__ import annotations

from dataclasses import fields
from decimal import Decimal
import json
import logging

from data.persistence.json_serialization import to_json_compatible
from presentation.premium_liquidation_view_model import CommercialBreakdownRow, PremiumLiquidationViewModel
from services.group_benchmark_service import BenchmarkMetric, PremiumGroupBenchmark

SCHEMA_VERSION = 3
logger = logging.getLogger(__name__)


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
    version = raw.get("schema_version")
    if version not in (1, 2, SCHEMA_VERSION): raise ValueError("Versión de snapshot documental no compatible")
    payload=dict(raw["model"])
    missing_fixed_prices = any(name not in payload for name in ("destruction_price", "rotten_price"))
    payload.setdefault("destruction_price", None)
    payload.setdefault("rotten_price", None)
    if missing_fixed_prices:
        if payload["destruction_price"] is None and payload.get("secondary_kg") and payload.get("secondary_amount") is not None:
            payload["destruction_price"] = str(Decimal(payload["secondary_amount"]) / Decimal(payload["secondary_kg"]))
            payload["secondary_price"] = payload["destruction_price"]
        if payload["rotten_price"] is None and payload.get("waste_kg") and payload.get("waste_amount") is not None:
            payload["rotten_price"] = str(Decimal(payload["waste_amount"]) / Decimal(payload["waste_kg"]))
            payload["waste_price"] = payload["rotten_price"]
        logger.warning(
            "[ProductionSummary] legacy snapshot without fixed remittance price; "
            "price derived from amount/kilograms"
        )
    # Version 1 remains readable so existing immutable documents can be
    # migrated from their persisted liquidation lines.
    for name in ("variety_code", "variety_name", "variety_group_code", "variety_group_name"):
        payload.setdefault(name, None)
    for name in ("applicable_hectares", "surface_group_code", "surface_group_name", "surface_source", "surface_fingerprint"):
        payload.setdefault(name, None)
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
