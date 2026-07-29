from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
import json
from pathlib import Path

import pytest

from data.persistence.json_serialization import to_json_compatible
from presentation.liquidation_document_snapshot import dump, load
from presentation.premium_liquidation_view_model import PremiumLiquidationViewModel
from services.group_benchmark_service import BenchmarkMetric, PremiumGroupBenchmark


class State(Enum):
    READY = "ready"


@dataclass(frozen=True)
class Nested:
    amount: Decimal


def _vm():
    metric = BenchmarkMetric(Decimal("0.31660"), Decimal("0.4"), Decimal("0.25"), Decimal("0.35455"), 2, 0, "ok", metric="FINAL_PRICE")
    benchmark = PremiumGroupBenchmark("Grupo ñ", "CITRICOS", "G", "SG", ("NÁVEL",), "2026", "1", "NORMAL", "A", metric, metric, metric)
    values = {field: (None if "amount" in field or "price" in field or "rate" in field else Decimal("1.20")) for field in PremiumLiquidationViewModel.__dataclass_fields__}
    values.update(member_id=1, member_name="José", tax_id_masked=None, remittance_name="Remesa", campaign="2026", company="1", crop="CITRICOS", varieties=("NÁVEL",), period_from="", period_to="", payment_date=None, commercial_breakdown=(), group_benchmark=benchmark, id_liqs=("CI1",), secondary_enabled=False, secondary_counts_as_commercial=False, primary_label="P", secondary_label=None, waste_label="W", commercial_breakdown_title="",)
    return PremiumLiquidationViewModel(**values)


def test_json_compatible_preserves_domain_structure_and_scalar_rules():
    value = to_json_compatible({"nested": Nested(Decimal("1.2300")), "state": State.READY, "when": datetime(2026, 1, 2, 3, 4), "path": Path("José"), "items": (1, {2})})
    assert value == {"nested": {"amount": "1.2300"}, "state": "ready", "when": "2026-01-02T03:04:00", "path": "José", "items": [1, [2]]}
    assert "José" in json.dumps(value, ensure_ascii=False)


def test_snapshot_round_trip_reconstructs_benchmark_decimals():
    original = _vm()
    restored = load(dump(original))
    assert restored.group_benchmark == original.group_benchmark
    assert restored.group_benchmark.price_per_kg.average_value == Decimal("0.35455")


def test_unknown_snapshot_type_fails_clearly():
    with pytest.raises(TypeError, match="Tipo no compatible con snapshot JSON: object"):
        to_json_compatible(object())
