from dataclasses import replace
from decimal import Decimal
import json

import pytest

from domain.calculation_models import GradeBreakdown, LiquidationHeader, MemberLiquidation
from presentation.liquidation_document_snapshot import SCHEMA_VERSION, dump, load
from presentation.premium_liquidation_view_model import from_member_liquidation


def header(pdestrio, pdmesa, ppodrido):
    return LiquidationHeader(2192, "Nadorcott Semana 3 NORMAL", "2026", "1", "CITRICOS", "", "", "", "NORMAL", "", "", ["MANDARINA"], {}, {
        "PDESTRIO": Decimal(pdestrio), "PDMESA": Decimal(pdmesa), "PPODRIDO": Decimal(ppodrido),
    })


def member(member_id=1, rotten_kg="136", rotten_amount="8.18", secondary_kg="37", secondary_amount="2.22"):
    return MemberLiquidation(
        member_id, f"Socio {member_id}", "MANDARINA", 1, Decimal("200"), Decimal("100"),
        Decimal(secondary_kg), Decimal(rotten_kg),
        (GradeBreakdown("1", "CAL 1", Decimal("40"), Decimal("0.5"), Decimal("20")),
         GradeBreakdown("2", "CAL 2", Decimal("60"), Decimal("0.8"), Decimal("48"))),
        Decimal("68"), destruction_amount=Decimal(secondary_amount), rotten_amount=Decimal(rotten_amount),
        # Poison the copied member values: the document must still use its header.
        destruction_price=Decimal("9"), table_destruction_price=Decimal("9"), rotten_price=Decimal("9"),
        national_market_price=Decimal("9"), rotten_leaves_price=Decimal("9"),
        gross_amount=Decimal("78.40"), effective_net_kg=Decimal("200"), commercial_average_price=Decimal("0.68"),
    )


@pytest.mark.parametrize("member_id,kg,amount", [(1, "6", "0.36"), (2, "26", "1.56"), (3, "136", "8.18"), (4, "2481", "148.92")])
def test_nadorcott_uses_header_price_not_rounded_amount_divided_by_kg(member_id, kg, amount):
    vm = from_member_liquidation(header("0.06002456", "0.06002456", "0.06002456"), member(member_id, kg, amount))
    assert vm.national_market_price == vm.secondary_price == Decimal("0.06002")
    assert vm.rotten_leaves_price == vm.waste_price == Decimal("0.06002")
    assert vm.waste_price != Decimal(amount) / Decimal(kg)
    assert vm.commercial_average_price == Decimal("0.68")


@pytest.mark.parametrize("prices,expected", [
    (("0.05722954", "0.05722954", "0.05722954"), (Decimal("0.05723"), Decimal("0.05723"))),
    (("0.145", "0.145", "-0.129"), (Decimal("0.14500"), Decimal("-0.12900"))),
])
def test_tango_and_citrus_fixed_prices_are_identical_for_every_member(prices, expected):
    for i, kg in enumerate(("6", "26", "37", "85"), 1):
        vm = from_member_liquidation(header(*prices), member(i, kg, "8.18"))
        assert (vm.secondary_price, vm.waste_price) == expected


def test_missing_or_mismatched_header_fixed_prices_blocks_new_document(caplog):
    with pytest.raises(ValueError, match="Falta el precio fijo"):
        from_member_liquidation(replace(header("0", "0", "0"), prices={}), member())
    with pytest.raises(ValueError, match="no coinciden"):
        from_member_liquidation(header("0.145", "0.144", "-0.129"), member())
    assert "[FixedRemittancePriceMismatch]" in caplog.text


def test_new_snapshot_keeps_fixed_prices_and_never_uses_amount_division():
    vm = from_member_liquidation(header("0.06002456", "0.06002456", "0.06002456"), member())
    raw = json.loads(dump(vm))
    assert raw["schema_version"] == SCHEMA_VERSION == 4
    raw["model"].update(secondary_amount="999", secondary_kg="1", waste_amount="999", waste_kg="1",
                        secondary_price="999", waste_price="999", destruction_price="999", rotten_price="999")
    restored = load(json.dumps(raw))
    assert restored.secondary_price == restored.national_market_price == Decimal("0.06002")
    assert restored.waste_price == restored.rotten_leaves_price == Decimal("0.06002")


def test_new_snapshot_without_explicit_fixed_prices_is_rejected_not_derived():
    raw = json.loads(dump(from_member_liquidation(header("0.145", "0.145", "-0.129"), member())))
    raw["model"].pop("national_market_price")
    with pytest.raises(ValueError, match="Snapshot nuevo sin precios fijos"):
        load(json.dumps(raw))


@pytest.mark.parametrize("field", ["national_market_price", "rotten_leaves_price"])
def test_dump_rejects_new_snapshot_with_missing_fixed_price(field):
    vm = from_member_liquidation(header("0.145", "0.145", "-0.129"), member())
    with pytest.raises(ValueError, match="No se puede crear un snapshot v4"):
        dump(replace(vm, **{field: None}))


def test_dump_normalizes_all_fixed_price_aliases():
    vm = from_member_liquidation(header("0.145", "0.145", "-0.129"), member())
    vm = replace(vm, destruction_price=Decimal("9"), secondary_price=Decimal("8"),
                 rotten_price=Decimal("7"), waste_price=Decimal("6"))
    restored = load(dump(vm))
    assert restored.destruction_price == restored.secondary_price == Decimal("0.14500")
    assert restored.rotten_price == restored.waste_price == Decimal("-0.12900")
