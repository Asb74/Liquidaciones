from decimal import Decimal
from types import SimpleNamespace

from domain.persistence_models import PersistencePreview, SplitPreviewLine
from presentation.premium_liquidation_view_model import from_persistence_preview
from tests.test_premium_pdf import _header, _member


def _line(recipient, factor, net, gross, base, total, *, name=None):
    return SplitPreviewLine(
        1462, "RAFAEL GARCIA", recipient, name or str(recipient), "BLANCA", Decimal(factor),
        Decimal(net), Decimal(gross), Decimal("10"), Decimal("20"), Decimal("30"),
        Decimal("40"), Decimal("50"), Decimal(base), Decimal("12"), Decimal("2"),
        Decimal("405.08"), Decimal("67.51"), Decimal(total),
        Decimal(gross) / Decimal(net), Decimal(total) / Decimal(net), split_rule_id=7,
        split_type="PERCENTAGE",
    )


def test_post_split_models_use_final_recipient_lines_and_conserve_values():
    member = _member(
        member_id=1462, member_name="RAFAEL GARCIA", effective_net_kg=Decimal("24906"),
        net_deliveries=Decimal("24906"), gross_amount=Decimal("7023.49"),
        taxable_base=Decimal("6751.27"), total_amount=Decimal("7410.19"),
    )
    result = SimpleNamespace(header=_header(), member_results=(member,))
    lines = (
        _line(453, "0.5", "12453", "3511.74", "3375.63", "3705.09", name="SOCIO 453"),
        _line(1462, "0.5", "12453", "3511.75", "3375.64", "3705.10", name="RAFAEL GARCIA"),
    )
    preview = PersistencePreview(result.header, lines, "fingerprint", 1)

    documents = from_persistence_preview(result, preview)

    assert set(documents) == {453, 1462}
    assert documents[453].effective_net_kg == Decimal("12453")
    assert documents[453].gross_amount == Decimal("3511.74")
    assert documents[453].taxable_base == Decimal("3375.63")
    assert documents[453].total_amount == Decimal("3705.09")
    assert documents[1462].effective_net_kg == Decimal("12453")
    assert documents[1462].gross_amount == Decimal("3511.75")
    assert documents[1462].taxable_base == Decimal("3375.64")
    assert documents[1462].total_amount == Decimal("3705.10")
    assert sum((vm.effective_net_kg for vm in documents.values()), Decimal("0")) == Decimal("24906")
    assert sum((vm.total_amount for vm in documents.values()), Decimal("0")) == Decimal("7410.19")


def test_unsplit_model_keeps_original_kilograms():
    member = _member(member_id=1462, member_name="RAFAEL GARCIA", effective_net_kg=Decimal("24906"), net_deliveries=Decimal("24906"))
    result = SimpleNamespace(header=_header(), member_results=(member,))
    line = _line(1462, "1", "24906", "69923.10", "59055.89", "64819.75", name="RAFAEL GARCIA")
    line = line.__class__(**{**line.__dict__, "split_rule_id": None, "split_type": None})
    preview = PersistencePreview(result.header, (line,), "fingerprint", 1)

    assert from_persistence_preview(result, preview)[1462].effective_net_kg == Decimal("24906")
