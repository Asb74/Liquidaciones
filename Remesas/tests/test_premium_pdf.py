from decimal import Decimal
from pathlib import Path

from domain.calculation_models import GradeBreakdown, LiquidationHeader, MemberLiquidation
from exporters.premium_pdf_exporter import export_premium_member_pdf
from services.group_benchmark_service import BenchmarkMetric, PremiumGroupBenchmark
from presentation.premium_liquidation_view_model import (
    format_kg, format_money, format_percent, format_signed_money, format_unit_price,
    from_member_liquidation, mask_tax_id, sanitize_filename,
)


def _header():
    return LiquidationHeader(1,"BLANCA TEMPRANA SEMANA 4 Y +","2026","SANSEBAS","CÍTRICOS","13/08/2026","10/12/2025","05/03/2026","Normal","Primera","",["BLANCA"],{}, {})


def _member(**kw):
    base = dict(
        member_id=1355, member_name="CANO MANZANARES, JUAN ANTONIO", variety="BLANCA", delivery_count=3,
        net_deliveries=Decimal("204739"), net_commercial=Decimal("185566"), net_waste=Decimal("18009"), net_rotten=Decimal("1165"),
        grades=(GradeBreakdown("1","I AA",Decimal("1000"),Decimal("0.37030"),Decimal("370.30")),),
        commercial_amount=Decimal("68722.69"), destruction_amount=Decimal("1350.68"), rotten_amount=Decimal("0"), gross_amount=Decimal("69923.10"),
        collection_amount=Decimal("13308.04"), hectare_fee_amount=Decimal("904.92"), quality_amount=Decimal("-10"), transport_amount=Decimal("12.30"), globalgap_amount=Decimal("3345.75"),
        taxable_base=Decimal("59055.89"), vat_rate=Decimal("12"), vat_amount=Decimal("7086.71"), withholding_rate=Decimal("2"), withholding_amount=Decimal("1322.85"),
        total_amount=Decimal("64819.75"), commercial_average_price=Decimal("0.37030"), final_average_price=Decimal("0.31660"), effective_net_kg=Decimal("204739"),
    )
    base.update(kw)
    return MemberLiquidation(**base)


def test_formats_and_signs():
    assert format_kg(Decimal("204739")) == "204.739 kg"
    assert format_money(Decimal("64819.75")) == "64.819,75 €"
    assert format_unit_price(Decimal("0.31660")) == "0,31660 €/kg"
    assert format_percent(Decimal("12")) == "12 %"
    assert format_signed_money(Decimal("10")) == "+10,00 €"
    assert format_signed_money(Decimal("-10")) == "−10,00 €"
    assert format_signed_money(Decimal("0")) == "—"
    assert mask_tax_id("12345678Z") == "12*****8Z"
    assert sanitize_filename('1355 CANO: BLANCA/SEMANA') == "1355_CANO_BLANCA_SEMANA"


def test_view_model_maps_without_recalculating_amounts():
    member = _member(taxable_base=Decimal("1.23"), total_amount=Decimal("4.56"), final_average_price=Decimal("0.00001"))
    vm = from_member_liquidation(_header(), member, tax_id="12345678Z")
    assert vm.taxable_base == Decimal("1.23")
    assert vm.total_amount == Decimal("4.56")
    assert vm.final_average_price == Decimal("0.00001")
    assert vm.final_average_price_pts == Decimal("0.00")
    assert vm.tax_id_masked == "12*****8Z"


def test_pdf_generates_single_landscape_page_without_internal_terms(tmp_path: Path):
    vm = from_member_liquidation(_header(), _member())
    path = export_premium_member_pdf(vm, tmp_path / "premium.pdf")
    data = path.read_bytes()
    assert path.exists() and path.stat().st_size > 1000
    assert data.count(b"/Type /Page\n") == 1
    assert b"/MediaBox [ 0 0 841.8898 595.2756 ]" in data
    text = data.decode("latin1", errors="ignore")
    assert "CANO MANZANARES" in text and "TOTAL A PERCIBIR" in text
    assert "Liquidaci\xf3n Administraci\xf3n" not in text
    assert "Auditor" not in text
    assert "C:/" not in text and "/workspace" not in text


def test_zero_values_show_dash_in_view_model_pdf(tmp_path: Path):
    vm = from_member_liquidation(_header(), _member(hectare_fee_amount=Decimal("0"), globalgap_amount=Decimal("0"), transport_amount=Decimal("0")))
    path = export_premium_member_pdf(vm, tmp_path / "zero.pdf")
    assert path.exists()


def test_pdf_with_group_benchmark_stays_single_page(tmp_path: Path):
    b = PremiumGroupBenchmark(
        "NAVEL TEMPRANA", "CITRICOS", "NAVEL", "TEMPRANA", ("FUKUMOTO", "NAVELINA", "NEWHALL"), "2026", "SANSEBAS", "Normal", "Primera",
        BenchmarkMetric(Decimal("0.31660"), Decimal("0.40000"), Decimal("0.25000"), Decimal("0.35455"), 2, 0, "ok"),
        BenchmarkMetric(Decimal("43212"), Decimal("50000"), Decimal("30000"), Decimal("41000"), 2, 0, "ok"),
        BenchmarkMetric(Decimal("15003"), Decimal("18000"), Decimal("10000"), Decimal("14500"), 2, 0, "ok"),
    )
    vm = from_member_liquidation(_header(), _member(variety="NAVELINA"), group_benchmark=b)
    path = export_premium_member_pdf(vm, tmp_path / "benchmark.pdf")
    text = path.read_bytes().decode("latin1", errors="ignore")
    assert path.read_bytes().count(b"/Type /Page\n") == 1
    assert "COMPARATIVA CON SU GRUPO VARIETAL" in text
    assert "Precio medio final" in text and "Producci" in text and "Importe final" in text
