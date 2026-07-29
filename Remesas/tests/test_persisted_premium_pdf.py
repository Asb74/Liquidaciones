from datetime import date
from decimal import Decimal

from domain.document_models import LiquidationDocumentMode
from exporters.persisted_liquidation_pdf_exporter import build_premium_view_model_from_persisted, export_persisted_liquidation_pdf
from presentation.persisted_liquidation_pdf_view_model import PersistedLiquidationPdfLine, PersistedLiquidationPdfTotals, PersistedLiquidationPdfViewModel
from presentation.premium_liquidation_view_model import PremiumLiquidationViewModel


def test_definitive_uses_premium_layout_has_ids_and_no_draft(tmp_path):
    d=Decimal
    lines=(PersistedLiquidationPdfLine("CI2026010128","NAVEL",1,d("100"),d("50"),d("0.5"),d("1"),d("2"),d("0"),d("0"),d("0"),d("47"),d("12"),d("2"),d("51.7"),d("0.517")),)
    vm=PersistedLiquidationPdfViewModel("batch",1,"REMESA","2026","1","CITRICOS",date(2026,1,31),10,"SOCIO",("CI2026010128",),lines,PersistedLiquidationPdfTotals(d("100"),d("50"),d("47"),d("51.7")))
    path=export_persisted_liquidation_pdf(build_premium_view_model_from_persisted(vm),tmp_path/"final.pdf")
    text=path.read_bytes().decode("latin1",errors="ignore")
    assert path.read_bytes().count(b"/Type /Page\n")==1
    assert "TOTAL A PERCIBIR" in text and "IdLiq" in text and "CI2026010128" in text
    assert "BORRADOR" not in text


def test_exporter_accepts_premium_model_without_accessing_persisted_lines(tmp_path, monkeypatch):
    legacy = PersistedLiquidationPdfViewModel(
        "batch", 1, "REMESA", "2026", "1", "CITRICOS", date(2026, 1, 31), 10, "SOCIO", (), (),
        PersistedLiquidationPdfTotals(Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")),
    )
    premium = build_premium_view_model_from_persisted(legacy)
    received = {}

    def render(self, vm, path, *, document_mode):
        received.update(vm=vm, mode=document_mode)
        path.write_bytes(b"pdf")
        return path

    monkeypatch.setattr("exporters.persisted_liquidation_pdf_exporter.PremiumLiquidationPdfRenderer.render", render)
    assert export_persisted_liquidation_pdf(premium, tmp_path / "final.pdf").exists()
    assert received["vm"] is premium
    assert isinstance(received["vm"], PremiumLiquidationViewModel)
    assert received["mode"] is LiquidationDocumentMode.FINAL
