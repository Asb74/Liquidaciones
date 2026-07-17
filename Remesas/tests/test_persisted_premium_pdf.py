from datetime import date
from decimal import Decimal

from exporters.persisted_liquidation_pdf_exporter import export_persisted_liquidation_pdf
from presentation.persisted_liquidation_pdf_view_model import PersistedLiquidationPdfLine, PersistedLiquidationPdfTotals, PersistedLiquidationPdfViewModel


def test_definitive_uses_premium_layout_has_ids_and_no_draft(tmp_path):
    d=Decimal
    lines=(PersistedLiquidationPdfLine("CI2026010128","NAVEL",1,d("100"),d("50"),d("0.5"),d("1"),d("2"),d("0"),d("0"),d("0"),d("47"),d("12"),d("2"),d("51.7"),d("0.517")),)
    vm=PersistedLiquidationPdfViewModel("batch",1,"REMESA","2026","1","CITRICOS",date(2026,1,31),10,"SOCIO",("CI2026010128",),lines,PersistedLiquidationPdfTotals(d("100"),d("50"),d("47"),d("51.7")))
    path=export_persisted_liquidation_pdf(vm,tmp_path/"final.pdf")
    text=path.read_bytes().decode("latin1",errors="ignore")
    assert path.read_bytes().count(b"/Type /Page\n")==1
    assert "TOTAL A PERCIBIR" in text and "IdLiq" in text and "CI2026010128" in text
    assert "BORRADOR" not in text
