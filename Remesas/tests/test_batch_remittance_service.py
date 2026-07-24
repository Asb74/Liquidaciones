from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from openpyxl import load_workbook

from exporters.batch_liquidation_excel_exporter import export_batch_liquidation_summary
from services.batch_remittance_service import BatchRemittanceService, FailedRemittanceResult, SelectedRemittance, SingleRemittanceBatchResult


def remittance(remittance_id=2204, name="BLANCA TEMPRANA sem 1"):
    return SelectedRemittance(remittance_id, name, date(2026, 7, 16), date(2026, 1, 1), date(2026, 1, 7), "1", "NORMAL", "2026", "1", "CITRICOS")


def calc(remittance_id=2204, member_id=1):
    # The persisted commercial average is deliberately different: the exporter
    # must derive P. Comer. from the same gross/net values it writes.
    member = SimpleNamespace(member_id=member_id, member_name="Socio", variety="NAVEL", net_kg=Decimal("100"), gross_amount=Decimal("50"), commercial_average_price=Decimal("0.7"), collection_amount=Decimal("1"), hectare_fee_amount=Decimal("2"), quality_amount=Decimal("3"), transport_amount=Decimal("4"), globalgap_amount=Decimal("5"), taxable_base=Decimal("35"), final_average_price=Decimal("0.35"), vat_rate=Decimal("12"), vat_amount=Decimal("4.2"), withholding_rate=Decimal("2"), withholding_amount=Decimal("0.7"), total_amount=Decimal("38.5"))
    totals = SimpleNamespace(net_kg=Decimal("100"), commercial_amount=Decimal("50"), collection_amount=Decimal("1"), quality_amount=Decimal("3"), transport_amount=Decimal("4"), globalgap_amount=Decimal("5"), hectare_fee_amount=Decimal("2"), taxable_base=Decimal("35"), vat_amount=Decimal("4.2"), withholding_amount=Decimal("0.7"), total_amount=Decimal("38.5"))
    header = SimpleNamespace(remesa_name=f"Remesa {remittance_id}")
    result = SimpleNamespace(member_results=(member,), totals=totals, header=header, warnings=(), variety_count=1, hectare_fee_master=SimpleNamespace(price_per_hectare=Decimal("195"), eligible_crops=("CITRICOS",)))
    return SimpleNamespace(result=result, member_count=1, delivery_count=2)


def batch_result(r):
    return SingleRemittanceBatchResult(r, calc(r.remittance_id), 1, 2, Path("/tmp") / str(r.remittance_id), ())


def test_batch_continues_after_single_remittance_failure(tmp_path):
    rems = [remittance(1), remittance(2), remittance(3)]
    def processor(r, cb):
        if r.remittance_id == 2:
            raise RuntimeError("boom")
        return batch_result(r)
    service = BatchRemittanceService(single_processor=processor, output_base=tmp_path, exporter=lambda *args, **kwargs: kwargs.get("output_path", args[2]) if False else args[2], log_dir=tmp_path / "logs")
    result = service.process(rems)
    assert result.remittances_completed == 2
    assert result.remittances_failed == 1
    assert [r.remittance.remittance_id for r in result.successful_results] == [1, 3]


def test_batch_cancellation_stops_before_next_and_exports_partial(tmp_path):
    rems = [remittance(1), remittance(2)]
    calls = []
    def processor(r, cb):
        calls.append(r.remittance_id)
        return batch_result(r)
    service = BatchRemittanceService(single_processor=processor, output_base=tmp_path, exporter=lambda *args, **kwargs: args[2], should_cancel=lambda: bool(calls), log_dir=tmp_path / "logs")
    result = service.process(rems)
    assert calls == [1]
    assert result.cancelled is True
    assert result.aggregate_excel_path.parent == tmp_path / "2026" / "CITRICOS"


def test_exporter_creates_required_sheets_continuous_detail_and_incidents(tmp_path):
    r1 = remittance(2204)
    r2 = remittance(2205, "BLANCA TEMPRANA sem 2")
    failed = [FailedRemittanceResult(remittance(2206), "CALCULATING", "ValueError", "bad data")]
    output = tmp_path / "Resumen_liquidaciones_CITRICOS_2026_20260716_074530.xlsx"
    export_batch_liquidation_summary([batch_result(r1), batch_result(r2)], failed, output, campaign="2026", company="1", crop="CITRICOS", execution_started_at=datetime(2026, 7, 16, 7, 45), execution_finished_at=datetime(2026, 7, 16, 7, 46))
    wb = load_workbook(output, data_only=False)
    assert wb.sheetnames == ["Resumen por remesa", "Detalle acumulado", "Incidencias", "Parámetros"]
    detail = wb["Detalle acumulado"]
    from exporters.excel_exporter import SUMMARY_HEADERS
    assert [detail.cell(1, col).value for col in range(1, 24)] == ["Id Remesa", "Remesa", *SUMMARY_HEADERS]
    assert [detail.cell(row, 1).value for row in (2, 3)] == [2204, 2205]
    assert [detail.cell(row, 2).value for row in (2, 3)] == ["BLANCA TEMPRANA sem 1", "BLANCA TEMPRANA sem 2"]
    values = [cell.value for row in detail.iter_rows() for cell in row]
    assert not any(isinstance(value, str) and value.startswith("REMESA ") for value in values)
    assert not any(isinstance(value, str) and value.startswith("SUBTOTAL REMESA") for value in values)
    assert "TOTAL GENERAL" in values
    assert len(detail.row_breaks.brk) == 0
    assert wb["Incidencias"][2][0].value == 2206


def test_detail_accumulated_reuses_individual_summary_headers_rows_and_general_total(tmp_path):
    r1 = remittance(2204)
    r2 = remittance(2205, "BLANCA TEMPRANA sem 2")
    output = tmp_path / "batch.xlsx"
    export_batch_liquidation_summary([batch_result(r1), batch_result(r2)], [], output, campaign="2026", company="1", crop="CITRICOS", execution_started_at=datetime(2026, 7, 16, 7, 45), execution_finished_at=datetime(2026, 7, 16, 7, 46))

    detail = load_workbook(output, data_only=False)["Detalle acumulado"]
    from exporters.excel_exporter import SUMMARY_HEADERS
    assert [detail.cell(1, col).value for col in range(1, 24)] == ["Id Remesa", "Remesa", *SUMMARY_HEADERS]
    assert detail.max_row == 4
    for row, remittance_id in ((2, 2204), (3, 2205)):
        assert [detail.cell(row, col).value for col in range(1, 24)] == [
            remittance_id, f"BLANCA TEMPRANA sem {1 if remittance_id == 2204 else 2}",
            1, "Socio", "NAVEL", 100, 50, 0.5, 1, 2, 3, 4, 5, 35, 0.35, Decimal("12"), 2, 38.5, f"Remesa {remittance_id}", None, f"=IFERROR(166.386*I{row}/F{row},0)", f"=IFERROR(166.386*M{row}/F{row},0)", f"=IFERROR(166.386*L{row}/F{row},0)",
        ]
    assert detail.cell(4, 4).value == "TOTAL GENERAL"
    assert detail.cell(4, 6).value == 200
    assert detail.cell(4, 15).value == 0.385


def test_detail_accumulated_keeps_same_member_in_each_remittance_without_merging(tmp_path):
    rems = [remittance(2204), remittance(2205), remittance(2206)]
    output = tmp_path / "same_member.xlsx"
    export_batch_liquidation_summary([batch_result(r) for r in rems], [], output, campaign="2026", company="1", crop="CITRICOS", execution_started_at=datetime(2026, 7, 16, 7, 45), execution_finished_at=datetime(2026, 7, 16, 7, 46))

    detail = load_workbook(output, data_only=False)["Detalle acumulado"]
    member_rows = [row for row in range(1, detail.max_row + 1) if detail.cell(row, 3).value == 1 and detail.cell(row, 4).value == "Socio"]
    assert len(member_rows) == 3
    assert [detail.cell(row, 1).value for row in member_rows] == [2204, 2205, 2206]


def test_detail_accumulated_empty_successful_remittance_keeps_table_header_without_subtotal(tmp_path):
    r = remittance(2210)
    empty_totals = SimpleNamespace(net_kg=None, commercial_amount=None, collection_amount=None, quality_amount=None, transport_amount=None, globalgap_amount=None, hectare_fee_amount=None, taxable_base=None, vat_amount=None, withholding_amount=None, total_amount=None)
    empty_calc = SimpleNamespace(result=SimpleNamespace(member_results=(), totals=empty_totals, header=SimpleNamespace(remesa_name="empty", cultivo="CITRICOS"), warnings=("Sin liquidaciones válidas.",), variety_count=0, hectare_fee_master=None), member_count=0, delivery_count=0)
    item = SingleRemittanceBatchResult(r, empty_calc, 0, 0, tmp_path / "2210", ())
    output = tmp_path / "empty.xlsx"
    export_batch_liquidation_summary([item], [], output, campaign="2026", company="1", crop="CITRICOS", execution_started_at=datetime(2026, 7, 16, 7, 45), execution_finished_at=datetime(2026, 7, 16, 7, 46))

    wb = load_workbook(output, data_only=False)
    detail = wb["Detalle acumulado"]
    values = [cell.value for row in detail.iter_rows() for cell in row]
    assert "Sin liquidaciones válidas." not in values
    assert "SUBTOTAL REMESA 2210" not in values
    assert detail.max_row == 1
    assert wb["Incidencias"][2][0].value == 2210
    assert wb["Incidencias"][2][6].value == "WARNING"
