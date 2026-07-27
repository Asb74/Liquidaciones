from pathlib import Path


def test_historical_and_consolidated_exporters_have_separate_entrypoints():
    root = Path(__file__).resolve().parents[1]
    frame = (root / "ui" / "remesas_frame.py").read_text(encoding="utf-8")
    assert "exporter = export_consolidated_liquidation_summary if excel_only else export_batch_liquidation_summary" in frame
    historical = (root / "exporters" / "batch_liquidation_excel_exporter.py").read_text(encoding="utf-8")
    consolidated = (root / "exporters" / "excel_consolidation_exporter.py").read_text(encoding="utf-8")
    assert 'ws.title = "Resumen por remesa"' in historical
    assert 'general.title = "Resumen general"' in consolidated


def test_consolidation_dialog_has_exact_empty_selection_message():
    source = (Path(__file__).resolve().parents[1] / "ui" / "excel_consolidation_dialog.py").read_text(encoding="utf-8")
    assert "Debe seleccionar al menos una liquidación, remesa o archivo." in source
