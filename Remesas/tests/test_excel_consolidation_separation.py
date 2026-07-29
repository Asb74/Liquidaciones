from pathlib import Path


def test_mass_excel_keeps_historical_exporter_and_sheet_structure():
    root = Path(__file__).resolve().parents[1]
    frame = (root / "ui" / "remesas_frame.py").read_text(encoding="utf-8")
    assert "exporter = export_batch_liquidation_summary" in frame
    historical = (root / "exporters" / "batch_liquidation_excel_exporter.py").read_text(encoding="utf-8")
    consolidated = (root / "exporters" / "excel_consolidation_exporter.py").read_text(encoding="utf-8")
    assert 'ws.title = "Resumen por remesa"' in historical
    assert 'general.title = "Resumen general"' in consolidated


def test_consolidation_dialog_has_exact_empty_selection_message():
    source = (Path(__file__).resolve().parents[1] / "ui" / "multi_remittance_selection_dialog.py").read_text(encoding="utf-8")
    assert 'EMPTY_SELECTION_MESSAGE = "Debe seleccionar al menos una remesa."' in source


def test_pdf_and_excel_reuse_the_same_selector():
    root = Path(__file__).resolve().parents[1]
    dialog = (root / "ui" / "pdf_merge_tool_dialog.py").read_text(encoding="utf-8")
    app = (root / "app.py").read_text(encoding="utf-8")
    assert 'text="Generar PDF combinado"' in dialog
    assert 'text="Generar resumen Excel"' in dialog
    assert "docs=[d for i,d in enumerate(self.documents) if i in self.selected]" in dialog
    assert "excel_callback=lambda selected" in app
    assert "open_excel_consolidation=" not in app
