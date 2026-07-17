from pathlib import Path

import pytest

from data.persistence.database import PersistenceDatabase
from data.persistence.liquidation_repository import LiquidationRepository
def test_member_and_date_filter_validation():
    pytest.importorskip("pypdf")
    from ui.pdf_merge_tool_dialog import PdfMergeToolDialog
    assert PdfMergeToolDialog.parse_member("") is None
    assert PdfMergeToolDialog.parse_member("453") == 453
    with pytest.raises(ValueError, match="número entero"):
        PdfMergeToolDialog.parse_member("socio")
    assert PdfMergeToolDialog.parse_date("17/07/2026") == "2026-07-17"


def test_draft_filter_options_are_dependent_and_real(tmp_path: Path):
    db=PersistenceDatabase(str(tmp_path/"liquidaciones.sqlite")); db.initialize()
    repository=LiquidationRepository(db)
    common=dict(recipient_member_id=1,member_name="Socio",crop="DIRECTO",file_path=str(tmp_path/"draft.pdf"),generated_at="2026-07-17T10:00:00+00:00")
    repository.record_exported_draft(remittance_id=2320,remittance_name="Final",campaign="2026",company="1",**common)
    repository.record_exported_draft(remittance_id=2321,remittance_name="Otra",campaign="2025",company="2",**common)

    all_options=repository.list_document_filter_options(document_kind="PDF_DRAFT")
    assert all_options["campaigns"] == ("2025","2026")
    selected=repository.list_document_filter_options(document_kind="PDF_DRAFT",campaign="2026",company="1",crop="DIRECTO")
    assert selected["companies"] == ("1",)
    assert selected["crops"] == ("DIRECTO",)
    assert selected["remittances"] == ((2320,"Final"),)
