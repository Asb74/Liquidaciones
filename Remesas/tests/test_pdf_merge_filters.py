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


def test_unique_remittances_use_real_id_and_preserve_first_appearance():
    pytest.importorskip("pypdf")
    from types import SimpleNamespace
    from ui.pdf_merge_tool_dialog import collect_unique_remittances

    documents = [
        SimpleNamespace(remittance_id=2, document_id=20),
        SimpleNamespace(remittance_id=1, document_id=10),
        SimpleNamespace(remittance_id=2, document_id=21),
        SimpleNamespace(remittance_id=3, document_id=30),
    ]
    resolved, missing = collect_unique_remittances(
        documents, lambda document: SimpleNamespace(remittance_id=document.remittance_id),
    )
    assert [item.remittance_id for item in resolved] == [2, 1, 3]
    assert missing == []


def test_unique_remittances_reports_documents_without_valid_remittance():
    pytest.importorskip("pypdf")
    from types import SimpleNamespace
    from ui.pdf_merge_tool_dialog import collect_unique_remittances

    no_id = SimpleNamespace(remittance_id=None, document_id=40)
    missing_in_repository = SimpleNamespace(remittance_id=99, document_id=41)
    resolved, missing = collect_unique_remittances(
        [no_id, missing_in_repository],
        lambda _document: (_ for _ in ()).throw(ValueError("No existe")),
    )
    assert resolved == []
    assert missing == [no_id, missing_in_repository]


def test_unique_batch_ids_preserve_order_and_report_missing():
    pytest.importorskip("pypdf")
    from types import SimpleNamespace
    from ui.pdf_merge_tool_dialog import collect_unique_batch_ids

    documents = [SimpleNamespace(batch_id="batch-2"), SimpleNamespace(batch_id="batch-1"),
                 SimpleNamespace(batch_id="batch-2"), SimpleNamespace(batch_id=None)]
    batch_ids, missing = collect_unique_batch_ids(documents)

    assert batch_ids == ["batch-2", "batch-1"]
    assert missing == [documents[-1]]


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
