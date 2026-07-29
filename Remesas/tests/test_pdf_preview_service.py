from pathlib import Path

import pytest

from services.pdf_preview_service import PdfPreviewService


def test_preview_path_is_session_temporary_and_cleanup_removes_it(tmp_path):
    service = PdfPreviewService(temp_root=tmp_path / "preview")
    path = service.create_preview_path(member_id=10, member_name="Socio Uno", remittance_name="Remesa A")
    assert path.parent == service.session_directory
    assert path.suffix == ".pdf"
    path.write_bytes("BORRADOR · NO GUARDADO".encode("utf-8"))
    session = service.session_directory
    service.cleanup()
    assert not session.exists()


def test_open_preview_uses_checked_platform_opener(tmp_path, monkeypatch):
    service = PdfPreviewService(temp_root=tmp_path)
    path = service.create_preview_path(member_id=1, member_name="A", remittance_name="R")
    path.write_bytes(b"pdf")
    opened = []
    monkeypatch.setattr("services.pdf_preview_service.open_path", opened.append)
    service.open_preview(path)
    assert opened == [path]
    service.cleanup()


def test_open_preview_rejects_missing_file(tmp_path):
    service = PdfPreviewService(temp_root=tmp_path)
    path = service.create_preview_path(member_id=1, member_name="A", remittance_name="R")
    with pytest.raises(FileNotFoundError):
        service.open_preview(path)
    service.cleanup()
