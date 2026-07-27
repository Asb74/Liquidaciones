from pathlib import Path

import pytest

from services import path_opener


def test_open_path_rejects_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError, match="No existe el archivo"):
        path_opener.open_path(tmp_path / "missing.xlsx")


def test_open_path_uses_platform_default_for_existing_file(tmp_path, monkeypatch):
    target = tmp_path / "created.xlsx"
    target.touch()
    calls = []
    monkeypatch.setattr(path_opener.os, "name", "posix")
    monkeypatch.setattr(path_opener.sys, "platform", "linux")
    monkeypatch.setattr(path_opener.subprocess, "run", lambda command, check: calls.append((command, check)))
    path_opener.open_path(Path(target))
    assert calls == [(["xdg-open", str(target)], True)]


def test_open_path_logs_and_propagates_handler_failure(tmp_path, monkeypatch, caplog):
    target = tmp_path / "created.pdf"
    target.touch()
    monkeypatch.setattr(path_opener.os, "name", "posix")
    monkeypatch.setattr(path_opener.sys, "platform", "linux")
    monkeypatch.setattr(path_opener.subprocess, "run", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("no handler")))
    with pytest.raises(OSError, match="no handler"):
        path_opener.open_path(target)
    assert "No se pudo abrir la ruta" in caplog.text
