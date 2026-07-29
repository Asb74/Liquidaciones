from pathlib import Path

from ui.remesas_frame import offer_open_generated_excel


def test_user_answering_no_does_not_open_existing_excel(tmp_path):
    path = tmp_path / "batch.xlsx"; path.touch()
    opened = []
    result = offer_open_generated_excel(path, ask=lambda *_a, **_k: False, opener=opened.append)
    assert result is False
    assert opened == []
    assert path.exists()


def test_open_failure_keeps_created_excel_and_reports_path(tmp_path):
    path = tmp_path / "batch.xlsx"; path.touch()
    warnings = []
    result = offer_open_generated_excel(
        Path(path), ask=lambda *_a, **_k: True,
        opener=lambda _path: (_ for _ in ()).throw(OSError("sin aplicación")),
        warn=lambda title, text, **_kwargs: warnings.append((title, text)),
    )
    assert result is False
    assert path.exists()
    assert str(path) in warnings[0][1]
    assert "No se pudo abrir automáticamente" in warnings[0][1]
    assert "sin aplicación" in warnings[0][1]


def test_existing_excel_is_opened_after_confirmation(tmp_path):
    path = tmp_path / "batch.xlsx"; path.touch()
    opened = []
    assert offer_open_generated_excel(path, ask=lambda *_a, **_k: True, opener=opened.append) is True
    assert opened == [path]


def test_batch_ui_restores_running_state_cursor_buttons_and_progress_window():
    source = (Path(__file__).resolve().parents[1] / "ui" / "remesas_frame.py").read_text(encoding="utf-8")
    finally_block = source.split("        finally:\n", 1)[1]
    assert 'self.batch_running=False' in finally_block
    assert 'self.configure(cursor="")' in finally_block
    assert 'progress_win.destroy()' in finally_block
    assert 'self._refresh_action_states()' in finally_block
