from pathlib import Path

from ui import hectare_fee_report_dialog as report_dialog


class FakeButton:
    def __init__(self):
        self.states = []

    def configure(self, **kwargs):
        self.states.append(kwargs["state"])


class FakeVariable:
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value


def _dialog():
    dialog = object.__new__(report_dialog.HectareFeeReportDialog)
    dialog.data = ((), {}, {}, ())
    dialog._export_in_progress = False
    dialog.export_button = FakeButton()
    dialog.campaign = FakeVariable("2026")
    dialog.company = FakeVariable("1")
    return dialog


def test_company_option_keeps_technical_code_separate_from_display_text():
    option = report_dialog.CompanyOption(company_id="42", code="01", name="Empresa demo")

    assert option.display_text == "01 - Empresa demo"
    assert option.company_id == "42"
    assert option.code == "01"


def test_selected_company_rejects_placeholder_and_resolves_display_map():
    dialog = _dialog()
    dialog.company_options = {"01 - Empresa demo": report_dialog.CompanyOption("42", "01", "Empresa demo")}
    dialog.company.value = report_dialog.SELECT_COMPANY
    assert dialog._selected_company() is None

    dialog.company.value = "01 - Empresa demo"
    assert dialog._selected_company().code == "01"


def _save_path(monkeypatch, path):
    monkeypatch.setattr(report_dialog.filedialog, "asksaveasfilename", lambda **kwargs: str(path))


def test_export_generates_excel_then_asks_whether_to_open(tmp_path, monkeypatch):
    dialog = _dialog()
    output = tmp_path / "report.xlsx"
    _save_path(monkeypatch, output)
    asked = []

    def export(path, *_data):
        Path(path).touch()
        return path

    monkeypatch.setattr(report_dialog, "export_hectare_fee_report", export)
    monkeypatch.setattr(report_dialog.messagebox, "askyesno", lambda *args, **kwargs: asked.append((args, kwargs)) or False)

    dialog.export()

    assert output.is_file()
    assert len(asked) == 1
    assert dialog.export_button.states == ["disabled", "normal"]


def test_yes_opens_the_generated_file(tmp_path, monkeypatch):
    dialog = _dialog()
    output = tmp_path / "report.xlsx"
    _save_path(monkeypatch, output)
    opened = []
    monkeypatch.setattr(report_dialog, "export_hectare_fee_report", lambda path, *_data: Path(path).touch() or path)
    monkeypatch.setattr(report_dialog.messagebox, "askyesno", lambda *args, **kwargs: True)
    monkeypatch.setattr(report_dialog, "open_path", opened.append)

    dialog.export()

    assert opened == [output.resolve()]


def test_no_does_not_open_the_generated_file(tmp_path, monkeypatch):
    dialog = _dialog()
    output = tmp_path / "report.xlsx"
    _save_path(monkeypatch, output)
    opened = []
    monkeypatch.setattr(report_dialog, "export_hectare_fee_report", lambda path, *_data: Path(path).touch() or path)
    monkeypatch.setattr(report_dialog.messagebox, "askyesno", lambda *args, **kwargs: False)
    monkeypatch.setattr(report_dialog, "open_path", opened.append)

    dialog.export()

    assert opened == []


def test_export_failure_does_not_ask_to_open_and_restores_button(tmp_path, monkeypatch):
    dialog = _dialog()
    _save_path(monkeypatch, tmp_path / "report.xlsx")
    errors = []
    monkeypatch.setattr(report_dialog, "export_hectare_fee_report", lambda *_args: (_ for _ in ()).throw(OSError("disk full")))
    monkeypatch.setattr(report_dialog.messagebox, "askyesno", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not ask")))
    monkeypatch.setattr(report_dialog.messagebox, "showerror", lambda *args, **kwargs: errors.append(args[1]))

    dialog.export()

    assert "No se pudo generar el informe Excel." in errors[0]
    assert dialog.export_button.states == ["disabled", "normal"]


def test_missing_or_invalid_export_file_does_not_ask(tmp_path, monkeypatch):
    dialog = _dialog()
    output = tmp_path / "missing.xlsx"
    _save_path(monkeypatch, output)
    errors = []
    monkeypatch.setattr(report_dialog, "export_hectare_fee_report", lambda *_args: output)
    monkeypatch.setattr(report_dialog.messagebox, "askyesno", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not ask")))
    monkeypatch.setattr(report_dialog.messagebox, "showerror", lambda *args, **kwargs: errors.append(args[1]))

    dialog.export()

    assert "No se pudo generar el informe Excel." in errors[0]


def test_open_failure_warns_without_marking_export_as_failed(tmp_path, monkeypatch):
    dialog = _dialog()
    output = tmp_path / "report.xlsx"
    _save_path(monkeypatch, output)
    warnings = []
    errors = []
    monkeypatch.setattr(report_dialog, "export_hectare_fee_report", lambda path, *_data: Path(path).touch() or path)
    monkeypatch.setattr(report_dialog.messagebox, "askyesno", lambda *args, **kwargs: True)
    monkeypatch.setattr(report_dialog, "open_path", lambda _path: (_ for _ in ()).throw(OSError("no handler")))
    monkeypatch.setattr(report_dialog.messagebox, "showwarning", lambda *args, **kwargs: warnings.append(args[1]))
    monkeypatch.setattr(report_dialog.messagebox, "showerror", lambda *args, **kwargs: errors.append(args[1]))

    dialog.export()

    assert "se generó correctamente, pero no se pudo abrir" in warnings[0]
    assert errors == []


def test_reentrant_export_is_ignored_while_export_is_in_progress(tmp_path, monkeypatch):
    dialog = _dialog()
    output = tmp_path / "report.xlsx"
    calls = []
    _save_path(monkeypatch, output)

    def export(path, *_data):
        calls.append(path)
        dialog.export()
        Path(path).touch()
        return path

    monkeypatch.setattr(report_dialog, "export_hectare_fee_report", export)
    monkeypatch.setattr(report_dialog.messagebox, "askyesno", lambda *args, **kwargs: False)

    dialog.export()

    assert calls == [str(output)]
    assert dialog.export_button.states == ["disabled", "normal"]
