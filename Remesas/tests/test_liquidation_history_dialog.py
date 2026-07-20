from datetime import date, datetime, timezone

from ui.liquidation_history_dialog import _date_label, _optional_filter, _status_label


def test_status_labels_are_translated_without_altering_unknown_values():
    assert _status_label("ACTIVE") == "Activa"
    assert _status_label("voided") == "Anulada"
    assert _status_label("PARTIAL") == "Parcial"
    assert _status_label("FAILED") == "Error"
    assert _status_label("GENERATED") == "Generado"
    assert _status_label("SUPERSEDED") == "Sustituido"
    assert _status_label("CUSTOM") == "CUSTOM"


def test_date_label_accepts_supported_date_formats():
    assert _date_label("2026-07-17") == "17/07/2026"
    assert _date_label("2026-07-17T14:05:33") == "17/07/2026"
    assert _date_label("2026-07-17T14:05:33+02:00", include_time=True) == "17/07/2026 14:05"
    assert _date_label("2026-07-17T12:05:33Z", include_time=True) == "17/07/2026 12:05"
    assert _date_label(date(2026, 7, 17)) == "17/07/2026"
    assert _date_label(datetime(2026, 7, 17, 12, 5, tzinfo=timezone.utc), include_time=True) == "17/07/2026 12:05"


def test_date_label_preserves_empty_and_unparseable_values():
    assert _date_label(None) == ""
    assert _date_label("") == ""
    assert _date_label("fecha desconocida") == "fecha desconocida"


def test_optional_filter_converts_all_unfiltered_combo_values_to_none():
    assert _optional_filter(None) is None
    assert _optional_filter("") is None
    assert _optional_filter("Todos") is None
    assert _optional_filter("2026") == "2026"
