from datetime import date, datetime, timezone

from ui.liquidation_history_dialog import (
    LiquidationHistoryDialog,
    _date_label,
    _optional_filter,
    _status_label,
)


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


def test_member_search_uses_only_explicit_context_filters():
    class Dialog:
        def _filters(self):
            return {
                'campaign': '2026', 'company': '1', 'crop': 'CITRICOS',
                'remittance_id': 7, 'status': 'ACTIVE', 'member_id': 1540,
                'date_from': '2026-01-01', 'date_to': '2026-12-31',
            }

    class History:
        def search_liquidation_members(self, text, **filters):
            self.text, self.filters = text, filters
            return ()

    dialog = Dialog()
    dialog.history = History()
    dialog._member_search_filters = (  # bind the production helper without Tk.
        lambda: LiquidationHistoryDialog._member_search_filters(dialog)
    )
    assert LiquidationHistoryDialog._search_members(dialog, 'sole') == ()
    assert dialog.history.text == 'sole'
    assert dialog.history.filters == {
        'campaign': '2026', 'company': '1', 'crop': 'CITRICOS',
        'remittance_id': 7, 'status': 'ACTIVE',
        'date_from': '2026-01-01', 'date_to': '2026-12-31',
    }
