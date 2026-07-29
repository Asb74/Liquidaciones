from decimal import Decimal
import unittest
from unittest.mock import Mock, patch

from ui.liquidation_split_master_dialog import LiquidationSplitMasterDialog


class Value:
    def __init__(self, value=""):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class LiquidationSplitMasterDialogTests(unittest.TestCase):
    def make_dialog(self, mode="new"):
        dialog = object.__new__(LiquidationSplitMasterDialog)
        dialog._mode = mode
        dialog._editing_id = 41
        dialog.repository = Mock()
        dialog.member_name_lookup = lambda member_id: {1: "Origen", 2: "Destino"}.get(member_id)
        dialog.vars = {key: Value(value) for key, value in {
            "id": "", "source_member_id": "1", "source_member_name": "Origen", "campaign": "2026",
            "crop": "NARANJA", "variety": "", "remittance_id": "", "effective_from": "",
            "effective_to": "", "priority": "100", "notes": "",
        }.items()}
        dialog.kind_label = Value(dialog.TYPE_LABELS["PERCENTAGE"])
        dialog.active = Value(True)
        dialog.recipients = [(2, "Destino", "100", False)]
        dialog._lookup_source = lambda _event=None: True
        dialog._reload = Mock()
        dialog.on_saved = Mock()
        return dialog

    def test_types_show_translated_text_and_resolve_internal_code(self):
        dialog = self.make_dialog()
        self.assertEqual("Por porcentaje", dialog.kind_label.get())
        self.assertEqual("PERCENTAGE", dialog._split_type())
        dialog.kind_label.set("Por ponderaciones")
        self.assertEqual("WEIGHTS", dialog._split_type())

    def test_new_mode_never_passes_selected_rule_id(self):
        dialog = self.make_dialog(mode="new")
        dialog.repository.save_rule.return_value = 99
        with patch("ui.liquidation_split_master_dialog.messagebox.showinfo"):
            dialog._save()
        self.assertIsNone(dialog.repository.save_rule.call_args.kwargs["rule_id"])
        dialog._reload.assert_called_once_with(99)

    def test_edit_mode_keeps_rule_id(self):
        dialog = self.make_dialog(mode="edit")
        dialog.repository.save_rule.return_value = 41
        with patch("ui.liquidation_split_master_dialog.messagebox.showinfo"):
            dialog._save()
        self.assertEqual(41, dialog.repository.save_rule.call_args.kwargs["rule_id"])

    def test_validation_collects_duplicate_unknown_and_bad_percentage(self):
        dialog = self.make_dialog()
        dialog.recipients = [(2, "Destino", "40", False), (2, "Destino", "40", False), (3, "", "5", False)]
        errors = dialog._validation_errors()
        self.assertTrue(any("duplicados" in error for error in errors))
        self.assertTrue(any("no existen" in error.lower() for error in errors))
        self.assertTrue(any("suma debe ser 100" in error for error in errors))

    def test_residual_percentage_requires_exactly_one_residual(self):
        dialog = self.make_dialog()
        dialog.kind_label.set(dialog.TYPE_LABELS["PERCENTAGE_WITH_RESIDUAL"])
        dialog.recipients = [(2, "Destino", Decimal("90"), False)]
        self.assertTrue(any("único destinatario residual" in error for error in dialog._validation_errors()))


if __name__ == "__main__":
    unittest.main()
