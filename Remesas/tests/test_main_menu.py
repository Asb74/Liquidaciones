from __future__ import annotations

import tkinter as tk
from pathlib import Path
import unittest

from ui.main_menu import MainMenuHandlers, build_main_menu


class MainMenuTests(unittest.TestCase):
    def test_contains_expected_menus_and_master_option(self):
        try:
            root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"Tk no disponible: {exc}")
        root.withdraw()
        try:
            menu = build_main_menu(root, MainMenuHandlers(root.destroy, lambda: None, lambda: None))
            labels = [menu.entrycget(i, "label") for i in range(menu.index("end") + 1)]
            self.assertEqual(labels, ["Archivo", "Maestros", "Ayuda"])
            masters = menu.nametowidget(menu.entrycget(1, "menu"))
            self.assertEqual(masters.entrycget(0, "label"), "Cuota por hectárea")
        finally:
            root.destroy()

    def test_remesas_frame_source_has_no_duplicate_config_buttons(self):
        source = Path(__file__).resolve().parents[1] / "ui" / "remesas_frame.py"
        self.assertNotIn("Configurar cuota Ha", source.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
