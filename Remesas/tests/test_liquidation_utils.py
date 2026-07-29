import unittest
from domain.utils import is_liquidated, format_file_timestamp, format_display_date

class LiquidatedTests(unittest.TestCase):
    def test_false_values(self):
        for value in (None, "", "N", "NO", "0", False, " false "):
            self.assertFalse(is_liquidated(value), value)

    def test_true_values(self):
        for value in ("S", "SI", "Sí", "1", True, " liquidado "):
            self.assertTrue(is_liquidated(value), value)

    def test_format_dates(self):
        self.assertEqual(format_display_date("2025-12-26 00:00:00"), "26/12/2025")
        self.assertRegex(format_file_timestamp(1783908977), r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}")

if __name__ == '__main__':
    unittest.main()

class YesNoParserTests(unittest.TestCase):
    def test_parse_yes_no_values(self):
        from domain.utils import parse_yes_no
        for value in ("S", "SI", "SÍ", "Y", "YES", "1", 1, True, "  s  ", "x", "TRUE"):
            self.assertTrue(parse_yes_no(value), value)
        for value in ("N", "NO", "0", 0, False, None, "", " false "):
            self.assertFalse(parse_yes_no(value), value)
