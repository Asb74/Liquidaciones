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
