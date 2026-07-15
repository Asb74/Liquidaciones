from decimal import Decimal
import unittest

from domain.calculation_models import CalculationStatus
from domain.liquidacion_calculator import calculate_fiscal_result, calculate_vat, calculate_withholding, calculate_total
from domain.utils import format_currency_es, format_integer_es, format_percentage_es, format_price_es, round_money, round_price, get_price_labels


class SpanishFormatTests(unittest.TestCase):
    def test_requested_examples(self):
        self.assertEqual(format_integer_es(105180), "105.180")
        self.assertEqual(format_currency_es(Decimal("84545.82")), "84.545,82 €")
        self.assertEqual(format_price_es(Decimal("0.80391")), "0,80391")
        self.assertEqual(format_percentage_es(Decimal("12")), "12,00 %")

    def test_negative_values(self):
        self.assertEqual(format_currency_es(Decimal("-1234.5")), "-1.234,50 €")

    def test_rounding_helpers(self):
        self.assertEqual(round_money(Decimal("1.235")), Decimal("1.24"))
        self.assertEqual(round_price(Decimal("0.123456")), Decimal("0.12346"))

    def test_citrus_labels(self):
        self.assertEqual(get_price_labels("MANDARINA")[:3], ["1 XXX", "1 XX", "1 X"])
        self.assertEqual(get_price_labels("DIRECTO")[:3], ["CAL 0", "CAL 1", "CAL 2"])


class CalculationStateTests(unittest.TestCase):
    def test_status_values(self):
        self.assertEqual(CalculationStatus.CALCULATED.value, "calculated")
        self.assertEqual(CalculationStatus.NOT_APPLICABLE.value, "not_applicable")
        self.assertEqual(CalculationStatus.PENDING.value, "pending")
        self.assertEqual(CalculationStatus.ERROR.value, "error")

    def test_tax_helpers(self):
        base = Decimal("100.00")
        vat = calculate_vat(base, Decimal("12"))
        wh = calculate_withholding(Decimal("112.00"), Decimal("2"))
        self.assertEqual(vat, Decimal("12.00"))
        self.assertEqual(wh, Decimal("2.24"))
        self.assertEqual(calculate_total(base, vat, wh), Decimal("109.76"))

    def test_perceco_real_fiscal_reference(self):
        fiscal = calculate_fiscal_result(Decimal("35675.39"), Decimal("62684"), Decimal("12"), Decimal("2"))
        self.assertEqual(fiscal.total_amount, Decimal("39157.31"))
        self.assertEqual(fiscal.final_average_price, Decimal("0.62468"))

    def test_without_withholding(self):
        fiscal = calculate_fiscal_result(Decimal("1000"), Decimal("1"), Decimal("4"), Decimal("0"))
        self.assertEqual(fiscal.total_amount, Decimal("1040.00"))

    def test_without_vat(self):
        fiscal = calculate_fiscal_result(Decimal("1000"), Decimal("1"), Decimal("0"), Decimal("2"))
        self.assertEqual(fiscal.total_amount, Decimal("980.00"))

    def test_vat_12_withholding_2(self):
        fiscal = calculate_fiscal_result(Decimal("1000"), Decimal("1"), Decimal("12"), Decimal("2"))
        self.assertEqual(fiscal.total_amount, Decimal("1097.60"))

    def test_zero_net_has_no_final_average_price(self):
        fiscal = calculate_fiscal_result(Decimal("1000"), Decimal("0"), Decimal("12"), Decimal("2"))
        self.assertIsNone(fiscal.final_average_price)


if __name__ == "__main__":
    unittest.main()
