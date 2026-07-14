from decimal import Decimal
import unittest

from domain.calculation_models import CalculationStatus
from domain.liquidacion_calculator import amount_for_taxable_base, calculate_taxable_base
from domain.utils import round_price


class TaxableBaseSignTests(unittest.TestCase):
    def test_positive_adjustments_keep_their_sign(self):
        self.assertEqual(calculate_taxable_base(Decimal("1000"), Decimal("100"), Decimal("50"), Decimal("20"), Decimal("30"), Decimal("40")), Decimal("940.00"))

    def test_negative_quality_reduces_base_without_abs(self):
        self.assertEqual(calculate_taxable_base(Decimal("1000"), Decimal("100"), Decimal("50"), Decimal("-20"), Decimal("30"), Decimal("40")), Decimal("900.00"))

    def test_negative_transport_reduces_base_without_abs(self):
        self.assertEqual(calculate_taxable_base(Decimal("1000"), Decimal("100"), Decimal("50"), Decimal("20"), Decimal("-30"), Decimal("40")), Decimal("880.00"))

    def test_negative_globalgap_reduces_base_without_abs(self):
        self.assertEqual(calculate_taxable_base(Decimal("1000"), Decimal("100"), Decimal("50"), Decimal("-20"), Decimal("10"), Decimal("-40")), Decimal("800.00"))

    def test_zero_concepts_leave_gross_amount(self):
        self.assertEqual(calculate_taxable_base(Decimal("1000"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")), Decimal("1000.00"))


class TaxableBaseRegression1355Tests(unittest.TestCase):
    def test_member_1355_uses_final_gross_amount_for_taxable_base_and_final_average_price(self):
        taxable_base = calculate_taxable_base(
            Decimal("42591.68"),
            Decimal("7834.00"),
            Decimal("81.67"),
            Decimal("0"),
            Decimal("0"),
            Decimal("999.40"),
        )
        self.assertEqual(taxable_base, Decimal("35675.41"))
        self.assertNotEqual(taxable_base, Decimal("35242.10"))
        self.assertEqual(round_price(taxable_base / Decimal("62684")), Decimal("0.56913"))


class TaxableBaseStatusTests(unittest.TestCase):
    def test_status_amount_mapping(self):
        self.assertEqual(amount_for_taxable_base(Decimal("1.23"), CalculationStatus.CALCULATED), Decimal("1.23"))
        self.assertEqual(amount_for_taxable_base(None, CalculationStatus.NOT_APPLICABLE), Decimal("0"))
        self.assertEqual(amount_for_taxable_base(None, CalculationStatus.DISABLED), Decimal("0"))
        self.assertIsNone(amount_for_taxable_base(None, CalculationStatus.PENDING))
        self.assertIsNone(amount_for_taxable_base(None, CalculationStatus.ERROR))


if __name__ == "__main__":
    unittest.main()
