from decimal import Decimal
import sqlite3
import unittest

from data.quality_repository import QualityRepository
from domain.financial_rules import calculate_quality_adjustment, effective_net_kg
from domain.hectare_fee import CalculationStatus, allocate_hectare_fees
from domain.utils import round_money


class EffectiveNetQualityHectareTests(unittest.TestCase):
    def test_effective_net_cases(self):
        cases = [
            (10000, 0, "10000"),
            (10000, None, "10000"),
            (10000, 9850, "9850"),
            (0, 8000, "8000"),
            (None, None, "0"),
        ]
        for net, batch, expected in cases:
            self.assertEqual(effective_net_kg(net, batch), Decimal(expected))

    def test_quality_adjustment(self):
        self.assertEqual(calculate_quality_adjustment(Decimal("10000"), Decimal("0.01"), True), Decimal("100.00"))
        self.assertEqual(calculate_quality_adjustment(Decimal("10000"), Decimal("-0.005"), True), Decimal("-50.000"))
        self.assertEqual(calculate_quality_adjustment(Decimal("10000"), Decimal("0.01"), False), Decimal("0"))

    def test_quality_repository_priority(self):
        conn = sqlite3.connect(":memory:")
        conn.execute('CREATE TABLE BonCalidad(IdSocio INTEGER, "Bon/Pen" TEXT, CULTIVO TEXT, CAMPAÑA TEXT, EMPRESA TEXT, CONCEPTO TEXT, IdConcepto INTEGER)')
        conn.executemany('INSERT INTO BonCalidad VALUES(?,?,?,?,?,?,?)', [
            (1, "0.01", "MANDARINA", "2026", "1", "general", 0),
            (1, "0.02", "MANDARINA", "2026", "1", "specific", 7),
            (2, "-0.005", "MANDARINA", "2026", "1", "general", 0),
        ])
        repo = QualityRepository(conn)
        self.assertEqual(repo.get_quality_rate(1, "2026", "1", "MANDARINA", 7).rate, Decimal("0.02"))
        self.assertEqual(repo.get_quality_rate(2, "2026", "1", "MANDARINA", 7).source, "general")
        missing = repo.get_quality_rate(3, "2026", "1", "MANDARINA", 7)
        self.assertEqual(missing.rate, Decimal("0"))
        self.assertTrue(missing.warnings)

    def test_hectare_fee_math(self):
        self.assertEqual(round_money(Decimal("3.6966") * Decimal("195")), Decimal("720.84"))
        self.assertEqual(effective_net_kg(10000, 0) + effective_net_kg(12000, 11500), Decimal("21500"))
        rate = Decimal("720.84") / Decimal("245183")
        self.assertEqual(rate, Decimal("720.84") / Decimal("245183"))
        self.assertEqual(round_money(Decimal("34624") * rate), round_money(Decimal("34624") * (Decimal("720.84") / Decimal("245183"))))
        amounts, diff = allocate_hectare_fees(Decimal("10.00"), Decimal("0.3333"), [(0, Decimal("10")), (1, Decimal("10")), (2, Decimal("10"))])
        self.assertEqual(sum(amounts.values(), Decimal("0")), Decimal("10.00"))

    def test_status_values(self):
        self.assertEqual(CalculationStatus.NOT_APPLICABLE.value, "not_applicable")
        self.assertEqual(CalculationStatus.ERROR.value, "error")


if __name__ == "__main__":
    unittest.main()
