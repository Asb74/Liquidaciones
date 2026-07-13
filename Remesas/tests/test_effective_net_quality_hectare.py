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
        self.assertEqual(amounts[0], Decimal("3.33"))
        self.assertEqual(sum(amounts.values(), Decimal("0")), Decimal("9.99"))
        self.assertEqual(diff, Decimal("0.01"))

    def test_hectare_denominator_crops_campaign_company(self):
        from data.hectare_repository import HectareRepository
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE PesosFres(IdSocio INTEGER, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, Neto REAL, NetoPartida REAL)")
        conn.executemany("INSERT INTO PesosFres VALUES(?,?,?,?,?,?)", [
            (1, "2026", "1", "CITRICOS", 100, 0),
            (1, "2026", "1", "MANDARINA", 100, 90),
            (1, "2026", "1", "DIRECTO", 100, 0),
            (1, "2026", "1", "DIRECTOCHF", 100, 0),
            (1, "2026", "1", "INDUSTRIA", 100, 0),
            (1, "2026", "1", "OTRO", 999, 0),
            (1, "2025", "1", "CITRICOS", 999, 0),
            (1, "2026", "2", "CITRICOS", 999, 0),
        ])
        repo = HectareRepository(conn)
        self.assertEqual(repo.total_effective_kg(1, "2026", "1", ("CITRICOS", "MANDARINA", "DIRECTO", "DIRECTOCHF", "INDUSTRIA")), Decimal("490"))

    def test_hectare_applicable_surface_uses_dparcela(self):
        from data.hectare_repository import HectareRepository
        conn = sqlite3.connect(":memory:")
        conn.execute("ATTACH DATABASE ':memory:' AS eepp")
        conn.execute("CREATE TABLE eepp.DEEPP(Boleta TEXT, IdSocio INTEGER, CAMPAÑA TEXT, EMPRESA TEXT, CHA INTEGER, SupCul REAL)")
        conn.execute("CREATE TABLE eepp.DParcela(Boleta TEXT, IdPM TEXT, Pol TEXT, Par TEXT, Recinto TEXT, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, SupCul REAL, BAJA TEXT, Año INTEGER)")
        conn.executemany("INSERT INTO eepp.DEEPP VALUES(?,?,?,?,?,?)", [("B1", 1, "2026", "1", -1, 99), ("B2", 1, "2026", "1", 0, 99)])
        conn.executemany("INSERT INTO eepp.DParcela VALUES(?,?,?,?,?,?,?,?,?,?,?)", [("B1", "PM1", "P", "A", "R", "2026", "1", "CITRICOS", 3.5, None, 2020), ("B1", "PM1", "P", "A", "R", "2026", "1", "CITRICOS", 3.5, None, 2020), ("B2", "PM2", "P", "B", "R", "2026", "1", "CITRICOS", 7, None, 2020)])
        hectares, warnings = HectareRepository(conn).calculate_applicable_hectares(1, "2026", "1")
        self.assertEqual(hectares, Decimal("3.5"))
        self.assertTrue(warnings)

    def test_status_values(self):
        self.assertEqual(CalculationStatus.NOT_APPLICABLE.value, "not_applicable")
        self.assertEqual(CalculationStatus.ERROR.value, "error")


if __name__ == "__main__":
    unittest.main()
