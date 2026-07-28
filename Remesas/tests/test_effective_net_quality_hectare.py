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

    def test_hectare_applicable_surface_uses_dparcela_rec_for_mandarina(self):
        from data.hectare_repository import HectareRepository
        conn = sqlite3.connect(":memory:")
        conn.execute("ATTACH DATABASE ':memory:' AS eepp")
        conn.execute("CREATE TABLE eepp.DEEPP(Boleta TEXT, IdSocio INTEGER, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, CHA INTEGER, BAJA TEXT, Recinto TEXT, SupCul REAL)")
        conn.execute("CREATE TABLE eepp.DParcela(Boleta TEXT, IdPM TEXT, Pol TEXT, Par TEXT, Rec TEXT, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, SupCul REAL, SupApor REAL, BAJA TEXT, Año INTEGER)")
        dparcela_columns = [row[1] for row in conn.execute("PRAGMA eepp.table_info('DParcela')").fetchall()]
        self.assertIn("Rec", dparcela_columns)
        self.assertNotIn("Recinto", dparcela_columns)
        conn.executemany("INSERT INTO eepp.DEEPP VALUES(?,?,?,?,?,?,?,?,?)", [("B1", 1, "2026", "1", "MANDARINA", -1, None, "R", 99), ("B2", 1, "2026", "1", "MANDARINA", 0, None, "R", 99)])
        conn.executemany("INSERT INTO eepp.DParcela VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", [("B1", "PM1", "P", "A", "R", "2026", "1", "MANDARINA", 99, 3.5, None, 2020), ("B1", "PM1", "P", "A", "R", "2026", "1", "MANDARINA", 99, 3.5, None, 2020), ("B2", "PM2", "P", "B", "R", "2026", "1", "MANDARINA", 99, 7, None, 2020)])
        try:
            hectares, warnings = HectareRepository(conn).calculate_applicable_hectares(1, "2026", "1")
        except sqlite3.OperationalError as exc:
            self.fail(f"La consulta real de superficie para MANDARINA no debe fallar: {exc}")
        self.assertEqual(hectares, Decimal("198.0"))
        self.assertFalse(warnings)


    def test_hectare_flags_age_baja_and_surface_rules(self):
        from data.hectare_repository import HectareRepository, is_active_flag, is_old_enough_for_hectare_fee
        self.assertTrue(is_active_flag(-1))
        self.assertTrue(is_active_flag("SÍ"))
        self.assertFalse(is_active_flag(0))
        self.assertTrue(is_old_enough_for_hectare_fee(2021, 2026))
        self.assertFalse(is_old_enough_for_hectare_fee(2022, 2026))
        self.assertFalse(is_old_enough_for_hectare_fee("", 2026))

        conn = sqlite3.connect(":memory:")
        conn.execute("ATTACH DATABASE ':memory:' AS eepp")
        conn.execute("CREATE TABLE eepp.DEEPP(Boleta TEXT, IdSocio INTEGER, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, CHA TEXT, BAJA TEXT, SupCul TEXT)")
        conn.execute("CREATE TABLE eepp.DParcela(Boleta TEXT, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, IdPM TEXT, Pol TEXT, Par TEXT, Rec TEXT, SupCul TEXT, SupApor TEXT, BAJA TEXT, Año TEXT)")
        conn.executemany("INSERT INTO eepp.DEEPP VALUES(?,?,?,?,?,?,?,?)", [
            ("B0", 1, "2026", "1", "CITRICOS", "0", None, "9"),
            ("B1", 1, "2026", "1", "CITRICOS", "-1", None, "9"),
            ("B2", 1, "2026", "1", "CITRICOS", "S", None, "9"),
            ("B3", 1, "2026", "1", "CITRICOS", "1", None, "9"),
        ])
        conn.executemany("INSERT INTO eepp.DParcela VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", [
            ("B0", "2026", "1", "CITRICOS", "PM0", "P", "A", "R", "0,44", "99", None, "2000"),
            ("B1", "2026", "1", "CITRICOS", "PM1", "P", "A", "R", "1,50", "99", None, "2021"),
            ("B1", "2026", "1", "CITRICOS", "PM2", "P", "B", "R", "2", "99", None, "2022"),
            ("B2", "2026", "1", "CITRICOS", "PM3", "P", "C", "R", "3", "99", "2024-01-01", "2000"),
            ("B3", "2026", "1", "CITRICOS", "PM4", "P", "D", "R", "4", "99", None, "abc"),
        ])
        repo = HectareRepository(conn)
        hectares, warnings = repo.calculate_applicable_hectares(1, "2026", "1", ("CITRICOS",))
        self.assertEqual(hectares, Decimal("1.50"))
        reasons = ";".join(str(r.get("Motivo exclusión")) for r in repo.last_surface_audit_rows)
        self.assertIn("CHA_NO_ACTIVO", reasons)
        self.assertIn("PLANTACION_MENOR_CINCO_ANOS", reasons)
        self.assertIn("PARCELA_DADA_DE_BAJA", reasons)
        self.assertIn("ANO_NO_VALIDO", reasons)

    def test_status_values(self):
        self.assertEqual(CalculationStatus.NOT_APPLICABLE.value, "not_applicable")
        self.assertEqual(CalculationStatus.ERROR.value, "error")


if __name__ == "__main__":
    unittest.main()
