from decimal import Decimal
import sqlite3
import unittest

from data.hectare_repository import HectareRepository
from domain.utils import round_money


class HectareSurfaceRowsTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("ATTACH DATABASE ':memory:' AS eepp")
        self.conn.execute("CREATE TABLE eepp.DEEPP(Boleta TEXT, IdSocio INTEGER, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, CHA TEXT, BAJA TEXT, SupCul TEXT)")
        self.conn.execute('CREATE TABLE eepp.DParcela(Boleta TEXT, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, IdPM TEXT, Pol TEXT, Par TEXT, Rec TEXT, SupCul TEXT, SupApor TEXT, BAJA TEXT, "Año" TEXT)')

    def add_deepp(self, boleta="512", socio=883, cha="-1", baja=None, sup="0"):
        self.conn.execute("INSERT INTO eepp.DEEPP VALUES(?,?,?,?,?,?,?,?)", (boleta, socio, "2026", "1", "CITRICOS", cha, baja, sup))

    def add_parcela(self, boleta="512", pol="12", par="62", rec="1", year="2002", sup="3,24", baja=None):
        self.conn.execute("INSERT INTO eepp.DParcela VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (boleta, "2026", "1", "CITRICOS", "PM", pol, par, rec, sup, "99", baja, year))

    def calculate(self, socio=883):
        return HectareRepository(self.conn).calculate_applicable_hectares(socio, "2026", "1", ("CITRICOS",))

    def test_one_valid_row(self):
        self.add_deepp(); self.add_parcela(sup="0,92")
        hectares, warnings = self.calculate()
        self.assertEqual(hectares, Decimal("0.92"))
        self.assertFalse(warnings)

    def test_regression_member_883_two_rows_same_location_are_summed(self):
        self.add_deepp()
        self.add_parcela(year="2002", sup="3,24")
        self.add_parcela(year="1990", sup="2,38")
        repo = HectareRepository(self.conn)
        hectares, warnings = repo.calculate_applicable_hectares(883, "2026", "1", ("CITRICOS",))
        self.assertEqual(hectares, Decimal("5.62"))
        self.assertEqual(round_money(hectares * Decimal("195")), Decimal("1095.90"))
        reasons = ";".join(str(r.get("Motivo") or r.get("Motivo exclusión")) for r in repo.last_surface_audit_rows)
        self.assertNotIn("CONFLICTO_SUPERFICIE", reasons)
        self.assertEqual(sum(1 for r in repo.last_surface_audit_rows if r.get("Incluida") == "Sí"), 2)

    def test_two_old_rows_with_different_year_are_summed(self):
        self.add_deepp(); self.add_parcela(year="2021", sup="1,10"); self.add_parcela(year="2000", sup="2,20")
        hectares, _ = self.calculate()
        self.assertEqual(hectares, Decimal("3.30"))

    def test_young_row_excludes_only_that_row(self):
        self.add_deepp(); self.add_parcela(year="2023", sup="9"); self.add_parcela(year="2021", sup="1")
        hectares, _ = self.calculate()
        self.assertEqual(hectares, Decimal("1"))

    def test_baja_row_excludes_only_that_row(self):
        self.add_deepp(); self.add_parcela(sup="9", baja="S"); self.add_parcela(sup="1")
        hectares, _ = self.calculate()
        self.assertEqual(hectares, Decimal("1"))

    def test_inactive_cha_generates_no_surface(self):
        self.add_deepp(cha="0"); self.add_parcela(sup="9")
        hectares, _ = self.calculate()
        self.assertEqual(hectares, Decimal("0"))

    def test_same_physical_row_repeated_by_deepp_join_is_counted_once(self):
        self.add_deepp(sup="A"); self.add_deepp(sup="B")
        self.add_parcela(sup="4")
        hectares, warnings = self.calculate()
        self.assertEqual(hectares, Decimal("4"))
        self.assertFalse(any("CONFLICTO_SUPERFICIE" in w for w in warnings))

    def test_two_physical_rows_with_identical_data_are_both_summed(self):
        self.add_deepp(); self.add_parcela(sup="2"); self.add_parcela(sup="2")
        hectares, _ = self.calculate()
        self.assertEqual(hectares, Decimal("4"))

    def test_null_or_zero_surface_excludes_only_that_row(self):
        self.add_deepp(); self.add_parcela(sup="0"); self.add_parcela(sup=None); self.add_parcela(sup="1")
        hectares, _ = self.calculate()
        self.assertEqual(hectares, Decimal("1"))

    def test_member_without_valid_rows_returns_zero_without_exception(self):
        self.add_deepp(); self.add_parcela(year="2023", sup="9")
        hectares, _ = self.calculate()
        self.assertEqual(hectares, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
