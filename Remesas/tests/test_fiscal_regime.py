from __future__ import annotations

import sqlite3
import unittest
from decimal import Decimal

from data.fiscal_regime_repository import FiscalRegimeRepository, normalize_fiscal_regime
from domain.liquidacion_calculator import LiquidacionCalculator
from domain.models import Delivery, Remesa


class FiscalRegimeRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("CREATE TABLE DSocio (IdSocio INTEGER, RegimeFiscal TEXT, Inactivo INTEGER, Baja TEXT)")
        self.conn.execute("CREATE TABLE MRegimenFiscal (Regimen TEXT, Iva NUMERIC, Retencion NUMERIC)")
        self.conn.executemany(
            "INSERT INTO MRegimenFiscal VALUES (?,?,?)",
            [("Módulo", 12, 2), ("Estimación Directa Ret.", 4, 2), ("Estimación Directa Sin Ret.", 4, 0)],
        )
        self.repo = FiscalRegimeRepository(self.conn)

    def test_normalization_ignores_case_strip_and_double_spaces(self):
        self.assertEqual(normalize_fiscal_regime("  Estimación   Directa Sin Ret. "), normalize_fiscal_regime("estimación directa sin ret."))

    def test_module_rates(self):
        self.conn.execute("INSERT INTO DSocio VALUES (1355, ' Módulo ', 0, NULL)")
        lookup = self.repo.get_for_member(1355)
        self.assertEqual(lookup.regime.vat_rate, Decimal("12"))
        self.assertEqual(lookup.regime.withholding_rate, Decimal("2"))

    def test_direct_ret_rates(self):
        self.conn.execute("INSERT INTO DSocio VALUES (1, 'Estimación Directa Ret.', 0, '')")
        lookup = self.repo.get_for_member(1)
        self.assertEqual(lookup.regime.vat_rate, Decimal("4"))
        self.assertEqual(lookup.regime.withholding_rate, Decimal("2"))

    def test_direct_without_ret_rates(self):
        self.conn.execute("INSERT INTO DSocio VALUES (2, 'Estimación Directa Sin Ret.', 0, '')")
        lookup = self.repo.get_for_member(2)
        self.assertEqual(lookup.regime.vat_rate, Decimal("4"))
        self.assertEqual(lookup.regime.withholding_rate, Decimal("0"))

    def test_duplicate_active_member_is_error(self):
        self.conn.executemany("INSERT INTO DSocio VALUES (?,?,?,?)", [(3, "Módulo", 0, None), (3, "Módulo", 0, "")])
        with self.assertRaisesRegex(ValueError, "Duplicidad de socio"):
            self.repo.get_for_member(3)


class FiscalRegimeCalculationTests(unittest.TestCase):
    def test_calculates_vat_withholding_and_total_from_repository(self):
        class FakeFiscalRepo:
            def get_for_member(self, member_id):
                from domain.calculation_models import FiscalRegime
                from data.fiscal_regime_repository import FiscalRegimeLookup
                return FiscalRegimeLookup(FiscalRegime("Módulo", Decimal("12"), Decimal("2")))

        delivery = Delivery("2026-01-01", 1, 1355, "Socio 1355", "TANGO", None, Decimal("100"), None, None, None, None, extra={"Cal0": Decimal("100")})
        remesa = Remesa({"IdREMESA": 1, "REMESA": "Test", "CAMPAÑA": "2026", "EMPRESA": "1", "CULTIVO": "OTRO", "P0": Decimal("356.7541"), "AplRec": "No", "AplTte": "No", "AplCal": "No", "AplGlobal": "No", "AplCHa": "No"})
        result = LiquidacionCalculator(fiscal_regime_repository=FakeFiscalRepo()).calculate([delivery], remesa).result
        member = result.member_results[0]
        self.assertEqual(member.taxable_base, Decimal("35675.41"))
        self.assertEqual(member.vat_rate, Decimal("12"))
        self.assertEqual(member.withholding_rate, Decimal("2"))
        self.assertEqual(member.vat_amount, Decimal("4281.05"))
        self.assertEqual(member.withholding_amount, Decimal("713.51"))
        self.assertEqual(member.total_amount, Decimal("39242.95"))


if __name__ == "__main__":
    unittest.main()
