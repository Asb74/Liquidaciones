from decimal import Decimal
import sqlite3
import unittest

from data.hectare_repository import HectareRepository
from domain.calculation_models import LiquidationCalculationResult
from domain.liquidacion_calculator import LiquidacionCalculator
from domain.models import Delivery, Remesa


class IndividualLiquidationHectareRegressionTests(unittest.TestCase):
    """Regression coverage for the individual-calculation Cuota Ha audit path."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("ATTACH DATABASE ':memory:' AS eepp")
        self.conn.execute("CREATE TABLE PesosFres(IdSocio INTEGER, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, Neto NUMERIC, NetoPartida NUMERIC)")
        self.conn.execute("CREATE TABLE eepp.DEEPP(Boleta TEXT, IdSocio INTEGER, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, CHA TEXT, BAJA TEXT, SupCul TEXT)")
        self.conn.execute('CREATE TABLE eepp.DParcela(Boleta TEXT, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, IdPM TEXT, Pol TEXT, Par TEXT, Rec TEXT, SupCul TEXT, SupApor TEXT, BAJA TEXT, "Año" TEXT)')

    def tearDown(self):
        self.conn.close()

    def add_boleta(self, boleta="B1", cha="1"):
        self.conn.execute("INSERT INTO eepp.DEEPP VALUES(?,?,?,?,?,?,?,?)", (boleta, 1, "2026", "1", "CITRICOS", cha, None, "0"))

    def add_parcela(self, boleta="B1", surface="1", year="2021", parcel="1"):
        self.conn.execute("INSERT INTO eepp.DParcela VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (boleta, "2026", "1", "CITRICOS", "PM", "1", parcel, "1", surface, "0", None, year))

    def calculate(self):
        self.conn.execute("INSERT INTO PesosFres VALUES(?,?,?,?,?,?)", (1, "2026", "1", "CITRICOS", 100, 0))
        delivery = Delivery("01/01/2026", "R1", 1, "Socio", "NAVEL", "NORMAL", Decimal("100"), "A", "B1", "P", "N", extra={"Cal0": Decimal("100")})
        remesa = Remesa({"IdREMESA": "1", "CAMPAÑA": "2026", "EMPRESA": "1", "CULTIVO": "CITRICOS", "AplCHa": "S", "P0": "1"})
        return LiquidacionCalculator(hectare_repository=HectareRepository(self.conn)).calculate([delivery], remesa)

    def assert_individual_calculation(self, expected_hectares, expected_incidents=()):
        calculation = self.calculate()
        self.assertIsInstance(calculation, LiquidationCalculationResult)
        member = calculation.result.member_results[0]
        self.assertEqual(member.applicable_hectares, Decimal(expected_hectares))
        self.assertEqual(member.hectare_fee_amount, Decimal(expected_hectares) * Decimal("195.00"))
        # The commercial amount remains unchanged; only the Cuota Ha deduction varies.
        self.assertEqual(member.gross_amount, Decimal("100.00"))
        self.assertEqual(member.taxable_base, Decimal("100.00") - member.hectare_fee_amount)
        incidents = {incident for boleta in member.hectare_fee_audit.reviewed_boletas for incident in boleta["incidencias"]}
        self.assertTrue(set(expected_incidents).issubset(incidents))
        return member

    def test_case_a_valid_parcel_calculates_individual_liquidation(self):
        self.add_boleta(); self.add_parcela(surface="1", year="2021")
        self.assert_individual_calculation("1")

    def test_case_b_zero_surface_is_audited_and_positive_surface_is_used(self):
        self.add_boleta(); self.add_parcela(surface="0", parcel="1"); self.add_parcela(surface="1", parcel="2")
        member = self.assert_individual_calculation("1", ("PARCELA_SUPERFICIE_CERO",))
        self.assertIn("PARCELA_SUPERFICIE_CERO", member.hectare_fee_audit.reason)

    def test_case_c_different_planting_years_are_recorded_as_an_incident(self):
        self.add_boleta(); self.add_parcela(surface="1", year="2021", parcel="1"); self.add_parcela(surface="2", year="2000", parcel="2")
        self.assert_individual_calculation("3", ("ANOS_PLANTACION_INCOHERENTES",))

    def test_case_d_young_parcel_is_excluded_from_fee(self):
        self.add_boleta(); self.add_parcela(surface="1", year="2022")
        member = self.assert_individual_calculation("0")
        self.assertEqual(member.hectare_fee_audit.young_parcels, 1)
        self.assertIn("PLANTACION_MENOR_CINCO_ANOS", member.hectare_fee_audit.reason)

    def test_case_e_inactive_cha_is_audited_without_name_error(self):
        self.add_boleta(cha="0"); self.add_parcela(surface="1")
        member = self.assert_individual_calculation("0")
        self.assertIn("CHA_NO_SELECCIONADO", member.hectare_fee_audit.reviewed_boletas[0]["motivos_exclusion"])


if __name__ == "__main__":
    unittest.main()
