from decimal import Decimal
import unittest

from domain.calculation_models import CalculationStatus, LiquidationHeader, MemberLiquidation
from domain.hectare_fee_master import HectareFeeMaster
from domain.liquidacion_calculator import LiquidacionCalculator


class FakeHectareRepository:
    def __init__(self, hectares=Decimal("2"), total_kg=Decimal("100000")):
        self.hectares = hectares
        self.total_kg = total_kg
        self.surface_calls = 0
        self.kg_calls = 0
        self.last_surface_audit_rows = (
            {"Boleta DEEPP": "A", "CHA activo": "Sí", "Boleta DParcela": "A", "Incluida": "Sí", "SupCul DParcela": Decimal("2"), "Motivo exclusión": ""},
            {"Boleta DEEPP": "B", "CHA activo": "No", "Incluida": "No", "Motivo exclusión": "CHA_NO_ACTIVO"},
            {"Boleta DEEPP": "C", "CHA activo": "Sí", "Boleta DParcela": "C", "Incluida": "No", "Motivo exclusión": "PLANTACION_MENOR_CINCO_ANOS"},
        )
        self.last_delivery_audit_rows = (
            {"Nº Socio": 1, "Socio": "Socio", "Cultivo": "CITRICOS", "NetoEfectivo": Decimal("40000"), "Relevancia de boleta": "No interviene en el prorrateo"},
            {"Nº Socio": 1, "Socio": "Socio", "Cultivo": "MANDARINA", "NetoEfectivo": Decimal("30000"), "Relevancia de boleta": "No interviene en el prorrateo"},
            {"Nº Socio": 1, "Socio": "Socio", "Cultivo": "DIRECTO", "NetoEfectivo": Decimal("30000"), "Relevancia de boleta": "No interviene en el prorrateo"},
        )

    def calculate_applicable_hectares(self, member_id, campaign, company, crops):
        self.surface_calls += 1
        self.received_surface_crops = tuple(crops)
        return self.hectares, ()

    def total_effective_kg(self, member_id, campaign, company, crops):
        self.kg_calls += 1
        self.received_delivery_crops = tuple(crops)
        return self.total_kg


def member(variety, kg):
    return MemberLiquidation(
        member_id=1, member_name="Socio", variety=variety, delivery_count=1,
        net_deliveries=kg, net_commercial=kg, net_waste=Decimal("0"), net_rotten=Decimal("0"),
        grades=(), commercial_amount=Decimal("0"), gross_amount=Decimal("0"),
        collection_amount=Decimal("0"), transport_amount=Decimal("0"), quality_amount=Decimal("0"),
        globalgap_amount=Decimal("0"), hectare_fee_amount=Decimal("0"), effective_net_kg=kg,
        statuses={"hectare_fee": CalculationStatus.PENDING}, source_deliveries=(),
    )


def header(crop="CITRICOS"):
    return LiquidationHeader("1", "BLANCA TEMPRANA", "2026", "1", crop, "", "", "", "", "", "", [], {"Cuota por hectárea": True}, {})


class HectareFeeProrationTests(unittest.TestCase):
    def test_non_apt_delivery_supports_global_fee(self):
        repo = FakeHectareRepository()
        calc = LiquidacionCalculator(hectare_repository=repo)
        calc.hectare_master = HectareFeeMaster(price_per_hectare=Decimal("195"), eligible_crops=("CITRICOS", "MANDARINA"))
        result = calc._apply_hectare_fee([member("NO_APTA", Decimal("30000"))], header(), True)[0]
        self.assertEqual(result.applicable_hectares, Decimal("2"))
        self.assertEqual(result.hectare_fee_total_member, Decimal("390.00"))
        self.assertEqual(result.hectare_fee_total_effective_kg, Decimal("100000"))
        self.assertEqual(result.hectare_fee_rate_per_kg, Decimal("0.0039"))
        self.assertEqual(result.hectare_fee_amount, Decimal("117.00"))
        self.assertEqual(result.hectare_fee_status, CalculationStatus.CALCULATED)
        self.assertEqual(repo.surface_calls, 1)
        self.assertEqual(repo.kg_calls, 1)
        self.assertEqual(repo.received_surface_crops, ("CITRICOS", "MANDARINA"))

    def test_young_parcel_kg_enter_denominator_but_not_surface(self):
        result = LiquidacionCalculator(hectare_repository=FakeHectareRepository())._apply_hectare_fee([member("JOVEN", Decimal("30000"))], header(), True)[0]
        self.assertEqual(result.hectare_fee_amount, Decimal("117.00"))
        self.assertEqual(result.hectare_fee_audit.young_parcels, 1)
        self.assertNotIn("DIRECTO", dict(result.hectare_fee_audit.kg_by_crop))
        self.assertEqual(result.hectare_fee_audit.eligible_crops, ("CITRICOS", "MANDARINA"))

    def test_same_member_multiple_varieties_reuse_same_index(self):
        repo = FakeHectareRepository()
        calc = LiquidacionCalculator(hectare_repository=repo)
        result = calc._apply_hectare_fee([member("A", Decimal("40000")), member("B", Decimal("60000"))], header(), True)
        self.assertEqual(result[0].hectare_fee_rate_per_kg, result[1].hectare_fee_rate_per_kg)
        self.assertEqual(result[0].hectare_fee_amount + result[1].hectare_fee_amount, Decimal("390.00"))
        self.assertEqual(repo.surface_calls, 1)
        self.assertEqual(repo.kg_calls, 1)

    def test_without_valid_parcels_is_not_applicable(self):
        result = LiquidacionCalculator(hectare_repository=FakeHectareRepository(Decimal("0"), Decimal("1000")))._apply_hectare_fee([member("A", Decimal("1000"))], header(), True)[0]
        self.assertEqual(result.hectare_fee_amount, Decimal("0"))
        self.assertEqual(result.hectare_fee_status, CalculationStatus.NOT_APPLICABLE)

    def test_positive_surface_without_kg_is_error(self):
        result = LiquidacionCalculator(hectare_repository=FakeHectareRepository(Decimal("2"), Decimal("0")))._apply_hectare_fee([member("A", Decimal("1000"))], header(), True)[0]
        self.assertIsNone(result.hectare_fee_amount)
        self.assertEqual(result.hectare_fee_status, CalculationStatus.ERROR)


if __name__ == "__main__":
    unittest.main()
