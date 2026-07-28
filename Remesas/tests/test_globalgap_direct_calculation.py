from __future__ import annotations

from decimal import Decimal
import sqlite3
import unittest

from data.globalgap_repository import GlobalGapRepository, normalize_certification
from domain.calculation_models import CalculationStatus
from domain.liquidacion_calculator import LiquidacionCalculator
from domain.models import Delivery, Remesa


def make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("ATTACH DATABASE ':memory:' AS eepp")
    conn.executescript('''
    CREATE TABLE eepp.DEEPP (IdSocio INTEGER, "CAMPAÑA" TEXT, EMPRESA TEXT, CULTIVO TEXT, Certificacion TEXT, NivelGlobal TEXT, Boleta TEXT, Variedad TEXT, BAJA TEXT);
    CREATE TABLE eepp.MNivelGlobal (Nivel TEXT, Indice TEXT);
    CREATE TABLE BonGlobal ("CAMPAÑA" TEXT, EMPRESA TEXT, CULTIVO TEXT, TipoLiq TEXT, Bonificacion TEXT, CATEGORIA INTEGER);
    INSERT INTO eepp.MNivelGlobal VALUES ('ORO','1'), ('PLATA','0.5'), ('NEG','-1');
    INSERT INTO BonGlobal VALUES ('2026','1','MANDARINA','', '0.015', 1);
    ''')
    return conn


def delivery(socio=1355, variety="TANGO", net="100", batch="0", cal0="80", cal1="20"):
    return Delivery(None, 1, socio, "Socio", variety, None, Decimal(net), None, None, None, None, Decimal(batch), extra={"Cal0": Decimal(cal0), "Cal1": Decimal(cal1)})


def remesa(apl="S", categoria=1):
    vals = {"IdREMESA": 1, "REMESA": "R", "CAMPAÑA": "2026", "EMPRESA": "1", "CULTIVO": "MANDARINA", "TipoLiq": "", "AplGlobal": apl}
    for i in range(12):
        vals[f"P{i}"] = "0"
    vals.update({"PDESTRIO":"0","PDMESA":"0","PPODRIDO":"0"})
    return Remesa(vals)


class GlobalGapDirectCalculationTest(unittest.TestCase):
    def test_normalize_certification_variants(self):
        self.assertEqual(normalize_certification("Global Gap"), "GLOBALGAP")
        self.assertEqual(normalize_certification("GLOBAL-GAP"), "GLOBALGAP")
        self.assertEqual(normalize_certification(" globalgap "), "GLOBALGAP")

    def test_certification_inconsistent_warns_and_calculates(self):
        conn = make_conn()
        conn.execute("INSERT INTO eepp.DEEPP VALUES (1355,'2026','1','CITRICOS','Global Gap','ORO',NULL,NULL,NULL)")
        conn.execute("INSERT INTO eepp.DEEPP VALUES (1355,'2026','1','MANDARINA','','ORO',NULL,NULL,NULL)")
        calc = LiquidacionCalculator(globalgap_repository=GlobalGapRepository(conn))
        result = calc.calculate([delivery()], remesa()).result
        member = result.member_results[0]
        self.assertEqual(member.globalgap_amount, Decimal("1.50"))
        self.assertEqual(member.statuses["globalgap"], CalculationStatus.CALCULATED)
        self.assertTrue(member.globalgap_audit.certification_inconsistent)
        self.assertIn("Revise DEEPP", "; ".join(member.globalgap_audit.warnings))

    def test_disabled_checkbox_keeps_zero(self):
        conn = make_conn()
        conn.execute("INSERT INTO eepp.DEEPP VALUES (1355,'2026','1','MANDARINA','GlobalGAP','ORO',NULL,NULL,NULL)")
        calc = LiquidacionCalculator(globalgap_repository=GlobalGapRepository(conn))
        member = calc.calculate([delivery()], remesa('N')).result.member_results[0]
        self.assertEqual(member.globalgap_amount, Decimal("0"))
        self.assertEqual(member.statuses["globalgap"], CalculationStatus.DISABLED)

    def test_not_certified_is_not_applicable(self):
        conn = make_conn()
        conn.execute("INSERT INTO eepp.DEEPP VALUES (1355,'2026','1','MANDARINA','',NULL,NULL,NULL,NULL)")
        member = LiquidacionCalculator(globalgap_repository=GlobalGapRepository(conn)).calculate([delivery()], remesa()).result.member_results[0]
        self.assertEqual(member.globalgap_amount, Decimal("0"))
        self.assertEqual(member.statuses["globalgap"], CalculationStatus.NOT_APPLICABLE)

    def test_multiple_levels_error(self):
        conn = make_conn()
        conn.execute("INSERT INTO eepp.DEEPP VALUES (1355,'2026','1','A','GlobalGAP','ORO',NULL,NULL,NULL)")
        conn.execute("INSERT INTO eepp.DEEPP VALUES (1355,'2026','1','B','GlobalGAP','PLATA',NULL,NULL,NULL)")
        member = LiquidacionCalculator(globalgap_repository=GlobalGapRepository(conn)).calculate([delivery()], remesa()).result.member_results[0]
        self.assertIsNone(member.globalgap_amount)
        self.assertEqual(member.statuses["globalgap"], CalculationStatus.ERROR)

    def test_category_zero_uses_commercial_net_and_no_global_table(self):
        executed = []
        class Guard(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                executed.append(sql.upper())
                return super().execute(sql, parameters)
        conn = sqlite3.connect(":memory:", factory=Guard)
        conn.row_factory = sqlite3.Row
        conn.execute("ATTACH DATABASE ':memory:' AS eepp")
        conn.executescript('''
        CREATE TABLE eepp.DEEPP (IdSocio INTEGER, "CAMPAÑA" TEXT, EMPRESA TEXT, CULTIVO TEXT, Certificacion TEXT, NivelGlobal TEXT, Boleta TEXT, Variedad TEXT, BAJA TEXT);
        CREATE TABLE eepp.MNivelGlobal (Nivel TEXT, Indice TEXT);
        CREATE TABLE BonGlobal ("CAMPAÑA" TEXT, EMPRESA TEXT, CULTIVO TEXT, Bonificacion TEXT, CATEGORIA INTEGER);
        INSERT INTO eepp.DEEPP VALUES (1355,'2026','1','MANDARINA','GlobalGAP','ORO',NULL,NULL,NULL);
        INSERT INTO eepp.MNivelGlobal VALUES ('ORO','2');
        INSERT INTO BonGlobal VALUES ('2026','1','MANDARINA','0.01',0);
        ''')
        member = LiquidacionCalculator(globalgap_repository=GlobalGapRepository(conn)).calculate([delivery(net="100", cal0="30", cal1="20")], remesa()).result.member_results[0]
        self.assertEqual(member.globalgap_audit.base_type, "neto_comercial")
        self.assertEqual(member.globalgap_audit.base_kg, Decimal("50"))
        self.assertEqual(member.globalgap_amount, Decimal("1.00"))
        statements = "\n".join(executed).replace("GLOBALGAP", "")
        self.assertNotIn("FROM GLOBAL", statements)
        self.assertNotIn("INSERT INTO GLOBAL", statements)
        self.assertNotIn("UPDATE GLOBAL", statements)
        self.assertNotIn("DELETE FROM GLOBAL", statements)


if __name__ == "__main__":
    unittest.main()
