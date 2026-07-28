from decimal import Decimal
import sqlite3
import unittest

from domain.liquidacion_calculator import (
    LiquidacionCalculator,
    calculate_delivery_collection,
    calculate_member_collection,
    calculate_member_transport,
)
from domain.models import Delivery, Remesa
from domain.utils import decimal_or_zero


def delivery(socio=1, variedad="TANGO", rec=0, ss=0, man=0, trans=0, neto=100):
    return Delivery(
        fecha="01/01/2026",
        registro=f"R{socio}{variedad}{rec}",
        socio=socio,
        nombre_socio=f"Socio {socio}",
        variedad=variedad,
        categoria="NORMAL",
        neto=decimal_or_zero(neto),
        albaran="A",
        boleta="B",
        plataforma="P",
        liquidado="N",
        collection_cost=decimal_or_zero(rec),
        social_security_collection=decimal_or_zero(ss),
        foreman_cost=decimal_or_zero(man),
        transport_cost=decimal_or_zero(trans),
        extra={f"Cal{i}": Decimal("0") for i in range(12)},
    )


class CollectionTransportTests(unittest.TestCase):
    def test_decimal_conversion(self):
        cases = [(None, "0"), ("", "0"), ("12.50", "12.50"), ("12,50", "12.50"), (0, "0"), (5, "5")]
        for value, expected in cases:
            self.assertEqual(decimal_or_zero(value), Decimal(expected))

    def test_delivery_collection(self):
        self.assertEqual(calculate_delivery_collection(delivery(rec=100, ss=20, man=5)), Decimal("125"))

    def test_null_values(self):
        d = delivery(rec=None, ss=20, man=None)
        self.assertEqual(calculate_delivery_collection(d), Decimal("20"))

    def test_member_collection_sum(self):
        detected, applied = calculate_member_collection([delivery(rec=100, ss=20, man=5), delivery(rec=200, ss=30, man=10)], True)
        self.assertEqual(detected, Decimal("365"))
        self.assertEqual(applied, Decimal("365"))

    def test_collection_disabled(self):
        detected, applied = calculate_member_collection([delivery(rec=100, ss=20, man=5), delivery(rec=200, ss=30, man=10)], False)
        self.assertEqual(detected, Decimal("365"))
        self.assertEqual(applied, Decimal("0"))

    def test_transport_sign(self):
        detected, applied = calculate_member_transport([delivery(trans=-100), delivery(trans=50), delivery(trans=0)], True)
        self.assertEqual(detected, Decimal("-50"))
        self.assertEqual(applied, Decimal("-50"))

    def test_transport_disabled(self):
        detected, applied = calculate_member_transport([delivery(trans=-100), delivery(trans=50), delivery(trans=0)], False)
        self.assertEqual(detected, Decimal("-50"))
        self.assertEqual(applied, Decimal("0"))

    def test_grouping_by_member_and_variety(self):
        result = LiquidacionCalculator().calculate(
            [delivery(1, "A", 100, 20, 5, 1), delivery(1, "B", 200, 30, 10, 2), delivery(2, "A", 300, 40, 15, 3)],
            Remesa({"AplRec": "S", "AplTte": "S"}),
        ).result
        values = {(m.member_id, m.variety): (m.collection_amount, m.transport_amount) for m in result.member_results}
        self.assertEqual(values[(1, "A")], (Decimal("125.00"), Decimal("1.00")))
        self.assertEqual(values[(1, "B")], (Decimal("240.00"), Decimal("2.00")))
        self.assertEqual(values[(2, "A")], (Decimal("355.00"), Decimal("3.00")))

    def test_general_totals_match_lines(self):
        result = LiquidacionCalculator().calculate([delivery(1, "A", 100, 20, 5, -100), delivery(1, "A", 200, 30, 10, 50)], Remesa({"AplRec": "S", "AplTte": "S"})).result
        self.assertEqual(result.totals.collection_amount, sum((m.collection_amount for m in result.member_results), Decimal("0")))
        self.assertEqual(result.totals.transport_amount, sum((m.transport_amount for m in result.member_results), Decimal("0")))

    def test_no_write_in_sqlite_calculation(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript("CREATE TABLE PesosFres(id INTEGER); CREATE TABLE PagosCIT(id INTEGER); CREATE TABLE DLiquidaciones(id INTEGER); INSERT INTO PesosFres VALUES (1);")
        before = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("PesosFres", "PagosCIT", "DLiquidaciones")}
        LiquidacionCalculator().calculate([delivery()], Remesa({"AplRec": "S", "AplTte": "S"}))
        after = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in before}
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
