from __future__ import annotations

from decimal import Decimal
import sqlite3
from pathlib import Path
import tempfile
import unittest

from data.hectare_fee_master_repository import HectareFeeCropRepository
from domain.hectare_fee_master import HectareFeeMaster, HectareFeeMasterRepository
from services.hectare_fee_master_service import HectareFeeMasterService


class HectareFeeMasterServiceTests(unittest.TestCase):
    def test_lists_crops_loads_saves_and_restores_master(self):
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            fruta = td_path / "DBfruta.sqlite"
            eepp = td_path / "DBEEPPL.sqlite"
            with sqlite3.connect(fruta) as conn:
                conn.execute("CREATE TABLE PesosFres (CULTIVO TEXT)")
                conn.executemany("INSERT INTO PesosFres (CULTIVO) VALUES (?)", [(" citricos ",), ("DIRECTO",), ("",), (None,)])
            with sqlite3.connect(eepp) as conn:
                conn.execute("CREATE TABLE DEEPP (CULTIVO TEXT)")
                conn.executemany("INSERT INTO DEEPP (CULTIVO) VALUES (?)", [("mandarina",), ("CITRICOS",), (" ",), (None,)])
            conn = sqlite3.connect(fruta)
            try:
                conn.execute(f"ATTACH DATABASE '{eepp}' AS eepp")
                service = HectareFeeMasterService(
                    HectareFeeMasterRepository(td_path / "maestro_cuota_ha.json"),
                    HectareFeeCropRepository(conn),
                )
                self.assertEqual(service.list_surface_crop_options(), ["CITRICOS", "MANDARINA"])
                self.assertEqual(service.list_delivery_crop_options(), ["CITRICOS", "DIRECTO"])
                self.assertEqual(service.load_master().price_per_hectare, Decimal("195.00"))
                service.save_master(HectareFeeMaster(Decimal("210.00"), ("CITRICOS",), ("DIRECTO",)))
                self.assertEqual(service.load_master().price_per_hectare, Decimal("210.00"))
                self.assertEqual(service.restore_defaults().price_per_hectare, Decimal("195.00"))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
