from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from domain.hectare_fee_master import HectareFeeMaster, HectareFeeMasterRepository, fingerprint_master, master_from_json, parse_decimal


class HectareFeeMasterTests(unittest.TestCase):
    def test_initial_creation_load_save_restore_and_fingerprint(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "config" / "maestro_cuota_ha.json"
            repo = HectareFeeMasterRepository(path)
            master = repo.load()
            self.assertTrue(path.exists())
            self.assertEqual(master.price_per_hectare, Decimal("195.00"))
            self.assertEqual(master.surface_crops, ("CITRICOS", "MANDARINA"))
            changed = HectareFeeMaster(Decimal("200.50"), ("CITRICOS",), ("DIRECTO", "INDUSTRIA"))
            repo.save(changed)
            loaded = repo.load()
            self.assertEqual(loaded.price_per_hectare, Decimal("200.50"))
            self.assertEqual(loaded.fingerprint, fingerprint_master(loaded))
            restored = repo.restore_defaults()
            self.assertEqual(restored.delivery_crops, ("CITRICOS", "MANDARINA", "DIRECTO", "DIRECTOCHF", "INDUSTRIA"))

    def test_corrupt_json_is_backed_up_and_defaults_restored(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "maestro_cuota_ha.json"
            path.write_text("{bad", encoding="utf-8")
            repo = HectareFeeMasterRepository(path)
            master = repo.load()
            self.assertEqual(master.price_per_hectare, Decimal("195.00"))
            self.assertTrue(list(path.parent.glob("maestro_cuota_ha_corrupto_*.json")))

    def test_normalizes_duplicates_and_rejects_invalid_price(self):
        data = {"version": 1, "price_per_hectare": "195,00", "surface_crops": [{"crop": " citricos ", "enabled": True}, {"crop": "CITRICOS", "enabled": True}, {"crop": "x", "enabled": False}], "delivery_crops": [" directo ", "DIRECTO"]}
        master = master_from_json(data)
        self.assertEqual(master.price_per_hectare, Decimal("195.00"))
        self.assertEqual(master.surface_crops, ("CITRICOS",))
        self.assertEqual(master.delivery_crops, ("DIRECTO",))
        with self.assertRaises(ValueError):
            parse_decimal("0")
        with self.assertRaises(ValueError):
            parse_decimal("abc")

    def test_atomic_write_leaves_valid_json(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "maestro_cuota_ha.json"
            repo = HectareFeeMasterRepository(path)
            repo.save(HectareFeeMaster(Decimal("195"), ("MANDARINA",), ("DIRECTO",)))
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["price_per_hectare"], "195.00")
            self.assertEqual(payload["surface_crops"][0]["crop"], "MANDARINA")


if __name__ == "__main__":
    unittest.main()
