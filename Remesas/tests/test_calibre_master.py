from __future__ import annotations
from decimal import Decimal
import tempfile, unittest
from pathlib import Path
from data.calibre_master_repository import CalibreMasterRepository
from domain.calibre_master import CalibreMasterItem, DEFAULT_CALIBRE_MASTER
from services.calibre_master_service import CalibreMasterService
from domain.calculation_models import GradeBreakdown, LiquidationHeader, MemberLiquidation
from presentation.premium_liquidation_view_model import from_member_liquidation

class CalibreMasterTests(unittest.TestCase):
    def service(self):
        tmp=tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup); path=Path(tmp.name)/"calibre_master.json"; repo=CalibreMasterRepository(path); repo.save_items(CalibreMasterItem(**i) for i in DEFAULT_CALIBRE_MASTER["items"]); return CalibreMasterService(repo, Path(tmp.name)/"calibre.log")
    def test_citricos(self):
        s=self.service(); self.assertEqual(s.resolve_label("CITRICOS",0),"CAL 0"); self.assertEqual(s.resolve_label("CITRICOS",11),"CAL 11")
    def test_kakis(self):
        s=self.service(); self.assertEqual(s.resolve_label("KAKIS",0),"AAA 1ª"); self.assertEqual(s.resolve_label("KAKIS",6),"AAA 2ª"); self.assertEqual(s.resolve_label("KAKIS",11),"D 2ª")
    def test_sandia(self):
        s=self.service(); self.assertEqual(s.resolve_label("SANDIA",0),"CAL 1 1ª"); self.assertEqual(s.resolve_label("SANDIA",10),"CAL 1/6 2ª"); self.assertEqual(s.resolve_label("SANDIA",11),"CAL 6/10 2ª")
    def test_alias(self): self.assertEqual(self.service().normalize_crop("CAQUI"),"KAKIS")
    def test_unknown_crop_fallback_never_uses_kakis(self):
        s=self.service(); label=s.resolve_label("DESCONOCIDO",0); self.assertEqual(label,"CAL 0"); self.assertNotEqual(label,"AAA 1ª")
    def test_order(self):
        tmp=tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup); repo=CalibreMasterRepository(Path(tmp.name)/"m.json"); repo.save_items(CalibreMasterItem(**i) for i in DEFAULT_CALIBRE_MASTER["items"]); self.assertEqual([i.base for i in repo.get_crop_items("CITRICOS")],[f"c{i}" for i in range(12)])
    def test_duplicates_rejected(self):
        tmp=tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup); repo=CalibreMasterRepository(Path(tmp.name)/"m.json")
        with self.assertRaises(ValueError): repo.save_items([CalibreMasterItem("c0","CITRICOS","CAL 0",0), CalibreMasterItem("c0","CITRICOS","CAL 0 bis",0)])
    def test_premium_summary_has_commercial_amount(self):
        header=LiquidationHeader("1","R","2026","1","CITRICOS","","","","","","",[],{}, {})
        member=MemberLiquidation(1,"Socio","Var",1,Decimal("10"),Decimal("10"),Decimal("0"),Decimal("0"),(GradeBreakdown("c0","CAL 0",Decimal("10"),Decimal("1"),Decimal("10")),),Decimal("10"),gross_amount=Decimal("10"),effective_net_kg=Decimal("10"),commercial_average_price=Decimal("1"))
        vm=from_member_liquidation(header, member); self.assertEqual(vm.commercial_amount, Decimal("10")); self.assertEqual(vm.commercial_breakdown_title,"DESGLOSE COMERCIAL POR CALIBRES")
if __name__ == "__main__": unittest.main()
