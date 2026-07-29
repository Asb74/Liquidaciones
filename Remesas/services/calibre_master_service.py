from __future__ import annotations
import logging
from pathlib import Path
from decimal import Decimal
from data.calibre_master_repository import CalibreMasterRepository
from domain.calibre_master import CalibreMasterItem, default_items, normalize_crop_value, validate_items
class CalibreMasterService:
    def __init__(self, repository: CalibreMasterRepository|None=None, log_path: str|Path="logs/calibre_master.log"):
        self.repository=repository or CalibreMasterRepository(); self.log_path=Path(log_path); self.log_path.parent.mkdir(parents=True,exist_ok=True); self.logger=logging.getLogger("calibre_master")
        if not any(isinstance(h,logging.FileHandler) and Path(h.baseFilename)==self.log_path.resolve() for h in self.logger.handlers):
            h=logging.FileHandler(self.log_path,encoding="utf-8"); h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s")); self.logger.addHandler(h)
        self.logger.setLevel(logging.INFO); self._items=self._safe_load_items(); self._aliases=self.repository.load_aliases(); self._display_types=self.repository.load_display_types()
    def _safe_load_items(self) -> tuple[CalibreMasterItem,...]:
        try: return self.repository.load_items()
        except Exception as exc:
            logging.getLogger(__name__).warning("Maestro de calibres inválido; se usan valores iniciales: %s", exc); self.logger.error("Maestro de calibres inválido; fallback inicial: %s", exc); return validate_items(default_items())
    def normalize_crop(self,crop: str) -> str: return normalize_crop_value(crop,self._aliases)
    def resolve_label(self,crop: str,calibre_index: int) -> str:
        idx=int(calibre_index); base=f"c{idx}"; normalized=self.normalize_crop(crop)
        for item in self._items:
            if item.base==base and item.crop==normalized and item.active: return item.label
        label=f"CAL {idx}"; self.logger.warning("No existe etiqueta para crop/base. crop=%s normalized_crop=%s base=%s Se usa %s.",crop,normalized,base,label); return label
    def display_type(self,crop: str) -> str: return self._display_types.get(self.normalize_crop(crop),"categorias")
    def commercial_breakdown_title(self,crop: str) -> str: return "DESGLOSE COMERCIAL POR CALIBRES" if self.display_type(crop)=="calibres" else "DESGLOSE COMERCIAL POR CATEGORÍAS"
    def audit_resolution(self,*,campaign: str,company: str,crop: str,calibre_index: int,label: str,kilograms: Decimal,price: Decimal,amount: Decimal):
        base=f"c{calibre_index}"; ok=any(i.base==base and i.crop==self.normalize_crop(crop) and i.active for i in self._items); warn="" if ok else f" warning=No existe etiqueta para crop/base. Se usa CAL {calibre_index}."
        self.logger.info("[CalibreResolution] campaign=%s company=%s crop=%s normalized_crop=%s base=%s source_field=Cal%s label=%s kilograms=%s price=%s amount=%s%s",campaign,company,crop,self.normalize_crop(crop),base,calibre_index,label,kilograms,price,amount,warn)
