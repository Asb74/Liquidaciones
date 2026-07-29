from __future__ import annotations
import json, shutil, tempfile
from pathlib import Path
from typing import Iterable
from domain.calibre_master import CalibreMasterItem, DEFAULT_CALIBRE_MASTER, DEFAULT_CROP_ALIASES, DEFAULT_DISPLAY_TYPES, normalize_crop_value, validate_items
class CalibreMasterRepository:
    def __init__(self, path: str|Path="config/calibre_master.json"): self.path=Path(path); self._raw=None
    def _load_raw(self):
        if not self.path.exists(): return dict(DEFAULT_CALIBRE_MASTER)
        with self.path.open("r",encoding="utf-8") as fh: return json.load(fh)
    def load_items(self):
        raw=self._load_raw(); self._raw=raw; return validate_items(tuple(CalibreMasterItem(**i) for i in raw.get("items",[])))
    def load_aliases(self):
        raw=self._raw or self._load_raw(); return {**DEFAULT_CROP_ALIASES, **{str(k).strip().upper():str(v).strip().upper() for k,v in raw.get("crop_aliases",{}).items()}}
    def load_display_types(self):
        raw=self._raw or self._load_raw(); return {**DEFAULT_DISPLAY_TYPES, **{str(k).strip().upper():str(v).strip().lower() for k,v in raw.get("display_types",{}).items()}}
    def save_items(self, items: Iterable[CalibreMasterItem]):
        valid=validate_items(tuple(items)); payload={"version":1,"crop_aliases":self.load_aliases(),"display_types":self.load_display_types(),"items":[i.__dict__ for i in valid]}; self.path.parent.mkdir(parents=True,exist_ok=True)
        fd,tmp=tempfile.mkstemp(prefix=self.path.name,suffix=".tmp",dir=str(self.path.parent)); Path(tmp).unlink(missing_ok=True)
        tmp_path=Path(tmp); tmp_path.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); json.loads(tmp_path.read_text(encoding="utf-8"))
        if self.path.exists(): shutil.copy2(self.path,self.path.with_suffix(self.path.suffix+".bak"))
        tmp_path.replace(self.path)
    def get_item(self, base: str, crop: str):
        b=str(base).strip().lower(); c=normalize_crop_value(crop,self.load_aliases()); return next((i for i in self.load_items() if i.base==b and i.crop==c and i.active),None)
    def get_crop_items(self,crop: str):
        c=normalize_crop_value(crop,self.load_aliases()); return tuple(sorted((i for i in self.load_items() if i.crop==c),key=lambda x:x.order))
    def get_crops(self): return tuple(sorted({i.crop for i in self.load_items()}))
    def upsert_item(self,item: CalibreMasterItem): self.save_items([i for i in self.load_items() if (i.base,i.crop)!=(item.base,item.crop)]+[item])
    def delete_item(self,base: str,crop: str):
        b=str(base).strip().lower(); c=normalize_crop_value(crop,self.load_aliases()); self.save_items(i for i in self.load_items() if (i.base,i.crop)!=(b,c))
