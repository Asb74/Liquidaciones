from __future__ import annotations
import json, shutil, tempfile
from pathlib import Path
from typing import Iterable
from domain.production_destination_master import DEFAULT_PRODUCTION_DESTINATION_MASTER, ProductionDestinationConfig

class ProductionDestinationMasterRepository:
    def __init__(self, path: str|Path="config/production_destination_master.json"):
        self.path=Path(path)
    def load_items(self) -> tuple[ProductionDestinationConfig, ...]:
        raw = DEFAULT_PRODUCTION_DESTINATION_MASTER if not self.path.exists() else json.loads(self.path.read_text(encoding="utf-8"))
        return tuple(ProductionDestinationConfig(**i) for i in raw.get("items", ()) if i.get("active", True))
    def save_items(self, items: Iterable[ProductionDestinationConfig]) -> None:
        payload={"version":1,"items":[i.__dict__ for i in items]}; self.path.parent.mkdir(parents=True,exist_ok=True)
        fd,tmp=tempfile.mkstemp(prefix=self.path.name,suffix=".tmp",dir=str(self.path.parent)); Path(tmp).unlink(missing_ok=True)
        p=Path(tmp); p.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); json.loads(p.read_text(encoding="utf-8"))
        if self.path.exists(): shutil.copy2(self.path,self.path.with_suffix(self.path.suffix+".bak"))
        p.replace(self.path)
