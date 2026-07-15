from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Mapping
VALID_BASES = tuple(f"c{i}" for i in range(12)); VALID_BASE_SET = set(VALID_BASES)
_INITIAL_LABELS = {"CITRICOS":[f"CAL {i}" for i in range(12)],"FRUTA":["AAA 1ª","AA 1ª","A 1ª","B 1ª","C 1ª","D 1ª","AAA 2ª","AA 2ª","A 2ª","B 2ª","C 2ª","D 2ª"],"CLEMENTINA":["1 XXX","1 XX","1 X","CAL 1","CAL 2","CAL 3","CAL 4","CAL 5","CAL 6","CAL 7","CAL 8","CAL 9"],"PRECALIBRADO":[f"CAL {i}" for i in range(12)],"MALLAS":["AAA 1ª","AA 1ª","A 1ª","B 1ª","C 1ª","D 1ª","AAA 2ª","AA 2ª","A 2ª","B 2ª","C 2ª","D 2ª"],"CIRUELA":[str(i) for i in range(4,16)],"MANDARINA":["1 XXX","1 XX","1 X","CAL 1","CAL 2","CAL 3","CAL 4","CAL 5","CAL 6","CAL 7","CAL 8","CAL 9"],"KAKIS":["AAA 1ª","AA 1ª","A 1ª","B 1ª","C 1ª","D 1ª","AAA 2ª","AA 2ª","A 2ª","B 2ª","C 2ª","D 2ª"],"SANDIA":["CAL 1 1ª","CAL 2 1ª","CAL 3 1ª","CAL 4 1ª","CAL 5 1ª","CAL 6 1ª","CAL 7 1ª","CAL 8 1ª","CAL 9 1ª","CAL 10 1ª","CAL 1/6 2ª","CAL 6/10 2ª"]}
DEFAULT_CROP_ALIASES={"CITRICO":"CITRICOS","CÍTRICOS":"CITRICOS","MANDARINAS":"MANDARINA","KAKI":"KAKIS","CAQUI":"KAKIS","CAQUIS":"KAKIS"}
DEFAULT_DISPLAY_TYPES={"CITRICOS":"calibres","CLEMENTINA":"calibres","PRECALIBRADO":"calibres","CIRUELA":"calibres","MANDARINA":"calibres","SANDIA":"calibres","FRUTA":"categorias","MALLAS":"categorias","KAKIS":"categorias"}
@dataclass(frozen=True)
class CalibreMasterItem:
    base: str; crop: str; label: str; order: int; active: bool=True
    def __post_init__(self):
        base=str(self.base or "").strip().lower(); crop=str(self.crop or "").strip().upper(); label=str(self.label or "").strip(); order=int(self.order)
        if base not in VALID_BASE_SET: raise ValueError(f"Base de calibre no válida: {self.base!r}")
        if not crop: raise ValueError("El cultivo no puede estar vacío")
        if not label: raise ValueError("La descripción no puede estar vacía")
        if order<0 or order>11: raise ValueError(f"Orden fuera de rango: {order}")
        object.__setattr__(self,"base",base); object.__setattr__(self,"crop",crop); object.__setattr__(self,"label",label); object.__setattr__(self,"order",order); object.__setattr__(self,"active",bool(self.active))
def validate_items(items: Iterable[CalibreMasterItem]) -> tuple[CalibreMasterItem,...]:
    result=tuple(items); seen=set(); per={}
    for item in result:
        key=(item.base,item.crop)
        if key in seen: raise ValueError(f"Duplicado base/cultivo: {item.base}/{item.crop}")
        seen.add(key); per[item.crop]=per.get(item.crop,0)+1
    overflow=[c for c,n in per.items() if n>12]
    if overflow: raise ValueError(f"Cultivos con más de 12 filas: {', '.join(overflow)}")
    return result
def normalize_crop_value(crop: str, aliases: Mapping[str,str]|None=None) -> str:
    normalized=str(crop or "").strip().upper(); amap={str(k).strip().upper():str(v).strip().upper() for k,v in (aliases or {}).items()}; return amap.get(normalized, normalized)
def default_items() -> tuple[CalibreMasterItem,...]: return tuple(CalibreMasterItem(f"c{i}",crop,label,i,True) for crop,labels in _INITIAL_LABELS.items() for i,label in enumerate(labels))
DEFAULT_CALIBRE_MASTER={"version":1,"crop_aliases":DEFAULT_CROP_ALIASES,"display_types":DEFAULT_DISPLAY_TYPES,"items":[item.__dict__ for item in default_items()]}
