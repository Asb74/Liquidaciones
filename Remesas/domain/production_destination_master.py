from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class ProductionDestinationConfig:
    crop: str
    primary_label: str
    secondary_enabled: bool
    secondary_label: str
    secondary_counts_as_commercial: bool
    waste_label: str
    active: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "crop", str(self.crop or "").strip().upper())
        for name in ("primary_label", "secondary_label", "waste_label"):
            object.__setattr__(self, name, str(getattr(self, name) or "").strip())

def fallback_config(crop: str) -> ProductionDestinationConfig:
    return ProductionDestinationConfig(str(crop or "").strip().upper(), "Comercial", True, "Destrío", False, "Podrido/Hojas", True)

DEFAULT_PRODUCTION_DESTINATION_MASTER = {
    "version": 1,
    "items": [
        {"crop":"CITRICOS","primary_label":"Exportación","secondary_enabled":True,"secondary_label":"Mercado nacional","secondary_counts_as_commercial":True,"waste_label":"Podrido/Hojas","active":True},
        {"crop":"MANDARINA","primary_label":"Exportación","secondary_enabled":True,"secondary_label":"Mercado nacional","secondary_counts_as_commercial":True,"waste_label":"Podrido/Hojas","active":True},
        {"crop":"KAKIS","primary_label":"Comercial","secondary_enabled":False,"secondary_label":"Destrío","secondary_counts_as_commercial":False,"waste_label":"Podrido/Hojas","active":True},
    ],
}
