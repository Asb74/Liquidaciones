from __future__ import annotations
import logging
from typing import Iterable
from data.production_destination_master_repository import ProductionDestinationMasterRepository
from domain.production_destination_master import ProductionDestinationConfig, fallback_config

logger=logging.getLogger(__name__)
class ProductionDestinationMasterService:
    def __init__(self, repository: ProductionDestinationMasterRepository|None=None): self.repository=repository or ProductionDestinationMasterRepository()
    def list_all(self) -> tuple[ProductionDestinationConfig, ...]: return self.repository.load_items()
    def save_all(self, items: Iterable[ProductionDestinationConfig]) -> None: self.repository.save_items(tuple(items))
    def get_for_crop(self, crop: str) -> ProductionDestinationConfig:
        key=str(crop or "").strip().upper()
        for item in self.list_all():
            if item.crop == key and item.active: return item
        logger.warning("No existe destino de producción para cultivo %s; se usa fallback conservador", key)
        return fallback_config(key)
