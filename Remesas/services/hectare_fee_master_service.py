from __future__ import annotations

from domain.hectare_fee_master import HectareFeeMaster, HectareFeeMasterRepository
from data.hectare_fee_master_repository import HectareFeeCropRepository


class HectareFeeMasterService:
    def __init__(self, master_repository: HectareFeeMasterRepository, crop_repository: HectareFeeCropRepository | None = None) -> None:
        self.master_repository = master_repository
        self.crop_repository = crop_repository

    def load_master(self) -> HectareFeeMaster:
        return self.master_repository.load()

    def save_master(self, master: HectareFeeMaster) -> None:
        self.master_repository.save(master)

    def restore_defaults(self) -> HectareFeeMaster:
        return self.master_repository.restore_defaults()

    def list_eligible_crop_options(self) -> list[str]:
        if self.crop_repository is None:
            return list(self.load_master().eligible_crops)
        return sorted(set(self.crop_repository.list_surface_crop_options()) | set(self.crop_repository.list_delivery_crop_options()))

    def list_surface_crop_options(self) -> list[str]:
        return self.list_eligible_crop_options()

    def list_delivery_crop_options(self) -> list[str]:
        return self.list_eligible_crop_options()
