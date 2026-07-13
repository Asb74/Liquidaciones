from __future__ import annotations
from data.remesas_repository import RemesasRepository

class RemesasService:
    def __init__(self, repo: RemesasRepository) -> None:
        self.repo = repo
    def list_remesas(self, campana: str, empresa: str, cultivo: str): return self.repo.list_remesas(campana, empresa, cultivo)
    def get_remesa(self, remesa_id): return self.repo.get_remesa(remesa_id)
