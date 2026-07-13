from __future__ import annotations
from data.remesas_repository import RemesasRepository

class RemesasService:
    def __init__(self, repo: RemesasRepository) -> None:
        self.repo = repo
    def list_remesas(self): return self.repo.list_remesas()
    def get_remesa(self, remesa_id): return self.repo.get_remesa(remesa_id)
