from __future__ import annotations
from datetime import date
from data.metadata_repository import MetadataRepository

class ContextService:
    def __init__(self, repo: MetadataRepository) -> None:
        self.repo = repo
    def campaigns(self) -> list[str]: return self.repo.campaigns()
    def empresas(self, campana: str) -> list[str]: return self.repo.empresas(campana)
    def cultivos(self, campana: str, empresa: str) -> list[str]: return self.repo.cultivos(campana, empresa)
    def variedades(self, campana: str, empresa: str, cultivo: str, desde: date | None = None, hasta: date | None = None) -> list[str]: return self.repo.variedades(campana, empresa, cultivo, desde, hasta)
