from __future__ import annotations

import sqlite3
from typing import Any

from domain.models import Remesa

REMESA_FIELDS = ["IdREMESA","REMESA","FECHARE","PERIODO1","PERIODO2","CAMPAÑA","CULTIVO","EMPRESA","CATEGORIA","TipoLiq","AplRec","AplTte","AplCal","AplGlobal","AplCHa","AplPrecalibrado","IdSocio","Observaciones",*[f"P{i}" for i in range(12)],"PDESTRIO","PDMESA","PPODRIDO"]

class RemesasRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def list_remesas(self, limit: int = 200) -> list[tuple[Any, str]]:
        rows = self.conn.execute("SELECT IdREMESA, REMESA FROM PagosCIT ORDER BY IdREMESA DESC LIMIT ?", (limit,)).fetchall()
        return [(r[0], str(r[1] or "")) for r in rows]

    def get_remesa(self, remesa_id: Any) -> Remesa:
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(PagosCIT)")}
        selected = [c for c in REMESA_FIELDS if c in cols]
        row = self.conn.execute(f"SELECT {', '.join(selected)} FROM PagosCIT WHERE IdREMESA=?", (remesa_id,)).fetchone()
        if row is None:
            raise ValueError("No se encontró la remesa seleccionada.")
        return Remesa(dict(zip(selected, row)))
