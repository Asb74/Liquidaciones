from __future__ import annotations

import sqlite3
from typing import Any

from domain.models import Remesa

REMESA_FIELDS = ["IdREMESA","REMESA","FECHARE","PERIODO1","PERIODO2","VARIEDAD",*[f"P{i}" for i in range(12)],"PDESTRIO","PDMESA","PPODRIDO","CAMPAÑA","CULTIVO","EMPRESA","CATEGORIA","TipoLiq","AplRec","AplTte","AplCal","AplGlobal","AplCHa","AplPrecalibrado","Observaciones","IdSocio"]

class RemesasRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def list_remesas(self, campana: str, empresa: str, cultivo: str) -> list[dict[str, Any]]:
        sql = """
        SELECT
            IdREMESA,
            REMESA,
            FECHARE,
            PERIODO1,
            PERIODO2,
            CATEGORIA,
            TipoLiq
        FROM PagosCIT
        WHERE CAMPAÑA = ?
          AND EMPRESA = ?
          AND UPPER(TRIM(CULTIVO)) = UPPER(TRIM(?))
        ORDER BY IdREMESA DESC
        """
        rows = self.conn.execute(sql, (campana, empresa, cultivo)).fetchall()
        cols = ["IdREMESA", "REMESA", "FECHARE", "PERIODO1", "PERIODO2", "CATEGORIA", "TipoLiq"]
        return [dict(zip(cols, row)) for row in rows]

    def get_remesa(self, remesa_id: Any) -> Remesa:
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(PagosCIT)")}
        selected = [c for c in REMESA_FIELDS if c in cols]
        row = self.conn.execute(f"SELECT {', '.join(selected)} FROM PagosCIT WHERE IdREMESA=?", (remesa_id,)).fetchone()
        if row is None:
            raise ValueError("No se encontró la remesa seleccionada.")
        return Remesa(dict(zip(selected, row)))
