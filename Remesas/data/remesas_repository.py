from __future__ import annotations

from data.repository import IDataRepository
from typing import Any

from domain.models import Remesa

REMESA_FIELDS = ["IdREMESA","REMESA","FECHARE","PERIODO1","PERIODO2","VARIEDAD",*[f"P{i}" for i in range(12)],"PDESTRIO","PDMESA","PPODRIDO","CAMPAÑA","CULTIVO","EMPRESA","CATEGORIA","TipoLiq","AplRec","AplTte","AplCal","AplGlobal","AplCHa","AplPrecalibrado","Observaciones","IdSocio"]

class RemesasRepository:
    def __init__(self, conn: IDataRepository) -> None:
        self.conn = conn

    def list_remittances_for_campaign(
        self, campana: str, empresa: str | None = None, cultivo: str | None = None
    ) -> list[dict[str, Any]]:
        """List every stored remittance in a campaign, without a status filter.

        Company and crop are explicit context filters when supplied.  Status is
        selected only as informational data when the legacy table provides it.
        """
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(PagosCIT)")}
        status_column = next((name for name in ("Estado", "ESTADO", "Status", "STATUS") if name in columns), None)
        status_sql = status_column if status_column else "''"
        sql = f"""
        SELECT
            IdREMESA,
            REMESA,
            CAMPAÑA,
            EMPRESA,
            CULTIVO,
            FECHARE,
            PERIODO1,
            PERIODO2,
            CATEGORIA,
            TipoLiq,
            {status_sql} AS ESTADO
        FROM PagosCIT
        WHERE CAMPAÑA = ?
          AND (? IS NULL OR EMPRESA = ?)
          AND (? IS NULL OR UPPER(TRIM(CULTIVO)) = UPPER(TRIM(?)))
        ORDER BY IdREMESA DESC
        """
        rows = self.conn.execute(sql, (campana, empresa, empresa, cultivo, cultivo)).fetchall()
        cols = ["IdREMESA", "REMESA", "CAMPAÑA", "EMPRESA", "CULTIVO", "FECHARE", "PERIODO1", "PERIODO2", "CATEGORIA", "TipoLiq", "ESTADO"]
        return [dict(zip(cols, row)) for row in rows]

    def list_remesas(self, campana: str, empresa: str, cultivo: str) -> list[dict[str, Any]]:
        """Backward-compatible name for the campaign selector query."""
        return self.list_remittances_for_campaign(campana, empresa, cultivo)

    def get_remesa(self, remesa_id: Any) -> Remesa:
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(PagosCIT)")}
        selected = [c for c in REMESA_FIELDS if c in cols]
        row = self.conn.execute(f"SELECT {', '.join(selected)} FROM PagosCIT WHERE IdREMESA=?", (remesa_id,)).fetchone()
        if row is None:
            raise ValueError("No se encontró la remesa seleccionada.")
        return Remesa(dict(zip(selected, row)))
