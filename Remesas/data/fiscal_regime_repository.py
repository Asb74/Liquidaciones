from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from domain.calculation_models import FiscalRegime
from domain.utils import to_decimal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FiscalRegimeLookup:
    regime: FiscalRegime
    warnings: tuple[str, ...] = ()


def normalize_fiscal_regime(value: Any) -> str:
    """Normaliza textos de régimen fiscal sólo para comparación."""
    text = "" if value is None else str(value)
    text = re.sub(r"\s+", " ", text.strip())
    return text.casefold()


class FiscalRegimeRepository:
    """Consulta DSocio y MRegimenFiscal en DBEEPPL.sqlite en modo lectura."""

    def __init__(self, conn: sqlite3.Connection, schema: str = "eepp") -> None:
        self.conn = conn
        self.schema = schema

    def get_for_member(self, member_id: int) -> FiscalRegimeLookup:
        socio_rows = self._member_rows(member_id)
        if not socio_rows:
            raise ValueError(f"No existe DSoocio.IdSocio={member_id}")

        active_rows = [row for row in socio_rows if self._is_active(row)]
        warnings: list[str] = []
        if len(active_rows) > 1:
            raise ValueError("Duplicidad de socio.")
        if len(active_rows) == 1:
            socio_row = active_rows[0]
        elif len(socio_rows) == 1:
            socio_row = socio_rows[0]
            warnings.append(f"Socio={member_id}: no existe registro activo; se utiliza el único disponible.")
            logger.warning("[RégimenFiscal] Socio=%s sin registro activo; se utiliza el único disponible", member_id)
        else:
            raise ValueError("Duplicidad de socio.")

        source_regime = str(socio_row["RegimeFiscal"] or "").strip()
        normalized = normalize_fiscal_regime(source_regime)
        regime_rows = [row for row in self._regime_rows() if normalize_fiscal_regime(row["Regimen"]) == normalized]
        if not regime_rows:
            raise ValueError(f'No existe régimen fiscal "{source_regime}"')
        if len(regime_rows) > 1:
            raise ValueError(f'Duplicidad de régimen fiscal "{source_regime}"')
        row = regime_rows[0]
        return FiscalRegimeLookup(
            FiscalRegime(str(row["Regimen"] or source_regime).strip(), to_decimal(row["Iva"]), to_decimal(row["Retencion"])),
            tuple(warnings),
        )

    def _member_rows(self, member_id: int) -> list[sqlite3.Row]:
        sql = f"""
        SELECT IdSocio, RegimeFiscal, Inactivo, Baja
        FROM {self.schema}.DSocio
        WHERE IdSocio = ?
        """
        try:
            return list(self.conn.execute(sql, (member_id,)).fetchall())
        except sqlite3.OperationalError:
            fallback = sql.replace(f"{self.schema}.", "")
            return list(self.conn.execute(fallback, (member_id,)).fetchall())

    def _regime_rows(self) -> list[sqlite3.Row]:
        sql = f"SELECT Regimen, Iva, Retencion FROM {self.schema}.MRegimenFiscal"
        try:
            return list(self.conn.execute(sql).fetchall())
        except sqlite3.OperationalError:
            fallback = sql.replace(f"{self.schema}.", "")
            return list(self.conn.execute(fallback).fetchall())

    @staticmethod
    def _is_active(row: sqlite3.Row) -> bool:
        inactive = to_decimal(row["Inactivo"])
        baja = row["Baja"]
        return inactive == Decimal("0") and (baja is None or str(baja).strip() == "")
