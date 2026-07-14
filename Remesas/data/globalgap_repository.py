from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import logging
import sqlite3
import unicodedata
from typing import Any

from domain.calculation_models import CalculationStatus, GlobalGapCertificationResult, GlobalGapLevelResult, GlobalGapRate
from domain.utils import decimal_or_zero


def normalize_certification(value: object) -> str:
    text = str(value or "").strip().upper()
    text = "".join(ch for ch in unicodedata.normalize("NFD", text) if unicodedata.category(ch) != "Mn")
    return text.replace(" ", "").replace("-", "")


def _norm_level(value: object) -> str:
    return str(value or "").strip().upper()


class GlobalGapRepository:
    """Read-only GlobalGAP data access. Never reads or writes the auxiliary table Global."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.logger = logging.getLogger(__name__)
        self.last_certification_rows: tuple[dict[str, Any], ...] = ()
        self.last_bonus_source = ""

    def get_member_certification(self, member_id: int, campaign: int | str, company: int | str) -> GlobalGapCertificationResult:
        rows = self.conn.execute(
            '''
            SELECT CULTIVO, Certificacion
            FROM eepp.DEEPP
            WHERE IdSocio = ?
              AND CAST("CAMPAÑA" AS TEXT) = CAST(? AS TEXT)
              AND CAST(EMPRESA AS TEXT) = CAST(? AS TEXT)
              AND (BAJA IS NULL OR TRIM(CAST(BAJA AS TEXT)) = '')
            ORDER BY TRIM(COALESCE(CULTIVO,'')), TRIM(COALESCE(Certificacion,''))
            ''',
            (member_id, campaign, company),
        ).fetchall()
        self.last_certification_rows = tuple({"cultivo": r[0], "certificacion": r[1]} for r in rows)
        certified_crops: list[str] = []
        non_certified_crops: list[str] = []
        raw_values: list[str] = []
        for crop, raw in rows:
            crop_text = str(crop or "").strip()
            raw_text = str(raw or "").strip()
            raw_values.append(f"{crop_text}: {raw_text}" if raw_text else f"{crop_text}: <vacío>")
            if normalize_certification(raw) == "GLOBALGAP":
                if crop_text and crop_text not in certified_crops:
                    certified_crops.append(crop_text)
            else:
                if crop_text and crop_text not in non_certified_crops:
                    non_certified_crops.append(crop_text)
        certified = bool(certified_crops)
        inconsistent = certified and bool(non_certified_crops)
        warnings: list[str] = []
        if inconsistent:
            warnings.append(
                f"El socio {member_id} figura certificado GlobalGAP en {', '.join(certified_crops)} pero no en {', '.join(non_certified_crops)}. "
                "Se ha aplicado la certificación global del socio. Revise DEEPP."
            )
        return GlobalGapCertificationResult(certified, inconsistent, tuple(certified_crops), tuple(non_certified_crops), tuple(raw_values), tuple(warnings))

    def get_member_levels(self, member_id: int, campaign: int | str, company: int | str) -> tuple[str, ...]:
        rows = self.conn.execute(
            '''
            SELECT DISTINCT TRIM(NivelGlobal) AS NivelGlobal
            FROM eepp.DEEPP
            WHERE IdSocio = ?
              AND CAST("CAMPAÑA" AS TEXT) = CAST(? AS TEXT)
              AND CAST(EMPRESA AS TEXT) = CAST(? AS TEXT)
              AND NivelGlobal IS NOT NULL
              AND TRIM(NivelGlobal) <> ''
              AND (BAJA IS NULL OR TRIM(CAST(BAJA AS TEXT)) = '')
            ORDER BY TRIM(NivelGlobal)
            ''',
            (member_id, campaign, company),
        ).fetchall()
        seen: dict[str, str] = {}
        for row in rows:
            value = str(row[0] or "").strip()
            if value:
                seen.setdefault(_norm_level(value), value)
        return tuple(seen.values())

    def get_level_index(self, level: str) -> GlobalGapLevelResult:
        rows = self.conn.execute(
            'SELECT Nivel, Indice FROM eepp.MNivelGlobal WHERE UPPER(TRIM(Nivel)) = UPPER(TRIM(?))',
            (level,),
        ).fetchall()
        if not rows:
            return GlobalGapLevelResult(level, None, CalculationStatus.ERROR, (f"No existe MNivelGlobal para el nivel {level}.",))
        if len(rows) > 1:
            return GlobalGapLevelResult(level, None, CalculationStatus.ERROR, (f"MNivelGlobal contiene {len(rows)} índices para el nivel {level}.",))
        return GlobalGapLevelResult(str(rows[0][0]).strip(), decimal_or_zero(rows[0][1]), CalculationStatus.CALCULATED, ())

    def get_bonus_rate(self, context: Any) -> GlobalGapRate:
        cols = [r[1] for r in self.conn.execute("PRAGMA table_info('BonGlobal')").fetchall()]
        required = {"Bonificacion", "CATEGORIA"}
        missing = sorted(required - set(cols))
        if missing:
            return GlobalGapRate(None, None, "BonGlobal", (f"BonGlobal no contiene columnas requeridas: {', '.join(missing)}. Columnas reales: {', '.join(cols)}",))
        filters: list[str] = []
        params: list[Any] = []
        for col, value in (("CAMPAÑA", getattr(context, "campana", "")), ("EMPRESA", getattr(context, "empresa", "")), ("CULTIVO", getattr(context, "cultivo", "")), ("TipoLiq", getattr(context, "tipo_liquidacion", ""))):
            if col in cols and str(value or "").strip():
                filters.append(f'CAST("{col}" AS TEXT) = CAST(? AS TEXT)')
                params.append(value)
        where = " WHERE " + " AND ".join(filters) if filters else ""
        sql = f'SELECT Bonificacion, CATEGORIA FROM BonGlobal{where} ORDER BY Bonificacion, CATEGORIA'
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        self.last_bonus_source = f"{sql} params={tuple(params)}"
        if not rows:
            return GlobalGapRate(None, None, self.last_bonus_source, ("No existe BonGlobal para el contexto de la remesa.",))
        unique = {(str(r[0]), str(r[1])) for r in rows}
        if len(unique) > 1:
            return GlobalGapRate(None, None, self.last_bonus_source, ("BonGlobal contiene más de una tarifa válida para el contexto.",))
        row = rows[0]
        return GlobalGapRate(decimal_or_zero(row[0]), int(decimal_or_zero(row[1])), self.last_bonus_source, ())
