from __future__ import annotations

from decimal import Decimal
import logging
import sqlite3
from typing import Sequence

from domain.financial_rules import EFFECTIVE_NET_SQL
from domain.utils import decimal_or_zero


class HectareRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.logger = logging.getLogger(__name__)

    def calculate_applicable_hectares(self, member_id: int, campaign: int | str, company: int | str, applicable_crops: Sequence[str] | None = None) -> tuple[Decimal, tuple[str, ...]]:
        """Return VB6-style CHA hectares from eepp.DEEPP joined to eepp.DParcela.

        Confirmed filters found in the user-provided VB6 notes:
        - DEEPP.IdSocio = member
        - DEEPP.CAMPAÑA = campaign
        - DEEPP.EMPRESA = company
        - DEEPP.CHA = -1
        - DParcela.BAJA is NULL/empty
        - DParcela.Año <= campaign - 5

        MfiltroCha/Mfiltro3/Mfiltro4 source files are not present in this repo, so no
        unconfirmed extra predicates are invented here.
        """
        year_limit = int(str(campaign)) - 5
        sql = """
            SELECT
                d.Boleta,
                dp.IdPM,
                dp.Pol,
                dp.Par,
                dp.Recinto,
                dp.CAMPAÑA,
                dp.EMPRESA,
                dp.CULTIVO,
                dp.SupCul
            FROM eepp.DEEPP AS d
            INNER JOIN eepp.DParcela AS dp
                ON TRIM(COALESCE(dp.Boleta, '')) = TRIM(COALESCE(d.Boleta, ''))
            WHERE d.IdSocio = ?
              AND d.CAMPAÑA = ?
              AND d.EMPRESA = ?
              AND COALESCE(d.CHA, 0) = -1
              AND (dp.BAJA IS NULL OR TRIM(dp.BAJA) = '')
              AND CAST(dp.Año AS INTEGER) <= ?
        """
        rows = self.conn.execute(sql, [member_id, str(campaign), str(company), year_limit]).fetchall()
        if not rows:
            rows = self.conn.execute(sql, [member_id, campaign, company, year_limit]).fetchall()

        seen: dict[tuple, Decimal] = {}
        warnings: list[str] = []
        for r in rows:
            key = (
                str(r[0] or "").strip(),  # Boleta
                str(r[1] or "").strip(),  # parcela id if present
                str(r[2] or "").strip(),  # poligono
                str(r[3] or "").strip(),  # parcela
                str(r[4] or "").strip(),  # recinto
                str(r[5] or "").strip(),  # campaña parcela if present
                str(r[6] or "").strip(),  # empresa parcela if present
            )
            sup = decimal_or_zero(r[8])
            if key in seen:
                if seen[key] != sup:
                    warnings.append(f"Superficie duplicada conflictiva socio={member_id} clave={key}: {seen[key]} vs {sup}; se conserva la primera.")
                continue
            seen[key] = sup
        if len(rows) != len(seen):
            warnings.append(f"Superficie deduplicada socio={member_id}: {len(rows)} filas -> {len(seen)} claves Boleta/IdPM/Pol/Par/Recinto/CAMPAÑA/EMPRESA.")
        self.logger.debug("Filtros cuota Ha superficie: IdSocio=%s CAMPAÑA=%s EMPRESA=%s CHA=-1 BAJA vacía Año<=%s; filas=%s dedup=%s", member_id, campaign, company, year_limit, len(rows), len(seen))
        return sum(seen.values(), Decimal("0")), tuple(warnings)

    def total_effective_kg(self, member_id: int, campaign: int | str, company: int | str, delivery_crops: Sequence[str]) -> Decimal:
        placeholders = ",".join("?" for _ in delivery_crops)
        sql = f"""
            SELECT COALESCE(SUM({EFFECTIVE_NET_SQL.format(alias='p')}), 0) AS TotalEffectiveKg
            FROM PesosFres AS p
            WHERE p.IdSocio=?
              AND p.CAMPAÑA=?
              AND p.EMPRESA=?
              AND UPPER(TRIM(p.CULTIVO)) IN ({placeholders})
        """
        params = [member_id, str(campaign), str(company), *delivery_crops]
        value = self.conn.execute(sql, params).fetchone()[0]
        if decimal_or_zero(value) == 0:
            value = self.conn.execute(sql, [member_id, campaign, company, *delivery_crops]).fetchone()[0]
        return decimal_or_zero(value)
