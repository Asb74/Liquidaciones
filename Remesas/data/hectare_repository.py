from __future__ import annotations

from decimal import Decimal
import logging
import sqlite3
import time
from typing import Sequence

from domain.audit import current_audit
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
                dp.Rec,
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
        params = [member_id, str(campaign), str(company), year_limit]
        start = time.perf_counter()
        rows = self.conn.execute(sql, params).fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000
        if not rows:
            params = [member_id, campaign, company, year_limit]
            start = time.perf_counter()
            rows = self.conn.execute(sql, params).fetchall()
            elapsed_ms = (time.perf_counter() - start) * 1000

        audit = current_audit()
        if audit:
            audit.subsection("SUPERFICIE")
            audit.audit_sql("superficie socio", sql, params, len(rows), elapsed_ms)
            audit.line(f"Número de parcelas obtenidas: {len(rows)}")
            audit.line("Detalle:")
            for r in rows:
                audit.line(f"Boleta={r[0]} Pol={r[2]} Par={r[3]} Rec={r[4]} SupCul={r[8]} CHA=-1 BAJA= Año={r[5]}")
            audit.line(f"Superficie total: {sum((decimal_or_zero(r[8]) for r in rows), Decimal('0'))}")
            audit.audit_filters("superficie", self._surface_filter_counts(member_id, campaign, company, year_limit))

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
            warnings.append(f"Superficie deduplicada socio={member_id}: {len(rows)} filas -> {len(seen)} claves Boleta/IdPM/Pol/Par/Rec/CAMPAÑA/EMPRESA.")
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
        start = time.perf_counter()
        value = self.conn.execute(sql, params).fetchone()[0]
        elapsed_ms = (time.perf_counter() - start) * 1000
        if decimal_or_zero(value) == 0:
            params = [member_id, campaign, company, *delivery_crops]
            start = time.perf_counter()
            value = self.conn.execute(sql, params).fetchone()[0]
            elapsed_ms = (time.perf_counter() - start) * 1000
        total = decimal_or_zero(value)
        audit = current_audit()
        if audit:
            audit.subsection("KILOS CAMPAÑA")
            audit.audit_sql("kilos campaña socio", sql, params, 1, elapsed_ms)
            audit.line("Detalle:")
            for crop in delivery_crops:
                crop_sql = f"SELECT COALESCE(SUM({EFFECTIVE_NET_SQL.format(alias='p')}), 0) FROM PesosFres AS p WHERE p.IdSocio=? AND p.CAMPAÑA=? AND p.EMPRESA=? AND UPPER(TRIM(p.CULTIVO))=?"
                crop_value = self.conn.execute(crop_sql, [member_id, str(campaign), str(company), crop]).fetchone()[0]
                audit.line(f"{crop}: {decimal_or_zero(crop_value)}")
            audit.line(f"Kilos totales: {total}")
        return total

    def _surface_filter_counts(self, member_id: int, campaign: int | str, company: int | str, year_limit: int) -> dict[str, int]:
        queries = [
            ("Registros iniciales", "SELECT COUNT(*) FROM eepp.DEEPP AS d INNER JOIN eepp.DParcela AS dp ON TRIM(COALESCE(dp.Boleta, '')) = TRIM(COALESCE(d.Boleta, '')) WHERE d.IdSocio=?", [member_id]),
            ("Tras campaña", "SELECT COUNT(*) FROM eepp.DEEPP AS d INNER JOIN eepp.DParcela AS dp ON TRIM(COALESCE(dp.Boleta, '')) = TRIM(COALESCE(d.Boleta, '')) WHERE d.IdSocio=? AND d.CAMPAÑA=?", [member_id, str(campaign)]),
            ("Tras empresa", "SELECT COUNT(*) FROM eepp.DEEPP AS d INNER JOIN eepp.DParcela AS dp ON TRIM(COALESCE(dp.Boleta, '')) = TRIM(COALESCE(d.Boleta, '')) WHERE d.IdSocio=? AND d.CAMPAÑA=? AND d.EMPRESA=?", [member_id, str(campaign), str(company)]),
            ("Tras CHA", "SELECT COUNT(*) FROM eepp.DEEPP AS d INNER JOIN eepp.DParcela AS dp ON TRIM(COALESCE(dp.Boleta, '')) = TRIM(COALESCE(d.Boleta, '')) WHERE d.IdSocio=? AND d.CAMPAÑA=? AND d.EMPRESA=? AND COALESCE(d.CHA,0)=-1", [member_id, str(campaign), str(company)]),
            ("Tras BAJA", "SELECT COUNT(*) FROM eepp.DEEPP AS d INNER JOIN eepp.DParcela AS dp ON TRIM(COALESCE(dp.Boleta, '')) = TRIM(COALESCE(d.Boleta, '')) WHERE d.IdSocio=? AND d.CAMPAÑA=? AND d.EMPRESA=? AND COALESCE(d.CHA,0)=-1 AND (dp.BAJA IS NULL OR TRIM(dp.BAJA)='')", [member_id, str(campaign), str(company)]),
            ("Tras antigüedad", "SELECT COUNT(*) FROM eepp.DEEPP AS d INNER JOIN eepp.DParcela AS dp ON TRIM(COALESCE(dp.Boleta, '')) = TRIM(COALESCE(d.Boleta, '')) WHERE d.IdSocio=? AND d.CAMPAÑA=? AND d.EMPRESA=? AND COALESCE(d.CHA,0)=-1 AND (dp.BAJA IS NULL OR TRIM(dp.BAJA)='') AND CAST(dp.Año AS INTEGER)<=?", [member_id, str(campaign), str(company), year_limit]),
        ]
        counts = {label: int(self.conn.execute(query, params).fetchone()[0] or 0) for label, query, params in queries}
        counts["Registros finales"] = counts.get("Tras antigüedad", 0)
        return counts
