from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

from domain.models import Delivery, DeliveryFilter, Summary


class DeliveriesRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.logger = logging.getLogger(__name__)

    def _date_expr(self) -> str:
        return "date(substr(p.Fcarga, 1, 10))"

    def _build_where(self, filters: DeliveryFilter) -> tuple[str, list[Any]]:
        clauses = ["p.CAMPAÑA=?", "p.EMPRESA=?", "p.CULTIVO=?", f"{self._date_expr()} BETWEEN date(?) AND date(?)"]
        params: list[Any] = [filters.context.campana, filters.context.empresa, filters.context.cultivo, filters.period.start.isoformat(), filters.period.end.isoformat()]
        if filters.varieties:
            clauses.append("p.Variedad IN (" + ",".join("?" for _ in filters.varieties) + ")")
            params.extend(filters.varieties)
        if filters.socio and str(filters.socio).strip() not in ("", "0"):
            clauses.append("p.IdSocio=?")
            params.append(filters.socio)
        if filters.categoria:
            clauses.append("p.Categoria=?")
            params.append(filters.categoria)
        return " AND ".join(clauses), params

    def sample_fcarga(self) -> list[Any]:
        return [r[0] for r in self.conn.execute("SELECT Fcarga FROM PesosFres WHERE Fcarga IS NOT NULL LIMIT 10")]

    def fetch(self, filters: DeliveryFilter) -> tuple[list[Delivery], Summary, float, int]:
        start = time.perf_counter()
        where_sql, params = self._build_where(filters)
        precal = "p.Precalibrado" if self._has_column("PesosFres", "Precalibrado") else "NULL"
        base_from = "FROM PesosFres AS p LEFT JOIN eepp.DSocio AS s ON s.IdSocio = p.IdSocio"
        count_sql = f"SELECT COUNT(*), COALESCE(SUM(COALESCE(p.Neto,0)),0), COUNT(DISTINCT p.IdSocio), COUNT(DISTINCT p.Variedad), MIN(p.Fcarga), MAX(p.Fcarga), SUM(CASE WHEN p.Liquidado IS NOT NULL AND p.Liquidado NOT IN (0,'0','') THEN 1 ELSE 0 END), SUM(CASE WHEN p.Variedad IS NULL OR TRIM(p.Variedad)='' THEN 1 ELSE 0 END), SUM(CASE WHEN s.IdSocio IS NULL THEN 1 ELSE 0 END), SUM(CASE WHEN p.Categoria IS NULL OR TRIM(p.Categoria)='' THEN 1 ELSE 0 END) {base_from} WHERE {where_sql}"
        stats = self.conn.execute(count_sql, params).fetchone()
        sql = f"SELECT p.Fcarga, p.Reg, p.IdSocio, s.Nombre, p.Variedad, p.Categoria, p.Neto, p.Albaran, p.Boleta, p.Plataforma, p.Liquidado, {precal} AS Precalibrado {base_from} WHERE {where_sql} ORDER BY p.Fcarga, p.Reg LIMIT ?"
        rows = self.conn.execute(sql, [*params, filters.limit]).fetchall()
        elapsed = time.perf_counter() - start
        self.logger.info("Consulta entregas: registros=%s visibles=%s tiempo=%.3f", stats[0] if stats else 0, len(rows), elapsed)
        deliveries = [Delivery(r[0], r[1], r[2], r[3], r[4], r[5], float(r[6] or 0), r[7], r[8], r[9], r[10], r[11]) for r in rows]
        summary = Summary(int(stats[0] or 0), int(stats[2] or 0), int(stats[3] or 0), float(stats[1] or 0), str(stats[4] or ""), str(stats[5] or ""), int(stats[6] or 0), int(stats[7] or 0), int(stats[8] or 0), int(stats[9] or 0))
        for label, value in (("registros no tienen variedad", summary.sin_variedad), ("socios no existen en DSocio", summary.sin_socio_valido), ("entregas ya figuran como liquidadas", summary.liquidadas)):
            if value:
                summary.warnings.append(f"Advertencia: {value} {label}.")
        return deliveries, summary, elapsed, int(stats[0] or 0)

    def _has_column(self, table: str, column: str) -> bool:
        return any(r[1].lower() == column.lower() for r in self.conn.execute(f"PRAGMA table_info({table})"))
