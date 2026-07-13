from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

from domain.models import Delivery, DeliveryFilter, Summary
from domain.utils import decimal_or_zero, format_display_date, is_liquidated


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
        economic_cols = ["Coste_Recoleccion", "SSocialRecoleccion", "Manijeria", "Coste_Trans"]
        missing_economic = [c for c in economic_cols if not self._has_column("PesosFres", c)]
        if missing_economic:
            raise RuntimeError("Faltan columnas económicas en PesosFres: " + ", ".join(missing_economic))
        extra_cols = [c for c in [*[f"Cal{i}" for i in range(12)], "DesLinea", "DesMesa", "Podrido"] if self._has_column("PesosFres", c)]
        extra_select = "".join(f", p.{c}" for c in extra_cols)
        base_from = "FROM PesosFres AS p LEFT JOIN eepp.DSocio AS s ON s.IdSocio = p.IdSocio"
        count_sql = f"SELECT COUNT(*), COALESCE(SUM(COALESCE(p.Neto,0)),0), COUNT(DISTINCT p.IdSocio), COUNT(DISTINCT p.Variedad), MIN(p.Fcarga), MAX(p.Fcarga), 0, SUM(CASE WHEN p.Variedad IS NULL OR TRIM(p.Variedad)='' THEN 1 ELSE 0 END), SUM(CASE WHEN s.IdSocio IS NULL THEN 1 ELSE 0 END), SUM(CASE WHEN p.Categoria IS NULL OR TRIM(p.Categoria)='' THEN 1 ELSE 0 END) {base_from} WHERE {where_sql}"
        stats = self.conn.execute(count_sql, params).fetchone()
        sql = f"SELECT p.Fcarga, p.Reg, p.IdSocio, s.Nombre, p.Variedad, p.Categoria, p.Neto, p.Albaran, p.Boleta, p.Plataforma, p.Liquidado, {precal} AS Precalibrado, p.Coste_Recoleccion, p.SSocialRecoleccion, p.Manijeria, p.Coste_Trans{extra_select} {base_from} WHERE {where_sql} ORDER BY p.Fcarga, p.Reg LIMIT ?"
        rows = self.conn.execute(sql, [*params, filters.limit]).fetchall()
        elapsed = time.perf_counter() - start
        self.logger.info("Consulta entregas: registros=%s visibles=%s tiempo=%.3f", stats[0] if stats else 0, len(rows), elapsed)
        deliveries = []
        for r in rows:
            try:
                deliveries.append(Delivery(format_display_date(r[0]), r[1], r[2], r[3], r[4], r[5], decimal_or_zero(r[6]), r[7], r[8], r[9], r[10], r[11], decimal_or_zero(r[12]), decimal_or_zero(r[13]), decimal_or_zero(r[14]), decimal_or_zero(r[15]), {c: r[16+i] for i, c in enumerate(extra_cols)}))
            except ValueError as exc:
                self.logger.warning("Valor económico no válido en entrega Reg=%r IdSocio=%r Variedad=%r: %s", r[1], r[2], r[4], exc)
                raise
        liquidated_count = sum(1 for d in deliveries if is_liquidated(d.liquidado))
        summary = Summary(int(stats[0] or 0), int(stats[2] or 0), int(stats[3] or 0), float(stats[1] or 0), format_display_date(stats[4]), format_display_date(stats[5]), liquidated_count, int(stats[7] or 0), int(stats[8] or 0), int(stats[9] or 0))
        for label, value in (("registros no tienen variedad", summary.sin_variedad), ("socios no existen en DSocio", summary.sin_socio_valido), ("entregas ya figuran como liquidadas", summary.liquidadas)):
            if value:
                summary.warnings.append(f"Advertencia: {value} {label}.")
        return deliveries, summary, elapsed, int(stats[0] or 0)

    def _has_column(self, table: str, column: str) -> bool:
        return any(r[1].lower() == column.lower() for r in self.conn.execute(f"PRAGMA table_info({table})"))
