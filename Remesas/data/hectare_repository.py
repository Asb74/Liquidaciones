from __future__ import annotations

from decimal import Decimal
import logging
import sqlite3
import time
from typing import Any, Sequence

from domain.audit import current_audit
from domain.financial_rules import EFFECTIVE_NET_SQL
from domain.utils import decimal_or_zero


class HectareRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.logger = logging.getLogger(__name__)
        self.last_surface_audit_rows: tuple[dict[str, Any], ...] = ()
        self.last_surface_filter_counts: dict[str, Any] = {}

    def calculate_applicable_hectares(self, member_id: int, campaign: int | str, company: int | str, applicable_crops: Sequence[str] | None = None) -> tuple[Decimal, tuple[str, ...]]:
        """Return applicable hectares as SUM(DParcela.SupApor) for valid CHA parcels.

        Confirmed rule:
        - filter DEEPP by IdSocio, CAMPAÑA, EMPRESA, configured surface CULTIVO and CHA=-1;
        - join DParcela by Boleta, CAMPAÑA, EMPRESA and CULTIVO;
        - exclude DEEPP/DParcela rows with BAJA informed;
        - use DParcela.SupApor, never SupCul, as the economic surface;
        - deduplicate by Boleta/CAMPAÑA/EMPRESA/CULTIVO/IdPM/Pol/Par/Rec before summing.
        """
        crops = tuple(dict.fromkeys((c or "").strip().upper() for c in (applicable_crops or ("CITRICOS", "MANDARINA")) if (c or "").strip()))
        if not crops:
            self.last_surface_audit_rows = ()
            self.last_surface_filter_counts = {"Cultivos superficie": "(vacío)", "Parcelas únicas": 0, "Suma SupApor": Decimal("0")}
            return Decimal("0"), ("Cuota Ha no calculable: no hay cultivos de superficie configurados.",)
        placeholders = ",".join("?" for _ in crops)
        sql = f"""
            SELECT DISTINCT
                dp.Boleta,
                dp.CAMPAÑA,
                dp.EMPRESA,
                dp.CULTIVO,
                dp.IdPM,
                dp.Pol,
                dp.Par,
                dp.Rec,
                COALESCE(dp.SupApor, 0) AS SupApor
            FROM eepp.DEEPP AS d
            INNER JOIN eepp.DParcela AS dp
                ON TRIM(COALESCE(dp.Boleta, '')) = TRIM(COALESCE(d.Boleta, ''))
               AND CAST(dp.CAMPAÑA AS TEXT) = CAST(d.CAMPAÑA AS TEXT)
               AND CAST(dp.EMPRESA AS TEXT) = CAST(d.EMPRESA AS TEXT)
               AND UPPER(TRIM(dp.CULTIVO)) = UPPER(TRIM(d.CULTIVO))
            WHERE d.IdSocio = ?
              AND CAST(d.CAMPAÑA AS TEXT) = CAST(? AS TEXT)
              AND CAST(d.EMPRESA AS TEXT) = CAST(? AS TEXT)
              AND UPPER(TRIM(d.CULTIVO)) IN ({placeholders})
              AND COALESCE(d.CHA, 0) = -1
              AND (d.BAJA IS NULL OR TRIM(CAST(d.BAJA AS TEXT)) = '')
              AND (dp.BAJA IS NULL OR TRIM(CAST(dp.BAJA AS TEXT)) = '')
              AND COALESCE(dp.SupApor, 0) > 0
        """
        params = [member_id, str(campaign), str(company), *crops]
        start = time.perf_counter()
        rows = self.conn.execute(sql, params).fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000

        parcel_rows = self._parcel_audit_rows(member_id, campaign, company, crops)
        self.last_surface_audit_rows = tuple(parcel_rows)
        seen: dict[tuple[str, ...], Decimal] = {}
        warnings: list[str] = []
        for r in rows:
            key = tuple(str(v or "").strip().upper() for v in r[:8])
            sup = decimal_or_zero(r[8])
            if key in seen:
                if seen[key] != sup:
                    warnings.append(f"Superficie duplicada conflictiva socio={member_id} clave={key}: {seen[key]} vs {sup}; se conserva la primera.")
                continue
            seen[key] = sup
        total = sum(seen.values(), Decimal("0"))
        counts = self._surface_filter_counts(member_id, campaign, company, crops, len(rows), len(seen), total)
        self.last_surface_filter_counts = counts
        if len(rows) != len(seen):
            warnings.append(f"Superficie deduplicada socio={member_id}: {len(rows)} filas -> {len(seen)} claves Boleta/CAMPAÑA/EMPRESA/CULTIVO/IdPM/Pol/Par/Rec.")
        audit = current_audit()
        if audit:
            audit.subsection("SUPERFICIE")
            audit.audit_sql("superficie socio SupApor", sql, params, len(rows), elapsed_ms)
            audit.audit_filters("superficie", counts)
            audit.line("Parcelas auditadas:")
            for item in parcel_rows:
                audit.line(" | ".join(f"{k}={v}" for k, v in item.items()))
        self.logger.debug("Filtros cuota Ha superficie: IdSocio=%s CAMPAÑA=%s EMPRESA=%s CULTIVOS=%s CHA=-1 BAJA vacía SupApor>0; filas=%s dedup=%s total=%s", member_id, campaign, company, crops, len(rows), len(seen), total)
        return total, tuple(warnings)

    def total_effective_kg(self, member_id: int, campaign: int | str, company: int | str, delivery_crops: Sequence[str]) -> Decimal:
        placeholders = ",".join("?" for _ in delivery_crops)
        sql = f"""
            SELECT COALESCE(SUM({EFFECTIVE_NET_SQL.format(alias='p')}), 0) AS TotalEffectiveKg
            FROM PesosFres AS p
            WHERE p.IdSocio=?
              AND CAST(p.CAMPAÑA AS TEXT)=CAST(? AS TEXT)
              AND CAST(p.EMPRESA AS TEXT)=CAST(? AS TEXT)
              AND UPPER(TRIM(p.CULTIVO)) IN ({placeholders})
        """
        params = [member_id, str(campaign), str(company), *delivery_crops]
        start = time.perf_counter(); value = self.conn.execute(sql, params).fetchone()[0]; elapsed_ms = (time.perf_counter() - start) * 1000
        total = decimal_or_zero(value)
        audit = current_audit()
        if audit:
            audit.subsection("KILOS CAMPAÑA"); audit.audit_sql("kilos campaña socio", sql, params, 1, elapsed_ms); audit.line(f"Kilos totales: {total}")
        return total

    def _active(self, expr: str) -> str:
        return f"({expr} IS NULL OR TRIM(CAST({expr} AS TEXT)) = '')"

    def _surface_filter_counts(self, member_id: int, campaign: int | str, company: int | str, crops: Sequence[str], joined_rows: int, unique_count: int, total: Decimal) -> dict[str, Any]:
        ph = ",".join("?" for _ in crops)
        base = "FROM eepp.DEEPP AS d WHERE d.IdSocio=?"
        c: dict[str, Any] = {"Cultivos superficie": ",".join(crops)}
        c["A. DEEPP sin filtros adicionales"] = int(self.conn.execute(f"SELECT COUNT(*) {base}", [member_id]).fetchone()[0] or 0)
        c["B. Después de campaña"] = int(self.conn.execute(f"SELECT COUNT(*) {base} AND CAST(d.CAMPAÑA AS TEXT)=CAST(? AS TEXT)", [member_id, str(campaign)]).fetchone()[0] or 0)
        c["C. Después de empresa"] = int(self.conn.execute(f"SELECT COUNT(*) {base} AND CAST(d.CAMPAÑA AS TEXT)=CAST(? AS TEXT) AND CAST(d.EMPRESA AS TEXT)=CAST(? AS TEXT)", [member_id, str(campaign), str(company)]).fetchone()[0] or 0)
        ctx_params = [member_id, str(campaign), str(company)]
        c["D. Después de cultivos de superficie"] = int(self.conn.execute(f"SELECT COUNT(*) {base} AND CAST(d.CAMPAÑA AS TEXT)=CAST(? AS TEXT) AND CAST(d.EMPRESA AS TEXT)=CAST(? AS TEXT) AND UPPER(TRIM(d.CULTIVO)) IN ({ph})", [*ctx_params, *crops]).fetchone()[0] or 0)
        c["E. Después de CHA = -1"] = int(self.conn.execute(f"SELECT COUNT(*) {base} AND CAST(d.CAMPAÑA AS TEXT)=CAST(? AS TEXT) AND CAST(d.EMPRESA AS TEXT)=CAST(? AS TEXT) AND UPPER(TRIM(d.CULTIVO)) IN ({ph}) AND COALESCE(d.CHA,0)=-1", [*ctx_params, *crops]).fetchone()[0] or 0)
        join_boleta = f"FROM eepp.DEEPP AS d INNER JOIN eepp.DParcela AS dp ON TRIM(COALESCE(dp.Boleta,''))=TRIM(COALESCE(d.Boleta,'')) WHERE d.IdSocio=? AND CAST(d.CAMPAÑA AS TEXT)=CAST(? AS TEXT) AND CAST(d.EMPRESA AS TEXT)=CAST(? AS TEXT) AND UPPER(TRIM(d.CULTIVO)) IN ({ph}) AND COALESCE(d.CHA,0)=-1"
        c["F. Cruce DParcela sólo por Boleta"] = int(self.conn.execute(f"SELECT COUNT(*) {join_boleta}", [*ctx_params, *crops]).fetchone()[0] or 0)
        join_full = join_boleta.replace(" WHERE ", " AND CAST(dp.CAMPAÑA AS TEXT)=CAST(d.CAMPAÑA AS TEXT) AND CAST(dp.EMPRESA AS TEXT)=CAST(d.EMPRESA AS TEXT) AND UPPER(TRIM(dp.CULTIVO))=UPPER(TRIM(d.CULTIVO)) WHERE ")
        c["G. Cruce completo"] = int(self.conn.execute(f"SELECT COUNT(*) {join_full}", [*ctx_params, *crops]).fetchone()[0] or 0)
        c["H. Después de bajas"] = int(self.conn.execute(f"SELECT COUNT(*) {join_full} AND {self._active('d.BAJA')} AND {self._active('dp.BAJA')}", [*ctx_params, *crops]).fetchone()[0] or 0)
        c["I. Después de SupApor > 0"] = joined_rows
        c["J. Después de deduplicación - parcelas únicas"] = unique_count
        c["J. Después de deduplicación - suma SupApor"] = total
        return c

    def _parcel_audit_rows(self, member_id: int, campaign: int | str, company: int | str, crops: Sequence[str]) -> list[dict[str, Any]]:
        ph = ",".join("?" for _ in crops)
        sql = f"""
            SELECT d.IdSocio, d.Boleta, dp.Boleta, d.CAMPAÑA, dp.CAMPAÑA, d.EMPRESA, dp.EMPRESA, d.CULTIVO, dp.CULTIVO, d.CHA, d.BAJA, dp.BAJA,
                   dp.IdPM, dp.Pol, dp.Par, d.Recinto, dp.Rec, d.SupCul, dp.SupCul, dp.SupApor
            FROM eepp.DEEPP AS d LEFT JOIN eepp.DParcela AS dp ON TRIM(COALESCE(dp.Boleta,''))=TRIM(COALESCE(d.Boleta,''))
             AND CAST(dp.CAMPAÑA AS TEXT)=CAST(d.CAMPAÑA AS TEXT) AND CAST(dp.EMPRESA AS TEXT)=CAST(d.EMPRESA AS TEXT) AND UPPER(TRIM(dp.CULTIVO))=UPPER(TRIM(d.CULTIVO))
            WHERE d.IdSocio=? AND CAST(d.CAMPAÑA AS TEXT)=CAST(? AS TEXT) AND CAST(d.EMPRESA AS TEXT)=CAST(? AS TEXT) AND UPPER(TRIM(d.CULTIVO)) IN ({ph}) AND COALESCE(d.CHA,0)=-1
        """
        rows = []
        seen = set()
        for r in self.conn.execute(sql, [member_id, str(campaign), str(company), *crops]).fetchall():
            key = "|".join(str(v or "").strip() for v in (r[2], r[4], r[6], r[8], r[12], r[13], r[14], r[16]))
            reason = ""
            if r[2] is None: reason = "Sin cruce DParcela completo"
            elif not ((r[10] is None or str(r[10]).strip() == "") and (r[11] is None or str(r[11]).strip() == "")): reason = "BAJA informada"
            elif decimal_or_zero(r[19]) <= 0: reason = "SupApor <= 0"
            elif key in seen: reason = "Duplicada"
            included = reason == ""
            if included: seen.add(key)
            rows.append({"IdSocio": r[0], "Boleta DEEPP": r[1], "Boleta DParcela": r[2], "Campaña DEEPP": r[3], "Campaña DParcela": r[4], "Empresa DEEPP": r[5], "Empresa DParcela": r[6], "Cultivo DEEPP": r[7], "Cultivo DParcela": r[8], "CHA": r[9], "BAJA DEEPP": r[10], "BAJA DParcela": r[11], "IdPM": r[12], "Pol": r[13], "Par": r[14], "Recinto DEEPP": r[15], "Rec DParcela": r[16], "SupCul DEEPP": r[17], "SupCul DParcela": r[18], "SupApor": r[19], "Incluida": "Sí" if included else "No", "Motivo exclusión": reason, "Clave deduplicación": key})
        return rows
