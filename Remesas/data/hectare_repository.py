from __future__ import annotations

from decimal import Decimal, InvalidOperation
import logging
import sqlite3
import time
from typing import Any, Sequence

from domain.audit import current_audit
from domain.financial_rules import EFFECTIVE_NET_SQL
from domain.utils import decimal_or_zero, parse_yes_no


def is_active_flag(value: object) -> bool:
    """Normalize Access/SQLite active flags for CHA."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value)) in {Decimal("-1"), Decimal("1")}
        except InvalidOperation:
            return False
    text = str(value or "").strip().replace(",", ".")
    try:
        return Decimal(text) in {Decimal("-1"), Decimal("1")}
    except InvalidOperation:
        return parse_yes_no(value)


def parse_plantation_year(value: object) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(Decimal(text.replace(",", ".")))
    except (InvalidOperation, ValueError):
        return None


def is_old_enough_for_hectare_fee(plantation_year: object, campaign: int) -> bool:
    year = parse_plantation_year(plantation_year)
    return year is not None and year <= int(campaign) - 5

def is_active_cha(value: object) -> bool:
    return is_active_flag(value)


def is_active_baja(value: object) -> bool:
    return value is None or str(value).strip() == ""


class HectareRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.logger = logging.getLogger(__name__)
        self.last_surface_audit_rows: tuple[dict[str, Any], ...] = ()
        self.last_surface_filter_counts: dict[str, Any] = {}
        self.last_delivery_audit_rows: tuple[dict[str, Any], ...] = ()
        self.last_deepp_sql = ""
        self.last_deepp_params: list[Any] = []
        self.last_dparcela_sql = ""

    def calculate_applicable_hectares(self, member_id: int, campaign: int | str, company: int | str, eligible_crops: Sequence[str] | None = None) -> tuple[Decimal, tuple[str, ...]]:
        """Return SUM(DParcela.SupCul) after auditable staged filtering.

        Stages:
        A. candidate DEEPP rows by socio/campaña/empresa and CITRICOS/MANDARINA;
        B. DParcela lookup only by Boleta;
        C. context filters in Python: campaña, empresa, productive crop, baja, Año and SupCul.
        """
        crops = tuple(dict.fromkeys((c or "").strip().upper() for c in (eligible_crops or ("CITRICOS", "MANDARINA")) if (c or "").strip()))
        start = time.perf_counter()
        self.last_deepp_sql = self._deepp_sql(crops)
        self.last_deepp_params = [member_id, str(campaign), str(company), *crops]
        deepp_rows = self._deepp_candidate_rows(member_id, campaign, company, crops)
        cha_summary = self._cha_summary(member_id, campaign, company, crops)
        active_deepp = [r for r in deepp_rows if is_active_cha(r[7])]

        audit_rows: list[dict[str, Any]] = []
        included_rows: list[tuple[str, Decimal]] = []
        warnings: list[str] = []
        dparcela_by_boleta: dict[str, list[sqlite3.Row | tuple[Any, ...]]] = {}
        processed_boletas: set[str] = set()
        filter_counts: dict[str, Any] = self._base_counts(member_id, campaign, company, crops)
        filter_counts["E. Valores reales de CHA"] = "; ".join(f"cultivo={cult} CHA={cha!r} tipo={type(cha).__name__} filas={cnt}" for cult, cha, cnt in cha_summary) or "(sin filas)"
        filter_counts["F. Filas consideradas CHA activas"] = len(active_deepp)
        filter_counts["G. Boletas activas"] = ", ".join(str(r[1]) for r in active_deepp) or "(ninguna)"

        for d in deepp_rows:
            if not is_active_cha(d[7]):
                row = self._audit_excluded_deepp(member_id, d)
                row["Número DEEPP encontrados"] = len(deepp_rows)
                audit_rows.append(row)

        for d in active_deepp:
            boleta = d[1]
            boleta_key = str(boleta).strip()
            if boleta_key in processed_boletas:
                continue
            processed_boletas.add(boleta_key)
            dp_rows = self._dparcela_by_boleta(boleta)
            dparcela_by_boleta[boleta_key] = dp_rows
            seen_physical_rows: set[str] = set()
            for idx, dp in enumerate(dp_rows):
                row, included_flag, reason, row_identity = self._audit_dp_row(member_id, d, dp, campaign, company, crops, idx)
                row["Número DEEPP encontrados"] = len(deepp_rows)
                if row_identity in seen_physical_rows:
                    included_flag = False
                    row["Incluida"] = "No"
                    row["Motivo exclusión"] = "DUPLICADO_TECNICO_JOIN"
                    warnings.append(f"Duplicación técnica de DParcela detectada socio={member_id} row_id={row_identity}; se suma una sola vez.")
                else:
                    seen_physical_rows.add(row_identity)
                if included_flag:
                    sup = decimal_or_zero(dp[9])
                    included_rows.append((row_identity, sup))
                audit_rows.append(row)
            if not dp_rows:
                row = self._audit_no_dp(member_id, d)
                row["Número DEEPP encontrados"] = len(deepp_rows)
                audit_rows.append(row)

        total = sum((sup for _, sup in included_rows), Decimal("0"))
        filter_counts.update(self._dparcela_counts(dparcela_by_boleta, active_deepp, campaign, company, crops, len(included_rows), total))
        self.last_surface_audit_rows = tuple(audit_rows)
        self.last_surface_filter_counts = filter_counts
        elapsed_ms = (time.perf_counter() - start) * 1000

        audit = current_audit()
        if audit:
            audit.subsection("SUPERFICIE")
            audit.audit_sql("DEEPP candidatas superficie", self._deepp_sql(crops), [member_id, str(campaign), str(company), *crops], len(deepp_rows), elapsed_ms)
            audit.audit_filters("superficie", filter_counts)
            audit.line("Valores CHA reales:")
            for cult, cha, cnt in cha_summary:
                audit.line(f"cultivo={cult} CHA={cha!r} tipo={type(cha).__name__} filas={cnt}")
            audit.line("Parcelas auditadas:")
            for item in audit_rows:
                audit.line(" | ".join(f"{k}={v}" for k, v in item.items()))
        self.logger.debug("Superficie socio=%s campaña=%s empresa=%s total=%s filas_incluidas=%s", member_id, campaign, company, total, len(included_rows))
        return total, tuple(warnings)

    def total_effective_kg(self, member_id: int, campaign: int | str, company: int | str, eligible_crops: Sequence[str]) -> Decimal:
        placeholders = ",".join("?" for _ in eligible_crops)
        sql = f"""
            SELECT COALESCE(SUM({EFFECTIVE_NET_SQL.format(alias='p')}), 0) AS TotalEffectiveKg
            FROM PesosFres AS p
            WHERE p.IdSocio=?
              AND CAST(p.CAMPAÑA AS TEXT)=CAST(? AS TEXT)
              AND CAST(p.EMPRESA AS TEXT)=CAST(? AS TEXT)
              AND UPPER(TRIM(p.CULTIVO)) IN ({placeholders})
        """
        params = [member_id, str(campaign), str(company), *eligible_crops]
        start = time.perf_counter(); value = self.conn.execute(sql, params).fetchone()[0]; elapsed_ms = (time.perf_counter() - start) * 1000
        total = decimal_or_zero(value)
        self.last_delivery_audit_rows = self._delivery_proration_rows(member_id, campaign, company, eligible_crops)
        audit = current_audit()
        if audit:
            audit.subsection("CuotaHa.Prorrateo"); audit.audit_sql("kilos campaña socio", sql, params, 1, elapsed_ms)
            for crop in eligible_crops:
                kg = sum((r["NetoEfectivo"] for r in self.last_delivery_audit_rows if r["Cultivo"] == crop), Decimal("0"))
                audit.line(f"kg_{crop.lower()}={kg}")
            audit.line(f"total_effective_kg={total}")
        return total

    # These report queries deliberately use PesosFres.Boleta.  A delivery is never
    # associated to a parcel through an inferred field such as Albaran.
    def list_fee_report_boletas(self, campaign, company, member_id=None, boleta=None, crop=None, date_from=None, date_to=None):
        where = ["p.IdSocio <> 0", "CAST(p.CAMPAÑA AS TEXT)=CAST(? AS TEXT)", "CAST(p.EMPRESA AS TEXT)=CAST(? AS TEXT)"]
        params: list[Any] = [str(campaign), str(company)]
        if member_id is not None: where.append("p.IdSocio=?"); params.append(member_id)
        if boleta not in (None, ""): where.append("TRIM(CAST(p.Boleta AS TEXT))=TRIM(CAST(? AS TEXT))"); params.append(boleta)
        if crop: where.append("UPPER(TRIM(p.CULTIVO))=?"); params.append(str(crop).strip().upper())
        if date_from and self._has_local_column("PesosFres", "Fcarga"): where.append("date(p.Fcarga)>=date(?)"); params.append(str(date_from))
        if date_to and self._has_local_column("PesosFres", "Fcarga"): where.append("date(p.Fcarga)<=date(?)"); params.append(str(date_to))
        sql = "SELECT DISTINCT p.IdSocio, COALESCE(" + self._local_col("PesosFres", "Socio").split(" AS ")[0] + ", ''), p.CAMPAÑA, p.EMPRESA, p.Boleta FROM PesosFres p WHERE " + " AND ".join(where) + " AND TRIM(CAST(p.Boleta AS TEXT)) <> '' AND TRIM(CAST(p.Boleta AS TEXT)) <> '0' ORDER BY p.IdSocio, p.Boleta"
        return self.conn.execute(sql, params).fetchall()

    def get_boleta_deliveries(self, member_id, boleta, campaign, company, crop=None, date_from=None, date_to=None):
        where = ["p.IdSocio=?", "CAST(p.CAMPAÑA AS TEXT)=CAST(? AS TEXT)", "CAST(p.EMPRESA AS TEXT)=CAST(? AS TEXT)", "TRIM(CAST(p.Boleta AS TEXT))=TRIM(CAST(? AS TEXT))"]
        params: list[Any] = [member_id, str(campaign), str(company), boleta]
        if crop: where.append("UPPER(TRIM(p.CULTIVO))=?"); params.append(str(crop).strip().upper())
        if date_from and self._has_local_column("PesosFres", "Fcarga"): where.append("date(p.Fcarga)>=date(?)"); params.append(str(date_from))
        if date_to and self._has_local_column("PesosFres", "Fcarga"): where.append("date(p.Fcarga)<=date(?)"); params.append(str(date_to))
        reg = self._local_col("PesosFres", "Registro").split(" AS ")[0]
        sql = f"SELECT {reg}, p.CULTIVO, p.Neto, p.NetoPartida, {EFFECTIVE_NET_SQL.format(alias='p')} FROM PesosFres p WHERE " + " AND ".join(where)
        return self.conn.execute(sql, params).fetchall()

    def list_deliveries_without_valid_boleta(self, campaign, company):
        reg = self._local_col("PesosFres", "Registro").split(" AS ")[0]; name = self._local_col("PesosFres", "Socio").split(" AS ")[0]
        sql = f"SELECT p.IdSocio, {name}, {reg}, p.CAMPAÑA, p.EMPRESA, p.CULTIVO, {EFFECTIVE_NET_SQL.format(alias='p')}, p.Boleta FROM PesosFres p WHERE p.IdSocio<>0 AND CAST(p.CAMPAÑA AS TEXT)=CAST(? AS TEXT) AND CAST(p.EMPRESA AS TEXT)=CAST(? AS TEXT) AND (p.Boleta IS NULL OR TRIM(CAST(p.Boleta AS TEXT)) IN ('', '0') OR TRIM(CAST(p.Boleta AS TEXT)) GLOB '*[^0-9]*')"
        return self.conn.execute(sql, [str(campaign), str(company)]).fetchall()

    def get_boleta_surface_details(self, member_id, boleta, campaign, company, eligible_crops):
        """Apply the existing DEEPP/DParcela rules to one boleta, not a member total."""
        crops = tuple(str(c).strip().upper() for c in eligible_crops)
        rows = [r for r in self._deepp_candidate_rows(member_id, campaign, company, crops) if str(r[1]).strip() == str(boleta).strip()]
        details, seen = [], set()
        for d in rows:
            if not is_active_cha(d[7]):
                continue
            for i, dp in enumerate(self._dparcela_by_boleta(boleta)):
                audit, included, reason, identity = self._audit_dp_row(member_id, d, dp, campaign, company, crops, i)
                if identity in seen: included, reason = False, "DUPLICADO_TECNICO_JOIN"
                seen.add(identity)
                details.append((audit, included, reason, dp))
        return details

    def _has_local_column(self, table: str, name: str) -> bool:
        return name.upper() in {r[1].upper() for r in self.conn.execute(f"PRAGMA table_info('{table}')")}

    def _delivery_proration_rows(self, member_id: int, campaign: int | str, company: int | str, eligible_crops: Sequence[str]) -> tuple[dict[str, Any], ...]:
        placeholders = ",".join("?" for _ in eligible_crops)
        sql = f"""
            SELECT p.IdSocio, {self._local_col('PesosFres','Socio')}, {self._local_col('PesosFres','Registro')},
                   {self._local_col('PesosFres','Fecha')}, p.CAMPAÑA, p.EMPRESA, p.CULTIVO,
                   {self._local_col('PesosFres','Variedad')}, {self._local_col('PesosFres','Boleta')},
                   p.Neto, p.NetoPartida, {EFFECTIVE_NET_SQL.format(alias='p')} AS NetoEfectivo
            FROM PesosFres AS p
            WHERE p.IdSocio=? AND CAST(p.CAMPAÑA AS TEXT)=CAST(? AS TEXT)
              AND CAST(p.EMPRESA AS TEXT)=CAST(? AS TEXT) AND UPPER(TRIM(p.CULTIVO)) IN ({placeholders})
            ORDER BY p.CULTIVO
        """
        rows = self.conn.execute(sql, [member_id, str(campaign), str(company), *eligible_crops]).fetchall()
        out = []
        for r in rows:
            out.append({"Nº Socio": r[0], "Socio": r[1], "Registro": r[2], "Fecha": r[3], "Campaña": r[4], "Empresa": r[5], "Cultivo": str(r[6] or "").strip().upper(), "Variedad": r[7], "Boleta": r[8], "Neto": r[9], "NetoPartida": r[10], "NetoEfectivo": decimal_or_zero(r[11]), "Incluida en denominador": "Sí", "Motivo exclusión": "", "Boleta apta para cuota": "Informativa", "Relevancia de boleta": "No interviene en el prorrateo"})
        return tuple(out)

    def _local_col(self, table: str, name: str, alias: str | None = None) -> str:
        cols = {r[1].upper() for r in self.conn.execute(f"PRAGMA table_info('{table}')").fetchall()}
        out = alias or name
        return f"p.{name} AS {out}" if name.upper() in cols else f"NULL AS {out}"

    def _col(self, table: str, name: str, alias: str | None = None) -> str:
        cols = {r[1].upper() for r in self.conn.execute(f"PRAGMA eepp.table_info('{table}')").fetchall()}
        out = alias or name
        return f"{name} AS {out}" if name.upper() in cols else f"NULL AS {out}"

    def _deepp_sql(self, crops: Sequence[str]) -> str:
        ph = ",".join("?" for _ in crops)
        return f"""
            SELECT DISTINCT d.IdSocio, d.Boleta, d.CAMPAÑA, d.EMPRESA, d.CULTIVO,
                {self._col('DEEPP','SubCultivo')}, {self._col('DEEPP','Variedad')}, d.CHA, d.SupCul, d.BAJA
            FROM eepp.DEEPP AS d
            WHERE d.IdSocio = ? AND CAST(d.CAMPAÑA AS TEXT) = CAST(? AS TEXT)
              AND CAST(d.EMPRESA AS TEXT) = CAST(? AS TEXT)
              AND UPPER(TRIM(d.CULTIVO)) IN ({ph})
            ORDER BY UPPER(TRIM(d.CULTIVO)), d.Boleta
        """

    def _deepp_candidate_rows(self, member_id: int, campaign: int | str, company: int | str, crops: Sequence[str]) -> list[Any]:
        return self.conn.execute(self._deepp_sql(crops), [member_id, str(campaign), str(company), *crops]).fetchall()

    def _cha_summary(self, member_id: int, campaign: int | str, company: int | str, crops: Sequence[str]) -> list[Any]:
        ph = ",".join("?" for _ in crops)
        sql = f"SELECT d.CULTIVO, d.CHA, COUNT(*) FROM eepp.DEEPP AS d WHERE d.IdSocio=? AND CAST(d.CAMPAÑA AS TEXT)=CAST(? AS TEXT) AND CAST(d.EMPRESA AS TEXT)=CAST(? AS TEXT) AND UPPER(TRIM(d.CULTIVO)) IN ({ph}) GROUP BY d.CULTIVO, d.CHA ORDER BY d.CULTIVO, d.CHA"
        return self.conn.execute(sql, [member_id, str(campaign), str(company), *crops]).fetchall()

    def _dparcela_by_boleta(self, boleta: Any) -> list[Any]:
        sql = f"""
            SELECT dp.rowid AS RowIdParcela, dp.Boleta, dp.CAMPAÑA, dp.EMPRESA, dp.CULTIVO, dp.IdPM, dp.Pol, dp.Par, dp.Rec,
                   dp.SupCul, {self._col('DParcela','SupRec')}, dp.SupApor, {self._col('DParcela','ALTA')}, dp.BAJA, {self._col('DParcela','Año')}
            FROM eepp.DParcela AS dp
            WHERE TRIM(CAST(dp.Boleta AS TEXT)) = TRIM(CAST(? AS TEXT))
            ORDER BY dp.rowid
        """
        self.last_dparcela_sql = sql
        return self.conn.execute(sql, [boleta]).fetchall()

    def _base_counts(self, member_id: int, campaign: int | str, company: int | str, crops: Sequence[str]) -> dict[str, Any]:
        ph = ",".join("?" for _ in crops)
        base = "FROM eepp.DEEPP AS d WHERE d.IdSocio=?"
        return {
            "Cultivos superficie": ",".join(crops),
            "A. DEEPP sin filtros adicionales": self.conn.execute(f"SELECT COUNT(*) {base}", [member_id]).fetchone()[0],
            "B. Después de campaña": self.conn.execute(f"SELECT COUNT(*) {base} AND CAST(d.CAMPAÑA AS TEXT)=CAST(? AS TEXT)", [member_id, str(campaign)]).fetchone()[0],
            "C. Después de empresa": self.conn.execute(f"SELECT COUNT(*) {base} AND CAST(d.CAMPAÑA AS TEXT)=CAST(? AS TEXT) AND CAST(d.EMPRESA AS TEXT)=CAST(? AS TEXT)", [member_id, str(campaign), str(company)]).fetchone()[0],
            "D. Después de cultivos de superficie activos": self.conn.execute(f"SELECT COUNT(*) {base} AND CAST(d.CAMPAÑA AS TEXT)=CAST(? AS TEXT) AND CAST(d.EMPRESA AS TEXT)=CAST(? AS TEXT) AND UPPER(TRIM(d.CULTIVO)) IN ({ph})", [member_id, str(campaign), str(company), *crops]).fetchone()[0],
        }

    def _dparcela_counts(self, by_boleta: dict[str, list[Any]], active_deepp: list[Any], campaign: Any, company: Any, crops: Sequence[str], unique_count: int, total: Decimal) -> dict[str, Any]:
        all_rows = [r for rows in by_boleta.values() for r in rows]
        camp = [r for r in all_rows if str(r[2]).strip() == str(campaign).strip()]
        emp = [r for r in camp if str(r[3]).strip() == str(company).strip()]
        crop = [r for r in emp if str(r[4] or '').strip().upper() in crops]
        baja = [r for r in crop if is_active_baja(r[13])]
        age = [r for r in baja if is_old_enough_for_hectare_fee(r[14], int(campaign))]
        sup = [r for r in age if decimal_or_zero(r[9]) > 0]
        return {
            "H. Filas DParcela sólo por boleta": len(all_rows),
            "I. Campañas disponibles": ", ".join(sorted({str(r[2]) for r in all_rows})) or "(ninguna)",
            "J. Empresas disponibles": ", ".join(sorted({str(r[3]) for r in all_rows})) or "(ninguna)",
            "K. Cultivos disponibles": ", ".join(sorted({str(r[4]) for r in all_rows})) or "(ninguno)",
            "L. Filas tras campaña": len(camp),
            "M. Filas tras empresa": len(emp),
            "N. Filas tras cultivo": len(crop),
            "O. Filas tras baja": len(baja),
            "P. Filas tras Año <= campaña - 5": len(age),
            "Q. Filas tras SupCul > 0": len(sup),
            "R. Filas físicas incluidas": unique_count,
            "S. Superficie final": total,
        }

    def _audit_dp_row(self, member_id: int, d: Any, dp: Any, campaign: Any, company: Any, crops: Sequence[str], index: int = 0) -> tuple[dict[str, Any], bool, str, str]:
        row_identity = str(dp[0] if dp[0] is not None else index)
        reasons = []
        if str(dp[2]).strip() != str(campaign).strip(): reasons.append("CAMPANA_DISTINTA")
        if str(dp[3]).strip() != str(company).strip(): reasons.append("EMPRESA_DISTINTA")
        if str(dp[4] or "").strip().upper() not in crops: reasons.append("CULTIVO_NO_CONFIGURADO")
        if not is_active_baja(dp[13]): reasons.append("PARCELA_DADA_DE_BAJA")
        year = parse_plantation_year(dp[14])
        if year is None: reasons.append("ANO_NO_VALIDO")
        elif not is_old_enough_for_hectare_fee(year, int(campaign)): reasons.append("PLANTACION_MENOR_CINCO_ANOS")
        if decimal_or_zero(dp[9]) <= 0: reasons.append("SUPERFICIE_CERO")
        reason = "; ".join(reasons)
        return ({"Consulta SQL DEEPP": self.last_deepp_sql, "Parámetros DEEPP": list(self.last_deepp_params), "Consulta SQL DParcela": self.last_dparcela_sql, "Parámetros DParcela": [dp[1]], "IdSocio": member_id, "Boleta DEEPP": d[1], "Cultivo DEEPP": d[4], "Campaña DEEPP": d[2], "Empresa DEEPP": d[3], "CHA original": d[7], "CHA activo": "Sí", "Baja DEEPP": d[9], "SupCul DEEPP": d[8], "Variedad DEEPP": d[6], "RowId parcela": row_identity, "Boleta DParcela": dp[1], "Campaña DParcela": dp[2], "Empresa DParcela": dp[3], "Cultivo DParcela": dp[4], "IdPM": dp[5], "Pol": dp[6], "Par": dp[7], "Rec": dp[8], "SupCul DParcela": dp[9], "SupRec": dp[10], "SupApor": dp[11], "Alta DParcela": dp[12], "Baja DParcela": dp[13], "Año": year, "Año máximo admitido": int(campaign)-5, "Antigüedad": (int(campaign) - year) if year is not None else "", "Antigüedad suficiente": "Sí" if year is not None and is_old_enough_for_hectare_fee(year, int(campaign)) else "No", "Incluida": "Sí" if not reason else "No", "Motivo": "VALIDA" if not reason else reason, "Motivo exclusión": reason, "Identidad fila física": row_identity}, not reason, reason, row_identity)

    def _audit_no_dp(self, member_id: int, d: Any) -> dict[str, Any]:
        return {"Consulta SQL DEEPP": self.last_deepp_sql, "Parámetros DEEPP": list(self.last_deepp_params), "IdSocio": member_id, "Boleta DEEPP": d[1], "Cultivo DEEPP": d[4], "Campaña DEEPP": d[2], "Empresa DEEPP": d[3], "CHA original": d[7], "CHA activo": "Sí", "Baja DEEPP": d[9], "SupCul DEEPP": d[8], "Variedad DEEPP": d[6], "Incluida": "No", "Motivo": "Sin filas DParcela por Boleta", "Motivo exclusión": "Sin filas DParcela por Boleta", "Identidad fila física": ""}

    def _audit_excluded_deepp(self, member_id: int, d: Any) -> dict[str, Any]:
        reasons = []
        if not is_active_cha(d[7]):
            reasons.append("CHA_NO_ACTIVO")
        if not is_active_baja(d[9]):
            reasons.append("BAJA DEEPP informada")
        return {"Consulta SQL DEEPP": self.last_deepp_sql, "Parámetros DEEPP": list(self.last_deepp_params), "IdSocio": member_id, "Boleta DEEPP": d[1], "Cultivo DEEPP": d[4], "Campaña DEEPP": d[2], "Empresa DEEPP": d[3], "CHA original": d[7], "CHA activo": "Sí" if is_active_cha(d[7]) else "No", "Baja DEEPP": d[9], "SupCul DEEPP": d[8], "Variedad DEEPP": d[6], "Incluida": "No", "Motivo": "; ".join(reasons), "Motivo exclusión": "; ".join(reasons), "Identidad fila física": ""}
