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

    def calculate_applicable_hectares(self, member_id: int, campaign: int | str, company: int | str, applicable_crops: Sequence[str] | None = None) -> tuple[Decimal, tuple[str, ...]]:
        """Return SUM(DParcela.SupCul) after auditable staged filtering.

        Stages:
        A. candidate DEEPP rows by socio/campaña/empresa and CITRICOS/MANDARINA;
        B. DParcela lookup only by Boleta;
        C. context filters in Python: campaña, empresa, productive crop, baja, Año and SupCul.
        """
        crops = tuple(dict.fromkeys((c or "").strip().upper() for c in (applicable_crops or ("CITRICOS", "MANDARINA")) if (c or "").strip()))
        start = time.perf_counter()
        deepp_rows = self._deepp_candidate_rows(member_id, campaign, company, crops)
        cha_summary = self._cha_summary(member_id, campaign, company, crops)
        active_deepp = [r for r in deepp_rows if is_active_cha(r[7])]

        audit_rows: list[dict[str, Any]] = []
        included: dict[tuple[str, ...], Decimal] = {}
        warnings: list[str] = []
        dparcela_by_boleta: dict[str, list[sqlite3.Row | tuple[Any, ...]]] = {}
        filter_counts: dict[str, Any] = self._base_counts(member_id, campaign, company, crops)
        filter_counts["E. Valores reales de CHA"] = "; ".join(f"cultivo={cult} CHA={cha!r} tipo={type(cha).__name__} filas={cnt}" for cult, cha, cnt in cha_summary) or "(sin filas)"
        filter_counts["F. Filas consideradas CHA activas"] = len(active_deepp)
        filter_counts["G. Boletas activas"] = ", ".join(str(r[1]) for r in active_deepp) or "(ninguna)"

        for d in deepp_rows:
            if not is_active_cha(d[7]):
                audit_rows.append(self._audit_excluded_deepp(member_id, d))

        for d in active_deepp:
            boleta = d[1]
            dp_rows = self._dparcela_by_boleta(boleta)
            dparcela_by_boleta[str(boleta)] = dp_rows
            for idx, dp in enumerate(dp_rows):
                row, included_flag, reason, key = self._audit_dp_row(member_id, d, dp, campaign, company, crops)
                if included_flag:
                    sup = decimal_or_zero(dp[8])
                    if key in included:
                        if included[key] != sup:
                            msg = f"CONFLICTO_SUPERFICIE socio={member_id} clave={key}: {included[key]} vs {sup}; se excluye hasta revisión."
                            warnings.append(msg)
                            included.pop(key, None)
                            row["Motivo exclusión"] = msg
                        else:
                            row["Motivo exclusión"] = "DUPLICADO"
                        row["Incluida"] = "No"
                    else:
                        included[key] = sup
                audit_rows.append(row)
            if not dp_rows:
                audit_rows.append(self._audit_no_dp(member_id, d))

        total = sum(included.values(), Decimal("0"))
        filter_counts.update(self._dparcela_counts(dparcela_by_boleta, active_deepp, campaign, company, crops, len(included), total))
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
        self.logger.debug("Superficie socio=%s campaña=%s empresa=%s total=%s parcelas_unicas=%s", member_id, campaign, company, total, len(included))
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
            SELECT dp.Boleta, dp.CAMPAÑA, dp.EMPRESA, dp.CULTIVO, dp.IdPM, dp.Pol, dp.Par, dp.Rec,
                   dp.SupCul, {self._col('DParcela','SupRec')}, dp.SupApor, {self._col('DParcela','ALTA')}, dp.BAJA, {self._col('DParcela','Año')}
            FROM eepp.DParcela AS dp
            WHERE TRIM(CAST(dp.Boleta AS TEXT)) = TRIM(CAST(? AS TEXT))
            ORDER BY dp.CAMPAÑA, dp.EMPRESA, dp.CULTIVO, dp.IdPM, dp.Pol, dp.Par, dp.Rec
        """
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
        camp = [r for r in all_rows if str(r[1]).strip() == str(campaign).strip()]
        emp = [r for r in camp if str(r[2]).strip() == str(company).strip()]
        crop = [r for r in emp if str(r[3] or '').strip().upper() in crops]
        baja = [r for r in crop if is_active_baja(r[12])]
        age = [r for r in baja if is_old_enough_for_hectare_fee(r[13], int(campaign))]
        sup = [r for r in age if decimal_or_zero(r[8]) > 0]
        return {
            "H. Filas DParcela sólo por boleta": len(all_rows),
            "I. Campañas disponibles": ", ".join(sorted({str(r[1]) for r in all_rows})) or "(ninguna)",
            "J. Empresas disponibles": ", ".join(sorted({str(r[2]) for r in all_rows})) or "(ninguna)",
            "K. Cultivos disponibles": ", ".join(sorted({str(r[3]) for r in all_rows})) or "(ninguno)",
            "L. Filas tras campaña": len(camp),
            "M. Filas tras empresa": len(emp),
            "N. Filas tras cultivo": len(crop),
            "O. Filas tras baja": len(baja),
            "P. Filas tras Año <= campaña - 5": len(age),
            "Q. Filas tras SupCul > 0": len(sup),
            "R. Parcelas únicas": unique_count,
            "S. Superficie final": total,
        }

    def _audit_dp_row(self, member_id: int, d: Any, dp: Any, campaign: Any, company: Any, crops: Sequence[str]) -> tuple[dict[str, Any], bool, str, tuple[str, ...]]:
        key = tuple(str(v or "").strip().upper() for v in (dp[0], dp[1], dp[2], dp[3], dp[4], dp[5], dp[6], dp[7]))
        reasons = []
        if str(dp[1]).strip() != str(campaign).strip(): reasons.append("CAMPANA_DISTINTA")
        if str(dp[2]).strip() != str(company).strip(): reasons.append("EMPRESA_DISTINTA")
        if str(dp[3] or "").strip().upper() not in crops: reasons.append("CULTIVO_NO_CONFIGURADO")
        if not is_active_baja(dp[12]): reasons.append("PARCELA_DADA_DE_BAJA")
        year = parse_plantation_year(dp[13])
        if year is None: reasons.append("ANO_NO_VALIDO")
        elif not is_old_enough_for_hectare_fee(year, int(campaign)): reasons.append("PLANTACION_MENOR_CINCO_ANOS")
        if decimal_or_zero(dp[8]) <= 0: reasons.append("SUPERFICIE_CERO")
        reason = "; ".join(reasons)
        return ({"IdSocio": member_id, "Boleta DEEPP": d[1], "Cultivo DEEPP": d[4], "Campaña DEEPP": d[2], "Empresa DEEPP": d[3], "CHA original": d[7], "CHA activo": "Sí", "Baja DEEPP": d[9], "SupCul DEEPP": d[8], "Boleta DParcela": dp[0], "Campaña DParcela": dp[1], "Empresa DParcela": dp[2], "Cultivo DParcela": dp[3], "IdPM": dp[4], "Pol": dp[5], "Par": dp[6], "Rec": dp[7], "SupCul DParcela": dp[8], "SupRec": dp[9], "SupApor": dp[10], "Baja DParcela": dp[12], "Año": year, "Año máximo admitido": int(campaign)-5, "Antigüedad suficiente": "Sí" if year is not None and is_old_enough_for_hectare_fee(year, int(campaign)) else "No", "Incluida": "Sí" if not reason else "No", "Motivo exclusión": reason, "Clave deduplicación": "|".join(key)}, not reason, reason, key)

    def _audit_no_dp(self, member_id: int, d: Any) -> dict[str, Any]:
        return {"IdSocio": member_id, "Boleta DEEPP": d[1], "Cultivo DEEPP": d[4], "Campaña DEEPP": d[2], "Empresa DEEPP": d[3], "CHA original": d[7], "CHA activo": "Sí", "Baja DEEPP": d[9], "SupCul DEEPP": d[8], "Incluida": "No", "Motivo exclusión": "Sin filas DParcela por Boleta", "Clave deduplicación": ""}

    def _audit_excluded_deepp(self, member_id: int, d: Any) -> dict[str, Any]:
        reasons = []
        if not is_active_cha(d[7]):
            reasons.append("CHA_NO_ACTIVO")
        if not is_active_baja(d[9]):
            reasons.append("BAJA DEEPP informada")
        return {"IdSocio": member_id, "Boleta DEEPP": d[1], "Cultivo DEEPP": d[4], "Campaña DEEPP": d[2], "Empresa DEEPP": d[3], "CHA original": d[7], "CHA activo": "Sí" if is_active_cha(d[7]) else "No", "Baja DEEPP": d[9], "SupCul DEEPP": d[8], "Incluida": "No", "Motivo exclusión": "; ".join(reasons), "Clave deduplicación": ""}
