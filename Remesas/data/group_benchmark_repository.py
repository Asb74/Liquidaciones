from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
import sqlite3
from group_benchmark_surface_audit import AUDIT_LOG_PATH, append_surface_audit


@dataclass(frozen=True)
class VarietalGroup:
    crop: str; group: str; subgroup: str; label: str; varieties: tuple[str, ...]


@dataclass(frozen=True)
class ProductiveSurfaceResult:
    hectares: Decimal
    parcel_count: int
    excluded_count: int
    warnings: tuple[str, ...]
    audit_rows: tuple[dict, ...]
    candidate_boletas: tuple[str, ...] = ()
    matched_boletas: tuple[str, ...] = ()
    included_boletas: tuple[str, ...] = ()
    parcel_row_count: int = 0
    invalid_row_count: int = 0
    missing_surface_boletas: tuple[str, ...] = ()
    status: str = "OK"


def _norm(value: object) -> str:
    return str(value or "").strip().upper()


def _display(value: object) -> str:
    return "" if value is None else str(value)


def _set_text(values: set[str]) -> str:
    return "|".join(sorted(values))


class GroupBenchmarkRepository:
    def __init__(self, conn: sqlite3.Connection, audit_log_path: str | Path = AUDIT_LOG_PATH) -> None:
        self.conn = conn
        self.audit_log_path = Path(audit_log_path).resolve()
        self.audit_run_id: str | None = None
        self.audit_parent_run_id: str | None = None
        self.audit_run_source: str = "REMESA_CALCULATION"

    def set_audit_run_id(self, run_id: str) -> None:
        self.audit_run_id = run_id

    def set_audit_context(self, parent_run_id: str | None, run_source: str) -> None:
        self.audit_parent_run_id = parent_run_id
        self.audit_run_source = run_source

    def _audit(self, section: str, **values: object) -> None:
        append_surface_audit(section, {"run_id": self.audit_run_id,
            "parent_run_id": self.audit_parent_run_id, "run_source": self.audit_run_source,
            **values}, self.audit_log_path)

    def get_varietal_group(self, crop: str, variety: str) -> VarietalGroup | None:
        sql = """SELECT CULTIVO, Variedad, GRUPO, SUBGRUPO FROM eepp.MVariedad
                 WHERE UPPER(TRIM(CULTIVO))=UPPER(TRIM(?)) AND UPPER(TRIM(Variedad))=UPPER(TRIM(?)) LIMIT 1"""
        row = self.conn.execute(sql, (crop, variety)).fetchone()
        if row is None:
            try:
                row = self.conn.execute(sql.replace("eepp.MVariedad", "MVariedad"), (crop, variety)).fetchone()
            except sqlite3.Error:
                row = None
        # Crop is not a varietal-group boundary.  Some destinations repeat the
        # variety under another crop while others only define it once.
        if row is None:
            fallback = """SELECT CULTIVO, Variedad, GRUPO, SUBGRUPO FROM eepp.MVariedad
                          WHERE UPPER(TRIM(Variedad))=UPPER(TRIM(?))
                          ORDER BY UPPER(TRIM(CULTIVO)) LIMIT 1"""
            try:
                row = self.conn.execute(fallback, (variety,)).fetchone()
            except sqlite3.Error:
                try:
                    row = self.conn.execute(fallback.replace("eepp.MVariedad", "MVariedad"), (variety,)).fetchone()
                except sqlite3.Error:
                    row = None
        if row is None:
            return None
        db_crop, group, subgroup = _norm(row["CULTIVO"]) or _norm(crop), _norm(row["GRUPO"]), _norm(row["SUBGRUPO"])
        rows = self.conn.execute(
            """SELECT DISTINCT Variedad FROM eepp.MVariedad
               WHERE UPPER(TRIM(GRUPO))=UPPER(TRIM(?))
                 AND UPPER(TRIM(COALESCE(SUBGRUPO,'')))=UPPER(TRIM(?)) AND TRIM(COALESCE(Variedad,''))<>''
               ORDER BY UPPER(TRIM(Variedad))""", (group, subgroup)
        ).fetchall()
        varieties = tuple(_norm(r["Variedad"]) for r in rows if _norm(r["Variedad"]))
        label = " ".join(p for p in (group, subgroup) if p) or _norm(variety)
        return VarietalGroup(db_crop, group, subgroup, label, varieties)

    def get_productive_hectares(self, member_id: int, campaign: str, company: str, crop: str, varieties: tuple[str, ...]) -> ProductiveSurfaceResult:
        """Sum each physical DParcela row once for the matching varietal group."""
        normalized_varieties = tuple(_norm(v) for v in varieties)
        normalized_set = set(normalized_varieties)
        self._audit(
            "ProductiveSurfaceQuery", member_id=member_id, campaign=campaign, company=company,
            crop=crop, variety_count=len(varieties), varieties_original=varieties,
            varieties_normalized=normalized_varieties, sql_parameter_count=3 + len(varieties),
            status="NO_VARIETIES" if not varieties else "EXECUTING",
        )

        candidate_sql = """SELECT IdSocio, Boleta, CAMPAÑA, EMPRESA, CULTIVO, Variedad
                           FROM eepp.DEEPP
                           WHERE CAST(IdSocio AS TEXT)=CAST(? AS TEXT)
                             AND UPPER(TRIM(CAMPAÑA))=UPPER(TRIM(?))
                             AND UPPER(TRIM(EMPRESA))=UPPER(TRIM(?))
                           ORDER BY Boleta, Variedad"""
        candidates = [dict(r) for r in self.conn.execute(candidate_sql, (member_id, campaign, company)).fetchall()]
        audit: list[dict] = []
        candidate_boletas: set[str] = set()
        matched_boletas: set[str] = set()
        for row in candidates:
            boleta = _display(row.get("Boleta"))
            normalized = _norm(row.get("Variedad"))
            matches = normalized in normalized_set
            candidate_boletas.add(boleta)
            if matches:
                matched_boletas.add(boleta)
            audit.append({"audit_type": "deepp_candidate", **row, "variety_normalized": normalized, "matches_group": matches})
            self._audit("ProductiveSurfaceDeeppCandidate", member_id=member_id, boleta=boleta,
                        variety_original=row.get("Variedad"), variety_normalized=normalized,
                        matches_group="yes" if matches else "no")

        missing_warning = "No existe superficie cultivable válida para las boletas coincidentes. Los datos deben ser completados por el usuario."
        if not varieties:
            warnings = ("Grupo sin variedades resueltas.", missing_warning)
            self._audit("ProductiveSurfaceResult", member_id=member_id, candidate_boletas=_set_text(candidate_boletas),
                        matched_boletas="", included_boletas="", parcel_row_count=0,
                        included_parcel_row_count=0, invalid_parcel_row_count=0, hectares=Decimal("0"),
                        status="MISSING_SURFACE_DATA", warnings="|".join(warnings))
            return ProductiveSurfaceResult(Decimal("0"), 0, 0, warnings, tuple(audit),
                                           tuple(sorted(candidate_boletas)), status="MISSING_SURFACE_DATA")

        placeholders = ",".join("?" for _ in varieties)
        # Fetch by boleta only so every physical row is auditable. Context and validity
        # rules are applied below, before summing each rowid once.
        sql = f"""WITH MatchingBoletas AS (
                    SELECT e.Boleta
                    FROM eepp.DEEPP e
                    WHERE CAST(e.IdSocio AS TEXT)=CAST(? AS TEXT)
                      AND UPPER(TRIM(e.CAMPAÑA))=UPPER(TRIM(?))
                      AND UPPER(TRIM(e.EMPRESA))=UPPER(TRIM(?))
                      AND UPPER(TRIM(e.Variedad)) IN ({placeholders})
                  )
                  SELECT m.Boleta AS DeeppBoleta, p.rowid AS ParcelaRowId,
                    p.Boleta AS ParcelaBoleta, p.CAMPAÑA AS ParcelaCampana,
                    p.EMPRESA AS ParcelaEmpresa, p.CULTIVO AS ParcelaCultivo,
                    p.IdPM, p.Pol, p.Par, p.Rec, p.SupCul, p.BAJA
                  FROM MatchingBoletas m LEFT JOIN eepp.DParcela p
                    ON p.Boleta=m.Boleta
                  ORDER BY m.Boleta, p.rowid"""
        params = (member_id, campaign, company, *varieties)
        rows = [dict(r) for r in self.conn.execute(sql, params).fetchall()]
        parcel_rows: dict[int, list[dict]] = {}
        joined_boletas: set[str] = set()
        for row in rows:
            boleta = _display(row.get("DeeppBoleta"))
            row_id = row.get("ParcelaRowId")
            audit.append({"audit_type": "join_row", **row, "parcel_row_id": row_id})
            self._audit("ProductiveSurfaceJoinRow", member_id=member_id, boleta=boleta,
                        parcel_row_id=row_id, parcel_boleta=row.get("ParcelaBoleta"),
                        parcel_campaign=row.get("ParcelaCampana"), parcel_company=row.get("ParcelaEmpresa"),
                        parcel_crop=row.get("ParcelaCultivo"), idpm=row.get("IdPM"), polygon=row.get("Pol"),
                        parcel=row.get("Par"), enclosure=row.get("Rec"), surface=row.get("SupCul"), baja=row.get("BAJA"))
            if row_id is None:
                continue
            joined_boletas.add(boleta)
            parcel_rows.setdefault(int(row_id), []).append(row)

        valid_by_boleta: dict[str, dict[int, Decimal]] = {b: {} for b in matched_boletas}
        invalid_by_boleta: dict[str, set[int]] = {b: set() for b in matched_boletas}
        raw_by_boleta: dict[str, list[object]] = {b: [] for b in matched_boletas}
        duplicate_count = 0
        for row_id, occurrences in parcel_rows.items():
            boleta = _display(occurrences[0].get("DeeppBoleta"))
            raw_values = [r.get("SupCul") for r in occurrences]
            raw_by_boleta.setdefault(boleta, []).append(raw_values[0])
            distinct_raw = {_display(value).strip() for value in raw_values}
            parsed: Decimal | None = None
            decision = "INCLUDED"
            reason = ""
            first = occurrences[0]
            if _norm(first.get("ParcelaCampana")) != _norm(campaign):
                decision, reason = "EXCLUDED", "CAMPAIGN_MISMATCH"
            elif _norm(first.get("ParcelaEmpresa")) != _norm(company):
                decision, reason = "EXCLUDED", "COMPANY_MISMATCH"
            elif first.get("BAJA") is not None and str(first.get("BAJA")).strip():
                decision, reason = "EXCLUDED", "INACTIVE_ROW"
            elif len(distinct_raw) > 1:
                decision = "ROW_ID_CONFLICT"
                reason = "SAME_ROW_ID_WITH_DIFFERENT_JOINED_VALUES"
            else:
                raw = raw_values[0]
                if raw is None or (isinstance(raw, str) and not raw.strip()):
                    decision = "INVALID_NULL"
                    reason = "SUPCUL_NULL"
                else:
                    try:
                        parsed = Decimal(str(raw).strip())
                        if not parsed.is_finite():
                            decision = "INVALID_FORMAT"
                            reason = "SUPCUL_INVALID_FORMAT"
                        elif parsed <= 0:
                            decision = "INVALID_ZERO"
                            reason = "SUPCUL_NOT_POSITIVE"
                    except Exception:
                        decision = "INVALID_FORMAT"
                        reason = "SUPCUL_INVALID_FORMAT"
            if decision == "INCLUDED" and parsed is not None:
                valid_by_boleta.setdefault(boleta, {})[row_id] = parsed
            else:
                invalid_by_boleta.setdefault(boleta, set()).add(row_id)
            for index, occurrence in enumerate(occurrences):
                row_decision = decision if index == 0 else ("DUPLICATE_JOIN_ROW" if decision != "ROW_ID_CONFLICT" else decision)
                row_reason = reason if index == 0 else ("SAME_DPARCELA_ROW_REPEATED_BY_JOIN" if decision != "ROW_ID_CONFLICT" else reason)
                if index:
                    duplicate_count += 1
                item = {"audit_type": "row_decision", "member_id": member_id, "boleta": boleta,
                        "parcel_row_id": row_id, "surface": occurrence.get("SupCul"), "decision": row_decision}
                item["reason"] = row_reason
                item["occurrence_index"] = index
                audit.append(item)
                self._audit("ProductiveSurfaceRowDecision", **item)

        included_boletas: set[str] = set()
        missing_boletas: set[str] = set()
        hectares = Decimal("0")
        for boleta in sorted(matched_boletas):
            valid = valid_by_boleta.get(boleta, {})
            invalid = invalid_by_boleta.get(boleta, set())
            boleta_hectares = sum(valid.values(), Decimal("0"))
            hectares += boleta_hectares
            if valid:
                included_boletas.add(boleta)
            else:
                missing_boletas.add(boleta)
            parcel_ids = sorted(set(valid) | invalid)
            status = "OK" if valid else ("NO_PARCEL_ROWS" if not parcel_ids else "NO_VALID_SURFACE")
            self._audit("ProductiveSurfaceBoletaCalculation", member_id=member_id, boleta=boleta,
                        parcel_row_ids="|".join(map(str, parcel_ids)), valid_row_count=len(valid),
                        invalid_row_count=len(invalid), surface_values="|".join(str(valid[k]) for k in sorted(valid)),
                        boleta_hectares=boleta_hectares, status=status)
            audit.append({"audit_type": "boleta_calculation", "member_id": member_id,
                          "boleta": boleta, "surface_sum": boleta_hectares, "status": status})
            if not parcel_ids:
                incident = "BOLETA_SIN_PARCELAS"
            elif not valid:
                raw = raw_by_boleta.get(boleta, [])
                nulls = [v is None or (isinstance(v, str) and not v.strip()) for v in raw]
                parsed_values: list[Decimal] = []
                formats_invalid = False
                for value in raw:
                    if value is None or (isinstance(value, str) and not value.strip()):
                        continue
                    try:
                        number = Decimal(str(value).strip())
                        formats_invalid |= not number.is_finite()
                        if number.is_finite(): parsed_values.append(number)
                    except Exception:
                        formats_invalid = True
                if raw and all(nulls):
                    incident = "BOLETA_SUPERFICIE_NULA"
                elif parsed_values and len(parsed_values) == len(raw) and all(v <= 0 for v in parsed_values):
                    incident = "BOLETA_SUPERFICIE_CERO"
                else:
                    incident = "BOLETA_SUPERFICIE_INVALIDA"
            else:
                incident = ""
            if incident:
                detail = "La boleta no dispone de una superficie cultivable válida."
                audit.append({"audit_type": "incident", "member_id": member_id, "boleta": boleta,
                              "incident_type": incident, "detail": detail})
                self._audit("ProductiveSurfaceIncident", member_id=member_id, boleta=boleta,
                            incident_type=incident, detail=detail)

        unmatched = candidate_boletas - matched_boletas
        for boleta in sorted(unmatched):
            audit.append({"audit_type": "incident", "member_id": member_id, "boleta": boleta,
                          "incident_type": "VARIEDAD_NO_COINCIDE", "detail": "Ninguna variedad coincide con el grupo."})
        warnings: list[str] = []
        if not included_boletas:
            status = "MISSING_SURFACE_DATA"
            warnings.append(missing_warning)
        elif missing_boletas:
            status = "PARTIAL_SURFACE"
            warnings.append("La superficie productiva está incompleta: existen boletas coincidentes sin superficie cultivable válida.")
        else:
            status = "OK"
        unique_count = len(parcel_rows)
        invalid_count = sum(len(v) for v in invalid_by_boleta.values())
        self._audit("ProductiveSurfaceBoletaSummary", member_id=member_id,
                    candidate_boletas=_set_text(candidate_boletas), matched_deepp_boletas=_set_text(matched_boletas),
                    joined_boletas=_set_text(joined_boletas), included_boletas=_set_text(included_boletas),
                    candidate_without_group_match=_set_text(unmatched), group_match_without_parcels=_set_text(missing_boletas - joined_boletas))
        self._audit("ProductiveSurfaceResult", member_id=member_id, candidate_boletas=_set_text(candidate_boletas),
                    matched_boletas=_set_text(matched_boletas), included_boletas=_set_text(included_boletas),
                    parcel_row_count=unique_count, included_parcel_row_count=unique_count - invalid_count,
                    invalid_parcel_row_count=invalid_count, duplicate_join_row_count=duplicate_count,
                    missing_surface_boletas=_set_text(missing_boletas), hectares=hectares,
                    status=status, warnings="|".join(warnings))
        return ProductiveSurfaceResult(hectares, unique_count - invalid_count, invalid_count, tuple(warnings), tuple(audit),
                                       tuple(sorted(candidate_boletas)), tuple(sorted(matched_boletas)),
                                       tuple(sorted(included_boletas)), unique_count, invalid_count,
                                       tuple(sorted(missing_boletas)), status)
