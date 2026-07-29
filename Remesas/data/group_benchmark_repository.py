from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
import sqlite3


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


def _norm(value: object) -> str:
    return str(value or "").strip().upper()


def _display(value: object) -> str:
    return "" if value is None else str(value)


def _set_text(values: set[str]) -> str:
    return "|".join(sorted(values))


class GroupBenchmarkRepository:
    def __init__(self, conn: sqlite3.Connection, audit_log_path: str | Path = "logs/group_benchmark_surface_audit.log") -> None:
        self.conn = conn
        self.audit_log_path = Path(audit_log_path)

    def _audit(self, section: str, **values: object) -> None:
        self.audit_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.audit_log_path.open("a", encoding="utf-8") as stream:
            stream.write(f"[{section}]\n")
            for key, value in values.items():
                stream.write(f"{key}={_display(value)}\n")
            stream.write("\n")

    def get_varietal_group(self, crop: str, variety: str) -> VarietalGroup | None:
        sql = """SELECT CULTIVO, Variedad, GRUPO, SUBGRUPO FROM eepp.MVariedad
                 WHERE UPPER(TRIM(CULTIVO))=UPPER(TRIM(?)) AND UPPER(TRIM(Variedad))=UPPER(TRIM(?)) LIMIT 1"""
        row = self.conn.execute(sql, (crop, variety)).fetchone()
        if row is None:
            try:
                row = self.conn.execute(sql.replace("eepp.MVariedad", "MVariedad"), (crop, variety)).fetchone()
            except sqlite3.Error:
                row = None
        if row is None:
            return None
        db_crop, group, subgroup = _norm(row["CULTIVO"]) or _norm(crop), _norm(row["GRUPO"]), _norm(row["SUBGRUPO"])
        rows = self.conn.execute(
            """SELECT DISTINCT Variedad FROM eepp.MVariedad
               WHERE UPPER(TRIM(CULTIVO))=UPPER(TRIM(?)) AND UPPER(TRIM(GRUPO))=UPPER(TRIM(?))
                 AND UPPER(TRIM(COALESCE(SUBGRUPO,'')))=UPPER(TRIM(?)) AND TRIM(COALESCE(Variedad,''))<>''
               ORDER BY UPPER(TRIM(Variedad))""", (db_crop, group, subgroup)
        ).fetchall()
        varieties = tuple(_norm(r["Variedad"]) for r in rows if _norm(r["Variedad"]))
        label = " ".join(p for p in (group, subgroup) if p) or _norm(variety)
        return VarietalGroup(db_crop, group, subgroup, label, varieties)

    def get_productive_hectares(self, member_id: int, campaign: str, company: str, crop: str, varieties: tuple[str, ...]) -> ProductiveSurfaceResult:
        normalized_varieties = tuple(_norm(v) for v in varieties)
        normalized_set = set(normalized_varieties)
        parameter_count = 7 + len(varieties)
        self._audit(
            "ProductiveSurfaceQuery", member_id=member_id, campaign=campaign, company=company,
            crop=crop, variety_count=len(varieties), varieties="|".join(map(str, varieties)),
            sql_parameter_count=parameter_count,
        )
        for original, normalized in zip(varieties, normalized_varieties):
            self._audit("ProductiveSurfaceVarieties", original=original, normalized=normalized)

        candidate_sql = """SELECT IdSocio, Boleta, CAMPAÑA, EMPRESA, CULTIVO, Variedad
                           FROM eepp.DEEPP
                           WHERE CAST(IdSocio AS TEXT)=CAST(? AS TEXT)
                             AND UPPER(TRIM(CAMPAÑA))=UPPER(TRIM(?))
                             AND UPPER(TRIM(EMPRESA))=UPPER(TRIM(?))
                             AND UPPER(TRIM(CULTIVO))=UPPER(TRIM(?))
                           ORDER BY Boleta, Variedad"""
        candidates = [dict(r) for r in self.conn.execute(candidate_sql, (member_id, campaign, company, crop)).fetchall()]
        audit: list[dict] = []
        candidate_boletas: set[str] = set()
        matched_boletas: set[str] = set()
        for row in candidates:
            boleta = _display(row.get("Boleta"))
            original = row.get("Variedad")
            normalized = _norm(original)
            matches = normalized in normalized_set
            candidate_boletas.add(boleta)
            if matches:
                matched_boletas.add(boleta)
            item = {"audit_type": "deepp_candidate", **row, "variety_normalized": normalized, "matches_group": matches}
            audit.append(item)
            self._audit("ProductiveSurfaceDeeppCandidate", boleta=boleta, variety_original=original,
                        variety_normalized=normalized, matches_group="yes" if matches else "no")

        if not varieties:
            warnings = ("Grupo sin variedades resueltas.",)
            self._audit("ProductiveSurfaceBoletaSummary", candidate_boletas=_set_text(candidate_boletas),
                        matched_deepp_boletas="", joined_boletas="", included_boletas="",
                        candidate_without_group_match=_set_text(candidate_boletas), group_match_without_parcels="")
            self._audit("ProductiveSurfaceResult", member_id=member_id, row_count=0, dedupe_key_count=0,
                        included_parcel_count=0, excluded_parcel_count=0, included_boleta_count=0,
                        hectares=Decimal("0"), warnings="|".join(warnings))
            return ProductiveSurfaceResult(Decimal("0"), 0, 0, warnings, tuple(audit), tuple(sorted(candidate_boletas)))

        placeholders = ",".join("?" for _ in varieties)
        sql = f"""SELECT
                    e.IdSocio AS DeeppIdSocio, e.Boleta AS DeeppBoleta,
                    e.CAMPAÑA AS DeeppCampana, e.EMPRESA AS DeeppEmpresa,
                    e.CULTIVO AS DeeppCultivo, e.Variedad AS DeeppVariedad,
                    p.Boleta AS ParcelaBoleta, p.CAMPAÑA AS ParcelaCampana,
                    p.EMPRESA AS ParcelaEmpresa, p.CULTIVO AS ParcelaCultivo,
                    p.IdPM, p.Pol, p.Par, p.Rec, p.SupCul, p.BAJA
                  FROM eepp.DEEPP e JOIN eepp.DParcela p ON p.Boleta=e.Boleta
                  WHERE CAST(e.IdSocio AS TEXT)=CAST(? AS TEXT)
                    AND UPPER(TRIM(e.CAMPAÑA))=UPPER(TRIM(?)) AND UPPER(TRIM(e.EMPRESA))=UPPER(TRIM(?))
                    AND UPPER(TRIM(e.CULTIVO))=UPPER(TRIM(?)) AND UPPER(TRIM(e.Variedad)) IN ({placeholders})
                    AND UPPER(TRIM(p.CAMPAÑA))=UPPER(TRIM(?)) AND UPPER(TRIM(p.EMPRESA))=UPPER(TRIM(?))
                    AND UPPER(TRIM(p.CULTIVO))=UPPER(TRIM(?)) AND (p.BAJA IS NULL OR TRIM(p.BAJA)='') AND CAST(p.SupCul AS REAL)>0"""
        params = (member_id, campaign, company, crop, *varieties, campaign, company, crop)
        rows = [dict(r) for r in self.conn.execute(sql, params).fetchall()]
        by_key: dict[tuple[str, ...], set[Decimal]] = {}
        key_boletas: dict[tuple[str, ...], set[str]] = {}
        joined_boletas: set[str] = set()
        for row in rows:
            key = tuple(_norm(row.get(k)) for k in ("ParcelaBoleta", "ParcelaCampana", "ParcelaEmpresa", "ParcelaCultivo", "IdPM", "Pol", "Par", "Rec"))
            surface = Decimal(str(row.get("SupCul") or "0"))
            boleta = _display(row.get("DeeppBoleta"))
            joined_boletas.add(boleta)
            by_key.setdefault(key, set()).add(surface)
            key_boletas.setdefault(key, set()).add(boleta)
            audit.append({"audit_type": "join_row", **row, "dedupe_key": key})
            self._audit(
                "ProductiveSurfaceJoinRow", member_id=member_id, deepp_boleta=boleta,
                deepp_variety=row.get("DeeppVariedad"), parcel_boleta=row.get("ParcelaBoleta"),
                parcel_campaign=row.get("ParcelaCampana"), parcel_company=row.get("ParcelaEmpresa"),
                parcel_crop=row.get("ParcelaCultivo"), idpm=row.get("IdPM"), polygon=row.get("Pol"),
                parcel=row.get("Par"), enclosure=row.get("Rec"), surface=row.get("SupCul"),
                inactive=row.get("BAJA"), dedupe_key="|".join(key),
            )

        hectares = Decimal("0")
        excluded = 0
        warnings: list[str] = []
        included_boletas: set[str] = set()
        for key, surfaces in by_key.items():
            surface_values = "|".join(str(value) for value in sorted(surfaces))
            if len(surfaces) == 1:
                included_surface = next(iter(surfaces))
                hectares += included_surface
                included_boletas.update(key_boletas[key])
                decision = "included"
            else:
                included_surface = ""
                decision = "excluded_conflicting_surfaces"
                excluded += 1
                warnings.append(f"Parcela duplicada con superficies distintas excluida: {key}")
            audit.append({"audit_type": "dedupe", "dedupe_key": key, "surface_values": tuple(sorted(surfaces)),
                          "decision": decision, "included_surface": included_surface})
            self._audit("ProductiveSurfaceDedupe", key="|".join(key), surface_values=surface_values,
                        decision=decision, included_surface=included_surface)

        unmatched = candidate_boletas - matched_boletas
        without_parcels = matched_boletas - joined_boletas
        for boleta in sorted(unmatched):
            audit.append({"audit_type": "incident", "member_id": member_id, "boleta": boleta,
                          "incident_type": "VARIEDAD_NO_COINCIDE", "detail": "Ninguna variedad coincide con el grupo."})
        for boleta in sorted(without_parcels):
            audit.append({"audit_type": "incident", "member_id": member_id, "boleta": boleta,
                          "incident_type": "BOLETA_SIN_PARCELAS", "detail": "La boleta coincidente no devolvió parcelas en el JOIN real."})
        self._audit("ProductiveSurfaceBoletaSummary", candidate_boletas=_set_text(candidate_boletas),
                    matched_deepp_boletas=_set_text(matched_boletas), joined_boletas=_set_text(joined_boletas),
                    included_boletas=_set_text(included_boletas), candidate_without_group_match=_set_text(unmatched),
                    group_match_without_parcels=_set_text(without_parcels))
        self._audit("ProductiveSurfaceResult", member_id=member_id, row_count=len(rows), dedupe_key_count=len(by_key),
                    included_parcel_count=len(by_key) - excluded, excluded_parcel_count=excluded,
                    included_boleta_count=len(included_boletas), hectares=hectares, warnings="|".join(warnings))
        return ProductiveSurfaceResult(hectares, len(by_key) - excluded, excluded, tuple(warnings), tuple(audit),
                                       tuple(sorted(candidate_boletas)), tuple(sorted(matched_boletas)),
                                       tuple(sorted(included_boletas)))
