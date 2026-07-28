from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
import sqlite3

@dataclass(frozen=True)
class VarietalGroup:
    crop: str; group: str; subgroup: str; label: str; varieties: tuple[str, ...]

@dataclass(frozen=True)
class ProductiveSurfaceResult:
    hectares: Decimal; parcel_count: int; excluded_count: int; warnings: tuple[str, ...]; audit_rows: tuple[dict, ...]

def _norm(value: object) -> str:
    return str(value or "").strip().upper()

class GroupBenchmarkRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

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
        if not varieties:
            return ProductiveSurfaceResult(Decimal("0"), 0, 0, ("Grupo sin variedades resueltas.",), ())
        placeholders = ",".join("?" for _ in varieties)
        sql = f"""SELECT p.Boleta,p.CAMPAÑA,p.EMPRESA,p.CULTIVO,p.IdPM,p.Pol,p.Par,p.Rec,p.SupCul
                  FROM eepp.DEEPP e JOIN eepp.DParcela p ON p.Boleta=e.Boleta
                  WHERE CAST(e.IdSocio AS TEXT)=CAST(? AS TEXT)
                    AND UPPER(TRIM(e.CAMPAÑA))=UPPER(TRIM(?)) AND UPPER(TRIM(e.EMPRESA))=UPPER(TRIM(?))
                    AND UPPER(TRIM(e.CULTIVO))=UPPER(TRIM(?)) AND UPPER(TRIM(e.Variedad)) IN ({placeholders})
                    AND UPPER(TRIM(p.CAMPAÑA))=UPPER(TRIM(?)) AND UPPER(TRIM(p.EMPRESA))=UPPER(TRIM(?))
                    AND UPPER(TRIM(p.CULTIVO))=UPPER(TRIM(?)) AND (p.BAJA IS NULL OR TRIM(p.BAJA)='') AND CAST(p.SupCul AS REAL)>0"""
        rows = [dict(r) for r in self.conn.execute(sql, (member_id, campaign, company, crop, *varieties, campaign, company, crop)).fetchall()]
        by_key: dict[tuple, set[Decimal]] = {}; audit=[]
        for r in rows:
            key = tuple(_norm(r.get(k)) for k in ("Boleta","CAMPAÑA","EMPRESA","CULTIVO","IdPM","Pol","Par","Rec"))
            sup = Decimal(str(r.get("SupCul") or "0")); by_key.setdefault(key, set()).add(sup); audit.append({**r, "dedupe_key": key})
        hectares=Decimal("0"); excluded=0; warnings=[]
        for key, surfaces in by_key.items():
            if len(surfaces) == 1: hectares += next(iter(surfaces))
            else: excluded += 1; warnings.append(f"Parcela duplicada con superficies distintas excluida: {key}")
        return ProductiveSurfaceResult(hectares, len(by_key)-excluded, excluded, tuple(warnings), tuple(audit))
