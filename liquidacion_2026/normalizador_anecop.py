"""Normalización de entrada ANECOP (Excel ECOC o CSV normalizado)."""

from __future__ import annotations

import re
from decimal import Decimal
from pathlib import Path

import pandas as pd

_WEEK = re.compile(r"(\d{1,2})(?:\s*-\s*\d{1,2})?")


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str:
    lookup = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lookup:
            return lookup[cand.lower()]
    for c in df.columns:
        c_low = c.lower()
        if all(piece in c_low for piece in candidates[0].lower().split()):
            return c
    raise ValueError(f"No se encontró columna esperada: {candidates}")


def _parse_week(value: object) -> int | None:
    if value is None:
        return None
    match = _WEEK.search(str(value))
    if not match:
        return None
    return int(match.group(1))


def _num(value: object) -> Decimal:
    if pd.isna(value) or value == "":
        return Decimal("0")
    return Decimal(str(value).replace(",", "."))


def _from_normalized_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"semana", "grupo_anecop", "kg", "valor_fruta"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV ANECOP normalizado incompleto. Faltan columnas: {sorted(missing)}")

    df["semana"] = pd.to_numeric(df["semana"], errors="coerce").astype("Int64")
    if df["semana"].isna().any():
        raise ValueError("CSV normalizado ANECOP contiene semana inválida.")
    return df


def _from_excel(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path)
    semana_col = _find_col(raw, ["semana"])

    groups = {
        "2/3": ("kg", "valor fruta"),
        "4": ("kg", "valor fruta"),
        "5": ("kg", "valor fruta"),
        "6": ("kg", "valor fruta"),
        "7/8": ("kg", "valor fruta"),
        "9/10": ("kg", "valor fruta"),
    }

    rows: list[dict[str, object]] = []
    for _, row in raw.iterrows():
        semana = _parse_week(row.get(semana_col))
        if semana is None:
            continue
        for group in groups:
            kg_col = next((c for c in raw.columns if group in str(c) and "kg" in str(c).lower()), None)
            val_col = next((c for c in raw.columns if group in str(c) and "valor" in str(c).lower()), None)
            if kg_col is None or val_col is None:
                raise ValueError(f"No se encontraron columnas kg/valor fruta para grupo {group} en Excel ANECOP.")
            rows.append(
                {
                    "semana": semana,
                    "grupo_anecop": group,
                    "kg": row.get(kg_col),
                    "valor_fruta": row.get(val_col),
                }
            )

    return pd.DataFrame(rows)


def cargar_anecop(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        df = _from_normalized_csv(path)
    else:
        df = _from_excel(path)

    if df.empty:
        raise ValueError("No se pudieron leer filas válidas de ANECOP.")

    out_rows: list[dict[str, object]] = []
    for semana, sdf in df.groupby("semana"):
        data = {str(r["grupo_anecop"]): (_num(r["kg"]), _num(r["valor_fruta"])) for _, r in sdf.iterrows()}

        p_aaa = data.get("2/3", (Decimal("0"), Decimal("0")))[1]

        kg4, p4 = data.get("4", (Decimal("0"), Decimal("0")))
        kg5, p5 = data.get("5", (Decimal("0"), Decimal("0")))
        sum_aa = kg4 + kg5
        p_aa = Decimal("0") if sum_aa <= 0 else ((kg4 * p4) + (kg5 * p5)) / sum_aa

        kg6, p6 = data.get("6", (Decimal("0"), Decimal("0")))
        kg78, p78 = data.get("7/8", (Decimal("0"), Decimal("0")))
        kg910, p910 = data.get("9/10", (Decimal("0"), Decimal("0")))
        sum_a = kg6 + kg78 + kg910
        p_a = Decimal("0") if sum_a <= 0 else ((kg6 * p6) + (kg78 * p78) + (kg910 * p910)) / sum_a

        out_rows.extend(
            [
                {"semana": int(semana), "grupo": "AAA", "precio_base": p_aaa},
                {"semana": int(semana), "grupo": "AA", "precio_base": p_aa},
                {"semana": int(semana), "grupo": "A", "precio_base": p_a},
            ]
        )

    return pd.DataFrame(out_rows)
