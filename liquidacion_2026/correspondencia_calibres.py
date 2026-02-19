"""Mapeo Cal0..Cal11 a grupo/categoría comercial."""

from __future__ import annotations

import re

import pandas as pd

from .config import CALIBRES, GRUPOS_COMERCIALES

_PATTERN = re.compile(r"^(AAA|AA|A)\s*(1[ªA]|2[ªA])$", re.IGNORECASE)


def build_calibre_mapping(correspondencias_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    normalized = correspondencias_df.copy()
    normalized["BASE"] = normalized["BASE"].astype(str).str.strip().str.lower()
    normalized["KAKIS"] = normalized["KAKIS"].astype(str).str.strip().str.upper()

    by_base = {row.BASE: row.KAKIS for row in normalized.itertuples(index=False)}

    for idx, cal in enumerate(CALIBRES):
        token = by_base.get(f"c{idx}", "")
        match = _PATTERN.match(token)
        if not match:
            continue
        grupo = match.group(1).upper()
        if grupo not in GRUPOS_COMERCIALES:
            continue
        categoria = "I" if match.group(2).startswith("1") else "II"
        rows.append({"calibre": cal, "grupo": grupo, "categoria": categoria})

    mapped = pd.DataFrame(rows)
    if mapped.empty:
        raise ValueError("No se pudo construir mapping comercial AAA/AA/A con categorías I/II.")
    return mapped
