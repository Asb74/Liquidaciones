from __future__ import annotations

from decimal import Decimal
import logging
import sqlite3
from typing import Sequence

from domain.financial_rules import effective_net_kg
from domain.utils import decimal_or_zero


class HectareRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.logger = logging.getLogger(__name__)

    def calculate_applicable_hectares(self, member_id: int, campaign: int | str, company: int | str, applicable_crops: Sequence[str]) -> tuple[Decimal, tuple[str, ...]]:
        placeholders = ",".join("?" for _ in applicable_crops)
        sql = f"SELECT Boleta, IdPM, Pol, Par, Recinto, CULTIVO, CAMPAÑA, EMPRESA, SupCul FROM eepp.DEEPP WHERE IdSocio=? AND CAMPAÑA=? AND EMPRESA=? AND UPPER(TRIM(CULTIVO)) IN ({placeholders})"
        rows = self.conn.execute(sql, [member_id, str(campaign), str(company), *applicable_crops]).fetchall()
        if not rows:
            rows = self.conn.execute(sql, [member_id, campaign, company, *applicable_crops]).fetchall()
        seen: dict[tuple, Decimal] = {}
        warnings: list[str] = []
        for r in rows:
            boleta = str(r[0] or "").strip()
            key = ("Boleta", boleta) if boleta else ("Parcela", member_id, r[1], r[2], r[3], r[4], str(r[5] or "").strip().upper(), r[6], r[7])
            sup = decimal_or_zero(r[8])
            if key in seen:
                if seen[key] != sup:
                    warnings.append(f"Superficie duplicada conflictiva socio={member_id} clave={key}: {seen[key]} vs {sup}; se conserva la primera.")
                continue
            seen[key] = sup
        return sum(seen.values(), Decimal("0")), tuple(warnings)

    def total_effective_kg(self, member_id: int, campaign: int | str, company: int | str, applicable_crops: Sequence[str]) -> Decimal:
        placeholders = ",".join("?" for _ in applicable_crops)
        sql = f"SELECT Neto, NetoPartida FROM PesosFres WHERE IdSocio=? AND CAMPAÑA=? AND EMPRESA=? AND UPPER(TRIM(CULTIVO)) IN ({placeholders})"
        rows = self.conn.execute(sql, [member_id, str(campaign), str(company), *applicable_crops]).fetchall()
        if not rows:
            rows = self.conn.execute(sql, [member_id, campaign, company, *applicable_crops]).fetchall()
        return sum((effective_net_kg(r[0], r[1]) for r in rows), Decimal("0"))
