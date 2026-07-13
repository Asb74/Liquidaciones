from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import logging
import sqlite3

from domain.utils import decimal_or_zero


@dataclass(frozen=True)
class QualityRateResult:
    rate: Decimal
    source: str
    warnings: tuple[str, ...]
    concept_id: int | None = None


class QualityRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.logger = logging.getLogger(__name__)

    def get_quality_rate(self, member_id: int, campaign: int | str, company: int | str, crop: str, remittance_id: int | str) -> QualityRateResult:
        warnings: list[str] = []
        for concept_id, source in ((remittance_id, "specific"), (0, "general")):
            rows = self.conn.execute(
                'SELECT IdConcepto, "Bon/Pen" FROM BonCalidad WHERE IdSocio=? AND CAMPAÑA=? AND EMPRESA=? AND CULTIVO=? AND COALESCE(IdConcepto,0)=?',
                (member_id, str(campaign), str(company), crop, int(concept_id or 0)),
            ).fetchall()
            if not rows:
                rows = self.conn.execute(
                    'SELECT IdConcepto, "Bon/Pen" FROM BonCalidad WHERE IdSocio=? AND CAMPAÑA=? AND EMPRESA=? AND CULTIVO=? AND COALESCE(IdConcepto,0)=?',
                    (member_id, campaign, company, crop, int(concept_id or 0)),
                ).fetchall()
            if len(rows) > 1:
                msg = f"Varias tarifas BonCalidad para socio={member_id} IdConcepto={int(concept_id or 0)}; se usa la primera por IdConcepto, Bon/Pen."
                warnings.append(msg)
                self.logger.warning(msg)
            if rows:
                row = sorted(rows, key=lambda r: (str(r[0]), str(r[1])))[0]
                return QualityRateResult(decimal_or_zero(row[1]), source, tuple(warnings), int(row[0] or 0))
        warnings.append(f"No existe tarifa BonCalidad para socio={member_id} campaña={campaign} empresa={company} cultivo={crop}.")
        return QualityRateResult(Decimal("0"), "not_found", tuple(warnings), None)
