"""Detailed, calculation-backed diagnostics for selected benchmark members."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


class MemberSurfaceDiagnosticService:
    """Write the exact inputs and decisions used by the production benchmark."""

    def __init__(self, logs_dir: str | Path) -> None:
        self.logs_dir = Path(logs_dir)

    @staticmethod
    def _text(value) -> str:
        return "" if value is None else str(value)

    def write(self, *, member_id, group, campaign, net_kg: Decimal, surface) -> Path:
        target = self.logs_dir / f"member_{member_id}_surface_diagnostic.log"
        target.parent.mkdir(parents=True, exist_ok=True)
        kg_ha = net_kg / surface.hectares if surface.hectares > 0 else None
        lines = [
            "[production]", f"member_id={member_id}", f"group={group}",
            f"campaign={campaign}", f"net_kg={net_kg}",
            f"expected_surface={surface.hectares}",
            f"expected_kg_ha={kg_ha.quantize(Decimal('0.01'), ROUND_HALF_UP) if kg_ha is not None else ''}", "",
            "[candidate_boletas]",
        ]
        for row in surface.audit_rows:
            if row.get("audit_type") == "deepp_candidate":
                lines.extend((f"boleta_id={self._text(row.get('Boleta'))}",
                              f"variety_original={self._text(row.get('Variedad'))}",
                              f"variety_normalized={self._text(row.get('variety_normalized'))}",
                              f"matches_group={'yes' if row.get('matches_group') else 'no'}", ""))
        lines.append("[dparcela_rows_and_decisions]")
        decisions = {(r.get("parcel_row_id"), r.get("occurrence_index", 0)): r
                     for r in surface.audit_rows if r.get("audit_type") == "row_decision"}
        occurrences: dict[object, int] = {}
        for row in surface.audit_rows:
            if row.get("audit_type") != "join_row" or row.get("parcel_row_id") is None:
                continue
            row_id = row.get("parcel_row_id")
            index = occurrences.get(row_id, 0)
            occurrences[row_id] = index + 1
            decision = decisions.get((row_id, index), {})
            raw_decision = self._text(decision.get("decision")).upper()
            included = raw_decision == "INCLUDED"
            lines.extend((f"dparcela_row_id={row_id}", f"boleta={self._text(row.get('ParcelaBoleta'))}",
                          f"campaign={self._text(row.get('ParcelaCampana'))}",
                          f"company={self._text(row.get('ParcelaEmpresa'))}",
                          f"crop={self._text(row.get('ParcelaCultivo'))}", f"idpm={self._text(row.get('IdPM'))}",
                          f"polygon={self._text(row.get('Pol'))}", f"parcel={self._text(row.get('Par'))}",
                          f"enclosure={self._text(row.get('Rec'))}", f"supcul={self._text(row.get('SupCul'))}",
                          f"baja={self._text(row.get('BAJA'))}",
                          f"decision={'included' if included else 'excluded'}",
                          f"reason={self._text(decision.get('reason')) if not included else ''}", ""))
        lines.append("[surface_by_boleta]")
        for row in surface.audit_rows:
            if row.get("audit_type") == "boleta_calculation":
                lines.append(f"boleta={row.get('boleta')}")
                lines.append(f"surface_sum={row.get('surface_sum')}")
        lines.extend(("", "[result]", f"total_surface={surface.hectares}", f"net_kg={net_kg}",
                      f"kg_per_ha={kg_ha.quantize(Decimal('0.01'), ROUND_HALF_UP) if kg_ha is not None else ''}", ""))
        target.write_text("\n".join(lines), encoding="utf-8")
        return target
