"""Read-only report of positive active liquidation scope conflicts."""
from __future__ import annotations

import argparse
import json

from data.persistence.database import PersistenceDatabase
from data.persistence.liquidation_repository import LiquidationRepository


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnostica liquidaciones activas duplicadas sin modificarlas")
    parser.add_argument("database", help="Ruta de la SQLite local")
    args = parser.parse_args()
    database = PersistenceDatabase(args.database)
    # Do not initialize here: a database with known conflicts deliberately
    # cannot install the partial unique index until it has been reviewed.
    repository = LiquidationRepository(database)
    report = []
    for conflict in repository.list_duplicate_active_scopes():
        item = dict(conflict)
        item["batches"] = []
        for batch_id in item.pop("batch_ids").split(","):
            batch = repository.get_batch(batch_id)
            lines = repository.list_batch_liquidations(batch_id)
            exports = repository.list_accounting_exports_for_batch(batch_id)
            item["batches"].append({
                "batch_id": batch_id,
                "created_at": batch["created_at"],
                "created_by": batch["created_by"],
                "operation_type": batch["operation_type"],
                "line_count": len(lines),
                "article_codes": sorted({row["cod_art"] for row in lines if row["cod_art"]}),
                "quality_amounts": [row["bp_calidad"] for row in lines],
                "total_amounts": [row["importe_total"] for row in lines],
                "accounting_export_ids": [row["id"] for row in exports],
            })
        exported = sum(bool(batch["accounting_export_ids"]) for batch in item["batches"])
        item["recommendation"] = ("INCIDENCIA_CONTABLE_GRAVE" if exported > 1 else
                                  "REVISAR_RECTIFICACION" if exported == 1 else
                                  "ELEGIR_VERSION_VIGENTE")
        report.append(item)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if report else 0


if __name__ == "__main__":
    raise SystemExit(main())
