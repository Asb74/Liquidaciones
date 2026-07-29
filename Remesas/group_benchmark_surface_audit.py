"""Central writer and project-relative path for varietal surface auditing."""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Mapping


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent
AUDIT_LOG_PATH = PROJECT_ROOT / "logs" / "group_benchmark_surface_audit.log"


def _audit_value(value: object) -> str:
    if value is None:
        return "None"
    if isinstance(value, (list, tuple, set, frozenset)):
        items = sorted(value, key=str) if isinstance(value, (set, frozenset)) else value
        return "|".join(str(item) for item in items)
    return str(value)


def append_surface_audit(
    section: str,
    values: Mapping[str, object],
    path: str | Path = AUDIT_LOG_PATH,
) -> None:
    """Append one readable audit block, reporting and propagating write failures."""
    target = Path(path).resolve()
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as stream:
            stream.write(f"[{section}]\n")
            for key, value in values.items():
                stream.write(f"{key}={_audit_value(value)}\n")
            stream.write("\n")
    except Exception:
        LOGGER.exception("No se pudo escribir la auditoría de superficie en %s", target)
        raise


def record_surface_audit_config(path: str | Path = AUDIT_LOG_PATH) -> Path:
    target = Path(path).resolve()
    existed = target.exists()
    append_surface_audit(
        "SurfaceAuditConfig",
        {
            "resolved_path": target,
            "exists_before": existed,
            "working_directory": os.getcwd(),
        },
        target,
    )
    return target
