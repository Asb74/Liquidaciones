"""Focused forensic trace from benchmark arithmetic through PDF rendering."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

TRACE_PATH = Path(__file__).resolve().parent.parent.parent / "logs" / "pdf_member_1623_value_trace.log"


def traced_member(member_id: object) -> bool:
    configured = os.getenv("PDF_BENCHMARK_TRACE_MEMBERS", "1623")
    return str(member_id) in {item.strip() for item in configured.split(",")}


def write_value_trace(section: str, values: Mapping[str, object], *, member_id=None, path=TRACE_PATH) -> None:
    if member_id is not None and not traced_member(member_id):
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as stream:
        stream.write(f"[{section}]\n")
        for key, value in values.items():
            if isinstance(value, (tuple, list, set, frozenset)):
                value = "|".join(str(item) for item in value)
            stream.write(f"{key}={'' if value is None else value}\n")
        stream.write("\n")
