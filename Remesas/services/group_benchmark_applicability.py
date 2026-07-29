from __future__ import annotations

import unicodedata


def normalize_benchmark_crop(crop: object) -> str:
    """Return the canonical crop name used by the varietal benchmark boundary."""
    text = unicodedata.normalize("NFKD", str(crop or ""))
    return " ".join(text.encode("ascii", "ignore").decode().upper().split())


def is_group_benchmark_applicable(
    crop: object,
    document_type: object = None,
    group_label: object = None,
) -> bool:
    """Whether a document participates in the varietal group benchmark.

    Crop and document type are audit attributes, never population boundaries.
    """
    del crop, document_type
    return group_label is None or bool(str(group_label).strip())
