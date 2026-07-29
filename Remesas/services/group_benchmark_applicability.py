from __future__ import annotations

import unicodedata


BENCHMARK_APPLICABLE_CROPS = frozenset({"CITRICOS", "MANDARINA"})
CROP_NOT_INCLUDED_REASON = "CROP_NOT_INCLUDED_IN_GROUP_BENCHMARK"


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

    ``document_type`` and ``group_label`` are deliberately accepted so every caller
    uses this single business-rule boundary if applicability later becomes finer
    grained.  At present the benchmark population is defined exclusively by crop.
    """
    del document_type, group_label
    return normalize_benchmark_crop(crop) in BENCHMARK_APPLICABLE_CROPS
