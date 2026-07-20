"""Text normalization shared by persisted-data searches."""
from __future__ import annotations

import unicodedata


def normalize_search_text(text) -> str:
    """Return uppercase, accent-insensitive text with normalized whitespace."""
    decomposed = unicodedata.normalize("NFD", str(text or "").upper())
    without_marks = "".join(char for char in decomposed if unicodedata.category(char) != "Mn")
    return " ".join(without_marks.split())
