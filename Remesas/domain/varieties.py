from __future__ import annotations

from dataclasses import dataclass
import re

STATUS_VARIETY = "VARIETY"
STATUS_GROUP = "GROUP"
STATUS_NOT_FOUND = "NOT_FOUND"
STATUS_EMPTY_GROUP = "EMPTY_GROUP"
STATUS_AMBIGUOUS = "AMBIGUOUS"


def normalize_variety_text(value: object) -> str:
    return re.sub(r"\s+", " ", "" if value is None else str(value).strip()).upper()


@dataclass(frozen=True)
class VarietyGroup:
    crop: str
    group: str
    subgroup: str

    @property
    def label(self) -> str:
        return f"{self.group} {self.subgroup}".strip()


@dataclass(frozen=True)
class VarietySelectionResolution:
    source_value: str
    normalized_value: str
    is_group: bool
    group: str | None
    subgroup: str | None
    varieties: tuple[str, ...]
    status: str
    warnings: tuple[str, ...] = ()
