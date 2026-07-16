from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from data.variety_repository import VarietyRepository


class VarietySelectionKind(str, Enum):
    VARIETY = "VARIETY"
    GROUP = "GROUP"
    NOT_FOUND = "NOT_FOUND"


def normalize_variety_token(value: str) -> str:
    return re.sub(r"\s+", " ", "" if value is None else str(value).strip()).upper()


@dataclass(frozen=True)
class ResolvedVarietySelection:
    source_value: str
    normalized_value: str
    kind: VarietySelectionKind
    selected_varieties: tuple[str, ...]
    group: str | None = None
    subgroup: str | None = None
    label: str | None = None
    warnings: tuple[str, ...] = ()


class VarietySelectionResolver:
    def __init__(self, repository: VarietyRepository, *, aliases_path: Path | None = None, log_path: Path | None = None) -> None:
        self.repository = repository
        root = Path(__file__).resolve().parents[1]
        self.aliases_path = aliases_path or root / "config" / "crop_aliases.json"
        self.log_path = log_path or root / "logs" / "variety_resolution.log"
        self.crop_aliases = self._load_aliases()
        self.logger = logging.getLogger(__name__)

    def resolve(self, crop: str, value: str) -> ResolvedVarietySelection:
        source_crop = str(crop or "").strip()
        master_crop = self.master_crop(source_crop)
        source = str(value or "").strip()
        normalized = normalize_variety_token(source)
        warnings: list[str] = []

        exact = self.repository.find_exact_variety(master_crop, normalized)
        group_search_performed = False
        group = self.repository.find_group_by_label(master_crop, normalized) if exact is None else None
        if exact:
            ambiguous_group = self.repository.find_group_by_label(master_crop, normalized)
            if ambiguous_group:
                warnings.append("Ambiguous value resolved as exact variety.")
            result = ResolvedVarietySelection(source, normalized, VarietySelectionKind.VARIETY, (exact,), label=exact, warnings=tuple(warnings))
        elif group:
            group_search_performed = True
            varieties = self.repository.list_group_varieties(master_crop, group.group, group.subgroup)
            if not varieties:
                warnings.append(f"El grupo varietal '{source}' no contiene variedades activas en MVariedad.")
                result = ResolvedVarietySelection(source, normalized, VarietySelectionKind.NOT_FOUND, (), group.group, group.subgroup, group.label, tuple(warnings))
            else:
                result = ResolvedVarietySelection(source, normalized, VarietySelectionKind.GROUP, varieties, group.group, group.subgroup, group.label, tuple(warnings))
        else:
            group_search_performed = True
            warnings.append(f"No se pudo resolver la variedad o grupo “{source}”.")
            result = ResolvedVarietySelection(source, normalized, VarietySelectionKind.NOT_FOUND, (), warnings=tuple(warnings))

        self._log_resolution(source_crop, master_crop, result, exact is not None, group_search_performed)
        return result

    def master_crop(self, crop: str) -> str:
        normalized = normalize_variety_token(crop)
        return self.crop_aliases.get(normalized, normalized)

    def _load_aliases(self) -> dict[str, str]:
        if not self.aliases_path.exists():
            return {}
        with self.aliases_path.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return {normalize_variety_token(k): normalize_variety_token(v) for k, v in raw.items()}

    def _log_resolution(self, source_crop: str, master_crop: str, result: ResolvedVarietySelection, exact_found: bool, group_search_performed: bool) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8") as log:
            log.write("[VarietyResolution]\n")
            log.write(f"source_crop={source_crop}\nmaster_crop={master_crop}\nsource_value={result.source_value}\nnormalized_value={result.normalized_value}\n")
            log.write(f"exact_variety_found={str(exact_found).lower()}\ngroup_search_performed={str(group_search_performed).lower()}\n")
            log.write(f"kind={result.kind.value}\ngroup={result.group or ''}\nsubgroup={result.subgroup or ''}\n")
            log.write(f"selected_count={len(result.selected_varieties)}\nselected_varieties={','.join(result.selected_varieties)}\nwarnings={';'.join(result.warnings)}\n\n")
