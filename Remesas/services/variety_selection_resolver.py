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
    AMBIGUOUS = "AMBIGUOUS"


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
    source_crop: str = ""
    resolved_master_crop: str | None = None
    candidate_master_crops: tuple[str, ...] = ()


class VarietySelectionResolver:
    def __init__(self, repository: VarietyRepository, *, aliases_path: Path | None = None, resolution_path: Path | None = None, log_path: Path | None = None) -> None:
        self.repository = repository
        root = Path(__file__).resolve().parents[1]
        self.aliases_path = aliases_path or root / "config" / "crop_aliases.json"
        self.resolution_path = resolution_path or root / "config" / "crop_resolution.json"
        self.log_path = log_path or root / "logs" / "variety_resolution.log"
        self.crop_aliases = self._load_aliases()
        self.mixed_output_crops = self._load_mixed_output_crops()
        self.logger = logging.getLogger(__name__)

    def resolve(self, crop: str, value: str) -> ResolvedVarietySelection:
        source_crop = normalize_variety_token(crop)
        candidates = self.candidate_master_crops(source_crop)
        source = str(value or "").strip()
        normalized = normalize_variety_token(source)
        exact_matches = self.repository.find_exact_varieties(candidates, normalized)
        group_matches = () if exact_matches else self.repository.find_groups_by_label(candidates, normalized)

        if len(exact_matches) == 1:
            exact = exact_matches[0]
            warnings = ("Ambiguous value resolved as exact variety.",) if self.repository.find_groups_by_label(candidates, normalized) else ()
            result = ResolvedVarietySelection(source, normalized, VarietySelectionKind.VARIETY, (exact.variety,), label=exact.variety, warnings=warnings, source_crop=source_crop, resolved_master_crop=exact.crop, candidate_master_crops=candidates)
        elif len(exact_matches) > 1:
            result = self._ambiguous(source_crop, source, normalized, candidates, tuple(match.crop for match in exact_matches))
        elif len(group_matches) == 1:
            group = group_matches[0]
            varieties = self.repository.list_group_varieties(group.crop, group.group, group.subgroup)
            if varieties:
                result = ResolvedVarietySelection(source, normalized, VarietySelectionKind.GROUP, varieties, group.group, group.subgroup, group.label, (), source_crop, group.crop, candidates)
            else:
                warning = f"El grupo varietal '{source}' no contiene variedades activas en MVariedad."
                result = ResolvedVarietySelection(source, normalized, VarietySelectionKind.NOT_FOUND, (), group.group, group.subgroup, group.label, (warning,), source_crop, group.crop, candidates)
        elif len(group_matches) > 1:
            result = self._ambiguous(source_crop, source, normalized, candidates, tuple(group.crop for group in group_matches))
        else:
            warning = f"No se pudo resolver la variedad o grupo “{source}”."
            result = ResolvedVarietySelection(source, normalized, VarietySelectionKind.NOT_FOUND, (), warnings=(warning,), source_crop=source_crop, candidate_master_crops=candidates)

        self._log_resolution(result, exact_matches, group_matches)
        return result

    def candidate_master_crops(self, source_crop: str) -> tuple[str, ...]:
        normalized = normalize_variety_token(source_crop)
        return self.mixed_output_crops.get(normalized, (self.crop_aliases.get(normalized, normalized),))

    def master_crop(self, crop: str) -> str:
        """Compatibility accessor for callers that need a single unambiguous master."""
        return self.candidate_master_crops(crop)[0]

    def _ambiguous(self, source_crop, source, normalized, candidates, matching_crops):
        crops = tuple(dict.fromkeys(matching_crops))
        warning = f"La variedad o grupo ‘{source}’ existe en varios cultivos maestros: {', '.join(crops)}."
        return ResolvedVarietySelection(source, normalized, VarietySelectionKind.AMBIGUOUS, (), warnings=(warning,), source_crop=source_crop, candidate_master_crops=candidates)

    def _load_aliases(self) -> dict[str, str]:
        if not self.aliases_path.exists():
            return {}
        with self.aliases_path.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return {normalize_variety_token(k): normalize_variety_token(v) for k, v in raw.items()}

    def _load_mixed_output_crops(self) -> dict[str, tuple[str, ...]]:
        if not self.resolution_path.exists():
            return {}
        with self.resolution_path.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return {normalize_variety_token(crop): tuple(dict.fromkeys(normalize_variety_token(item) for item in masters)) for crop, masters in raw.get("mixed_output_crops", {}).items()}

    def _log_resolution(self, result, exact_matches, group_matches) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8") as log:
            log.write("[VarietyResolution]\n")
            log.write(f"source_crop={result.source_crop}\ncandidate_master_crops={','.join(result.candidate_master_crops)}\nresolved_master_crop={result.resolved_master_crop or ''}\n")
            log.write(f"source_value={result.source_value}\nnormalized_value={result.normalized_value}\n")
            log.write(f"exact_matches={','.join(f'{match.crop}:{match.variety}' for match in exact_matches)}\ngroup_matches={','.join(f'{group.crop}:{group.label}' for group in group_matches)}\n")
            log.write(f"kind={result.kind.value}\ngroup={result.group or ''}\nsubgroup={result.subgroup or ''}\n")
            log.write(f"selected_count={len(result.selected_varieties)}\nselected_varieties={','.join(result.selected_varieties)}\nwarnings={';'.join(result.warnings)}\n\n")
