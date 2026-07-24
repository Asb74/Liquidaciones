from __future__ import annotations

import logging
from collections.abc import Iterable

from data.variety_repository import VarietyRepository
from domain.varieties import (
    STATUS_EMPTY_GROUP,
    STATUS_AMBIGUOUS,
    STATUS_GROUP,
    STATUS_NOT_FOUND,
    STATUS_VARIETY,
    VarietySelectionResolution,
    normalize_variety_text,
)
from services.variety_selection_resolver import VarietySelectionKind, VarietySelectionResolver

logger = logging.getLogger(__name__)


class VarietyGroupService:
    def __init__(self, repository: VarietyRepository) -> None:
        self.repository = repository
        self.resolver = VarietySelectionResolver(repository)

    def list_selection_options(self, crop: str) -> tuple[str, ...]:
        master_crops = self.resolver.candidate_master_crops(crop)
        groups = [g.label for master_crop in master_crops for g in self.repository.list_groups(master_crop)]
        varieties = [variety for master_crop in master_crops for variety in self.repository.list_varieties(master_crop)]
        if not groups and not varieties:
            logger.warning("[Variedades] cultivo=%s master_crops=%s sin opciones en MVariedad; se conserva fallback de variedades de entregas", crop, master_crops)
        options: list[str] = []
        seen: set[str] = set()
        for option in [*groups, *varieties]:
            key = normalize_variety_text(option)
            if key not in seen:
                seen.add(key)
                options.append(option)
        return tuple(options)

    def resolve_selection(self, crop: str, value: str) -> VarietySelectionResolution:
        resolved = self.resolver.resolve(crop, value)
        status = STATUS_VARIETY if resolved.kind == VarietySelectionKind.VARIETY else STATUS_GROUP if resolved.kind == VarietySelectionKind.GROUP else STATUS_AMBIGUOUS if resolved.kind == VarietySelectionKind.AMBIGUOUS else STATUS_EMPTY_GROUP if resolved.group and resolved.subgroup else STATUS_NOT_FOUND
        is_group = resolved.kind == VarietySelectionKind.GROUP
        return VarietySelectionResolution(
            resolved.source_value,
            resolved.normalized_value,
            is_group,
            resolved.group,
            resolved.subgroup,
            resolved.selected_varieties,
            status,
            resolved.warnings,
            resolved.source_crop,
            resolved.resolved_master_crop,
            resolved.candidate_master_crops,
        )

    def resolve_many(self, crop: str, values: Iterable[str]) -> tuple[tuple[VarietySelectionResolution, ...], tuple[str, ...]]:
        resolutions = tuple(self.resolve_selection(crop, value) for value in values if str(value or "").strip())
        varieties: list[str] = []
        seen: set[str] = set()
        for res in resolutions:
            if res.status in {STATUS_NOT_FOUND, STATUS_EMPTY_GROUP, STATUS_AMBIGUOUS}:
                continue
            for variety in res.varieties:
                key = normalize_variety_text(variety)
                if key not in seen:
                    seen.add(key)
                    varieties.append(variety)
        return resolutions, tuple(varieties)

    @staticmethod
    def validate_resolved_varieties(resolutions: Iterable[VarietySelectionResolution]) -> tuple[str, ...]:
        """Return resolution errors shared by individual and batch calculation flows."""
        return tuple(
            warning or f"No se pudo resolver la variedad o grupo “{resolution.source_value}”."
            for resolution in resolutions
            if resolution.status in {STATUS_NOT_FOUND, STATUS_EMPTY_GROUP, STATUS_AMBIGUOUS}
            for warning in (resolution.warnings[:1] or ("",))
        )
