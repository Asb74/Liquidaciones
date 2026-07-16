from __future__ import annotations

import logging
from collections.abc import Iterable

from data.variety_repository import VarietyRepository
from domain.varieties import (
    STATUS_EMPTY_GROUP,
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
        master_crop = self.resolver.master_crop(crop)
        groups = [g.label for g in self.repository.list_groups(master_crop)]
        varieties = list(self.repository.list_varieties(master_crop))
        if not groups and not varieties:
            logger.warning("[Variedades] cultivo=%s master_crop=%s sin opciones en MVariedad; se conserva fallback de variedades de entregas", crop, master_crop)
        return tuple(dict.fromkeys([*groups, *varieties]))

    def resolve_selection(self, crop: str, value: str) -> VarietySelectionResolution:
        resolved = self.resolver.resolve(crop, value)
        status = STATUS_VARIETY if resolved.kind == VarietySelectionKind.VARIETY else STATUS_GROUP if resolved.kind == VarietySelectionKind.GROUP else STATUS_EMPTY_GROUP if resolved.group and resolved.subgroup else STATUS_NOT_FOUND
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
        )

    def resolve_many(self, crop: str, values: Iterable[str]) -> tuple[tuple[VarietySelectionResolution, ...], tuple[str, ...]]:
        resolutions = tuple(self.resolve_selection(crop, value) for value in values if str(value or "").strip())
        varieties: list[str] = []
        seen: set[str] = set()
        for res in resolutions:
            if res.status in {STATUS_NOT_FOUND, STATUS_EMPTY_GROUP}:
                continue
            for variety in res.varieties:
                key = normalize_variety_text(variety)
                if key not in seen:
                    seen.add(key)
                    varieties.append(variety)
        return resolutions, tuple(varieties)
