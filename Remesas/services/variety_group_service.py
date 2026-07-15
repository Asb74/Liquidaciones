from __future__ import annotations

import logging
from collections.abc import Iterable

from data.variety_repository import VarietyRepository
from domain.varieties import STATUS_AMBIGUOUS, STATUS_EMPTY_GROUP, STATUS_GROUP, STATUS_NOT_FOUND, STATUS_VARIETY, VarietyGroup, VarietySelectionResolution, normalize_variety_text

logger = logging.getLogger(__name__)


class VarietyGroupService:
    def __init__(self, repository: VarietyRepository) -> None:
        self.repository = repository

    def list_selection_options(self, crop: str) -> tuple[str, ...]:
        groups = [g.label for g in self.repository.list_groups(crop)]
        varieties = list(self.repository.list_varieties(crop))
        if not groups and not varieties:
            logger.warning("[Variedades] cultivo=%s sin opciones en MVariedad; se conserva fallback de variedades de entregas", crop)
        return tuple(dict.fromkeys([*groups, *varieties]))

    def resolve_selection(self, crop: str, value: str) -> VarietySelectionResolution:
        source = str(value or "").strip()
        normalized = normalize_variety_text(source)
        variety = self.repository.find_variety(crop, source)
        group = self._find_group(crop, source)
        if variety and group:
            msg = f"El valor '{source}' existe como variedad y como grupo varietal. Revise MVariedad."
            res = VarietySelectionResolution(source, normalized, False, group.group, group.subgroup, (), STATUS_AMBIGUOUS, (msg,))
        elif variety:
            res = VarietySelectionResolution(source, normalized, False, None, None, (variety,), STATUS_VARIETY)
        elif group:
            varieties = self.repository.resolve_group(crop, group.group, group.subgroup)
            status = STATUS_GROUP if varieties else STATUS_EMPTY_GROUP
            warnings = () if varieties else (f"El grupo varietal '{source}' no contiene variedades activas en MVariedad.",)
            res = VarietySelectionResolution(source, normalized, True, group.group, group.subgroup, varieties, status, warnings)
        else:
            res = VarietySelectionResolution(source, normalized, False, None, None, (), STATUS_NOT_FOUND, (f"Variedad o grupo no encontrado en MVariedad: {source}",))
        self.log_resolution(crop, res)
        return res

    def resolve_many(self, crop: str, values: Iterable[str]) -> tuple[tuple[VarietySelectionResolution, ...], tuple[str, ...]]:
        resolutions = tuple(self.resolve_selection(crop, value) for value in values if str(value or "").strip())
        varieties: list[str] = []
        seen: set[str] = set()
        for res in resolutions:
            if res.status in {STATUS_AMBIGUOUS, STATUS_NOT_FOUND, STATUS_EMPTY_GROUP}:
                continue
            for variety in res.varieties:
                key = normalize_variety_text(variety)
                if key not in seen:
                    seen.add(key); varieties.append(variety)
        return resolutions, tuple(varieties)

    def _find_group(self, crop: str, value: str) -> VarietyGroup | None:
        wanted = normalize_variety_text(value)
        for group in self.repository.list_groups(crop):
            if normalize_variety_text(group.label) == wanted:
                return group
        return None

    def log_resolution(self, crop: str, res: VarietySelectionResolution) -> None:
        logger.info("[Variedades] cultivo=%s source_value=%s resolution_type=%s group=%s subgroup=%s resolved_varieties=%s status=%s warnings=%s", crop, res.source_value, "GROUP" if res.is_group else res.status, res.group or "", res.subgroup or "", ",".join(res.varieties), res.status, ";".join(res.warnings))
