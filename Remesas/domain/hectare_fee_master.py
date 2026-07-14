from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import logging
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterable

logger = logging.getLogger(__name__)

DEFAULT_MASTER_PATH = Path(__file__).resolve().parents[1] / "config" / "maestro_cuota_ha.json"

DEFAULT_MASTER_JSON: dict[str, Any] = {
    "version": 1,
    "price_per_hectare": "195.00",
    "surface_crops": [
        {"crop": "CITRICOS", "enabled": True},
        {"crop": "MANDARINA", "enabled": True},
    ],
    "delivery_crops": [
        {"crop": "CITRICOS", "enabled": True},
        {"crop": "MANDARINA", "enabled": True},
        {"crop": "DIRECTO", "enabled": True},
        {"crop": "DIRECTOCHF", "enabled": True},
        {"crop": "INDUSTRIA", "enabled": True},
    ],
}


def normalize_crop(value: object) -> str:
    return str(value or "").strip().upper()


def normalize_crops(values: Iterable[object]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        crop = normalize_crop(value)
        if crop and crop not in seen:
            seen.add(crop)
            result.append(crop)
    return tuple(result)


def parse_decimal(value: object, field_name: str = "price_per_hectare") -> Decimal:
    try:
        parsed = Decimal(str(value).strip().replace(",", "."))
    except (InvalidOperation, AttributeError) as exc:
        raise ValueError(f"{field_name} debe ser un Decimal válido") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} debe ser mayor que cero")
    return parsed


@dataclass(frozen=True)
class HectareFeeMaster:
    price_per_hectare: Decimal
    surface_crops: tuple[str, ...]
    delivery_crops: tuple[str, ...]
    version: int = 1
    path: str = ""
    fingerprint: str = ""
    loaded_at: datetime | None = None

    def stable_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "price_per_hectare": format(self.price_per_hectare, "f"),
            "surface_crops": list(self.surface_crops),
            "delivery_crops": list(self.delivery_crops),
        }

    def with_metadata(self, path: Path | str, loaded_at: datetime | None = None) -> "HectareFeeMaster":
        from dataclasses import replace
        loaded = loaded_at or datetime.now()
        return replace(self, path=str(path), fingerprint=fingerprint_master(self), loaded_at=loaded)


def fingerprint_master(master: HectareFeeMaster) -> str:
    raw = json.dumps(master.stable_payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _entries_to_crops(entries: object) -> tuple[str, ...]:
    if not isinstance(entries, list):
        raise ValueError("La lista de cultivos no es válida")
    enabled = []
    for item in entries:
        if isinstance(item, dict):
            if item.get("enabled", True):
                enabled.append(item.get("crop", ""))
        else:
            enabled.append(item)
    return normalize_crops(enabled)


def master_from_json(data: dict[str, Any]) -> HectareFeeMaster:
    if not isinstance(data, dict):
        raise ValueError("El maestro debe ser un objeto JSON")
    price = parse_decimal(data.get("price_per_hectare"))
    surface = _entries_to_crops(data.get("surface_crops", []))
    delivery = _entries_to_crops(data.get("delivery_crops", []))
    if not surface:
        raise ValueError("Debe seleccionar al menos un cultivo de superficie")
    if not delivery:
        raise ValueError("Debe seleccionar al menos un cultivo de entrega")
    return HectareFeeMaster(price, surface, delivery, int(data.get("version", 1)))


def master_to_json(master: HectareFeeMaster) -> dict[str, Any]:
    return {
        "version": master.version,
        "price_per_hectare": f"{master.price_per_hectare:.2f}",
        "surface_crops": [{"crop": c, "enabled": True} for c in master.surface_crops],
        "delivery_crops": [{"crop": c, "enabled": True} for c in master.delivery_crops],
    }


class HectareFeeMasterRepository:
    def __init__(self, path: Path | str = DEFAULT_MASTER_PATH) -> None:
        self.path = Path(path)

    def defaults(self) -> HectareFeeMaster:
        return master_from_json(DEFAULT_MASTER_JSON)

    def load(self) -> HectareFeeMaster:
        if not self.path.exists():
            logger.info("Maestro cuota Ha no existe; se crea en %s", self.path)
            return self.restore_defaults()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            master = master_from_json(data).with_metadata(self.path)
            logger.info("Maestro cuota Ha cargado ruta=%s huella=%s precio=%s superficie=%s kilos=%s", self.path, master.fingerprint, master.price_per_hectare, master.surface_crops, master.delivery_crops)
            return master
        except Exception:
            backup = self.path.with_name(f"maestro_cuota_ha_corrupto_{datetime.now():%Y%m%d_%H%M%S}.json")
            self.path.replace(backup)
            logger.exception("Maestro cuota Ha corrupto; copia en %s y restauración de valores iniciales", backup)
            return self.restore_defaults()

    def save(self, master: HectareFeeMaster) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = master_to_json(master)
        text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
        with NamedTemporaryFile("w", encoding="utf-8", dir=self.path.parent, prefix=f".{self.path.name}.", delete=False) as tmp:
            tmp.write(text)
            tmp_name = tmp.name
        try:
            master_from_json(json.loads(Path(tmp_name).read_text(encoding="utf-8")))
            os.replace(tmp_name, self.path)
        finally:
            tmp_path = Path(tmp_name)
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
        logger.info("Maestro cuota Ha guardado ruta=%s huella=%s", self.path, fingerprint_master(master))

    def restore_defaults(self) -> HectareFeeMaster:
        master = self.defaults()
        self.save(master)
        return master.with_metadata(self.path)
