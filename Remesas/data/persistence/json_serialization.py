"""Explicit JSON conversion for immutable persistence payloads.

Snapshots are an interchange format, not a display format: unsupported domain
objects must fail loudly instead of being silently stringified.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path


def to_json_compatible(value):
    """Return a recursively JSON-compatible representation of *value*."""
    if value is None or isinstance(value, (str, int, bool, float)):
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Enum):
        return to_json_compatible(value.value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: to_json_compatible(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): to_json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_json_compatible(item) for item in value]
    # Avoid treating strings as sequences (already handled above), while still
    # supporting other nested sequence implementations.
    if isinstance(value, Sequence):
        return [to_json_compatible(item) for item in value]
    raise TypeError(f"Tipo no compatible con snapshot JSON: {type(value).__name__}")
