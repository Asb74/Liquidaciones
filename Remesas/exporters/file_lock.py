from __future__ import annotations

from pathlib import Path


class FileLockedError(Exception):
    def __init__(self, path: Path) -> None:
        self.path = path
        super().__init__(f"El archivo está abierto o bloqueado: {path}")


def ensure_target_is_writable(path: Path) -> None:
    """Raise FileLockedError when an existing target cannot be updated.

    Missing files are allowed. Existing files are probed in read/write binary
    mode before the exporter builds the replacement workbook. The final atomic
    os.replace() is still guarded by the caller because Excel/Windows can lock
    files after this preflight check.
    """
    if not path.exists():
        return
    try:
        with path.open("r+b"):
            pass
    except PermissionError as exc:
        raise FileLockedError(path) from exc
