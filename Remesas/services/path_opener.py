from __future__ import annotations

import os
from pathlib import Path
import logging
import sys
import subprocess

logger = logging.getLogger(__name__)


def open_path(path: str | Path) -> None:
    """Open an existing file or directory with the platform's default handler."""
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"No existe el archivo:\n{target}")
    try:
        if os.name == "nt":
            os.startfile(str(target))
        elif sys.platform == "darwin":
            subprocess.run(["open", str(target)], check=True)
        else:
            subprocess.run(["xdg-open", str(target)], check=True)
    except Exception:
        logger.exception("No se pudo abrir la ruta con la aplicación predeterminada: %s", target)
        raise
