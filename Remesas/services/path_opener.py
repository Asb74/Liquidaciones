from __future__ import annotations

import os
from pathlib import Path
import subprocess


def open_path(path: str | Path) -> None:
    """Open an existing file or directory with the platform's default handler."""
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"No existe el archivo:\n{target}")
    if os.name == "nt":
        os.startfile(str(target))
    else:
        subprocess.Popen(["xdg-open", str(target)])
