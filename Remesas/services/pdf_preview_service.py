from __future__ import annotations

import logging
from pathlib import Path
import tempfile
from uuid import uuid4

from domain.utils import safe_path_part
from services.path_opener import open_path

logger = logging.getLogger(__name__)


class PdfPreviewService:
    """Owns draft PDFs for one application session and removes them on exit."""

    def __init__(self, *, temp_root: str | Path | None = None) -> None:
        base = Path(temp_root) if temp_root else Path(tempfile.gettempdir()) / "Liquidaciones" / "preview"
        base.mkdir(parents=True, exist_ok=True)
        self._temporary_directory = tempfile.TemporaryDirectory(prefix=f"{uuid4().hex}_", dir=base)
        self.session_directory = Path(self._temporary_directory.name)

    def create_preview_path(self, *, member_id, member_name, remittance_name) -> Path:
        filename = "Vista_previa_{}_{}_{}.pdf".format(
            safe_path_part(member_id), safe_path_part(member_name), safe_path_part(remittance_name)
        )
        path = self.session_directory / filename
        index = 2
        while path.exists():
            path = self.session_directory / f"{Path(filename).stem}_v{index}.pdf"
            index += 1
        logger.info("[PdfPreviewCreated]\nmember_id=%s\nremittance=%s\npath=%s", member_id, remittance_name, path)
        return path

    def open_preview(self, path: str | Path) -> None:
        open_path(path)
        logger.info("[PdfPreviewOpened]\npath=%s", path)

    def cleanup(self) -> None:
        try:
            self._temporary_directory.cleanup()
        except Exception:
            logger.warning("No se pudieron limpiar las vistas previas temporales", exc_info=True)
