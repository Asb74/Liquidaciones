from __future__ import annotations

import logging
from pathlib import Path


def diagnostic_logger(log_path: str | Path = "logs/document_snapshot_diagnostic.log") -> logging.Logger:
    """Return the dedicated, non-propagating snapshot diagnostic logger."""
    path = Path(log_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("document_snapshot_diagnostic")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not any(isinstance(handler, logging.FileHandler) and Path(handler.baseFilename) == path
               for handler in logger.handlers):
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        logger.addHandler(handler)
    return logger
