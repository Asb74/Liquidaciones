from __future__ import annotations

import logging
from pathlib import Path


def split_document_logger() -> logging.Logger:
    logger = logging.getLogger("split_document_generation")
    if not logger.handlers:
        path = Path(__file__).resolve().parents[2] / "logs" / "split_document_generation.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
