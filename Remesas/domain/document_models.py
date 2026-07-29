from enum import Enum


class LiquidationDocumentMode(str, Enum):
    """Explicit rendering mode; it affects only the draft watermark."""

    DRAFT = "DRAFT"
    FINAL = "FINAL"


class DocumentType(str, Enum):
    """Tipos documentales admitidos; CSV queda preparado para una fase posterior."""

    PDF_MEMBER = "PDF_MEMBER"
    PDF_DRAFT = "PDF_DRAFT"
    PDF_MERGED_PRINT = "PDF_MERGED_PRINT"
    CSV_ADMINISTRATION = "CSV_ADMINISTRATION"
