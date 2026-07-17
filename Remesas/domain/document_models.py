from enum import Enum


class DocumentType(str, Enum):
    """Tipos documentales admitidos; CSV queda preparado para una fase posterior."""

    PDF_MEMBER = "PDF_MEMBER"
    PDF_DRAFT = "PDF_DRAFT"
    PDF_MERGED_PRINT = "PDF_MERGED_PRINT"
    CSV_ADMINISTRATION = "CSV_ADMINISTRATION"
