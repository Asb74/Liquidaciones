from enum import Enum


class DocumentType(str, Enum):
    """Tipos documentales admitidos; CSV queda preparado para una fase posterior."""

    PDF_MEMBER = "PDF_MEMBER"
    CSV_ADMINISTRATION = "CSV_ADMINISTRATION"
