"""Compatibility import for the shared massive-document selector."""

from ui.multi_remittance_selection_dialog import (
    COLUMNS,
    EMPTY_SELECTION_MESSAGE,
    MultiRemittanceSelectionDialog,
)


class ExcelConsolidationDialog(MultiRemittanceSelectionDialog):
    def __init__(self, parent, items, context, *, selection_factory, on_generate):
        super().__init__(
            parent, items, context, selection_factory=selection_factory,
            purpose="excel", on_generate=on_generate,
        )
