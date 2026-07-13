from __future__ import annotations

from domain.liquidacion_calculator import LiquidacionCalculator
from domain.models import AppConfig, Delivery, Remesa


class CalculationService:
    def __init__(self, conn=None, config: AppConfig | None = None) -> None:
        quality_repository = hectare_repository = None
        if conn is not None:
            from data.quality_repository import QualityRepository
            from data.hectare_repository import HectareRepository
            quality_repository = QualityRepository(conn)
            hectare_repository = HectareRepository(conn)
        self.calculator = LiquidacionCalculator(quality_repository, hectare_repository, config)

    def calculate(self, deliveries: list[Delivery], remesa: Remesa | None):
        return self.calculator.calculate(deliveries, remesa)
