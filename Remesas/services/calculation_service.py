from __future__ import annotations

from domain.liquidacion_calculator import LiquidacionCalculator
from domain.models import Delivery, Remesa


class CalculationService:
    def __init__(self) -> None:
        self.calculator = LiquidacionCalculator()

    def calculate(self, deliveries: list[Delivery], remesa: Remesa | None):
        return self.calculator.calculate(deliveries, remesa)
