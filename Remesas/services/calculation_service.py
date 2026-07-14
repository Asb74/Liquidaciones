from __future__ import annotations

from domain.liquidacion_calculator import LiquidacionCalculator
from domain.models import AppConfig, Delivery, Remesa
from domain.audit import AuditLogger
from domain.hectare_fee_master import HectareFeeMasterRepository


class CalculationService:
    def __init__(self, conn=None, config: AppConfig | None = None) -> None:
        quality_repository = hectare_repository = globalgap_repository = fiscal_regime_repository = None
        if conn is not None:
            from data.quality_repository import QualityRepository
            from data.hectare_repository import HectareRepository
            from data.globalgap_repository import GlobalGapRepository
            from data.fiscal_regime_repository import FiscalRegimeRepository
            quality_repository = QualityRepository(conn)
            hectare_repository = HectareRepository(conn)
            globalgap_repository = GlobalGapRepository(conn)
            fiscal_regime_repository = FiscalRegimeRepository(conn)
        self.config = config
        self.master_repository = HectareFeeMasterRepository()
        self.calculator = LiquidacionCalculator(quality_repository, hectare_repository, config, globalgap_repository, fiscal_regime_repository)

    def calculate(self, deliveries: list[Delivery], remesa: Remesa | None):
        master = self.master_repository.load()
        self.calculator.hectare_master = master
        with AuditLogger.for_calculation(self.calculator.hectare_config, remesa, deliveries, master) as audit:
            if audit:
                audit.audit_deliveries()
            return self.calculator.calculate(deliveries, remesa)
