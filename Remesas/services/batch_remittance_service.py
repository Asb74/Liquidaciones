from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Sequence

from domain.utils import safe_path_part

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SelectedRemittance:
    remittance_id: int
    name: str
    payment_date: date | None
    period_from: date | None
    period_to: date | None
    category: str
    liquidation_type: str
    campaign: str
    company: str
    crop: str


@dataclass(frozen=True)
class BatchProgress:
    total_remittances: int
    current_index: int
    current_remittance_id: int | None
    current_remittance_name: str | None
    phase: str
    processed_members: int | None = None
    total_members: int | None = None
    message: str = ""


@dataclass(frozen=True)
class SingleRemittanceBatchResult:
    remittance: SelectedRemittance
    calculation_result: object
    member_count: int
    delivery_count: int
    output_directory: Path
    generated_files: tuple[Path, ...]
    draft_documents_generated: int = 0
    draft_document_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class FailedRemittanceResult:
    remittance: SelectedRemittance
    phase: str
    error_type: str
    error_message: str


class RemittanceProcessingError(ValueError):
    """A controlled, reportable configuration error for one remittance."""

    def __init__(self, message: str, *, phase: str, error_type: str) -> None:
        super().__init__(message)
        self.phase = phase
        self.error_type = error_type


@dataclass(frozen=True)
class BatchRemittanceResult:
    remittances_requested: int
    remittances_completed: int
    remittances_failed: int
    successful_results: tuple[SingleRemittanceBatchResult, ...]
    failed_results: tuple[FailedRemittanceResult, ...]
    aggregate_excel_path: Path | None
    started_at: datetime
    finished_at: datetime
    cancelled: bool = False
    log_path: Path | None = None

    @property
    def drafts_generated(self): return sum(x.draft_documents_generated for x in self.successful_results)
    @property
    def draft_errors(self): return sum(len(x.draft_document_errors) for x in self.successful_results)


@dataclass(frozen=True)
class BatchLiquidationExcelResult:
    """Lossless view of a batch used by consolidated exporters."""
    selected_remittances: tuple[SelectedRemittance, ...]
    successful_results: tuple[SingleRemittanceBatchResult, ...]
    failed_results: tuple[FailedRemittanceResult, ...]

    @property
    def total_remittances(self): return len(self.selected_remittances)
    @property
    def total_members(self): return sum(item.member_count for item in self.successful_results)
    @property
    def total_deliveries(self): return sum(item.delivery_count for item in self.successful_results)

    def _total(self, name):
        values=[]
        for item in self.successful_results:
            result=item.calculation_result.result if hasattr(item.calculation_result,"result") else item.calculation_result
            values.append(getattr(result.totals,name,None))
        return None if self.failed_results or not values or any(v is None for v in values) else sum(values, start=__import__("decimal").Decimal("0"))

    total_net_kg=property(lambda self:self._total("net_kg"))
    total_commercial_amount=property(lambda self:self._total("commercial_amount"))
    total_collection_amount=property(lambda self:self._total("collection_amount"))
    total_transport_amount=property(lambda self:self._total("transport_amount"))
    total_quality_amount=property(lambda self:self._total("quality_amount"))
    total_globalgap_amount=property(lambda self:self._total("globalgap_amount"))
    total_hectare_fee_amount=property(lambda self:self._total("hectare_fee_amount"))
    total_taxable_base=property(lambda self:self._total("taxable_base"))
    total_vat_amount=property(lambda self:self._total("vat_amount"))
    total_withholding_amount=property(lambda self:self._total("withholding_amount"))
    total_amount=property(lambda self:self._total("total_amount"))


class BatchRemittanceService:
    def __init__(self, *, single_processor: Callable[[SelectedRemittance, Callable[[BatchProgress], None] | None], SingleRemittanceBatchResult], output_base: Path | None = None, exporter: Callable[..., Path] | None = None, should_cancel: Callable[[], bool] | None = None, log_dir: Path | None = None) -> None:
        self.single_processor = single_processor
        self.output_base = output_base or (Path("C:/Liquidaciones/salidas/remesas") if Path("C:/").exists() else Path.cwd().parents[0] / "salidas" / "remesas")
        self.exporter = exporter
        self.should_cancel = should_cancel or (lambda: False)
        self.log_dir = log_dir or Path.cwd() / "logs"

    def process(self, remittances: Sequence[SelectedRemittance], *, progress_callback: Callable[[BatchProgress], None] | None = None, aggregate_output_path: Path | None = None) -> BatchRemittanceResult:
        started_at = datetime.now()
        total = len(remittances)
        successful: list[SingleRemittanceBatchResult] = []
        failed: list[FailedRemittanceResult] = []
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.log_dir / f"batch_remittance_{started_at:%Y%m%d_%H%M%S}.log"
        aggregate_path: Path | None = None
        cancelled = False
        logger.info("[BatchExcel] operation=start selected_count=%s remittance_ids=%s", total, [r.remittance_id for r in remittances])
        self._emit(progress_callback, total, 0, None, "PREPARING", "Preparando lote")
        with log_path.open("w", encoding="utf-8") as log:
            if remittances:
                first = remittances[0]
                log.write(f"campaña={first.campaign}\nempresa={first.company}\ncultivo={first.crop}\n")
            log.write(f"inicio={started_at.isoformat()}\nremesas={[r.remittance_id for r in remittances]}\n")
            for index, remittance in enumerate(remittances, start=1):
                if self.should_cancel():
                    cancelled = True
                    log.write(f"cancelled_before_id={remittance.remittance_id}\n")
                    break
                item_started = datetime.now()
                try:
                    self._emit(progress_callback, total, index, remittance, "LOADING", "Cargando remesa")
                    result = self.single_processor(remittance, self._child_callback(progress_callback, total, index, remittance))
                    successful.append(result)
                    calculated=result.calculation_result.result if hasattr(result.calculation_result,"result") else result.calculation_result
                    logger.info("[BatchExcelRemittance] remittance_id=%s member_count=%s delivery_count=%s net_kg=%s total_amount=%s status=success", remittance.remittance_id, result.member_count, result.delivery_count, getattr(calculated.totals,"net_kg",None), getattr(calculated.totals,"total_amount",None))
                    duration = (datetime.now() - item_started).total_seconds()
                    log.write("[BatchRemittance]\n")
                    log.write(f"index={index}\ntotal={total}\nid={remittance.remittance_id}\nname={remittance.name}\nstatus=SUCCESS\ndeliveries={result.delivery_count}\nmembers={result.member_count}\noutput_dir={result.output_directory}\ngenerated_files={[str(p) for p in result.generated_files]}\nduration_seconds={duration:.2f}\n")
                except Exception as exc:
                    logger.exception("Error procesando remesa %s", remittance.remittance_id)
                    phase = getattr(exc, "phase", "ERROR")
                    error_type = getattr(exc, "error_type", type(exc).__name__)
                    failed.append(FailedRemittanceResult(remittance, phase, error_type, str(exc)))
                    self._emit(progress_callback, total, index, remittance, "CONFIGURATION_ERROR" if phase == "VARIETY_RESOLUTION" else "ERROR", str(exc))
                    log.write("[BatchRemittance]\n")
                    log.write(f"index={index}\ntotal={total}\nid={remittance.remittance_id}\nname={remittance.name}\nstatus=ERROR\nphase={phase}\nerror_type={error_type}\nerror_message={exc}\n")
            finished_at = datetime.now()
            if successful and self.exporter:
                first = remittances[0]
                folder = self.output_base / safe_path_part(first.campaign) / safe_path_part(first.crop)
                folder.mkdir(parents=True, exist_ok=True)
                filename = f"Resumen_liquidaciones_{safe_path_part(first.crop)}_{safe_path_part(first.campaign)}_{finished_at:%Y%m%d_%H%M%S}.xlsx"
                aggregate_path = Path(aggregate_output_path) if aggregate_output_path else folder / filename
                self._emit(progress_callback, total, len(successful), None, "BUILDING_AGGREGATE_EXCEL", "Generando Excel acumulado")
                aggregate_path = self.exporter(successful, failed, aggregate_path, campaign=first.campaign, company=first.company, crop=first.crop, execution_started_at=started_at, execution_finished_at=finished_at)
            log.write(f"fin={finished_at.isoformat()}\nduracion_segundos={(finished_at-started_at).total_seconds():.2f}\nexcel_acumulado={aggregate_path}\n")
        self._emit(progress_callback, total, len(successful), None, "FINISHED", "Lote finalizado")
        logger.info("[BatchExcel] operation=finish successful_count=%s failed_count=%s output_path=%s",len(successful),len(failed),aggregate_path)
        return BatchRemittanceResult(total, len(successful), len(failed), tuple(successful), tuple(failed), aggregate_path, started_at, finished_at, cancelled, log_path)

    def _emit(self, callback, total, index, remittance, phase, message):
        if callback:
            callback(BatchProgress(total, index, getattr(remittance, "remittance_id", None), getattr(remittance, "name", None), phase, message=message))

    def _child_callback(self, callback, total, index, remittance):
        if callback is None:
            return None
        def forward(progress: BatchProgress):
            callback(BatchProgress(total, index, remittance.remittance_id, remittance.name, progress.phase, progress.processed_members, progress.total_members, progress.message))
        return forward
