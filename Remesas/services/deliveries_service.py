from __future__ import annotations
from domain.models import DeliveryFilter, Period, WorkContext
from domain.validators import validate_context, validate_period
from data.deliveries_repository import DeliveriesRepository

class DeliveriesService:
    def __init__(self, repo: DeliveriesRepository) -> None:
        self.repo = repo
    def search(self, filters: DeliveryFilter):
        validate_context(filters.context); validate_period(filters.period)
        return self.repo.fetch(filters)
