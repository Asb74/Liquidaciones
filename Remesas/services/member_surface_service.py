"""Explicit access to the exact productive surface used by Cuota Ha."""
from __future__ import annotations

from decimal import Decimal
from hashlib import sha256
import json

from domain.hectare_fee_master import HectareFeeMasterRepository


class MemberSurfaceService:
    """Keeps historical benchmark recovery on the Cuota Ha calculation path."""

    def __init__(self, hectare_repository, master_repository=None):
        self.repository = hectare_repository
        self.master_repository = master_repository or HectareFeeMasterRepository()

    def get_member_variety_group_surface(self, *, campaign, company, member_id,
                                         variety_group_code):
        master = self.master_repository.load()
        hectares, warnings = self.repository.calculate_applicable_hectares(
            member_id, campaign, company, master.eligible_crops
        )
        if hectares is None or hectares <= 0:
            return None
        fingerprint = sha256(json.dumps({
            "campaign": str(campaign), "company": str(company),
            "member_id": int(member_id), "variety_group_code": str(variety_group_code),
            "hectares": str(hectares), "master": master.fingerprint,
            "eligible_crops": master.eligible_crops,
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return Decimal(hectares), fingerprint, tuple(warnings)
