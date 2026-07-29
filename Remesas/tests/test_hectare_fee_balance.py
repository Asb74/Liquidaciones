from decimal import Decimal

from domain.calculation_models import HectareFeeBalance


def test_balance_open_closed_and_over_applied():
    open_balance = HectareFeeBalance(1, Decimal("1000"), Decimal("600"), Decimal("300"), Decimal("900"), Decimal("100"), False)
    assert open_balance.status == "OPEN"

    closed = HectareFeeBalance(1, Decimal("1000"), Decimal("700"), Decimal("300"), Decimal("1000"), Decimal("0"), True)
    assert closed.status == "CLOSED"

    over = HectareFeeBalance(1, Decimal("1000"), Decimal("900"), Decimal("150"), Decimal("1050"), Decimal("-50"), False)
    assert over.status == "OVER_APPLIED"
