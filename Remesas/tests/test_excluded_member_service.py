import sqlite3

from data.excluded_member_repository import ExcludedMemberRepository
from domain.member_rules import ExcludedMemberService, DSOCIO_TIPO_OTROS_REASON, SYSTEM_MEMBER_ZERO_REASON


def _database():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE DSocio (IdSocio INTEGER, Tipo TEXT)")
    conn.executemany("INSERT INTO DSocio VALUES (?, ?)", [(1, "OTROS"), (2, " otros "), (3, None), (4, ""), (5, "AGRICULTOR")])
    return conn


def test_other_type_and_zero_are_excluded_with_normalized_values():
    service = ExcludedMemberService(ExcludedMemberRepository(connection=_database()))
    assert service.get_excluded_member_ids() == frozenset({0, 1, 2})
    assert service.is_excluded_member(0)
    assert service.is_excluded_member(" 2 ")
    assert not service.is_excluded_member(3)
    assert service.reason_for_exclusion(0) == SYSTEM_MEMBER_ZERO_REASON
    assert service.reason_for_exclusion(1) == DSOCIO_TIPO_OTROS_REASON


def test_refresh_updates_the_cache_after_database_change():
    conn = _database()
    service = ExcludedMemberService(ExcludedMemberRepository(connection=conn))
    assert not service.is_excluded_member(5)
    conn.execute("UPDATE DSocio SET Tipo='OTROS' WHERE IdSocio=5")
    assert not service.is_excluded_member(5)
    service.refresh_excluded_members()
    assert service.is_excluded_member(5)
    assert service.filter_eligible_member_ids([0, 1, 3, "5", None]) == (3,)
