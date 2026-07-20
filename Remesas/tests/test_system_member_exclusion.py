from domain.member_rules import is_excluded_member


def test_system_member_rule_recognises_safe_zero_representations_only():
    assert is_excluded_member(0)
    assert is_excluded_member("0")
    assert is_excluded_member(" 0.0 ")
    assert not is_excluded_member(None)
    assert not is_excluded_member("")
    assert not is_excluded_member("CENTRAL")
    assert not is_excluded_member(1)
