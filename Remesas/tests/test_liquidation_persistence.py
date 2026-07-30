from decimal import Decimal

from data.persistence.database import PersistenceDatabase
from data.persistence.migrations import _migrate_article_code_as_text
from data.persistence.master_repository import LiquidationMasterRepository
from domain.persistence_models import SplitRecipient, SplitRule
from services.liquidation_split_service import LiquidationSplitService


def test_migrations_seed_confirmed_prefixes(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"liquidaciones.sqlite")); db.initialize(); db.initialize()
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]==14
        assert dict(conn.execute("SELECT crop,prefix FROM liquidation_prefixes"))["DIRECTO"]=="VE"
        assert dict(conn.execute("SELECT crop,prefix FROM liquidation_prefixes"))["DIRECTOCHF"]=="VC"
        assert dict(conn.execute("SELECT crop,prefix FROM liquidation_prefixes"))["CIRUELA"]=="CR"
        column = next(row for row in conn.execute("PRAGMA table_info(liquidaciones)") if row[1] == "cod_art")
        assert column[2] == "TEXT"


def test_article_code_migration_preserves_existing_values_and_indexes(tmp_path):
    import sqlite3
    connection=sqlite3.connect(tmp_path/"legacy.sqlite")
    connection.execute("CREATE TABLE liquidaciones(id INTEGER PRIMARY KEY, cod_art INTEGER, variedad TEXT)")
    connection.execute("CREATE INDEX ix_liq_variety ON liquidaciones(variedad)")
    connection.executemany("INSERT INTO liquidaciones VALUES(?,?,?)", ((1, 3970, "NAVELINA"), (2, "B391", "TANGO")))

    _migrate_article_code_as_text(connection)

    assert [row[0] for row in connection.execute("SELECT cod_art FROM liquidaciones ORDER BY id")] == ["3970", "B391"]
    assert next(row for row in connection.execute("PRAGMA table_info(liquidaciones)") if row[1] == "cod_art")[2] == "TEXT"
    assert connection.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='ix_liq_variety'").fetchone()


def test_split_factor_modes_and_historical_residual():
    recipients=(SplitRecipient(2,value=Decimal("2")),SplitRecipient(3,value=Decimal("1")),SplitRecipient(4,value=Decimal("1")))
    weights=SplitRule(1,1,"WEIGHTS",recipients)
    assert [x[1] for x in LiquidationSplitService.factors(weights,1,"Origen")]==[Decimal("0.5"),Decimal("0.25"),Decimal("0.25")]
    historical=SplitRule(2,5970,"PERCENTAGE_WITH_RESIDUAL",(SplitRecipient(5893,value=Decimal("50")),))
    factors=LiquidationSplitService.factors(historical,5970,"Origen")
    assert [(x.recipient_member_id,f) for x,f in factors]==[(5893,Decimal("0.5")),(5970,Decimal("0.5"))]


def test_percentage_residual_only_controls_rounding():
    rule=SplitRule(3,453,"PERCENTAGE_WITH_RESIDUAL",(
        SplitRecipient(1462,"Destino",Decimal("50"),True),))
    factors=LiquidationSplitService.factors(rule,453,"Origen")
    assert [(x.recipient_member_id,f,x.is_residual) for x,f in factors] == [
        (1462,Decimal("0.5"),True),(453,Decimal("0.5"),False)]
    values=LiquidationSplitService._allocate(Decimal("8.881"),[f for _,f in factors],Decimal("0.001"),0)
    assert values == [Decimal("4.440"),Decimal("4.441")]
    assert sum(values)==Decimal("8.881")
    money=LiquidationSplitService._allocate(Decimal("100.01"),[f for _,f in factors],Decimal("0.01"),0)
    assert money == [Decimal("50.00"),Decimal("50.01")]
    assert sum(money)==Decimal("100.01")


def test_percentage_full_allocation_and_overallocation():
    full=SplitRule(4,453,"PERCENTAGE",(
        SplitRecipient(1462,value=Decimal("50")),SplitRecipient(5970,value=Decimal("50"))))
    assert [x.recipient_member_id for x,_ in LiquidationSplitService.factors(full,453,"Origen")]==[1462,5970]
    excessive=SplitRule(5,453,"PERCENTAGE",(
        SplitRecipient(1462,value=Decimal("60")),SplitRecipient(5970,value=Decimal("50"))))
    import pytest
    with pytest.raises(ValueError,match="más de 100"):
        LiquidationSplitService.factors(excessive,453,"Origen")


def test_crossed_half_rules_conserve_total_kilos():
    factors=[Decimal("0.5"),Decimal("0.5")]
    first=LiquidationSplitService._allocate(Decimal("12.947"),factors,Decimal("0.001"),0)
    second=LiquidationSplitService._allocate(Decimal("24.906"),factors,Decimal("0.001"),0)
    totals=[first[1]+second[0],first[0]+second[1]]
    assert totals == [Decimal("18.927"),Decimal("18.926")]
    assert sum(totals)==Decimal("37.853")


def test_prefix_crud_normalizes_and_rejects_duplicates(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"liq.sqlite")); db.initialize(); repo=LiquidationMasterRepository(db)
    repo.save_prefix(" nuevo "," nv ")
    assert any(x["crop"]=="NUEVO" and x["prefix"]=="NV" for x in repo.list_prefixes())


def test_delete_rule_removes_recipients_before_parent(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"liq.sqlite")); db.initialize(); repo=LiquidationMasterRepository(db)
    rule_id=repo.save_rule(453,"PERCENTAGE_WITH_RESIDUAL",[(1462,"Destino","50",True)])
    repo.delete_rule(rule_id)
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM split_rule_recipients WHERE rule_id=?",(rule_id,)).fetchone()[0]==0
        assert conn.execute("SELECT COUNT(*) FROM split_rules WHERE id=?",(rule_id,)).fetchone()[0]==0
