from decimal import Decimal

from data.persistence.database import PersistenceDatabase
from data.persistence.migrations import _migrate_article_code_as_text
from data.persistence.master_repository import LiquidationMasterRepository
from domain.persistence_models import SplitRecipient, SplitRule
from services.liquidation_split_service import LiquidationSplitService


def test_migrations_seed_confirmed_prefixes(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"liquidaciones.sqlite")); db.initialize(); db.initialize()
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]==9
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


def test_prefix_crud_normalizes_and_rejects_duplicates(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"liq.sqlite")); db.initialize(); repo=LiquidationMasterRepository(db)
    repo.save_prefix(" nuevo "," nv ")
    assert any(x["crop"]=="NUEVO" and x["prefix"]=="NV" for x in repo.list_prefixes())
