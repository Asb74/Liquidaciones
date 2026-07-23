from decimal import Decimal

from data.persistence.database import PersistenceDatabase
from data.persistence.master_repository import LiquidationMasterRepository
from domain.persistence_models import SplitRecipient, SplitRule
from services.liquidation_split_service import LiquidationSplitService


def test_migrations_seed_confirmed_prefixes(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"liquidaciones.sqlite")); db.initialize(); db.initialize()
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]==8
        assert dict(conn.execute("SELECT crop,prefix FROM liquidation_prefixes"))["DIRECTO"]=="VE"
        assert dict(conn.execute("SELECT crop,prefix FROM liquidation_prefixes"))["DIRECTOCHF"]=="VC"
        assert dict(conn.execute("SELECT crop,prefix FROM liquidation_prefixes"))["CIRUELA"]=="CR"


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
