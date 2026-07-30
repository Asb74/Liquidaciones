from decimal import Decimal
from types import SimpleNamespace

from data.persistence.database import PersistenceDatabase
from data.persistence.migrations import _migrate_article_code_as_text
from data.persistence.master_repository import LiquidationMasterRepository
from domain.persistence_models import SplitRecipient, SplitRule
from services.liquidation_split_service import LiquidationSplitService
from services.liquidation_persistence_service import LiquidationPersistenceService


class _FiscalRepository:
    def get_for_member(self, member_id):
        return SimpleNamespace(regime=SimpleNamespace(vat_rate=Decimal("12"),withholding_rate=Decimal("2")),warnings=())


def _split_service(rule):
    service=object.__new__(LiquidationSplitService)
    service.fiscal=_FiscalRepository()
    service.audit=SimpleNamespace(info=lambda *args,**kwargs: None)
    service.logger=SimpleNamespace()
    service.resolve_rule=lambda member,header: rule
    return service


def _member(**changes):
    values=dict(member_id=453,member_name="Origen",variety="WASHINGTON",net_kg=Decimal("12947"),
        gross_amount=Decimal("3651.05"),collection_amount=Decimal("0"),hectare_fee_amount=Decimal("0"),
        quality_amount=Decimal("0"),transport_amount=Decimal("0"),globalgap_amount=Decimal("0"),
        taxable_base=Decimal("3551.50"),vat_amount=Decimal("426.18"),withholding_amount=Decimal("79.55"),
        total_amount=Decimal("3898.13"),destruction_price=None,table_destruction_price=None,rotten_price=None,
        national_market_price=None,rotten_leaves_price=None)
    values.update(changes)
    return SimpleNamespace(**values)


def _header():
    return SimpleNamespace(remesa_id=2285,campana="2026",cultivo="CITRICOS")


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


def test_real_half_split_reconciles_fiscal_cent_into_configured_residual():
    rule=SplitRule(5,453,"PERCENTAGE",(
        SplitRecipient(1462,"Destino",Decimal("50"),True),
        SplitRecipient(453,"Origen",Decimal("50"),False)))

    lines=_split_service(rule).split(_member(),_header())

    assert sum((x.net_kg for x in lines),Decimal(0)) == Decimal("12947.000")
    assert sum((x.gross_amount for x in lines),Decimal(0)) == Decimal("3651.05")
    assert sum((x.taxable_base for x in lines),Decimal(0)) == Decimal("3551.50")
    assert sum((x.vat_amount for x in lines),Decimal(0)) == Decimal("426.18")
    assert sum((x.withholding_amount for x in lines),Decimal(0)) == Decimal("79.55")
    assert [x.total_amount for x in lines] == [Decimal("1949.07"),Decimal("1949.06")]
    assert sum((x.total_amount for x in lines),Decimal(0)) == Decimal("3898.13")


def test_split_without_rounding_difference_does_not_adjust_total():
    rule=SplitRule(6,453,"PERCENTAGE",(
        SplitRecipient(1462,"Destino",Decimal("50"),True),SplitRecipient(453,"Origen",Decimal("50"))))
    member=_member(taxable_base=Decimal("100.00"),gross_amount=Decimal("100.00"),vat_amount=Decimal("12.00"),
        withholding_amount=Decimal("2.24"),total_amount=Decimal("109.76"))

    assert [x.total_amount for x in _split_service(rule).split(member,_header())] == [Decimal("54.88"),Decimal("54.88")]


def test_real_fiscal_loss_is_not_hidden_by_residual_reconciliation():
    import pytest
    rule=SplitRule(7,453,"PERCENTAGE",(
        SplitRecipient(1462,"Destino",Decimal("50"),True),SplitRecipient(453,"Origen",Decimal("50"))))

    with pytest.raises(ValueError,match=r"field=total_amount.*difference=2.01.*quantum=0.01"):
        _split_service(rule).split(_member(total_amount=Decimal("3900.13")),_header())


def test_multiple_recipients_conserve_quantized_values():
    factors=[Decimal("0.3333"),Decimal("0.3333"),Decimal("0.3334")]
    for source,quantum in ((Decimal("8.881"),Decimal("0.001")),(Decimal("100.01"),Decimal("0.01"))):
        parts=LiquidationSplitService._allocate(source,factors,quantum,2)
        assert sum(parts,Decimal(0)) == source


def test_missing_residual_falls_back_to_source_member():
    rule=SplitRule(8,453,"PERCENTAGE",(
        SplitRecipient(1462,"Destino",Decimal("50")),SplitRecipient(453,"Origen",Decimal("50"))))

    lines=_split_service(rule).split(_member(),_header())

    assert [x.total_amount for x in lines] == [Decimal("1949.06"),Decimal("1949.07")]


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


def test_replacement_state_only_accounting_export_closes_batch(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"liq.sqlite")); db.initialize()
    service=object.__new__(LiquidationPersistenceService); service.database=db
    with db.connect() as conn:
        conn.execute("""INSERT INTO liquidation_batches
            (batch_id,remesa_id,remesa_name,campaign,company,crop,payment_date,
             calculation_fingerprint,original_line_count,final_line_count,status,created_at,operation_type)
            VALUES('old',7,'Remesa siete','2026','1','CITRICOS','2026-01-01','old-fp',1,1,'ACTIVE','2026-01-01','ORIGINAL')""")
        conn.execute("""INSERT INTO generated_documents
            (batch_id,remittance_id,recipient_member_id,document_type,file_path,status)
            VALUES('old',7,10,'PDF_MEMBER','old.pdf','GENERATED')""")

    state=service.get_replacement_state(campaign="2026",company="1",crop="citricos",remittance_id=7)
    assert state.has_active_liquidation and state.can_replace
    assert not state.is_accounting_exported

    with db.connect() as conn:
        export_id=conn.execute("""INSERT INTO accounting_exports
            (batch_id,export_type,file_path,status,created_at) VALUES('old','CSV','accounting.csv','GENERATED','2026-01-02')""").lastrowid
        conn.execute("INSERT INTO accounting_export_items(export_id,batch_id,created_at) VALUES(?,?,?)",(export_id,"old","2026-01-02"))

    state=service.get_replacement_state(campaign="2026",company="1",crop="CITRICOS",remittance_id=7)
    assert state.is_accounting_exported and not state.can_replace
    assert state.reason == "ACCOUNTING_EXPORTED"
