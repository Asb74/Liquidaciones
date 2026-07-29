from decimal import Decimal
from pathlib import Path
from data.production_destination_master_repository import ProductionDestinationMasterRepository
from services.production_destination_master_service import ProductionDestinationMasterService
from domain.calculation_models import LiquidationHeader, MemberLiquidation
from presentation.premium_liquidation_view_model import from_member_liquidation


def test_master_defaults_and_fallback(tmp_path):
    service=ProductionDestinationMasterService(ProductionDestinationMasterRepository(tmp_path/'m.json'))
    cit=service.get_for_crop('citricos'); assert cit.primary_label=='Exportación'; assert cit.secondary_label=='Mercado nacional'; assert cit.secondary_counts_as_commercial is True
    man=service.get_for_crop('MANDARINA'); assert man.secondary_counts_as_commercial is True
    kak=service.get_for_crop('KAKIS'); assert kak.secondary_enabled is False
    unk=service.get_for_crop('X'); assert unk.primary_label=='Comercial'; assert unk.secondary_label=='Destrío'; assert unk.secondary_counts_as_commercial is False

def test_citricos_summary_prices_and_commercial_kg():
    h=LiquidationHeader(1,'REM','2026','1','CITRICOS','','','','Normal','Primera','','',{}, {})
    m=MemberLiquidation(869,'Socio','NAVELINA',1,Decimal('34508'),Decimal('29891'),Decimal('4293'),Decimal('213'),(),Decimal('12190.51'),destruction_amount=Decimal('622.43'),rotten_amount=Decimal('-27.46'),destruction_price=Decimal('0.14500'),table_destruction_price=Decimal('0.14500'),rotten_price=Decimal('-0.12900'),gross_amount=Decimal('12785.48'),effective_net_kg=Decimal('34508'),total_amount=Decimal('1'),final_average_price=Decimal('1'),commercial_average_price=Decimal('0.40784'))
    vm=from_member_liquidation(h,m)
    assert vm.primary_label=='Exportación'; assert vm.secondary_label=='Mercado nacional'; assert vm.waste_label=='Podrido/Hojas'
    assert vm.commercial_kg == Decimal('34184')
    assert vm.secondary_price == Decimal('0.14500')
    assert vm.waste_price == Decimal('-0.12900')


def test_fixed_prices_are_identical_despite_member_amount_rounding():
    h=LiquidationHeader(1,'REM','2026','1','CITRICOS','','','','Normal','Primera','','',{}, {})
    prices=[]
    for member_id, kg in enumerate((Decimal('1327'), Decimal('2481'), Decimal('3116')), 1):
        secondary_amount=(kg*Decimal('0.14500')).quantize(Decimal('0.01'))
        rotten_amount=(kg*Decimal('-0.12900')).quantize(Decimal('0.01'))
        member=MemberLiquidation(member_id,'Socio','NAVELINA',1,kg*2,Decimal('0'),kg,kg,(),Decimal('0'),
            destruction_amount=secondary_amount,rotten_amount=rotten_amount,
            destruction_price=Decimal('0.14500'),table_destruction_price=Decimal('0.14500'),
            rotten_price=Decimal('-0.12900'),gross_amount=secondary_amount+rotten_amount,effective_net_kg=kg*2)
        vm=from_member_liquidation(h,member)
        prices.append((vm.secondary_price,vm.waste_price))
    assert prices == [(Decimal('0.14500'),Decimal('-0.12900'))]*3
