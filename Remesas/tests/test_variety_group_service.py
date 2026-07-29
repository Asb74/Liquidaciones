import sqlite3
import unittest

from data.deliveries_repository import DeliveriesRepository
from data.variety_repository import VarietyRepository
from domain.models import DeliveryFilter, Period, WorkContext
from domain.varieties import STATUS_EMPTY_GROUP, STATUS_GROUP, STATUS_NOT_FOUND, STATUS_VARIETY
from services.variety_group_service import VarietyGroupService
from datetime import date


class VarietyGroupServiceTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.execute("ATTACH DATABASE ':memory:' AS eepp")
        self.conn.execute('CREATE TABLE eepp.MVariedad(Id INTEGER, CULTIVO TEXT, Variedad TEXT, GRUPO TEXT, SUBGRUPO TEXT, CODROPA TEXT, ARTICULO TEXT, PRODUCTO TEXT, COLOR TEXT)')
        self.conn.executemany('INSERT INTO eepp.MVariedad VALUES(?,?,?,?,?,?,?,?,?)', [
            (1,'CITRICOS','SALUSTIANA','BLANCA','TEMPRANA',None,None,None,None),
            (2,'CITRICOS','NAVELINA','NAVEL','TEMPRANA',None,None,None,None),
            (3,'CITRICOS','FUKUMOTO','NAVEL','TEMPRANA',None,None,None,None),
            (4,'CITRICOS','NEWHALL','NAVEL','TEMPRANA',None,None,None,None),
            (5,'CITRICOS','NAVELATE','NAVEL','TEMPRANA',None,None,None,None),
            (6,'CITRICOS',None,'VACIO','GRUPO',None,None,None,None),
        ])
        self.service = VarietyGroupService(VarietyRepository(self.conn))

    def tearDown(self):
        self.conn.close()

    def test_real_variety(self):
        res = self.service.resolve_selection('CITRICOS', 'SALUSTIANA')
        self.assertEqual(res.status, STATUS_VARIETY)
        self.assertFalse(res.is_group)
        self.assertEqual(res.varieties, ('SALUSTIANA',))

    def test_blanca_temprana_group_includes_salustiana(self):
        res = self.service.resolve_selection('CITRICOS', 'BLANCA TEMPRANA')
        self.assertEqual(res.status, STATUS_GROUP)
        self.assertIn('SALUSTIANA', res.varieties)

    def test_navel_temprana_group_dynamic(self):
        res = self.service.resolve_selection('CITRICOS', 'NAVEL TEMPRANA')
        self.assertEqual(res.status, STATUS_GROUP)
        self.assertTrue(res.varieties)
        rows = self.conn.execute("SELECT Variedad FROM eepp.MVariedad WHERE CULTIVO='CITRICOS' AND GRUPO='NAVEL' AND SUBGRUPO='TEMPRANA'").fetchall()
        self.assertEqual(set(res.varieties), {r[0] for r in rows})

    def test_not_found(self):
        self.assertEqual(self.service.resolve_selection('CITRICOS', 'NO EXISTE').status, STATUS_NOT_FOUND)

    def test_empty_group(self):
        self.assertEqual(self.service.resolve_selection('CITRICOS', 'VACIO GRUPO').status, STATUS_EMPTY_GROUP)

    def test_duplicates_removed(self):
        _, varieties = self.service.resolve_many('CITRICOS', ['NAVEL TEMPRANA', 'NAVELINA'])
        self.assertEqual(len(varieties), len(set(varieties)))

    def test_deliveries_query_uses_resolved_varieties(self):
        self.conn.execute('CREATE TABLE PesosFres(CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, Fcarga TEXT, Reg INTEGER, IdSocio INTEGER, Variedad TEXT, Categoria TEXT, Neto REAL, NetoPartida REAL, Albaran TEXT, Boleta TEXT, Plataforma TEXT, Liquidado INTEGER, Coste_Recoleccion TEXT, SSocialRecoleccion TEXT, Manijeria TEXT, Coste_Trans TEXT)')
        self.conn.execute('CREATE TABLE eepp.DSocio(IdSocio INTEGER, Nombre TEXT)')
        self.conn.executemany('INSERT INTO PesosFres VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [
            ('2026','1','CITRICOS','2026-01-01',1,1,'SALUSTIANA','A',10,0,'','','',0,'0','0','0','0'),
            ('2026','1','CITRICOS','2026-01-01',2,1,'BLANCA TEMPRANA','A',10,0,'','','',0,'0','0','0','0'),
        ])
        res = self.service.resolve_selection('CITRICOS', 'BLANCA TEMPRANA')
        filters = DeliveryFilter(WorkContext('2026','1','CITRICOS'), Period(date(2026,1,1), date(2026,1,2)), list(res.varieties))
        rows, summary, *_ = DeliveriesRepository(self.conn).fetch(filters)
        self.assertEqual([r.variedad for r in rows], ['SALUSTIANA'])
