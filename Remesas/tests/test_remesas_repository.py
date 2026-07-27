from __future__ import annotations
import sqlite3, unittest
from data.remesas_repository import RemesasRepository

class RemesasRepositoryTests(unittest.TestCase):
    def test_list_remesas_filters_context(self):
        conn=sqlite3.connect(":memory:")
        conn.execute('CREATE TABLE PagosCIT(IdREMESA INTEGER, REMESA TEXT, FECHARE TEXT, PERIODO1 TEXT, PERIODO2 TEXT, CATEGORIA TEXT, TipoLiq TEXT, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT)')
        conn.executemany('INSERT INTO PagosCIT VALUES(?,?,?,?,?,?,?,?,?,?)', [(1,'Mandarina','2026-01-01','2026-01-01','2026-01-07','NORMAL','T','2026','1','MANDARINA'),(2,'Directo','2026-01-01','2026-01-01','2026-01-07','NORMAL','T','2026','1','DIRECTO'),(3,'Otra empresa','2026-01-01','2026-01-01','2026-01-07','NORMAL','T','2026','2','MANDARINA')])
        rows=RemesasRepository(conn).list_remesas('2026','1',' mandarina ')
        self.assertEqual([r['IdREMESA'] for r in rows], [1])

    def test_campaign_listing_includes_all_states_and_excludes_other_contexts(self):
        conn=sqlite3.connect(":memory:")
        conn.execute('CREATE TABLE PagosCIT(IdREMESA INTEGER, REMESA TEXT, FECHARE TEXT, PERIODO1 TEXT, PERIODO2 TEXT, CATEGORIA TEXT, TipoLiq TEXT, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, ESTADO TEXT)')
        conn.executemany('INSERT INTO PagosCIT VALUES(?,?,?,?,?,?,?,?,?,?,?)', [
            (1,'Pendiente antigua','2025-01-01','2025-01-01','2025-01-07','A','PARCIAL','2026','1','CITRICOS','Pendiente'),
            (2,'Liquidada','2026-07-01','2026-06-01','2026-06-30','B','FINAL','2026','1','CITRICOS','Liquidada'),
            (3,'Otra campaña',None,None,None,'A','FINAL','2025','1','CITRICOS','Guardada'),
            (4,'Otra empresa',None,None,None,'A','FINAL','2026','2','CITRICOS','Guardada'),
            (5,'Otro cultivo',None,None,None,'A','FINAL','2026','1','MANDARINA','Guardada'),
        ])
        rows=RemesasRepository(conn).list_remittances_for_campaign('2026','1','citricos')
        self.assertEqual([row['IdREMESA'] for row in rows], [2, 1])
        self.assertEqual({row['ESTADO'] for row in rows}, {'Pendiente', 'Liquidada'})

    def test_company_and_crop_are_optional_but_campaign_is_always_required(self):
        conn=sqlite3.connect(":memory:")
        conn.execute('CREATE TABLE PagosCIT(IdREMESA INTEGER, REMESA TEXT, FECHARE TEXT, PERIODO1 TEXT, PERIODO2 TEXT, CATEGORIA TEXT, TipoLiq TEXT, CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT)')
        conn.executemany('INSERT INTO PagosCIT VALUES(?,?,?,?,?,?,?,?,?,?)', [
            (1,'Uno',None,None,None,'A','T','2026','1','CITRICOS'),
            (2,'Dos',None,None,None,'A','T','2026','2','MANDARINA'),
            (3,'Tres',None,None,None,'A','T','2025','1','CITRICOS'),
        ])
        rows=RemesasRepository(conn).list_remittances_for_campaign('2026')
        self.assertEqual([row['IdREMESA'] for row in rows], [2, 1])

if __name__ == '__main__': unittest.main()
