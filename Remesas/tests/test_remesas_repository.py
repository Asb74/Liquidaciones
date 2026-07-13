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

if __name__ == '__main__': unittest.main()
