import sqlite3
import unittest
from data.metadata_repository import MetadataRepository

class SpecialVarietiesTests(unittest.TestCase):
    def setUp(self):
        self.conn=sqlite3.connect(':memory:')
        self.conn.execute("ATTACH DATABASE ':memory:' AS eepp")
        self.conn.execute('CREATE TABLE PesosFres(CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, Fcarga TEXT, Variedad TEXT)')
        self.conn.execute('CREATE TABLE PesosFresCon(CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, Fcarga TEXT, Variedad TEXT)')
        self.conn.execute('CREATE TABLE eepp.DEEPP(CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, Variedad TEXT)')
        self.conn.executemany('INSERT INTO eepp.DEEPP VALUES(?,?,?,?)',[('2026','01','MANDARINA','ORT'),('2026','01','MANDARINA','ORT'),('2026','01','NARANJA','NAV')])
        rows=[('2026','01','DIRECTO','2026-01-01','KAKI'),('2026','01','DIRECTO','2026-01-02',''),('2026','01','DIRECTO','2026-01-03',None),('2026','01','DIRECTOCHF','2026-01-01','SANDIA'),('2026','01','INDUSTRIA','2026-01-01','ALB')]
        self.conn.executemany('INSERT INTO PesosFres VALUES(?,?,?,?,?)', rows)
        self.conn.execute('INSERT INTO PesosFresCon VALUES(?,?,?,?,?)', ('2026','01','DIRECTO','2026-01-04','LIMON'))
        self.repo=MetadataRepository(self.conn)

    def test_normal_crop_uses_deepp(self):
        self.assertEqual(self.repo.variedades('2026','01','MANDARINA'), ['ORT'])

    def test_special_crops_use_real_deliveries(self):
        self.assertEqual(self.repo.variedades('2026','01','DIRECTO'), ['KAKI','LIMON'])
        self.assertEqual(self.repo.variedades('2026','01','DIRECTOCHF'), ['SANDIA'])
        self.assertEqual(self.repo.variedades('2026','01','INDUSTRIA'), ['ALB'])

if __name__ == '__main__':
    unittest.main()
