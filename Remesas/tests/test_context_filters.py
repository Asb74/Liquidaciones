from __future__ import annotations
import sqlite3, tempfile, unittest
from pathlib import Path
from data.db_connection import AppConfig, ReadOnlyDatabase
from data.metadata_repository import MetadataRepository
from domain.models import Period, WorkContext
from domain.validators import validate_period
from datetime import date

class ContextFilterTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); p=Path(self.tmp.name); self.fruta=p/'f.sqlite'; self.eepp=p/'e.sqlite'
        with sqlite3.connect(self.fruta) as c:
            c.execute('CREATE TABLE PesosFres(CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT)'); c.executemany('INSERT INTO PesosFres VALUES(?,?,?)',[('2026','01','NAR'),('2026','02','CIT')])
        with sqlite3.connect(self.eepp) as c:
            c.execute('CREATE TABLE DEEPP(CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, Variedad TEXT)'); c.executemany('INSERT INTO DEEPP VALUES(?,?,?,?)',[('2026','01','NAR','V1'),('2026','01','NAR','V1'),('2026','01','NAR',None)])
            c.execute('CREATE TABLE DSocio(IdSocio INTEGER, Nombre TEXT)')
        cfg=AppConfig(str(self.fruta),str(self.eepp),'app','TEST',1,1,str(p/'x.log'),'INFO'); self.repo=MetadataRepository(ReadOnlyDatabase(cfg).connect_fruta_with_eepp())
    def tearDown(self): self.tmp.cleanup()
    def test_load_context_and_varieties(self):
        self.assertEqual(self.repo.campaigns(), ['2026']); self.assertEqual(self.repo.empresas('2026'), ['01','02']); self.assertEqual(self.repo.cultivos('2026','01'), ['NAR']); self.assertEqual(self.repo.variedades('2026','01','NAR'), ['V1'])
    def test_invalid_period(self):
        with self.assertRaises(ValueError): validate_period(Period(date(2026,2,1), date(2026,1,1)))
if __name__ == '__main__': unittest.main()
