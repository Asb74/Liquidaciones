from __future__ import annotations
import os, sqlite3, tempfile, unittest
from pathlib import Path
from data.db_connection import AppConfig, ReadOnlyDatabase
from data.metadata_repository import MetadataRepository

class ConnectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); p=Path(self.tmp.name)
        self.fruta=p/'fruta.sqlite'; self.eepp=p/'eepp.sqlite'
        with sqlite3.connect(self.fruta) as c:
            c.execute('CREATE TABLE PagosCIT(IdREMESA INTEGER, REMESA TEXT)'); c.execute('CREATE TABLE PesosFres(CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, Fcarga TEXT, Reg INTEGER, IdSocio INTEGER, Variedad TEXT, Categoria TEXT, Neto REAL, Albaran TEXT, Boleta TEXT, Plataforma TEXT, Liquidado INTEGER)'); c.execute('CREATE TABLE PesosFresCon(x INTEGER)'); c.execute('CREATE TABLE DLiquidaciones(x INTEGER)')
        with sqlite3.connect(self.eepp) as c:
            c.execute('CREATE TABLE DEEPP(CAMPAÑA TEXT, EMPRESA TEXT, CULTIVO TEXT, Variedad TEXT)'); c.execute('CREATE TABLE DSocio(IdSocio INTEGER, Nombre TEXT)')
        self.cfg=AppConfig(str(self.fruta),str(self.eepp),'app','TEST',1,1,str(p/'x.log'),'INFO')
    def tearDown(self): self.tmp.cleanup()
    def test_access_and_tables(self):
        conn=ReadOnlyDatabase(self.cfg).connect_fruta_with_eepp(); repo=MetadataRepository(conn)
        for t in ['PagosCIT','PesosFres','PesosFresCon','DLiquidaciones']: self.assertTrue(repo.table_exists(t))
        for t in ['DEEPP','DSocio']: self.assertTrue(repo.table_exists(t,'eepp'))
    def test_missing_database(self):
        bad=AppConfig('missing.sqlite',self.cfg.db_eepp,'app','TEST',1,1,self.cfg.log_file,'INFO')
        with self.assertRaises(sqlite3.OperationalError): ReadOnlyDatabase(bad).connect_fruta_with_eepp()
if __name__ == '__main__': unittest.main()
