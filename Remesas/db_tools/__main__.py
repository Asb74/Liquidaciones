from __future__ import annotations
import argparse, json
from pathlib import Path
from .migrations import migrate
from .postgres import PostgresConnectionFactory,PostgresSettings
from .sqlite_migrator import inspect_sqlite,migrate_data

def main():
 p=argparse.ArgumentParser(); sub=p.add_subparsers(dest="group",required=True); db=sub.add_parser("db"); cmd=db.add_subparsers(dest="command",required=True)
 cmd.add_parser("migrate")
 inv=cmd.add_parser("inventory"); inv.add_argument("--source",type=Path,required=True); inv.add_argument("--output",type=Path,default=Path("sqlite_inventory.json"))
 data=cmd.add_parser("migrate-data"); data.add_argument("--source",type=Path,required=True); data.add_argument("--target",choices=["postgresql"],default="postgresql"); data.add_argument("--dry-run",action="store_true"); data.add_argument("--validate-only",action="store_true"); data.add_argument("--resume",action="store_true"); data.add_argument("--batch-size",type=int,default=500); data.add_argument("--report-dir",type=Path,default=Path("migration-report"))
 val=cmd.add_parser("validate"); val.add_argument("--source",type=Path,required=True); val.add_argument("--report-dir",type=Path,default=Path("migration-report"))
 rb=cmd.add_parser("rollback-test"); rb.add_argument("--run-id",required=True)
 a=p.parse_args()
 if a.command=="inventory": a.output.write_text(json.dumps(inspect_sqlite(a.source),indent=2,ensure_ascii=False),encoding="utf-8"); print(a.output); return
 factory=PostgresConnectionFactory(PostgresSettings())
 with factory.connect() as conn:
  if a.command=="migrate": print("Migraciones aplicadas:",migrate(conn,Path(__file__).resolve().parents[1]/"migrations"/"postgresql"))
  elif a.command in ("migrate-data","validate"): print(migrate_data(a.source,conn,dry_run=getattr(a,"dry_run",False),validate_only=a.command=="validate" or getattr(a,"validate_only",False),resume=getattr(a,"resume",False),batch_size=getattr(a,"batch_size",500),report_dir=a.report_dir)["status"])
  else:
   # Sólo elimina metadatos de una ejecución; las cargas funcionales deben hacerse en esquema temporal para rollback seguro.
   conn.execute("DELETE FROM liquidaciones.migration_runs WHERE run_id=%s",(a.run_id,)); print("Metadatos de ejecución de prueba eliminados")
if __name__=="__main__":main()
