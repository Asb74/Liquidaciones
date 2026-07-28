from __future__ import annotations
import csv, hashlib, json, sqlite3, time, uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

TABLE_ORDER=("liquidation_prefixes","liquidation_sequences","split_rules","split_rule_recipients","liquidation_batches","liquidaciones","legacy_imports","liquidation_document_snapshots","generated_documents","exported_draft_documents","accounting_exports","accounting_export_items","liquidation_audit")
JSON_COLUMNS={"details","details_json","payload_json","batch_ids_json"}
BOOL_COLUMNS={"active","is_residual","is_split"}
MONEY_HINTS=("neto","imp_bruto","precio","recoleccion","cuota_ha","bp_calidad","b_transporte","b_global","base_i","iva","retencion","importe_total","amount_total","net_total","split_factor","value")

def normalize(value):
    if value is None:return {"type":"null"}
    if isinstance(value,Decimal): return {"type":"decimal","value":format(value,"f")}
    if isinstance(value,(datetime,date)): return {"type":"datetime","value":value.isoformat()}
    if isinstance(value,(dict,list)): return {"type":"json","value":value}
    return {"type":type(value).__name__,"value":value}

def fingerprint(rows, columns):
    digest=hashlib.sha256()
    for row in rows:
        digest.update(json.dumps([normalize(row[c]) for c in columns],sort_keys=True,separators=(",",":"),ensure_ascii=False).encode())
        digest.update(b"\n")
    return digest.hexdigest()

def inspect_sqlite(path:Path):
    result={"path":str(path.resolve()),"size":path.stat().st_size,"sha256":hashlib.sha256(path.read_bytes()).hexdigest(),"pragmas":{},"tables":[]}
    with sqlite3.connect(f"file:{path.as_posix()}?mode=ro",uri=True) as db:
        db.row_factory=sqlite3.Row
        for p in ("user_version","journal_mode","foreign_keys"): result["pragmas"][p]=db.execute(f"PRAGMA {p}").fetchone()[0]
        for item in db.execute("SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"):
            if item["type"]!="table":continue
            name=item["name"]; count=db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
            columns=[dict(r) for r in db.execute(f'PRAGMA table_info("{name}")')]
            fks=[dict(r) for r in db.execute(f'PRAGMA foreign_key_list("{name}")')]
            indexes=[]
            for idx in db.execute(f'PRAGMA index_list("{name}")'):
                indexes.append({"name":idx[1],"unique":bool(idx[2]),"columns":[r[2] for r in db.execute(f'PRAGMA index_info("{idx[1]}")')]})
            result["tables"].append({"name":name,"sql":item["sql"],"columns":columns,"foreign_keys":fks,"indexes":indexes,"row_count":count})
    return result

def _convert(column,value):
    if value is None:return None
    if column in JSON_COLUMNS:return json.loads(value) if isinstance(value,str) else value
    if column in BOOL_COLUMNS:return bool(value)
    if any(h==column for h in MONEY_HINTS):return Decimal(str(value))
    return value

def migrate_data(source:Path,connection,*,dry_run=False,validate_only=False,resume=False,batch_size=500,report_dir=Path("migration-report")):
    started=time.monotonic(); inventory=inspect_sqlite(source); report_dir.mkdir(parents=True,exist_ok=True)
    report={"date":datetime.now(timezone.utc).isoformat(),"source":inventory,"target":"postgresql://perceco_engine@192.168.1.3:5432/perceco","counts":{},"conflicts":[],"relationships":{},"fingerprints":{},"economic":{},"status":"FAILED"}
    if dry_run or validate_only:
        report["status"]="SUCCESS_WITH_WARNINGS"; _write(report,report_dir); return report
    run_id=uuid.uuid4(); connection.execute("INSERT INTO liquidaciones.migration_runs(run_id,source_path,source_sha256,status) VALUES(%s,%s,%s,'RUNNING')",(run_id,str(source),inventory["sha256"]))
    with sqlite3.connect(f"file:{source.as_posix()}?mode=ro",uri=True) as src:
        src.row_factory=sqlite3.Row
        existing={t["name"] for t in inventory["tables"]}
        for table in TABLE_ORDER:
            if table not in existing:continue
            cols=[r[1] for r in src.execute(f'PRAGMA table_info("{table}")')]; pk=[r[1] for r in src.execute(f'PRAGMA table_info("{table}")') if r[5]]
            total=src.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]; copied=0
            cur=src.execute(f'SELECT * FROM "{table}" ORDER BY '+(",".join(f'"{c}"' for c in pk) if pk else "rowid"))
            while rows:=cur.fetchmany(batch_size):
                for row in rows:
                    values=[_convert(c,row[c]) for c in cols]
                    if pk:
                        where=" AND ".join(f'"{c}"=%s' for c in pk); keys=[_convert(c,row[c]) for c in pk]
                        found=connection.execute(f'SELECT {",".join(chr(34)+c+chr(34) for c in cols)} FROM liquidaciones."{table}" WHERE {where}',keys).fetchone()
                        if found:
                            if [normalize(v) for v in found] != [normalize(v) for v in values]:
                                report["conflicts"].append({"table":table,"key":dict(zip(pk,[str(x) for x in keys]))}); raise RuntimeError(f"Conflicto de datos en {table}")
                            copied+=1; continue
                    placeholders=",".join(["%s"]*len(cols)); names=",".join(f'"{c}"' for c in cols)
                    connection.execute(f'INSERT INTO liquidaciones."{table}"({names}) VALUES({placeholders})',values); copied+=1
                connection.execute("INSERT INTO liquidaciones.migration_progress VALUES(%s,%s,%s,%s,CURRENT_TIMESTAMP) ON CONFLICT(run_id,table_name) DO UPDATE SET rows_copied=excluded.rows_copied,updated_at=excluded.updated_at",(run_id,table,None,copied))
            target=connection.execute(f'SELECT count(*) FROM liquidaciones."{table}"').fetchone()[0]
            report["counts"][table]={"source":total,"target":target,"copied_or_matched":copied}
    report["status"]="SUCCESS" if not report["conflicts"] else "FAILED"; report["duration_seconds"]=round(time.monotonic()-started,3)
    connection.execute("UPDATE liquidaciones.migration_runs SET status=%s,finished_at=CURRENT_TIMESTAMP,report_json=%s WHERE run_id=%s",(report["status"],json.dumps(report,default=str),run_id)); _write(report,report_dir); return report

def _write(report,directory):
    (directory/"migration_report.json").write_text(json.dumps(report,indent=2,ensure_ascii=False,default=str),encoding="utf-8")
    with (directory/"migration_counts.csv").open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(("table","source","target","copied_or_matched")); [w.writerow((k,*[v.get(x,"") for x in ("source","target","copied_or_matched")])) for k,v in report.get("counts",{}).items()]
    (directory/"migration_counts.json").write_text(json.dumps(report.get("counts",{}),indent=2),encoding="utf-8")
