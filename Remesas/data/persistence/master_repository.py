from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from .database import PersistenceDatabase


def _now(): return datetime.now(timezone.utc).isoformat()


class LiquidationMasterRepository:
    """CRUD local de prefijos y reglas; nunca abre las bases de Perceco."""
    def __init__(self,database: PersistenceDatabase): self.database=database

    def list_prefixes(self):
        with self.database.connect() as conn: return tuple(dict(r) for r in conn.execute("SELECT * FROM liquidation_prefixes ORDER BY crop"))

    def save_prefix(self,crop: str,prefix: str,*,active: bool=True,description: str | None=None) -> None:
        crop=crop.strip().upper(); prefix=prefix.strip().upper()
        if not crop: raise ValueError("El cultivo es obligatorio")
        if not prefix or " " in prefix: raise ValueError("El prefijo es obligatorio y no admite espacios")
        if len(prefix)!=2: raise ValueError("El prefijo debe tener dos caracteres")
        now=_now()
        with self.database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO liquidation_prefixes(crop,prefix,active,description,created_at,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(crop) DO UPDATE SET prefix=excluded.prefix,active=excluded.active,description=excluded.description,updated_at=excluded.updated_at",(crop,prefix,int(active),description,now,now)); conn.commit()

    def delete_prefix(self,crop: str) -> None:
        with self.database.connect() as conn: conn.execute("DELETE FROM liquidation_prefixes WHERE crop=?",(crop.strip().upper(),))

    def list_rules(self):
        with self.database.connect() as conn: return tuple(dict(r) for r in conn.execute("SELECT * FROM split_rules ORDER BY source_member_id,priority,id"))

    def get_rule(self,rule_id: int):
        with self.database.connect() as conn:
            row=conn.execute("SELECT * FROM split_rules WHERE id=?",(rule_id,)).fetchone()
            if row is None: raise ValueError("La regla ya no existe")
            result=dict(row); result["recipients"]=[(x["recipient_member_id"],x["recipient_member_name"] or "",x["value"],bool(x["is_residual"])) for x in conn.execute("SELECT * FROM split_rule_recipients WHERE rule_id=? ORDER BY sort_order,id",(rule_id,))]
            return result

    def save_rule(self,source_member_id: int,split_type: str,recipients,**filters) -> int:
        kind=split_type.strip().upper()
        if kind not in {"PERCENTAGE","PERCENTAGE_WITH_RESIDUAL","EQUAL_PARTS","WEIGHTS"}: raise ValueError("Tipo de reparto no soportado")
        if not recipients: raise ValueError("Debe indicar destinatarios")
        now=_now()
        with self.database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rule_id=filters.get("rule_id")
            common=(int(source_member_id),filters.get("source_member_name"),kind,filters.get("campaign"),filters.get("crop"),filters.get("variety"),filters.get("remittance_id"),filters.get("effective_from"),filters.get("effective_to"),int(filters.get("active",True)),int(filters.get("priority",100)),filters.get("notes"),now)
            if rule_id:
                conn.execute("UPDATE split_rules SET source_member_id=?,source_member_name=?,split_type=?,campaign=?,crop=?,variety=?,remittance_id=?,effective_from=?,effective_to=?,active=?,priority=?,notes=?,updated_at=? WHERE id=?",common+(int(rule_id),)); conn.execute("DELETE FROM split_rule_recipients WHERE rule_id=?",(rule_id,)); saved_id=int(rule_id)
            else:
                cur=conn.execute("INSERT INTO split_rules(source_member_id,source_member_name,split_type,campaign,crop,variety,remittance_id,effective_from,effective_to,active,priority,notes,source,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",common[:-1]+("LOCAL_MASTER",now,now)); saved_id=int(cur.lastrowid)
            for order,item in enumerate(recipients):
                member_id,name,value,residual=item
                conn.execute("INSERT INTO split_rule_recipients(rule_id,recipient_member_id,recipient_member_name,value,is_residual,sort_order) VALUES(?,?,?,?,?,?)",(saved_id,int(member_id),name,format(Decimal(value),"f"),int(residual),order))
            conn.commit(); return saved_id

    def delete_rule(self,rule_id: int) -> None:
        with self.database.connect() as conn: conn.execute("DELETE FROM split_rules WHERE id=?",(rule_id,))
