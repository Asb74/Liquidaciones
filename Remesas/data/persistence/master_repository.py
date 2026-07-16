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

    def save_rule(self,source_member_id: int,split_type: str,recipients,**filters) -> int:
        kind=split_type.strip().upper()
        if kind not in {"PERCENTAGE","PERCENTAGE_WITH_RESIDUAL","EQUAL_PARTS","WEIGHTS"}: raise ValueError("Tipo de reparto no soportado")
        if not recipients: raise ValueError("Debe indicar destinatarios")
        now=_now()
        with self.database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur=conn.execute("INSERT INTO split_rules(source_member_id,source_member_name,split_type,campaign,crop,variety,remittance_id,priority,notes,source,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",(int(source_member_id),filters.get("source_member_name"),kind,filters.get("campaign"),filters.get("crop"),filters.get("variety"),filters.get("remittance_id"),int(filters.get("priority",100)),filters.get("notes"),"LOCAL_MASTER",now,now))
            for order,item in enumerate(recipients):
                member_id,name,value,residual=item
                conn.execute("INSERT INTO split_rule_recipients(rule_id,recipient_member_id,recipient_member_name,value,is_residual,sort_order) VALUES(?,?,?,?,?,?)",(cur.lastrowid,int(member_id),name,format(Decimal(value),"f"),int(residual),order))
            conn.commit(); return int(cur.lastrowid)

    def delete_rule(self,rule_id: int) -> None:
        with self.database.connect() as conn: conn.execute("DELETE FROM split_rules WHERE id=?",(rule_id,))
