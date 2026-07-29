"""Idempotent backfill and diagnostics for persisted varietal classification."""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace
from presentation.liquidation_document_snapshot import dump, load, SCHEMA_VERSION
from services.persisted_variety_benchmark_service import variety_group_code
from services.variety_group_service import VarietyGroupResolutionError
from services.variety_selection_resolver import normalize_variety_token


@dataclass(frozen=True)
class VarietyGroupMigrationReport:
    total_rows: int
    resolved_rows: int
    rows_without_variety: int
    unrecognized_varieties: tuple[str, ...]
    groups_not_found: tuple[str, ...]


@dataclass(frozen=True)
class UnresolvedVarietyGroupDiagnostic:
    original_variety: str
    normalized_variety: str
    resolved_code: str | None
    group_found: str | None
    failure_reason: str
    affected_liquidations: int


def migrate_persisted_variety_groups(repository, group_service) -> VarietyGroupMigrationReport:
    """Backfill from ``liquidaciones.variedad``; never from remittance text."""
    with repository.database.connect() as conn:
        rows=conn.execute("""SELECT id,cultivo,variedad,variety_group_code
          FROM liquidaciones ORDER BY id""").fetchall()
    resolved=missing=0; unknown=[]; no_group=[]
    for row in rows:
        variety=str(row["variedad"] or "").strip()
        if not variety:
            missing+=1; continue
        if row["variety_group_code"]:
            resolved+=1; continue
        try:
            group,resolution=group_service.resolve_variety_group(str(row["cultivo"]),variety)
            repository.update_liquidation_variety_group(row["id"],variety_code=resolution.normalized_value,
                variety_name=resolution.selected_varieties[0],group_code=variety_group_code(group.group,group.subgroup),group_name=group.label)
            resolved+=1
        except VarietyGroupResolutionError as exc:
            (no_group if "grupo varietal" in str(exc) else unknown).append(variety)
    # Upgrade old document snapshots only from the now-classified lines. A
    # mixed-group document remains untouched so regeneration can report it.
    with repository.database.connect() as conn:
        snapshots=conn.execute("SELECT batch_id,recipient_member_id,payload_json FROM liquidation_document_snapshots").fetchall()
        for snapshot in snapshots:
            groups=conn.execute("""SELECT DISTINCT variety_code,variety_name,variety_group_code,variety_group_name
              FROM liquidaciones WHERE batch_id=? AND recipient_member_id=?
                AND TRIM(COALESCE(variety_group_code,''))<>''""",
                (snapshot["batch_id"],snapshot["recipient_member_id"])).fetchall()
            if len({row["variety_group_code"] for row in groups}) != 1:
                continue
            item=groups[0]
            vm=load(snapshot["payload_json"])
            payload=dump(replace(vm,variety_code=item["variety_code"],variety_name=item["variety_name"],
                variety_group_code=item["variety_group_code"],variety_group_name=item["variety_group_name"]))
            conn.execute("""UPDATE liquidation_document_snapshots SET payload_json=?,schema_version=?
              WHERE batch_id=? AND recipient_member_id=?""",
              (payload,SCHEMA_VERSION,snapshot["batch_id"],snapshot["recipient_member_id"]))
    return VarietyGroupMigrationReport(len(rows),resolved,missing,tuple(sorted(set(unknown))),tuple(sorted(set(no_group))))


def diagnose_unresolved_variety_groups(repository, group_service) -> tuple[UnresolvedVarietyGroupDiagnostic, ...]:
    with repository.database.connect() as conn:
        rows=conn.execute("""SELECT cultivo,variedad,COUNT(*) affected
          FROM liquidaciones WHERE TRIM(COALESCE(variety_group_code,''))=''
          GROUP BY cultivo,variedad ORDER BY affected DESC,variedad""").fetchall()
    result=[]
    for row in rows:
        original=str(row["variedad"] or "")
        code=group_name=None
        try:
            group,resolution=group_service.resolve_variety_group(str(row["cultivo"]),original)
            code=resolution.normalized_value; group_name=group.label; reason="Pendiente de migración"
        except VarietyGroupResolutionError as exc:
            reason=str(exc)
        result.append(UnresolvedVarietyGroupDiagnostic(original,normalize_variety_token(original),code,group_name,reason,int(row["affected"])))
    return tuple(result)
