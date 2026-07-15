from __future__ import annotations

from dataclasses import is_dataclass
from datetime import datetime
import logging
from decimal import Decimal
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEBUG_HECTARE_FEE = True
PERCECO_REFERENCE: dict[int, Decimal] = {883: Decimal("1036.55"), 869: Decimal("179.40")}


def _s(v: Any) -> str:
    return "" if v is None else str(v)


def _money(v: Any) -> str:
    return "" if v is None else f"{v} €"


def _sheet(wb, title: str, rows: list[dict[str, Any]]):
    ws = wb.create_sheet(title)
    headers = []
    for r in rows:
        for k in r:
            if k not in headers:
                headers.append(k)
    if not headers:
        headers = ["Mensaje"]
        rows = [{"Mensaje": "Sin datos"}]
    ws.append(headers)
    for r in rows:
        ws.append([_s(r.get(h)) for h in headers])
    return ws


def export_hectare_fee_audit(result: Any, output_dir: Path) -> tuple[Path, Path] | None:
    if not DEBUG_HECTARE_FEE:
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "auditoria_cuota_ha.log"
    xlsx_path = output_dir / "auditoria_cuota_ha.xlsx"
    header = result.header
    logger.info("LiquidationHeader=%s", vars(header) if is_dataclass(header) else header)
    master = getattr(result, "hectare_fee_master", None)
    price = getattr(master, "price_per_hectare", None)
    surface_crops = tuple(getattr(master, "surface_crops", ()))
    delivery_crops = tuple(getattr(master, "delivery_crops", ()))
    lines: list[str] = []
    socios_rows: list[dict[str, Any]] = []
    deepp_rows: list[dict[str, Any]] = []
    dparcela_rows: list[dict[str, Any]] = []
    pesos_rows: list[dict[str, Any]] = []
    diag_rows: list[dict[str, Any]] = []

    def line(x=""):
        lines.append(str(x))

    line("AUDITORÍA CUOTA HA")
    line(f"Fecha/hora: {datetime.now().isoformat(sep=' ', timespec='seconds')}")
    line(f"Campaña: {header.campana}")
    line(f"Empresa: {header.empresa}")
    line(f"Cultivo remesa: {header.cultivo}")
    line(f"Remesa: {header.remesa_name} ({header.remesa_id})")
    line(f"Precio hectárea: {price}")
    line(f"Cultivos configurados para calcular superficie: {', '.join(surface_crops)}")
    line(f"Cultivos configurados para calcular kilos: {', '.join(delivery_crops)}")

    for m in result.member_results:
        audit = getattr(m, "hectare_fee_audit", None)
        parcels = list(getattr(m, "hectare_fee_parcels", ()) or ())
        deliveries = list(getattr(m, "hectare_fee_delivery_audit", ()) or ())
        perceco = PERCECO_REFERENCE.get(int(m.member_id)) if str(m.member_id).isdigit() else None
        diff = None if perceco is None or m.hectare_fee_amount is None else Decimal(str(m.hectare_fee_amount)) - perceco
        estado = "OK" if diff == 0 else ("NO COINCIDE" if perceco is not None else "SIN REFERENCIA")
        socios_rows.append({"IdSocio": m.member_id, "Nombre": m.member_name, "Variedad": m.variety, "Kg línea": m.net_kg, "Superficie": m.applicable_hectares, "Cuota anual": m.hectare_fee_total_member, "Kg campaña": m.hectare_fee_total_effective_kg, "Índice": m.hectare_fee_rate_per_kg, "Cuota Python": m.hectare_fee_amount, "Cuota Perceco": perceco, "Diferencia": diff, "Estado": estado})
        line("\n====================================================")
        line("SOCIO")
        line("====================================================")
        line(f"IdSocio: {m.member_id}")
        line(f"Nombre: {m.member_name}")
        line(f"Variedad de la línea: {m.variety}")
        line(f"Kg de la línea: {m.net_kg}")
        line("\nFASE 1 - BUSCAR REGISTROS EN DEEPP")
        sql = next((r.get("Consulta SQL DEEPP") for r in parcels if r.get("Consulta SQL DEEPP")), "")
        params = next((r.get("Parámetros DEEPP") for r in parcels if r.get("Parámetros DEEPP")), "")
        deepp_count = next((r.get("Número DEEPP encontrados") for r in parcels if r.get("Número DEEPP encontrados") is not None), len({r.get("Boleta DEEPP") for r in parcels if r.get("Boleta DEEPP")}))
        line(sql); line(f"Parámetros: {params}"); line(f"Número de registros encontrados: {deepp_count}")
        for r in parcels:
            deepp = {"IdSocio": m.member_id, "Boleta": r.get("Boleta DEEPP"), "CHA": r.get("CHA original"), "Cultivo": r.get("Cultivo DEEPP"), "Campaña": r.get("Campaña DEEPP"), "Empresa": r.get("Empresa DEEPP"), "Variedad": r.get("Variedad DEEPP"), "FechaPlantacion": r.get("FechaPlantacion"), "Certificacion": r.get("Certificacion"), "NivelGlobal": r.get("NivelGlobal")}
            deepp_rows.append(deepp); line(" | ".join(f"{k}={v}" for k, v in deepp.items()))
        boletas_originales = [r.get("Boleta DEEPP") for r in parcels if r.get("Boleta DEEPP")]
        boletas_norm = [str(b).strip() for b in boletas_originales]
        boletas_unique = list(dict.fromkeys(boletas_norm))
        line("\nFASE 2 - NORMALIZAR BOLETAS")
        line(f"Boletas originales: {boletas_originales}"); line(f"Boletas normalizadas: {boletas_norm}"); line(f"Boletas únicas: {boletas_unique}")
        line("\nFASE 3/4/5 - DPARCELA, EVALUACIÓN Y SUPERFICIE")
        acc = Decimal("0")
        for r in parcels:
            dparcela_rows.append({**r, "IdSocio": m.member_id})
            line(f"Consulta SQL: {r.get('Consulta SQL DParcela','')}"); line(f"Parámetros: {r.get('Parámetros DParcela','')}")
            line(" | ".join(f"{k}={r.get(k)}" for k in ("Boleta DParcela","Campaña DParcela","Empresa DParcela","Cultivo DParcela","Pol","Par","Rec","SupCul DParcela","SupRec","SupApor","Año","Alta DParcela","Baja DParcela")))
            included = r.get("Incluida") == "Sí"
            line(f"CHA activo: {r.get('CHA activo')} | Parcela dada de baja: {'No' if not r.get('Baja DParcela') else 'Sí'} | Año plantación: {r.get('Año')} | Antigüedad: {r.get('Antigüedad')} | Mayor de cinco años: {r.get('Antigüedad suficiente')} | Cultivo permitido: {'Sí' if r.get('Cultivo DParcela') in surface_crops else 'No'} | Empresa correcta: {'Sí' if str(r.get('Empresa DParcela')) == str(header.empresa) else 'No'} | Campaña correcta: {'Sí' if str(r.get('Campaña DParcela')) == str(header.campana) else 'No'} | Superficie válida: {'Sí' if Decimal(str(r.get('SupCul DParcela') or 0)) > 0 else 'No'}")
            line(f"Resultado: {'INCLUIDA' if included else 'EXCLUIDA'} {r.get('Motivo exclusión','')}")
            if included:
                acc += Decimal(str(r.get("SupCul DParcela") or 0)); line(f"Parcela: {r.get('Clave deduplicación')} | Superficie: {r.get('SupCul DParcela')} | Acumulado: {acc}")
        line(f"Total superficie: {m.applicable_hectares}")
        line("\nFASE 6 - CUOTA ANUAL"); line(f"{m.applicable_hectares}\n×\n{price}\n=\n{_money(m.hectare_fee_total_member)}")
        line("\nFASE 7 - KILOS CAMPAÑA")
        kg_acc = Decimal("0")
        for r in deliveries:
            pesos_rows.append({**r, "IdSocio": m.member_id})
            used = r.get("NetoEfectivo") or Decimal("0"); kg_acc += used
            rule = "NetoPartida = 0: usa Neto" if Decimal(str(r.get("NetoPartida") or 0)) == 0 else "NetoPartida distinto de 0: usa NetoPartida"
            line(f"Registro {r.get('Registro')} | Fecha={r.get('Fecha')} | Cultivo={r.get('Cultivo')} | Boleta={r.get('Boleta')} | Neto={r.get('Neto')} | NetoPartida={r.get('NetoPartida')} | Usado={used} | {rule} | Acumulado={kg_acc}")
        line(f"TOTAL KG CAMPAÑA: {m.hectare_fee_total_effective_kg}")
        line("\nFASE 8 - ÍNDICE"); line(f"{m.hectare_fee_total_member}\n/\n{m.hectare_fee_total_effective_kg}\n=\n{m.hectare_fee_rate_per_kg}")
        line("\nFASE 9 - CUOTA DE ESTA LÍNEA"); line(f"{m.net_kg}\n×\n{m.hectare_fee_rate_per_kg}\n=\n{_money(m.hectare_fee_amount)}")
        line("\nFASE 10 - COMPARACIÓN CON PERCECO"); line(f"Python: {m.hectare_fee_amount} | Perceco: {perceco} | Diferencia: {diff}")
        first = "Sin referencia Perceco detallada; primera comparación disponible: cuota final." if perceco is None else ("Sin diferencias detectadas en referencia disponible." if diff == 0 else f"*************\nPRIMERA DIFERENCIA DETECTADA\n*************\nCuota línea Python={m.hectare_fee_amount} Perceco={perceco} Diferencia={diff}")
        line("\nFASE 11 - PRIMER DATO DISTINTO"); line(first)
        diag_rows.append({"IdSocio": m.member_id, "Primer dato distinto": first, "Estado": estado})
        line("\nRESUMEN FINAL"); line(f"Superficie={m.applicable_hectares} | Cuota anual={m.hectare_fee_total_member} | Kg campaña={m.hectare_fee_total_effective_kg} | Índice={m.hectare_fee_rate_per_kg} | Kg línea={m.net_kg} | Cuota Python={m.hectare_fee_amount} | Cuota Perceco={perceco} | Diferencia={diff} | Estado={estado}")

    log_path.write_text("\n".join(lines), encoding="utf-8")
    from openpyxl import Workbook
    wb = Workbook(); wb.remove(wb.active)
    _sheet(wb, "1 Socios", socios_rows); _sheet(wb, "2 DEEPP", deepp_rows); _sheet(wb, "3 DParcela", dparcela_rows); _sheet(wb, "4 PesosFres", pesos_rows); _sheet(wb, "5 Diagnóstico", diag_rows)
    wb.save(xlsx_path)
    return log_path, xlsx_path
