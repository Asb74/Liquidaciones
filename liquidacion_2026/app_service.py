"""Orquestación del proceso de liquidación para UI."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd

from .calculador import ResultadoCalculo, calcular_modelo_final
from .config import CALIBRES, DESTRIOS, DBPaths, LiquidacionConfig
from .correspondencia_calibres import build_calibre_mapping
from .exportador import exportar_todo
from .extractor_sqlite import SQLiteExtractor
from .globalgap import calcular_fondo_globalgap
from .normalizador_anecop import cargar_anecop
from .utils import parse_decimal

LOGGER = logging.getLogger(__name__)


@dataclass
class RunOutput:
    resultado: ResultadoCalculo
    files: dict[str, Path]
    auditoria: dict[str, pd.DataFrame]


def build_config(
    *,
    campana: int,
    empresa: int,
    cultivo: str,
    bruto_campana: Decimal,
    otros_fondos: Decimal,
    ratio_categoria_ii: Decimal,
    anecop_path: Path,
    db_fruta: Path,
    db_calidad: Path,
    db_eeppl: Path,
    precios_destrio: dict[str, Decimal],
) -> LiquidacionConfig:
    if campana != 2025:
        raise ValueError("Validación configurada para campaña 2025.")
    if empresa != 1:
        raise ValueError("Empresa válida para este modelo: 1.")
    if cultivo.strip().upper() != "KAKIS":
        raise ValueError("Cultivo válido para este modelo: KAKIS.")

    return LiquidacionConfig(
        campana=campana,
        empresa=empresa,
        cultivo=cultivo.strip().upper(),
        bruto_campana=bruto_campana,
        otros_fondos=otros_fondos,
        ratio_categoria_ii=ratio_categoria_ii,
        precios_destrio=precios_destrio,
        anecop_path=anecop_path,
        db_paths=DBPaths(fruta=db_fruta, calidad=db_calidad, eeppl=db_eeppl),
        output_dir=Path("salidas"),
    )


def configurar_logging(output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    log_file = output_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")],
        force=True,
    )
    return log_file


def _build_audit_kilos_semana_df(
    *,
    pesos_df: pd.DataFrame,
    calibre_map: pd.DataFrame,
    campana: int,
    empresa: int,
    cultivo: str,
) -> pd.DataFrame:
    long_comercial = pesos_df.melt(
        id_vars=["semana"],
        value_vars=CALIBRES,
        var_name="calibre",
        value_name="kilos",
    )
    long_comercial["kilos"] = pd.to_numeric(long_comercial["kilos"], errors="coerce").fillna(0)
    long_comercial = long_comercial.merge(calibre_map, on="calibre", how="inner", validate="m:1")
    long_comercial = long_comercial.groupby(["semana", "grupo", "categoria"], as_index=False)["kilos"].sum()
    long_comercial["concepto"] = "comercial"

    long_destrio = pesos_df.melt(id_vars=["semana"], value_vars=DESTRIOS, var_name="destrio", value_name="kilos")
    long_destrio["kilos"] = pd.to_numeric(long_destrio["kilos"], errors="coerce").fillna(0)
    long_destrio = long_destrio.groupby(["semana", "destrio"], as_index=False)["kilos"].sum()
    long_destrio["concepto"] = "destrio"
    long_destrio["grupo"] = "DESTRIO"
    long_destrio["categoria"] = long_destrio["destrio"].str.upper()

    audit_kilos = pd.concat(
        [
            long_comercial[["semana", "concepto", "grupo", "categoria", "kilos"]],
            long_destrio[["semana", "concepto", "grupo", "categoria", "kilos"]],
        ],
        ignore_index=True,
    )

    audit_kilos.insert(0, "campaña", campana)
    audit_kilos.insert(1, "empresa", empresa)
    audit_kilos.insert(2, "cultivo", cultivo)
    return audit_kilos.sort_values(["semana", "concepto", "grupo", "categoria"]).reset_index(drop=True)


def _quantize_df(df: pd.DataFrame, columns: list[str], decimals: int = 4) -> pd.DataFrame:
    quant = Decimal("1").scaleb(-decimals)
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = out[col].map(lambda value: parse_decimal(value).quantize(quant, rounding=ROUND_HALF_UP))
    return out


def run(config: LiquidacionConfig) -> RunOutput:
    extractor = SQLiteExtractor(str(config.db_paths.fruta), str(config.db_paths.calidad), str(config.db_paths.eeppl))

    anecop_df = cargar_anecop(config.anecop_path)
    pesos_df = extractor.fetch_pesosfres(config.campana, config.empresa, config.cultivo)
    calibre_map = build_calibre_mapping(extractor.fetch_correspondencias_calibres())

    deepp_df = extractor.fetch_deepp()
    mnivel_df = extractor.fetch_mnivel_global()
    bon_global_df = extractor.fetch_bon_global(config.campana, config.cultivo, config.empresa)

    fondo_gg_total, audit_globalgap_socios_df, audit_df = calcular_fondo_globalgap(pesos_df, deepp_df, mnivel_df, bon_global_df)

    audit_kilos_semana_df = _build_audit_kilos_semana_df(
        pesos_df=pesos_df,
        calibre_map=calibre_map,
        campana=config.campana,
        empresa=config.empresa,
        cultivo=config.cultivo,
    )

    audit_globalgap_socios_df.insert(0, "campaña", config.campana)
    audit_globalgap_socios_df.insert(1, "empresa", config.empresa)
    audit_globalgap_socios_df.insert(2, "cultivo", config.cultivo)

    total_kilos_comercial_por_semana = sum(
        audit_kilos_semana_df.loc[audit_kilos_semana_df["concepto"] == "comercial", "kilos"].map(parse_decimal),
        Decimal("0"),
    )
    total_kilos_gg = sum(audit_globalgap_socios_df["kilos_gg"].map(parse_decimal), Decimal("0"))
    fondo_gg_total_audit = sum(audit_globalgap_socios_df["fondo_gg_eur"].map(parse_decimal), Decimal("0"))

    LOGGER.info("total_kilos_comercial_por_semana=%s", total_kilos_comercial_por_semana)
    LOGGER.info("total_kilos_gg=%s", total_kilos_gg)
    LOGGER.info("fondo_gg_total=%s", fondo_gg_total_audit)

    if abs(fondo_gg_total_audit - fondo_gg_total) > Decimal("0.01"):
        raise ValueError(
            "El fondo GG auditado no coincide con el fondo usado en cálculo "
            f"({fondo_gg_total_audit} vs {fondo_gg_total})."
        )

    resultado = calcular_modelo_final(
        pesos_df=pesos_df,
        calibre_map=calibre_map,
        anecop_df=anecop_df,
        precios_destrio=config.precios_destrio,
        bruto_campana=config.bruto_campana,
        otros_fondos=config.otros_fondos,
        fondo_gg_total=fondo_gg_total,
        ratio_categoria_ii=config.ratio_categoria_ii,
    )
    resultado.resumen_metricas["bruto"] = config.bruto_campana
    resultado.resumen_metricas["otros_fondos"] = config.otros_fondos

    files = exportar_todo(
        precios_df=resultado.precios_df,
        campana=config.campana,
        cultivo=config.cultivo,
        audit_df=audit_df,
        resumen_metricas=resultado.resumen_metricas,
        output_dir=config.output_dir,
        export_decimals=config.export_decimals,
    )

    auditoria = {
        "audit_kilos_semana_df": _quantize_df(audit_kilos_semana_df, ["kilos"]),
        "audit_globalgap_socios_df": _quantize_df(
            audit_globalgap_socios_df,
            ["kilos_gg", "indice", "bonif_eur_kg", "fondo_gg_eur"],
        ),
    }

    LOGGER.info("Liquidación completada. Archivo Perceco: %s", files["perceco"])
    return RunOutput(resultado=resultado, files=files, auditoria=auditoria)
