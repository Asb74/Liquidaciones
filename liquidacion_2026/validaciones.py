"""Validaciones de consistencia de liquidación."""

from __future__ import annotations

from decimal import Decimal

import pandas as pd


class ValidationError(ValueError):
    pass


def validar_columnas_minimas_pesosfres(df: pd.DataFrame) -> None:
    requeridas = [
        "Apodo",
        "semana",
        "Boleta",
        "Cal0",
        "Cal1",
        "Cal2",
        "Cal6",
        "Cal7",
        "Cal8",
        "DesLinea",
        "DesMesa",
        "Podrido",
    ]
    faltantes = [col for col in requeridas if col not in df.columns]
    if faltantes:
        raise ValidationError(f"Faltan columnas mínimas en PesosFres: {faltantes}")


def validar_tabla_no_vacia(df: pd.DataFrame, nombre: str) -> None:
    if df.empty:
        raise ValidationError(f"La tabla/dataset '{nombre}' está vacío.")


def validar_semanas_kilos_vs_anecop(kilos_semanas: set[int], anecop_semanas: set[int]) -> None:
    missing = sorted(kilos_semanas - anecop_semanas)
    if missing:
        raise ValidationError(f"Hay semanas con kilos comerciales sin ANECOP: {missing}")


def validar_referencia(ref: Decimal, semana_ref: int) -> None:
    if ref <= 0:
        raise ValidationError(f"Semana de referencia {semana_ref} inválida: precio AAA <= 0.")


def validar_total_rel(total_rel: Decimal) -> None:
    if total_rel <= 0:
        raise ValidationError("Total relativo de campaña <= 0. No se puede calcular coeficiente global.")


def validar_cuadre(recon: Decimal, objetivo: Decimal, tolerancia: Decimal = Decimal("0.01")) -> Decimal:
    descuadre = abs(recon - objetivo)
    if descuadre > tolerancia:
        raise ValidationError(
            f"Descuadre superior a tolerancia: recon={recon} objetivo={objetivo} descuadre={descuadre}"
        )
    return descuadre
