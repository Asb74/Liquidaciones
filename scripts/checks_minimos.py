from decimal import Decimal

import pandas as pd

from liquidacion_2026.calculador import calcular_modelo_final
from liquidacion_2026.globalgap import calcular_fondo_globalgap


def q(x):
    return Decimal(str(x))


pesos_df = pd.DataFrame(
    [
        {"semana": 1, "apodo": 1, "boleta": "B1", "idsocio": "S1", **{f"cal{i}": 10 if i == 0 else 0 for i in range(12)}, "deslinea": 1, "desmesa": 1, "podrido": 1},
        {"semana": 1, "apodo": 1, "boleta": "B2", "idsocio": "S2", **{f"cal{i}": 20 if i == 1 else 0 for i in range(12)}, "deslinea": 0, "desmesa": 0, "podrido": 0},
    ]
)

deepp_df = pd.DataFrame(
    [
        {"boleta": "B1", "idsocio": "S1", "certificacion": " global gap ", "nivelglobal": "N1"},
        {"boleta": "B2", "idsocio": "S2", "certificacion": "OTRA", "nivelglobal": "N1"},
    ]
)
mnivel_df = pd.DataFrame([{"nivel": "N1", "indice": q("1")}])
bon_global_df = pd.DataFrame([{"bonificacion": q("0.1"), "campaña": 2026, "cultivo": "KAKIS", "empresa": 1}])

fondo, audit_socios, audit_inc = calcular_fondo_globalgap(pesos_df, deepp_df, mnivel_df, bon_global_df)
assert fondo == q("1"), fondo
assert "S2" not in set(audit_socios["idsocio"])
assert "socio_sin_gg" in set(audit_inc["tipo"]) if not audit_inc.empty else False

calibre_map = pd.DataFrame([
    {"calibre": "cal0", "grupo": "AAA", "categoria": "I"},
    {"calibre": "cal1", "grupo": "AAA", "categoria": "II"},
])
anecop_df = pd.DataFrame([
    {"semana": 1, "grupo": "AAA", "precio_base": q("2")},
    {"semana": 1, "grupo": "AA", "precio_base": q("1")},
    {"semana": 1, "grupo": "A", "precio_base": q("0.5")},
])

res = calcular_modelo_final(
    pesos_df=pesos_df,
    calibre_map=calibre_map,
    anecop_df=anecop_df,
    precios_destrio={"deslinea": q("0"), "desmesa": q("0"), "podrido": q("0")},
    bruto_campana=q("100"),
    otros_fondos=q("0"),
    fondo_gg_total=q("1"),
    ratio_categoria_ii=q("0.5"),
)

df = res.precios_df.pivot(index=["semana", "calibre"], columns="categoria", values="precio_final").reset_index()
for _, row in df.iterrows():
    assert row["II"] == (row["I"] * q("0.5")).quantize(q("0.0001"))

print("OK checks_minimos")
