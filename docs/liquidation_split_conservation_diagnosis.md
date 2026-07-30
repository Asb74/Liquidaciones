# Diagnóstico de conservación del reparto

- **Archivo:** `Remesas/services/liquidation_split_service.py`
- **Función:** `LiquidationSplitService.split()` (aproximadamente líneas 108–127 antes de la corrección)
- **Origen:** `LiquidationSplitService._allocate()` ya compensa el redondeo de los
  conceptos repartidos, pero `split()` no lo usa para los resultados fiscales.
- **Campo que provoca la excepción:** `total_amount`.
- **Valor origen:** `3898.13`.
- **Valor repartido:** `1949.06 + 1949.06 = 3898.12`.
- **Diferencia:** `0.01`.
- **Quantum aplicado:** `0.01` (dinero).

`split()` reparte `taxable_base` y calcula independientemente la fiscalidad de
cada participante mediante `calculate_fiscal_result()`. Los dos cálculos de la
mitad se redondean al céntimo antes de sumarse. A diferencia de los campos que
pasan por `_allocate()`, el total fiscal no recibe después la diferencia en el
participante residual. La validación final compara por tanto `3898.13` con
`3898.12` y lanza el error genérico aunque el céntimo sea asignable.

La corrección debe reconciliar explícitamente IVA, retención y total contra las
magnitudes originales mediante aritmética `Decimal`, asignar cada diferencia al
participante residual y comprobar igualdad exacta después del ajuste. Una
diferencia no se considerará válida solamente por estar dentro de una
tolerancia.
