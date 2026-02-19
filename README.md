# liquidacion_2026

Sistema modular en **Python 3.11+** para normalización de precios y liquidación de la campaña 2026 de **kakis**, leyendo datos desde SQLite y exportando CSV compatible con Perceco.

## Estructura

- `liquidacion_2026/config.py`: configuración y constantes.
- `liquidacion_2026/extractor_sqlite.py`: acceso a SQLite.
- `liquidacion_2026/correspondencia_calibres.py`: mapping de calibres a grupo económico.
- `liquidacion_2026/globalgap.py`: cálculo de fondo GlobalGAP.
- `liquidacion_2026/calculador.py`: motor de cálculo económico.
- `liquidacion_2026/validaciones.py`: validaciones obligatorias.
- `liquidacion_2026/exportador.py`: exportación de resultados.
- `liquidacion_2026/main.py`: CLI y orquestación.

## Requisitos

- Python 3.11+
- pandas

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Ejecución

```bash
python -m liquidacion_2026.main \
  --campana 2026 \
  --empresa "MI_EMPRESA" \
  --cultivo "KAKIS" \
  --db-fruta /ruta/DBfruta.sqlite \
  --db-calidad /ruta/BdCalidad.sqlite \
  --db-eeppl /ruta/DBEEPPL.sqlite \
  --precios-anecop '{"45": {"AAA": 0.50, "AA": 0.42, "A": 0.35}}' \
  --precios-destrio '{"DesLinea": -0.01, "DesMesa": -0.02, "Podrido": -0.05}' \
  --output salida_liquidacion.csv \
  --audit-globalgap auditoria_globalgap_no_match.csv
```

## Validaciones implementadas

- Merge con `validate='m:1'`.
- Control de duplicados en claves críticas.
- Control de semanas sin precio orientativo.
- Control de calibres sin mapping económico.
- Control de ingreso teórico ANECOP igual a cero.
- Cuadre económico final con tolerancia de 0.01.
- Generación de `auditoria_globalgap_no_match.csv` para boletas sin nivel.

## Notas de cálculo

- Redondeo interno a 4 decimales con `Decimal`.
- Redondeo de exportación a 2 decimales.
- Salida final: `campaña, semana, calibre, categoria, precio_final`.
