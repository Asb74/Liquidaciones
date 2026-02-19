# liquidacion_2026

Aplicación de escritorio para calcular la liquidación final KAKIS por campaña usando matriz relativa ANECOP + coeficiente global único.

## Ejecutar

```bash
python -m liquidacion_2026
```

## Flujo de validación (campaña 2025)

1. Abrir la aplicación con `python -m liquidacion_2026`.
2. Cargar:
   - Campaña `2025`
   - Empresa `1`
   - Cultivo `KAKIS`
   - Bruto campaña (ejemplo real de campaña)
   - Otros fondos (si aplica)
   - Precios de destrío (DesLinea/DesMesa/Podrido)
3. Seleccionar rutas de:
   - `DBfruta.sqlite`
   - `BdCalidad.sqlite`
   - `DBEEPPL.sqlite`
   - ANECOP (Excel ECOC tal cual o CSV normalizado `semana,grupo_anecop,kg,valor_fruta`)
4. Pulsar **Ejecutar**.
5. Revisar resumen auditable y validar que el descuadre absoluto sea <= 0.01.
6. Exportar o revisar archivos en `salidas/`.

## Salidas

- `salidas/precios_perceco_{campania}_{cultivo}.csv`
- `salidas/auditoria_gg_boletas_no_match.csv`
- `salidas/resumen_campania.csv`
- `salidas/run_YYYYMMDD_HHMM.log`
