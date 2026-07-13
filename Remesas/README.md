# Liquidaciones - Remesas

Primera versión visual y funcional del módulo `Remesas`, ejecutable de forma independiente e integrable como `ttk.Frame` mediante `ui.remesas_frame.RemesasFrame`.

## Arranque en Windows

```bat
cd /d C:\Liquidaciones\Remesas
python app.py
```

## Configuración

Las rutas SQLite se leen desde `config.ini`. Las conexiones se abren con `sqlite3` en modo lectura y `PRAGMA query_only = ON`. `DBEEPPL.sqlite` se adjunta como esquema `eepp` para cruzar `PesosFres` con `eepp.DSocio`.

## Tablas usadas

- `PesosFres`: entregas por campaña, empresa, cultivo y periodo.
- `PesosFresCon`: se valida su existencia para fases posteriores.
- `PagosCIT`: lectura de remesas existentes y precios `P0` a `P11`, `PDESTRIO`, `PDMESA`, `PPODRIDO`.
- `DLiquidaciones`: se valida su existencia, sin escritura.
- `eepp.DEEPP`: variedades reales por contexto.
- `eepp.DSocio`: nombre de socio por `IdSocio`.

## Limitaciones de fase 1

No calcula importes económicos, no guarda liquidaciones, no modifica SQLite, no toca Access y no genera PDF final. Los botones de cálculo, guardado, anulación y PDF quedan deshabilitados.
