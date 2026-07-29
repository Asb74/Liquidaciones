# Diagnóstico del benchmark de producción del socio 1623

## Causa raíz identificada antes de la corrección

La implementación histórica de `GroupBenchmarkRepository.get_productive_hectares`
no identificaba una fila de `DParcela` por su identidad física. Construía una clave
con boleta, campaña, empresa, cultivo, `IdPM`, polígono, parcela y recinto, y agrupaba
las superficies en un conjunto. Cuando dos filas físicas distintas compartían esos
datos catastrales pero tenían superficies diferentes, el grupo completo se marcaba
como `excluded_conflicting_surfaces`; cuando tenían la misma superficie, solo se
sumaba una. Por tanto, se descartaban superficies productivas legítimas.

En el caso 1623 esa deduplicación catastral reducía las siete boletas del grupo
NAVEL TEMPRANA a aproximadamente 1,0552 ha. Al dividir 112.745 kg por esa superficie
incompleta se obtenían los 106.851 kg/ha mostrados en el PDF.

La identidad correcta para deduplicar es el identificador de la fila física de
`DParcela` (`rowid` de SQLite): apariciones repetidas del mismo identificador por un
`JOIN` se cuentan una vez, pero identificadores distintos se suman aunque el resto
de sus columnas coincida.

## Alcance de la corrección

La consulta debe recuperar todas las filas de `DParcela` asociadas a todas las
boletas coincidentes y la validación debe decidir fila por fila. Después se suma
primero por boleta y luego por socio/grupo. El mismo resultado calculado se entrega
al servicio que construye el benchmark consumido por el PDF; no existe una fórmula
alternativa para la auditoría.
