# Corrección del reparto porcentual y borrado de reglas

## Causa raíz

`LiquidationSplitService.factors()` sumaba el porcentaje no configurado al factor
del destinatario marcado como residual. Por ello, un destinatario configurado al
50 % terminaba con factor 1 y recibía la entrega completa, en vez de reservar el
50 % restante para el socio origen. La marca residual debe decidir únicamente
quién absorbe la diferencia producida por el redondeo de `_allocate()`.

Por otra parte, `LiquidationMasterRepository.delete_rule()` borraba primero la
fila padre de `split_rules`, aunque aún existían filas hijas en
`split_rule_recipients`. Las claves foráneas de las bases existentes impiden ese
orden y provocaban `sqlite3.IntegrityError`.
