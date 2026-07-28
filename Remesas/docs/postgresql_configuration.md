# Configuración de PostgreSQL en Windows

La aplicación obtiene la contraseña **exclusivamente** de la variable de entorno
`POSTGRES_PASSWORD`. No la escriba en el código, en archivos versionados ni en
incidencias o capturas de pantalla.

## Sesión actual de PowerShell

```powershell
$env:POSTGRES_PASSWORD = "CONTRASEÑA_REAL"
```

Este valor sólo dura durante esa consola. Inicie la aplicación desde ella. No
guarde ese comando con la contraseña real en Git ni en un archivo `.ps1`
versionado, y evite que aparezca en una captura.

## Configuración persistente del usuario

```powershell
[Environment]::SetEnvironmentVariable(
    "POSTGRES_PASSWORD",
    "CONTRASEÑA_REAL",
    "User"
)
```

Cierre y vuelva a abrir PowerShell y la aplicación para que reciban el nuevo
entorno. El comando puede quedar en el historial de PowerShell; bórrelo siguiendo
la política de seguridad local y nunca publique ese historial.

## Comprobación segura

Esta comprobación no imprime el secreto:

```powershell
if ([string]::IsNullOrWhiteSpace($env:POSTGRES_PASSWORD)) {
    Write-Host "POSTGRES_PASSWORD no configurada"
} else {
    Write-Host "POSTGRES_PASSWORD configurada"
}
```

Desde `C:\Liquidaciones\Remesas`, el diagnóstico de solo lectura se ejecuta con:

```powershell
python -m db_tools db check
python -m db_tools db check --json
```

El diagnóstico muestra host, puerto, base, usuario, esquema y migraciones, pero
nunca muestra la contraseña ni una cadena de conexión que la contenga.
