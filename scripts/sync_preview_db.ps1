# =====================================================================
# SYNC PREVIEW DB - Clona DB principal a DB preview de forma consistente
# Uso: .\scripts\sync_preview_db.ps1
# =====================================================================

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"

$sourceDb = Join-Path $projectRoot "data\db\fo_reporting.db"
$targetDb = Join-Path $projectRoot "data\db\fo_reporting_preview.db"

Write-Host ""
Write-Host "=== Sincronizando DB PREVIEW ===" -ForegroundColor Cyan
Write-Host "  Source: $sourceDb"
Write-Host "  Target: $targetDb"

if (-not (Test-Path $pythonExe)) {
    Write-Host "  ERROR: No se encontro .venv\\Scripts\\python.exe" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $sourceDb)) {
    Write-Host "  ERROR: No existe DB origen: $sourceDb" -ForegroundColor Red
    exit 1
}

$previewListening = Get-NetTCPConnection -LocalPort 8100,8601 -State Listen -ErrorAction SilentlyContinue
if ($previewListening) {
    Write-Host "  ERROR: Preview esta corriendo (puertos 8100/8601)." -ForegroundColor Red
    Write-Host "  Ejecuta primero: .\scripts\stop_preview.ps1" -ForegroundColor Red
    exit 1
}

$targetDir = Split-Path -Parent $targetDb
if (-not (Test-Path $targetDir)) {
    New-Item -ItemType Directory -Path $targetDir -Force | Out-Null
}

# Backup nativo de SQLite (consistente con WAL) sin romper por rutas con espacios.
& $pythonExe -c "import sqlite3,sys; src=sqlite3.connect(sys.argv[1]); dst=sqlite3.connect(sys.argv[2]); src.backup(dst); dst.close(); src.close()" $sourceDb $targetDb
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ERROR: Fallo el backup de DB" -ForegroundColor Red
    exit 1
}

Write-Host "  OK: DB preview sincronizada" -ForegroundColor Green
Write-Host ""
