# ══════════════════════════════════════════════════════════════
# START NEW FRONTEND — Sirve "Reporting APP" (HTML estático)
# Uso: .\scripts\start_new_frontend.ps1
#
# IMPORTANTE:
#   - Este script NO toca la app antigua (Streamlit 8501).
#   - Sirve el frontend nuevo en puerto 8701, bindeado a 0.0.0.0
#     para que sea accesible desde la red de oficina.
#   - El backend (FastAPI 8000) debe estar corriendo (.\scripts\start.ps1)
#     para que los endpoints /api/v1/{master,dictionary,reporting} respondan.
# ══════════════════════════════════════════════════════════════

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$appDir = Join-Path $projectRoot "Reporting APP"
$port = 8701
$stdOut = Join-Path $projectRoot "new_frontend_runtime.log"
$stdErr = Join-Path $projectRoot "new_frontend_runtime.err"

Write-Host ""
Write-Host "=== Reporting APP (frontend nuevo) ===" -ForegroundColor Cyan
Write-Host "  Directorio: $appDir"
Write-Host ""

if (-not (Test-Path $appDir)) {
    Write-Host "  ERROR: No existe '$appDir'" -ForegroundColor Red
    exit 1
}

# ── 1. Detener instancia previa ─────────────────────────────────
Write-Host "[1/3] Deteniendo instancia previa del frontend nuevo..." -ForegroundColor Yellow
& "$PSScriptRoot\stop_new_frontend.ps1"

# ── 2. Verificar venv (usamos el python del proyecto) ──────────
Write-Host "[2/3] Verificando entorno virtual..." -ForegroundColor Yellow
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    Write-Host "  ERROR: No se encontro .venv\Scripts\python.exe" -ForegroundColor Red
    exit 1
}
Write-Host "  Python: $pythonExe" -ForegroundColor Gray

foreach ($logFile in @($stdOut, $stdErr)) {
    if (Test-Path $logFile) { Remove-Item $logFile -Force -ErrorAction SilentlyContinue }
}

# ── 3. Levantar servidor estático ──────────────────────────────
Write-Host "[3/3] Iniciando servidor estatico (puerto $port)..." -ForegroundColor Yellow

# python -m http.server <port> --bind 0.0.0.0 --directory "<appDir>"
# IMPORTANTE: la ruta tiene espacios. Se envuelve en "" dentro del argumento
# para que el proceso hijo reciba un solo token en --directory.
$quotedAppDir = '"' + $appDir + '"'
$job = Start-Process -FilePath $pythonExe `
    -ArgumentList "-m", "http.server", "$port", "--bind", "0.0.0.0", "--directory", $quotedAppDir `
    -WorkingDirectory $projectRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdOut `
    -RedirectStandardError $stdErr `
    -PassThru

Write-Host "  Frontend nuevo PID: $($job.Id)"

# Esperar a que el servidor escuche
Write-Host "  Esperando servidor..." -NoNewline
$ready = $false
for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Seconds 1
    Write-Host "." -NoNewline
    if ($job.HasExited) { break }
    $listening = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    if ($listening) { $ready = $true; break }
}
Write-Host ""

if (-not $ready) {
    Write-Host "  ERROR: Servidor estatico no respondio en puerto $port" -ForegroundColor Red
    if (Test-Path $stdErr) { Get-Content $stdErr -Tail 40 }
    exit 1
}
Write-Host "  Servidor estatico OK" -ForegroundColor Green

# Resumen
Write-Host ""
Write-Host "Estado final:" -ForegroundColor Cyan
Write-Host "  Frontend nuevo local: http://localhost:$port" -ForegroundColor White
try {
    $ip = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -notmatch "Loopback" -and $_.IPAddress -notmatch "^169\.254" } | Select-Object -First 1).IPAddress
    if ($ip) {
        Write-Host "  Frontend nuevo red:   http://${ip}:$port" -ForegroundColor Green
    }
} catch { }
Write-Host ""
Write-Host "  Recuerda: necesitas el backend corriendo en 8000 (.\scripts\start.ps1)." -ForegroundColor Gray
Write-Host "  Para detener: .\scripts\stop_new_frontend.ps1" -ForegroundColor Gray
Write-Host ""
