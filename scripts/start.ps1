# ══════════════════════════════════════════════════════════════
# START — Inicia backend y frontend del Family Office Reporting
# Uso: .\scripts\start.ps1
#
# IMPORTANTE: Este script:
#   1. Detiene cualquier instancia previa (puertos 8000/8501)
#   2. Activa el venv
#   3. Levanta backend (puerto 8000) SIN --reload
#   4. Levanta frontend (puerto 8501)
#   5. Verifica que ambos responden
#
# SIN --reload: Los cambios de código requieren reiniciar
# manualmente con .\scripts\stop.ps1 y luego .\scripts\start.ps1
# ══════════════════════════════════════════════════════════════

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot

Write-Host ""
Write-Host "=== Family Office Reporting ===" -ForegroundColor Cyan
Write-Host "  Directorio: $projectRoot"
Write-Host ""

# ── 1. Detener instancias previas ───────────────────────────
Write-Host "[1/5] Deteniendo instancias previas..." -ForegroundColor Yellow
& "$PSScriptRoot\stop.ps1"

# ── 2. Verificar venv ────────────────────────────────────────
Write-Host "[2/5] Verificando entorno virtual..." -ForegroundColor Yellow
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    Write-Host "  ERROR: No se encontro .venv\Scripts\python.exe" -ForegroundColor Red
    Write-Host "  Ejecuta: python -m venv .venv ; .\.venv\Scripts\Activate.ps1 ; pip install -e '.[dev]'" -ForegroundColor Red
    exit 1
}
Write-Host "  Python: $pythonExe" -ForegroundColor Gray

# ── 3. Levantar backend ─────────────────────────────────────
Write-Host "[3/5] Iniciando backend (puerto 8000)..." -ForegroundColor Yellow

$backendJob = Start-Process -FilePath $pythonExe `
    -ArgumentList "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000" `
    -WorkingDirectory $projectRoot `
    -WindowStyle Hidden `
    -PassThru

Write-Host "  Backend PID: $($backendJob.Id)"

# Esperar a que el backend esté listo (detectar puerto escuchando)
Write-Host "  Esperando backend..." -NoNewline
$ready = $false
for ($i = 0; $i -lt 45; $i++) {
    Start-Sleep -Seconds 1
    Write-Host "." -NoNewline
    $listening = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue
    if ($listening) {
        # Puerto abierto, ahora verificar que responde HTTP
        Start-Sleep -Seconds 2
        try {
            $check = & $pythonExe -c "import httpx; r = httpx.get('http://localhost:8000/api/v1/health', timeout=5); print(r.status_code)" 2>$null
            if ($check -match "200") {
                $ready = $true
                break
            }
        } catch { }
    }
}
Write-Host ""

if (-not $ready) {
    Write-Host "  ERROR: Backend no responde en puerto 8000" -ForegroundColor Red
    exit 1
}
Write-Host "  Backend OK" -ForegroundColor Green

# ── 4. Levantar frontend ────────────────────────────────────
Write-Host "[4/5] Iniciando frontend (puerto 8501)..." -ForegroundColor Yellow
$frontendJob = Start-Process -FilePath $pythonExe `
    -ArgumentList "-m", "streamlit", "run", "frontend/app.py", "--server.port", "8501", "--server.headless", "true" `
    -WorkingDirectory $projectRoot `
    -WindowStyle Hidden `
    -PassThru

Write-Host "  Frontend PID: $($frontendJob.Id)"

# Esperar a que el frontend esté listo (detectar puerto escuchando)
Write-Host "  Esperando frontend..." -NoNewline
$ready2 = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    Write-Host "." -NoNewline
    $listening2 = Get-NetTCPConnection -LocalPort 8501 -State Listen -ErrorAction SilentlyContinue
    if ($listening2) {
        $ready2 = $true
        break
    }
}
Write-Host ""

if (-not $ready2) {
    Write-Host "  ADVERTENCIA: Frontend no responde aun, puede tardar unos segundos mas" -ForegroundColor Yellow
} else {
    Write-Host "  Frontend OK" -ForegroundColor Green
}

# ── 5. Resumen ───────────────────────────────────────────────
Write-Host ""
Write-Host "[5/5] Estado final:" -ForegroundColor Cyan
Write-Host "  Backend:  http://localhost:8000/api/v1/health" -ForegroundColor White
Write-Host "  Frontend: http://localhost:8501" -ForegroundColor White
Write-Host ""
Write-Host "  Para detener: .\scripts\stop.ps1" -ForegroundColor Gray
Write-Host "  Para reiniciar tras cambios: .\scripts\stop.ps1 ; .\scripts\start.ps1" -ForegroundColor Gray
Write-Host ""
