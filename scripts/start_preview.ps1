# ============================================================================
# START PREVIEW - Levanta instancia de prueba (backend 8100 + frontend 8601)
# Uso: .\scripts\start_preview.ps1
# ============================================================================

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot

$backendPort = 8100
$frontendPort = 8601
$backendUrl = "http://localhost:$backendPort"
$backendApiUrl = "$backendUrl/api/v1"

$previewDbPath = Join-Path $projectRoot "data\db\fo_reporting_preview.db"
$previewDbUrl = "sqlite:///" + ($previewDbPath -replace "\\", "/")

Write-Host ""
Write-Host "=== Family Office Reporting PREVIEW ===" -ForegroundColor Cyan
Write-Host "  Directorio: $projectRoot"
Write-Host ""

Write-Host "[1/6] Deteniendo instancia preview previa..." -ForegroundColor Yellow
& "$PSScriptRoot\stop_preview.ps1"

Write-Host "[2/6] Verificando entorno virtual..." -ForegroundColor Yellow
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    Write-Host "  ERROR: No se encontro .venv\Scripts\python.exe" -ForegroundColor Red
    Write-Host "  Ejecuta: python -m venv .venv ; .\.venv\Scripts\Activate.ps1 ; pip install -e '.[dev]'" -ForegroundColor Red
    exit 1
}
Write-Host "  Python: $pythonExe" -ForegroundColor Gray

Write-Host "[3/6] Sincronizando DB preview..." -ForegroundColor Yellow
& "$PSScriptRoot\sync_preview_db.ps1"

# Guardar variables previas para no contaminar la sesion actual
$oldDbUrl = $env:FO_DATABASE_URL
$oldCorsOrigins = $env:FO_CORS_ORIGINS
$oldBackendApiUrl = $env:FO_BACKEND_API_URL
$oldUiMode = $env:FO_UI_MODE

try {
    Write-Host "[4/6] Iniciando backend preview (puerto $backendPort)..." -ForegroundColor Yellow
    $env:FO_DATABASE_URL = $previewDbUrl
    $env:FO_CORS_ORIGINS = ('["http://localhost:{0}","http://127.0.0.1:{0}"]' -f $frontendPort)

    $backendJob = Start-Process -FilePath $pythonExe `
        -ArgumentList "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "$backendPort" `
        -WorkingDirectory $projectRoot `
        -WindowStyle Hidden `
        -PassThru

    Write-Host "  Backend PREVIEW PID: $($backendJob.Id)"

    Write-Host "  Esperando backend preview..." -NoNewline
    $ready = $false
    for ($i = 0; $i -lt 45; $i++) {
        Start-Sleep -Seconds 1
        Write-Host "." -NoNewline
        $listening = Get-NetTCPConnection -LocalPort $backendPort -State Listen -ErrorAction SilentlyContinue
        if ($listening) {
            Start-Sleep -Seconds 2
            try {
                $check = & $pythonExe -c "import httpx; r = httpx.get('$backendApiUrl/health', timeout=5); print(r.status_code)" 2>$null
                if ($check -match "200") {
                    $ready = $true
                    break
                }
            } catch { }
        }
    }
    Write-Host ""

    if (-not $ready) {
        Write-Host "  ERROR: Backend preview no responde" -ForegroundColor Red
        exit 1
    }
    Write-Host "  Backend PREVIEW OK" -ForegroundColor Green

    Write-Host "[5/6] Iniciando frontend preview (puerto $frontendPort)..." -ForegroundColor Yellow
    $env:FO_BACKEND_API_URL = $backendApiUrl
    $env:FO_UI_MODE = "preview"

    $frontendJob = Start-Process -FilePath $pythonExe `
        -ArgumentList "-m", "streamlit", "run", "frontend/app.py", "--server.port", "$frontendPort", "--server.headless", "true" `
        -WorkingDirectory $projectRoot `
        -WindowStyle Hidden `
        -PassThru

    Write-Host "  Frontend PREVIEW PID: $($frontendJob.Id)"

    Write-Host "  Esperando frontend preview..." -NoNewline
    $ready2 = $false
    for ($i = 0; $i -lt 35; $i++) {
        Start-Sleep -Seconds 1
        Write-Host "." -NoNewline
        $listening2 = Get-NetTCPConnection -LocalPort $frontendPort -State Listen -ErrorAction SilentlyContinue
        if ($listening2) {
            $ready2 = $true
            break
        }
    }
    Write-Host ""

    if (-not $ready2) {
        Write-Host "  ADVERTENCIA: Frontend preview aun no responde, puede tardar unos segundos" -ForegroundColor Yellow
    } else {
        Write-Host "  Frontend PREVIEW OK" -ForegroundColor Green
    }
}
finally {
    if ($null -eq $oldDbUrl) { Remove-Item Env:FO_DATABASE_URL -ErrorAction SilentlyContinue } else { $env:FO_DATABASE_URL = $oldDbUrl }
    if ($null -eq $oldCorsOrigins) { Remove-Item Env:FO_CORS_ORIGINS -ErrorAction SilentlyContinue } else { $env:FO_CORS_ORIGINS = $oldCorsOrigins }
    if ($null -eq $oldBackendApiUrl) { Remove-Item Env:FO_BACKEND_API_URL -ErrorAction SilentlyContinue } else { $env:FO_BACKEND_API_URL = $oldBackendApiUrl }
    if ($null -eq $oldUiMode) { Remove-Item Env:FO_UI_MODE -ErrorAction SilentlyContinue } else { $env:FO_UI_MODE = $oldUiMode }
}

Write-Host ""
Write-Host "[6/6] Estado final PREVIEW:" -ForegroundColor Cyan
Write-Host "  Backend PREVIEW:  $backendApiUrl/health" -ForegroundColor White
Write-Host "  Frontend PREVIEW: http://localhost:$frontendPort" -ForegroundColor White
Write-Host ""
Write-Host "  Para detener preview: .\scripts\stop_preview.ps1" -ForegroundColor Gray
Write-Host "  Para re-sincronizar DB preview: .\scripts\sync_preview_db.ps1" -ForegroundColor Gray
Write-Host ""
