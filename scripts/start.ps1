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

# ── 1b. Detectar otro backend "intruso" en puertos 8000/8501 ─
# Salvaguarda: otro proyecto (p.ej. "Reporting empresas operativas") corre
# uvicorn con --reload y vuelve a apoderarse del puerto. Si detectamos un
# proceso Python en 8000/8501 cuyo cmdline NO menciona "backend.main",
# "frontend/app.py" o este project root, lo matamos ANTES de levantar lo nuestro.
$foRoot = $projectRoot
foreach ($port in @(8000, 8501)) {
    $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    if ($conn) {
        $pidnum = $conn.OwningProcess
        $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $pidnum" -ErrorAction SilentlyContinue
        if ($proc) {
            $cmd = $proc.CommandLine
            $isFo = $cmd -and ($cmd -match "backend\.main" -or $cmd -match "frontend[/\\]app\.py" -or $cmd -match [regex]::Escape($foRoot))
            if (-not $isFo) {
                Write-Host "  Puerto ${port}: proceso ajeno al FO detectado (PID $pidnum). Cmd:" -ForegroundColor Yellow
                Write-Host "    $cmd" -ForegroundColor DarkYellow
                Write-Host "  Matando para liberar ${port}..." -ForegroundColor Red
                Stop-Process -Id $pidnum -Force -ErrorAction SilentlyContinue
                Start-Sleep -Seconds 1
                # Matar también sus hijos Python (caso uvicorn --reload que spawnea child)
                Get-CimInstance Win32_Process -Filter "ParentProcessId = $pidnum" -ErrorAction SilentlyContinue | ForEach-Object {
                    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
                }
            }
        }
    }
}

# ── 2. Verificar venv ────────────────────────────────────────
Write-Host "[2/5] Verificando entorno virtual..." -ForegroundColor Yellow
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
$backendStdOut = Join-Path $projectRoot "backend_runtime.log"
$backendStdErr = Join-Path $projectRoot "backend_runtime.err"
$frontendStdOut = Join-Path $projectRoot "frontend_runtime.log"
$frontendStdErr = Join-Path $projectRoot "frontend_runtime.err"
if (-not (Test-Path $pythonExe)) {
    Write-Host "  ERROR: No se encontro .venv\Scripts\python.exe" -ForegroundColor Red
    Write-Host "  Ejecuta: python -m venv .venv ; .\.venv\Scripts\Activate.ps1 ; pip install -e '.[dev]'" -ForegroundColor Red
    exit 1
}
Write-Host "  Python: $pythonExe" -ForegroundColor Gray

foreach ($logFile in @($backendStdOut, $backendStdErr, $frontendStdOut, $frontendStdErr)) {
    if (Test-Path $logFile) {
        Remove-Item $logFile -Force -ErrorAction SilentlyContinue
    }
}

# ── 3. Levantar backend ─────────────────────────────────────
Write-Host "[3/5] Iniciando backend (puerto 8000)..." -ForegroundColor Yellow

$backendJob = Start-Process -FilePath $pythonExe `
    -ArgumentList "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000" `
    -WorkingDirectory $projectRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $backendStdOut `
    -RedirectStandardError $backendStdErr `
    -PassThru

Write-Host "  Backend PID: $($backendJob.Id)"

# Esperar a que el backend esté listo (detectar puerto escuchando)
Write-Host "  Esperando backend..." -NoNewline
$ready = $false
for ($i = 0; $i -lt 45; $i++) {
    Start-Sleep -Seconds 1
    Write-Host "." -NoNewline
    if ($backendJob.HasExited) {
        break
    }
    try {
        $check = & $pythonExe -c "import httpx; r = httpx.get('http://127.0.0.1:8000/api/v1/health', timeout=5); print(r.status_code)" 2>$null
        if ($check -match "200") {
            $ready = $true
            break
        }
    } catch { }
}
Write-Host ""

if (-not $ready) {
    Write-Host "  ERROR: Backend no responde en puerto 8000" -ForegroundColor Red
    if ($backendJob.HasExited) {
        Write-Host "  El proceso backend termino antes de responder." -ForegroundColor Red
    }
    if (Test-Path $backendStdErr) {
        Write-Host "  --- backend stderr (ultimas 40 lineas) ---" -ForegroundColor DarkYellow
        Get-Content $backendStdErr -Tail 40
    }
    if (Test-Path $backendStdOut) {
        Write-Host "  --- backend stdout (ultimas 40 lineas) ---" -ForegroundColor DarkYellow
        Get-Content $backendStdOut -Tail 40
    }
    exit 1
}
Write-Host "  Backend OK" -ForegroundColor Green

# ── 4. Levantar frontend ────────────────────────────────────
Write-Host "[4/5] Iniciando frontend (puerto 8501)..." -ForegroundColor Yellow
$frontendJob = Start-Process -FilePath $pythonExe `
    -ArgumentList "-m", "streamlit", "run", "frontend/app.py", "--server.port", "8501", "--server.address", "0.0.0.0", "--server.headless", "true" `
    -WorkingDirectory $projectRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $frontendStdOut `
    -RedirectStandardError $frontendStdErr `
    -PassThru

Write-Host "  Frontend PID: $($frontendJob.Id)"

# Esperar a que el frontend esté listo (detectar puerto escuchando)
Write-Host "  Esperando frontend..." -NoNewline
$ready2 = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    Write-Host "." -NoNewline
    if ($frontendJob.HasExited) {
        break
    }
    try {
        $check2 = & $pythonExe -c "import httpx; r = httpx.get('http://127.0.0.1:8501', timeout=5); print(r.status_code)" 2>$null
        if ($check2 -match "200") {
            $ready2 = $true
            break
        }
    } catch { }
}
Write-Host ""

if (-not $ready2) {
    Write-Host "  ADVERTENCIA: Frontend no responde aun." -ForegroundColor Yellow
    if ($frontendJob.HasExited) {
        Write-Host "  El proceso frontend termino antes de responder." -ForegroundColor Yellow
    }
    if (Test-Path $frontendStdErr) {
        Write-Host "  --- frontend stderr (ultimas 40 lineas) ---" -ForegroundColor DarkYellow
        Get-Content $frontendStdErr -Tail 40
    }
} else {
    Write-Host "  Frontend OK" -ForegroundColor Green
}

# ── 5. Resumen ───────────────────────────────────────────────
Write-Host ""
Write-Host "[5/5] Estado final:" -ForegroundColor Cyan
Write-Host "  Backend:  http://localhost:8000/api/v1/health" -ForegroundColor White
Write-Host "  Frontend: http://localhost:8501" -ForegroundColor White
# Mostrar enlace para otros en la red local (IP del equipo)
try {
    $ip = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -notmatch "Loopback" -and $_.IPAddress -notmatch "^169\.254" } | Select-Object -First 1).IPAddress
    if ($ip) {
        Write-Host "  Enlace para otros en la red: http://${ip}:8501" -ForegroundColor Green
    }
} catch { }
Write-Host ""
Write-Host "  Para detener: .\scripts\stop.ps1" -ForegroundColor Gray
Write-Host "  Si alguien de la red no puede entrar: en esta PC, Firewall de Windows debe permitir entrante en puerto 8501." -ForegroundColor Gray
Write-Host "  Para reiniciar tras cambios: .\scripts\stop.ps1 ; .\scripts\start.ps1" -ForegroundColor Gray
Write-Host ""
