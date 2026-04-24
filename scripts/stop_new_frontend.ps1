# ══════════════════════════════════════════════════════════════
# STOP NEW FRONTEND — Detiene el servidor estatico del frontend nuevo
# Uso: .\scripts\stop_new_frontend.ps1
# ══════════════════════════════════════════════════════════════

Write-Host ""
Write-Host "=== Deteniendo Reporting APP (frontend nuevo) ===" -ForegroundColor Yellow

$port = 8701
$conns = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
foreach ($conn in $conns) {
    $proc = Get-Process -Id $conn.OwningProcess -ErrorAction SilentlyContinue
    if ($proc) {
        Write-Host "  Matando $($proc.ProcessName) (PID $($proc.Id)) en puerto $port" -ForegroundColor Red
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    }
}

Start-Sleep -Seconds 1

$still = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
if ($still) {
    Write-Host "  ADVERTENCIA: puerto $port aun ocupado" -ForegroundColor Red
    exit 1
}

Write-Host "  OK: Puerto $port libre" -ForegroundColor Green
Write-Host ""
