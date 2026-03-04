# ══════════════════════════════════════════════════════════════
# STOP — Detiene backend y frontend del Family Office Reporting
# Uso: .\scripts\stop.ps1
# ══════════════════════════════════════════════════════════════

Write-Host ""
Write-Host "=== Deteniendo Family Office Reporting ===" -ForegroundColor Yellow

# 1. Matar procesos Python en los puertos 8000 y 8501
$ports = @(8000, 8501)
foreach ($port in $ports) {
    $conns = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    foreach ($conn in $conns) {
        $proc = Get-Process -Id $conn.OwningProcess -ErrorAction SilentlyContinue
        if ($proc) {
            Write-Host "  Matando $($proc.ProcessName) (PID $($proc.Id)) en puerto $port" -ForegroundColor Red
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        }
    }
}

# 2. Esperar a que los puertos se liberen
Start-Sleep -Seconds 2

# 3. Verificar
$still = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue
$still2 = Get-NetTCPConnection -LocalPort 8501 -State Listen -ErrorAction SilentlyContinue

if ($still -or $still2) {
    Write-Host "  ADVERTENCIA: Aun hay procesos en los puertos. Forzando solo PIDs de 8000/8501..." -ForegroundColor Red
    $remaining = @()
    if ($still) { $remaining += $still }
    if ($still2) { $remaining += $still2 }

    $pids = $remaining | Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($pid in $pids) {
        if ($pid) {
            $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Host "  Forzando $($proc.ProcessName) (PID $pid)" -ForegroundColor Red
                Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
            }
        }
    }
    Start-Sleep -Seconds 2
}

$final8000 = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue
$final8501 = Get-NetTCPConnection -LocalPort 8501 -State Listen -ErrorAction SilentlyContinue

if ($final8000 -or $final8501) {
    Write-Host "  ERROR: No se pudieron liberar los puertos 8000/8501" -ForegroundColor Red
    exit 1
}

Write-Host "  OK: Puertos 8000 y 8501 libres" -ForegroundColor Green
Write-Host ""
