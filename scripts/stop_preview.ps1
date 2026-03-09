# ================================================================
# STOP PREVIEW - Detiene backend y frontend de la instancia preview
# Uso: .\scripts\stop_preview.ps1
# ================================================================

Write-Host ""
Write-Host "=== Deteniendo PREVIEW Family Office Reporting ===" -ForegroundColor Yellow

$ports = @(8100, 8601)
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

Start-Sleep -Seconds 2

$still8100 = Get-NetTCPConnection -LocalPort 8100 -State Listen -ErrorAction SilentlyContinue
$still8601 = Get-NetTCPConnection -LocalPort 8601 -State Listen -ErrorAction SilentlyContinue

if ($still8100 -or $still8601) {
    Write-Host "  ADVERTENCIA: Aun hay procesos en 8100/8601. Forzando PIDs remanentes..." -ForegroundColor Red
    $remaining = @()
    if ($still8100) { $remaining += $still8100 }
    if ($still8601) { $remaining += $still8601 }

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

$final8100 = Get-NetTCPConnection -LocalPort 8100 -State Listen -ErrorAction SilentlyContinue
$final8601 = Get-NetTCPConnection -LocalPort 8601 -State Listen -ErrorAction SilentlyContinue

if ($final8100 -or $final8601) {
    Write-Host "  ERROR: No se pudieron liberar los puertos 8100/8601" -ForegroundColor Red
    exit 1
}

Write-Host "  OK: Puertos 8100 y 8601 libres" -ForegroundColor Green
Write-Host ""
