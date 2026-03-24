# ══════════════════════════════════════════════════════════════
# TÚNEL PÚBLICO — Expone el frontend (8501) con un enlace HTTPS
# para que otros abran la app sin tocar firewall (p. ej. desde otra PC).
#
# Uso:
#   1. Tener la app corriendo: .\scripts\start.ps1
#   2. En otra ventana de PowerShell: .\scripts\start_tunnel.ps1
#   3. Copiar la URL https://... que muestre y enviarla a quien quieras.
#
# Requisito: cloudflared instalado (una vez).
#   winget install --id Cloudflare.cloudflared
#   (o descarga: https://github.com/cloudflare/cloudflared/releases)
# ══════════════════════════════════════════════════════════════

$ErrorActionPreference = "Stop"
$scriptDir = $PSScriptRoot
$projectRoot = Split-Path -Parent $scriptDir

# Buscar cloudflared en PATH o en ubicaciones típicas
$cloudflared = $null
if (Get-Command cloudflared -ErrorAction SilentlyContinue) {
    $cloudflared = "cloudflared"
} else {
    $paths = @(
        "$env:LOCALAPPDATA\cloudflared\cloudflared.exe",
        "${env:ProgramFiles}\cloudflared\cloudflared.exe"
    )
    foreach ($p in $paths) {
        if (Test-Path $p) {
            $cloudflared = $p
            break
        }
    }
}

if (-not $cloudflared) {
    Write-Host ""
    Write-Host "  cloudflared no encontrado." -ForegroundColor Red
    Write-Host "  Instalalo una vez con (PowerShell como administrador):" -ForegroundColor Yellow
    Write-Host "    winget install --id Cloudflare.cloudflared" -ForegroundColor White
    Write-Host "  Luego cierra y abre de nuevo PowerShell y vuelve a ejecutar este script." -ForegroundColor Gray
    Write-Host "  Alternativa: https://github.com/cloudflare/cloudflared/releases" -ForegroundColor Gray
    Write-Host ""
    exit 1
}

Write-Host ""
Write-Host "=== Túnel para Family Office Reporting ===" -ForegroundColor Cyan
Write-Host "  Asegurate de que la app este corriendo: .\scripts\start.ps1" -ForegroundColor Gray
Write-Host "  Exponiendo http://localhost:8501 ..." -ForegroundColor Gray
Write-Host ""
Write-Host "  Cuando aparezca una URL https://....trycloudflare.com ," -ForegroundColor Yellow
Write-Host "  copiala y enviala a quien deba entrar. Para salir: Ctrl+C." -ForegroundColor Yellow
Write-Host ""

& $cloudflared tunnel --url http://localhost:8501
