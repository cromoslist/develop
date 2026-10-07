<#
    Entorno de desarrollo con recarga en caliente.

      .\dev.cmd   (o:  powershell -ExecutionPolicy Bypass -File .\dev.ps1)

    Arranca dos procesos y los mantiene vivos hasta que pulses Ctrl+C:

      - Backend  : dotnet watch run  ->  http://localhost:7000  (reinicia solo al tocar .cs)
      - Frontend : ng serve          ->  http://localhost:4200  (recarga el navegador al tocar Angular)

    Las llamadas /api del frontend se redirigen al backend mediante proxy.conf.json,
    asi que no hay que recompilar ni reiniciar nada al cambiar el codigo Angular.
#>

$ErrorActionPreference = 'Stop'

$root       = $PSScriptRoot
$frontend   = Join-Path $root 'Frontend\CromosList'
$backendCs  = Join-Path $root 'Backend\CromosList\CromosList.csproj'

$procesos = @()

function Detener-Arbol([int]$id) {
    taskkill /PID $id /T /F 2>&1 | Out-Null
}

try {
    Write-Host ''
    Write-Host '===============================================' -ForegroundColor Cyan
    Write-Host ' CromosList - desarrollo' -ForegroundColor Cyan
    Write-Host '===============================================' -ForegroundColor Cyan
    Write-Host ' Frontend : http://localhost:4200  (ng serve, recarga en caliente)'
    Write-Host ' Backend  : http://localhost:7000  (dotnet watch)'
    Write-Host ' Swagger  : http://localhost:7000/swagger'
    Write-Host ''
    Write-Host ' Ctrl+C para detener ambos procesos.'
    Write-Host ''

    $procesos += Start-Process -FilePath 'dotnet' `
        -ArgumentList @('watch', 'run', '--project', $backendCs, '--launch-profile', 'CromosList') `
        -WorkingDirectory (Join-Path $root 'Backend\CromosList') `
        -NoNewWindow -PassThru

    Start-Sleep -Seconds 3

    $procesos += Start-Process -FilePath 'npm.cmd' `
        -ArgumentList @('start', '--', '--port', '4200') `
        -WorkingDirectory $frontend `
        -NoNewWindow -PassThru

    while ($true) {
        Start-Sleep -Seconds 2

        foreach ($p in $procesos) {
            if ($p.HasExited) {
                throw "El proceso '$($p.ProcessName)' (PID $($p.Id)) ha terminado con codigo $($p.ExitCode)."
            }
        }
    }
}
catch {
    Write-Host ''
    Write-Host $_.Exception.Message -ForegroundColor Red
}
finally {
    Write-Host ''
    Write-Host 'Deteniendo procesos...' -ForegroundColor Yellow

    foreach ($p in $procesos) {
        if ($p -and -not $p.HasExited) {
            Detener-Arbol -id $p.Id
        }
    }
}