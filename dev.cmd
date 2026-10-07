@echo off
REM Lanzador del entorno de desarrollo (backend + frontend con recarga en caliente).
REM Detenlo con Ctrl+C en la ventana que se abra.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0dev.ps1"
pause