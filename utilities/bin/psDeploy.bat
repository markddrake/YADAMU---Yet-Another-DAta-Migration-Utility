@echo off
REM Wrapper to launch deploy.ps1 - passes all arguments through
REM Usage: deploy.bat [-Clean] [-DryRun] [-Destination "C:\path"]
powershell -ExecutionPolicy Bypass -File "%~dp0deploy.ps1" %*
