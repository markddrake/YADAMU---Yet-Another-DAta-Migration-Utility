for /f "tokens=1,2" %%i in ('docker info ^| findstr OSType') do set DOCKER_ENGINE=%%j
if "%DOCKER_ENGINE%" == "linux" ( call :linuxContainer ) else ( call :windowsContainer)
goto :eof
:linuxContainer
  call %~dp0\linux\configureOracle.bat   ORA2103-01
  call %~dp0\linux\configureOracle.bat   ORA1903-01
  call %~dp0\linux\configureOracle.bat   ORA1803-01
  call %~dp0\linux\configureOracle.bat   ORA1220-01
  REM call %~dp0\linux\configureOracle.bat   ORA1210-01
  call %~dp0\linux\configureOracle.bat   ORA1120-01
  call %~dp0\linux\configureMySQL.bat    MYSQL80-01
  call %~dp0\linux\configureMariaDB.bat  MARIA10-01
  call %~dp0\linux\configureMsSQL.bat    MSSQL17-01
  call %~dp0\linux\configureMsSQL.bat    MSSQL19-01
  REM call %~dp0\linux\configurePostgres.bat PGSQL12-01
  REM call %~dp0\linux\configurePostgres.bat PGSQL13-01
  call %~dp0\linux\configurePostgres.bat PGSQL14-01
  exit /b
:end

:windowsContainer
  call %~dp0\windows\configureOracle.bat   ORA2103-01
  call %~dp0\windows\configureOracle.bat   ORA1903-01
  call %~dp0\windows\configureOracle.bat   ORA1803-01
  call %~dp0\windows\configureOracle.bat   ORA1220-01
  REM call %~dp0\windows\configureOracle.bat   ORA1210-01
  call %~dp0\windows\configureOracle.bat   ORA1120-01
  call %~dp0\windows\configureMySQL.bat    MYSQL80-01
  call %~dp0\windows\configureMariaDB.bat  MARIA10-01
  call %~dp0\windows\configureMsSQL.bat    MSSQL17-01
  call %~dp0\windows\configureMsSQL.bat    MSSQL19-01
  REM call %~dp0\windows\configurePostgres.bat PGSQL12-01
  REM call %~dp0\windows\configurePostgres.bat PGSQL13-01
  call %~dp0\windows\configurePostgres.bat PGSQL14-01
  exit /b
:end







