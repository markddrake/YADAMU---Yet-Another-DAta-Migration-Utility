for /f "tokens=1,2" %%i in ('docker info ^| findstr OSType') do set DOCKER_ENGINE=%%j
if "%DOCKER_ENGINE%" == "linux" ( call :linuxContainer ) else ( call :windowsContainer)
goto :eof
:linuxContainer
  REM call %~dp0\linux\configureOracle.bat   ORA2303-01
  REM call %~dp0\linux\configureOracle.bat   ORA2103-01
  call %~dp0\linux\configureOracle.bat   ORA1903-01
  call %~dp0\linux\configureOracle.bat   ORA1803-01
  call %~dp0\linux\configureOracle.bat   ORA1220-01
  REM call %~dp0\linux\configureOracle.bat   ORA1210-01
  call %~dp0\linux\configureOracle.bat   ORA1120-01
  REM call %~dp0\linux\configureMySQL.bat    MYSQL80-01
  call %~dp0\linux\configureMySQL.bat    MYSQL90-01
  REM call %~dp0\linux\configureMariaDB.bat  MARIA10-01
  call %~dp0\linux\configureMariaDB.bat  MARIA11-01
  REM call %~dp0\linux\configureMsSQL.bat    MSSQL17-01
  call %~dp0\linux\configureMsSQL.bat    MSSQL19-01
  call %~dp0\linux\configureMsSQL.bat    MSSQL22-01
  REM call %~dp0\linux\configurePostgres.bat PGSQL12-01
  REM call %~dp0\linux\configurePostgres.bat PGSQL13-01
  REM call %~dp0\linux\configurePostgres.bat PGSQL14-01
  REM call %~dp0\linux\configurePostgres.bat PGSQL15-01
  REM call %~dp0\linux\configurePostgres.bat PGSQL16-01
  call %~dp0\linux\configurePostgres.bat PGSQL17-01
  REM call %~dp0\linux\configureMongoDB.bat  MONGO40-01
  REM call %~dp0\linux\configureMongoDB.bat  MONGO50-01
  REM call %~dp0\linux\configureMongoDB.bat  MONGO60-01
  REM call %~dp0\linux\configureMongoDB.bat  MONGO70-01
  call %~dp0\linux\configureMongoDB.bat  MONGO80-01
  REM call %~dp0\linux\configureVertica.bat  VRTCA09-01
  call %~dp0\linux\configureVertica.bat  VRTCA10-01
  REM call %~dp0\linux\configureVertica.bat  VRTCA11-01
  REM call %~dp0\linux\configureVertica.bat  VRTCA12-01
  REM call %~dp0\linux\configureVertica.bat  VRTCA23-01
  call %~dp0\linux\configureVertica.bat  VRTCA24-01
  exit /b
:end

:windowsContainer
  REM call %~dp0\windows\configureOracle.bat   ORA2303-01
  REM call %~dp0\windows\configureOracle.bat   ORA2103-01
  call %~dp0\windows\configureOracle.bat   ORA1903-01
  call %~dp0\windows\configureOracle.bat   ORA1803-01
  call %~dp0\windows\configureOracle.bat   ORA1220-01
  REM call %~dp0\windows\configureOracle.bat   ORA1210-01
  call %~dp0\windows\configureOracle.bat   ORA1120-01
  REM call %~dp0\windows\configureMySQL.bat    MYSQL80-01 8.0
  call %~dp0\windows\configureMySQL.bat    MYSQL90-01 8.1
  REM call %~dp0\windows\configureMariaDB.bat  MARIA10-01 10
  call %~dp0\windows\configureMariaDB.bat  MARIA11-01 11
  call %~dp0\windows\configureMsSQL.bat    MSSQL17-01
  call %~dp0\windows\configureMsSQL.bat    MSSQL19-01
  REM call %~dp0\windows\configureMsSQL.bat    MSSQL22-01
  REM call %~dp0\windows\configurePostgres.bat PGSQL12-01
  REM call %~dp0\windows\configurePostgres.bat PGSQL13-01
  REM call %~dp0\windows\configurePostgres.bat PGSQL14-01
  REM call %~dp0\windows\configurePostgres.bat PGSQL15-01
  call %~dp0\windows\configurePostgres.bat PGSQL16-01
  REM call %~dp0\windows\configurePostgres.bat PGSQL17-01
  REM call %~dp0\windows\configureMongoDB.bat  MONGO40-01
  REM call %~dp0\windows\configureMongoDB.bat  MONGO40-01
  REM call %~dp0\windows\configureMongoDB.bat  MONGO50-01
  REM call %~dp0\windows\configureMongoDB.bat  MONGO60-01
  REM call %~dp0\windows\configureMongoDB.bat  MONGO70-01
  call %~dp0\windows\configureMongoDB.bat  MONGO80-01
  exit /b
:end







