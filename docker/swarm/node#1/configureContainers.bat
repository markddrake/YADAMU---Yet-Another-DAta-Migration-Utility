for /f "tokens=1,2" %%i in ('docker info ^| findstr OSType') do set DOCKER_ENGINE=%%j
if "%DOCKER_ENGINE%" == "linux" ( call :linuxContainer ) else ( call :windowsContainer)
goto :eof
:linuxContainer
  call %~dp0\..\..\rdbms\configuration\linux\configureOracle.bat   ORA1903-01
  call %~dp0\..\..\rdbms\configuration\linux\configureOracle.bat   ORA1803-01
  call %~dp0\..\..\rdbms\configuration\linux\configureOracle.bat   ORA1220-01
  call %~dp0\..\..\rdbms\configuration\linux\configureOracle.bat   ORA1120-01
  call %~dp0\..\..\rdbms\configuration\linux\configureMySQL.bat    MYSQL80-01
  call %~dp0\..\..\rdbms\configuration\linux\configureMariaDB.bat  MARIA10-01
  exit /b
:end

:windowsContainer
  call %~dp0\..\..\rdbms\configuration\windows\configureOracle.bat   ORA1903-01
  call %~dp0\..\..\rdbms\configuration\windows\configureOracle.bat   ORA1803-01
  call %~dp0\..\..\rdbms\configuration\windows\configureOracle.bat   ORA1220-01
  call %~dp0\..\..\rdbms\configuration\windows\configureOracle.bat   ORA1120-01
  call %~dp0\..\..\rdbms\configuration\windows\configureMySQL.bat    MYSQL80-01
  call %~dp0\..\..\rdbms\configuration\windows\configureMariaDB.bat  MARIA10-01
  exit /b
:end







