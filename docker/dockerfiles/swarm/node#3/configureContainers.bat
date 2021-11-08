for /f "tokens=1,2" %%i in ('docker info ^| findstr OSType') do set DOCKER_ENGINE=%%j
if "%DOCKER_ENGINE%" == "linux" ( call :linuxContainer ) else ( call :windowsContainer)
goto :eof
:linuxContainer
  call %~dp0\..\..\..\rdbms\configuration\\linux\configureMsSQL.bat    MSSQL17-01
  call %~dp0\..\..\..\rdbms\configuration\\linux\configureMsSQL.bat    MSSQL19-01
  call %~dp0\..\..\..\rdbms\configuration\\linux\configurePostgres.bat PGSQL14-01
  exit /b
:end

:windowsContainer
  call %~dp0\..\..\..\rdbms\configuration\\windows\configureMsSQL.bat    MSSQL17-01
  call %~dp0\..\..\..\rdbms\configuration\\windows\configureMsSQL.bat    MSSQL19-01
  call %~dp0\..\..\..\rdbms\configuration\\windows\configurePostgres.bat PGSQL14-01
  exit /b
:end







