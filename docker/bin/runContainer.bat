@set CONTAINER_NAME=%1
@set TARGET_IMAGE=%2
@set YADAMU_TEST_NAME=%3
@set CONFIGURATION_FILE=%4
@set HOSTS=""
if defined MSSQL14 set HOST_LIST="MSSQL14-01:%MSSQL14%"
if defined HOST_LIST set HOSTS=--add-host=%HOST_LIST%
REM echo %HOSTS%
docker stop %CONTAINER_NAME%
docker rm %CONTAINER_NAME%
for /f "tokens=1,2" %%i in ('docker info ^| findstr OSType') do set DOCKER_ENGINE=%%j
if "%DOCKER_ENGINE%" == "linux" ( call :linuxContainer ) else ( call :windowsContainer)
docker logs %CONTAINER_NAME%
goto :eof
:linuxContainer
  @set MAX_MEMORY="16g"
  @set SECURITY_OPTION="seccomp:unconfined"
  docker run --name %CONTAINER_NAME% --network YADAMU-NET --memory %MAX_MEMORY%  --security-opt %SECURITY_OPTION% -v YADAMU_01-SHARED:/usr/src/YADAMU/mnt -e YADAMU_TEST_NAME=%YADAMU_TEST_NAME% %HOSTS% -e TESTNAME=%CONFIGURATION_FILE% -d %TARGET_IMAGE%
  exit /b
:end

:windowsContainer
  @set MAX_MEMORY="4g"
  @set DISC_SIZE="100GB"
  docker run --name %CONTAINER_NAME% --network YADAMU-NET --storage-opt "size=%DISC_SIZE%"  --memory %MAX_MEMORY% -v C:\SHARED:C:\YADAMU\mnt -e YADAMU_TEST_NAME=%YADAMU_TEST_NAME% %HOST_LIST% %HOSTS% -e TESTNAME=%CONFIGURATION_FILE% -d %TARGET_IMAGE%
  exit /b
:end
